"""Model training — Ridge regression with MLflow tracking.

Implements the training pipeline:
    1. Read feature view from Parquet
    2. Temporal train/test split (80/20 by completed_at, no shuffle)
    3. Z-score normalize numeric features (fit on train only)
    4. Impute NULLs with training-set means
    5. Train Ridge regression (L2 regularization)
    6. Train DummyRegressor baseline (mean strategy)
    7. Evaluate on test: MAE, RMSE, R²
    8. Log everything to MLflow (local file tracking)
    9. Extract feature importance from coefficients
    10. Upload model artifact to MinIO for persistence
"""

from __future__ import annotations

import io
import logging
import pickle
from pathlib import Path

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from minio import Minio
from sklearn.dummy import DummyRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, r2_score, root_mean_squared_error
from sklearn.preprocessing import StandardScaler

from orakel.config import settings
from orakel.ml.schemas import FEATURE_COLUMNS, TARGET_COLUMN

logger = logging.getLogger(__name__)


def upload_model_to_minio(
    model: Ridge,
    scaler: StandardScaler,
    feature_names: list[str],
    run_id: str,
    season: str,
) -> str:
    """Upload trained model + scaler + feature_names to MinIO.

    Serializes the model, scaler, and feature list as a single pickle
    artifact and uploads it to ``s3a://{MINIO_BUCKET}/ml_models/{run_id}/``.

    Args:
        model: Trained Ridge model.
        scaler: Fitted StandardScaler.
        feature_names: List of feature column names used during training.
        run_id: MLflow run ID used as the storage key.
        season: Season identifier for logging.

    Returns:
        MinIO object key (e.g. ``ml_models/{run_id}/model.pkl``).
    """
    artifact = {
        "model": model,
        "scaler": scaler,
        "feature_names": feature_names,
        "season": season,
        "run_id": run_id,
    }

    buf = io.BytesIO(pickle.dumps(artifact))
    object_key = f"ml_models/{run_id}/model.pkl"

    client = Minio(
        endpoint=settings.MINIO_ENDPOINT,
        access_key=settings.MINIO_ACCESS_KEY,
        secret_key=settings.MINIO_SECRET_KEY,
        secure=settings.MINIO_SECURE,
    )

    # Ensure bucket exists
    if not client.bucket_exists(settings.MINIO_BUCKET):
        client.make_bucket(settings.MINIO_BUCKET)

    client.put_object(
        bucket_name=settings.MINIO_BUCKET,
        object_name=object_key,
        data=buf,
        length=buf.getbuffer().nbytes,
        content_type="application/octet-stream",
    )

    logger.info("Model uploaded to MinIO: %s/%s", settings.MINIO_BUCKET, object_key)
    return object_key


def train_model(df: pd.DataFrame, spark=None) -> tuple:
    """Train a Ridge regression model on the feature view data.

    Args:
        df: Pandas DataFrame from the feature view Parquet. Must contain
            all columns in FEATURE_COLUMNS plus TARGET_COLUMN and
            ``completed_at`` for temporal ordering.

    Returns:
        Tuple of (model, metrics_dict). If < 10 rows, model is None
        and metrics_dict contains a ``skipped`` key with the reason.
    """
    # ── Minimum data check ────────────────────────────────────────────────────
    total_rows = len(df)
    if total_rows < 10:
        logger.warning("Datos insuficientes: %d < 10 runs. Skipping model training.", total_rows)
        return None, {
            "skipped": True,
            "reason": f"Datos insuficientes: {total_rows} < 10 runs",
            "row_count": total_rows,
        }

    # ── Temporal split (no shuffle — avoids data leakage) ──────────────────────
    df = df.sort_values("completed_at").reset_index(drop=True)
    split_idx = int(len(df) * 0.8)
    train_df = df.iloc[:split_idx].copy()
    test_df = df.iloc[split_idx:].copy()

    logger.info(
        "Train/test split: %d train rows, %d test rows (temporal 80/20)",
        len(train_df),
        len(test_df),
    )

    # ── Prepare feature matrices ──────────────────────────────────────────────
    # Select only columns that exist in the DataFrame
    available_features = [c for c in FEATURE_COLUMNS if c in df.columns]

    X_train = train_df[available_features].copy()
    X_test = test_df[available_features].copy()
    y_train = train_df[TARGET_COLUMN].values
    y_test = test_df[TARGET_COLUMN].values

    # ── Z-score normalization (fit on train only) ──────────────────────────────
    numeric_cols_to_scale = [c for c in available_features if c in _get_numeric_feature_names(available_features)]
    scaler = StandardScaler()

    if numeric_cols_to_scale:
        X_train[numeric_cols_to_scale] = scaler.fit_transform(X_train[numeric_cols_to_scale])
        X_test[numeric_cols_to_scale] = scaler.transform(X_test[numeric_cols_to_scale])

    # ── Impute NULLs with training-set means ──────────────────────────────────
    train_means = X_train.mean()
    X_train = X_train.fillna(train_means)
    X_test = X_test.fillna(train_means)

    # ── Train Ridge regression ─────────────────────────────────────────────────
    model = Ridge(alpha=1.0, fit_intercept=True, random_state=42)
    model.fit(X_train.values, y_train)

    # ── Train baseline (mean predictor) ────────────────────────────────────────
    baseline = DummyRegressor(strategy="mean")
    baseline.fit(X_train.values, y_train)

    # ── Evaluate ───────────────────────────────────────────────────────────────
    y_pred = model.predict(X_test.values)
    y_baseline = baseline.predict(X_test.values)

    mae = mean_absolute_error(y_test, y_pred)
    rmse = root_mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    mae_baseline = mean_absolute_error(y_test, y_baseline)
    r2_baseline = r2_score(y_test, y_baseline)

    # ── Log warnings if model underperforms ───────────────────────────────────
    if r2 < 0:
        logger.warning(
            "R² is negative (%.4f) — model worse than baseline. "
            "Consider more data or better feature engineering.",
            r2,
        )
    if mae > mae_baseline:
        logger.warning(
            "MAE (%.2f) > baseline MAE (%.2f) — model does not outperform baseline.",
            mae,
            mae_baseline,
        )

    # ── Feature importance (coefficients) ──────────────────────────────────────
    coef_df = pd.DataFrame({
        "feature": available_features,
        "coefficient": model.coef_,
    }).sort_values("coefficient", key=abs, ascending=False)

    top5_positive = coef_df.nlargest(5, "coefficient")[["feature", "coefficient"]].to_dict("records")
    top5_negative = coef_df.nsmallest(5, "coefficient")[["feature", "coefficient"]].to_dict("records")

    logger.info("Top 5 positive features: %s", top5_positive)
    logger.info("Top 5 negative features: %s", top5_negative)

    # ── MLflow tracking ────────────────────────────────────────────────────────
    tracking_uri = getattr(settings, "MLFLOW_TRACKING_URI", f"file:///{Path.cwd() / 'mlruns'}")
    mlflow.set_tracking_uri(tracking_uri)

    with mlflow.start_run(run_name="ridge_clear_time"):
        # Log parameters
        mlflow.log_params({
            "model_type": "Ridge",
            "alpha": 1.0,
            "fit_intercept": True,
            "features": ",".join(available_features),
            "train_size": len(train_df),
            "test_size": len(test_df),
            "split_method": "temporal_80_20",
            "total_rows": total_rows,
        })

        # Log metrics
        mlflow.log_metrics({
            "mae": mae,
            "rmse": rmse,
            "r2": r2,
            "baseline_mae": mae_baseline,
            "baseline_r2": r2_baseline,
        })

        # Log model
        mlflow.sklearn.log_model(model, "model")

        # Log feature importance as artifact
        importance_path = Path(mlflow.get_artifact_uri()) / "feature_importance.csv"
        importance_path.parent.mkdir(parents=True, exist_ok=True)
        coef_df.to_csv(str(importance_path), index=False)
        mlflow.log_artifact(str(importance_path))

        run_id = mlflow.active_run().info.run_id
        logger.info("MLflow run %s — MAE=%.2f, RMSE=%.2f, R²=%.4f", run_id, mae, rmse, r2)

    # ── Upload model artifact to MinIO ─────────────────────────────────────────
    minio_key = ""
    try:
        minio_key = upload_model_to_minio(
            model=model,
            scaler=scaler,
            feature_names=available_features,
            run_id=run_id,
            season=settings.SEASON,
        )
    except Exception as exc:
        logger.warning("Failed to upload model to MinIO: %s", exc)

    # Log MinIO path in MLflow params (re-open run to add param)
    if minio_key:
        with mlflow.start_run(run_id=run_id):
            mlflow.log_param("minio_model_path", f"{settings.MINIO_BUCKET}/{minio_key}")

    metrics_dict = {
        "mae": mae,
        "rmse": rmse,
        "r2": r2,
        "baseline_mae": mae_baseline,
        "baseline_r2": r2_baseline,
        "train_size": len(train_df),
        "test_size": len(test_df),
        "total_rows": total_rows,
        "top5_positive": top5_positive,
        "top5_negative": top5_negative,
        "mlflow_run_id": run_id,
        "minio_model_path": f"{settings.MINIO_BUCKET}/{minio_key}" if minio_key else "",
        "skipped": False,
    }

    return model, metrics_dict


def _get_numeric_feature_names(available_features: list[str]) -> list[str]:
    """Return feature names that should be z-score normalized.

    These are the continuous numeric features, excluding binary flags
    (affix columns, dungeon columns, role count columns are 0/1 or small ints
    that don't need normalization in the same way).
    """
    from orakel.ml.schemas import NUMERIC_FEATURES
    return [f for f in NUMERIC_FEATURES if f in available_features]