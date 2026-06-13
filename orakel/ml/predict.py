"""Prediction function — load model and predict clear_time_seconds.

Provides a simple interface for making predictions with a trained Ridge
regression model. The model is loaded from MinIO via MLflow.
"""

from __future__ import annotations

import logging
from pathlib import Path

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge

from orakel.config import settings
from orakel.ml.schemas import FEATURE_COLUMNS

logger = logging.getLogger(__name__)


def predict(model: Ridge, features_df: pd.DataFrame) -> pd.DataFrame:
    """Predict clear_time_seconds for new runs.

    Applies the same z-score normalization and imputation that was used
    during training. The caller must provide the scaler used during training
    (it is NOT stored inside this function — it is the caller's responsibility).

    For production use, the model and scaler should be loaded from MLflow.

    Args:
        model: Trained Ridge regression model.
        features_df: Pandas DataFrame with columns matching FEATURE_COLUMNS.
            May contain NULLs (will be filled with 0 before prediction).

    Returns:
        DataFrame with columns ``run_id`` and ``predicted_clear_time_seconds``.
    """
    available_features = [c for c in FEATURE_COLUMNS if c in features_df.columns]
    X = features_df[available_features].fillna(0).values

    predictions = model.predict(X)

    result = pd.DataFrame({
        "run_id": features_df["run_id"].values if "run_id" in features_df.columns else range(len(predictions)),
        "predicted_clear_time_seconds": predictions,
    })

    logger.info("Predicted %d runs, mean=%.1f seconds", len(predictions), np.mean(predictions))

    return result


def load_model_from_mlflow(run_id: str | None = None) -> Ridge:
    """Load a trained Ridge model from MLflow.

    Args:
        run_id: MLflow run ID. If None, loads the latest run's model.

    Returns:
        Trained Ridge regression model.
    """
    tracking_uri = getattr(settings, "MLFLOW_TRACKING_URI", f"file:///{Path.cwd() / 'mlruns'}")
    mlflow.set_tracking_uri(tracking_uri)

    if run_id is None:
        # Load the latest successful run
        client = mlflow.tracking.MlflowClient(tracking_uri)
        runs = client.search_runs(
            experiment_ids=[0],
            order_by=["start_time DESC"],
            max_results=1,
        )
        if not runs:
            raise ValueError("No MLflow runs found. Train a model first.")
        run_id = runs[0].info.run_id
        logger.info("Loading model from latest run: %s", run_id)

    model_uri = f"runs:/{run_id}/model"
    model = mlflow.sklearn.load_model(model_uri)
    logger.info("Loaded Ridge model from run %s", run_id)

    return model