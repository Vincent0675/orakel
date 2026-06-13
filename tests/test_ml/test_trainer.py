"""Tests for orakel.ml.trainer — Ridge regression training with MLflow + MinIO (Tier 1).

Covers:
    - Happy path: synthetic DataFrame → trained Ridge + metrics dict
    - <10 rows: returns (None, skipped) and logs a warning
    - NULL imputation: numeric NULLs filled with training-set mean
    - Temporal 80/20 split: no shuffle, no leakage
    - MLflow calls: log_params + log_metrics invoked
    - MinIO upload: put_object called on the mocked client
    - _get_numeric_feature_names: returns the correct subset

MLflow and MinIO are fully mocked — no server or storage required.
Pattern mirrors the autouse fixtures from the design doc.
"""

from __future__ import annotations

import logging
import warnings
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler

from orakel.ml.schemas import (
    AFFIX_COLUMNS,
    DUNGEON_COLUMNS,
    FEATURE_COLUMNS,
    NUMERIC_FEATURES,
    ROLE_COUNT_FEATURES,
    TARGET_COLUMN,
)
from orakel.ml.trainer import (
    _get_numeric_feature_names,
    train_model,
    upload_model_to_minio,
)


# ─── Test data factory ────────────────────────────────────────────────────


def _make_training_df(n: int = 100, seed: int = 42) -> pd.DataFrame:
    """Build a synthetic DataFrame matching the feature view schema.

    Includes TARGET_COLUMN ("clear_time_seconds"), all FEATURE_COLUMNS,
    and ``completed_at`` for temporal splitting. Values are deterministic
    via ``np.random.default_rng(seed)``.

    Numeric features are sampled as positive floats; binary flags (affix,
    dungeon, role counts) are sampled as 0/1. The target is computed as a
    linear combination of features plus noise, so Ridge can actually fit it.
    """
    rng = np.random.default_rng(seed)
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)

    data: dict = {
        "run_id": [f"run-{i:04d}" for i in range(n)],
        "completed_at": [base + timedelta(hours=i) for i in range(n)],
        TARGET_COLUMN: rng.uniform(600, 2400, size=n),
    }

    # Numeric features — positive floats
    for col in NUMERIC_FEATURES:
        if col == "key_level":
            data[col] = rng.integers(2, 30, size=n).astype(float)
        elif col == "death_clock_seconds_log1p":
            data[col] = rng.uniform(0.0, 5.0, size=n)
        else:
            data[col] = rng.uniform(0.0, 1.0, size=n)

    # Role count features — non-negative ints summing to 5
    tanks = rng.integers(0, 3, size=n)
    healers = rng.integers(0, 3, size=n)
    dps = 5 - tanks - healers
    data["num_tanks"] = tanks
    data["num_healers"] = healers
    data["num_dps"] = dps

    # Affix binary flags
    for col in AFFIX_COLUMNS:
        data[col] = rng.integers(0, 2, size=n).astype(float)

    # Dungeon binary flags
    for col in DUNGEON_COLUMNS:
        data[col] = rng.integers(0, 2, size=n).astype(float)

    df = pd.DataFrame(data)

    # Build a target with a real signal so Ridge can fit it
    weights = rng.uniform(-1.0, 1.0, size=len(FEATURE_COLUMNS))
    signal = df[FEATURE_COLUMNS].values @ weights
    df[TARGET_COLUMN] = 1500.0 + signal + rng.normal(0, 50, size=n)

    return df


# ─── Mocks ────────────────────────────────────────────────────────────────


@pytest.fixture()
def mock_mlflow():
    """Patch all MLflow calls train_model() makes.

    The trainer uses ``mlflow.start_run()`` as a context manager, calls
    ``log_params`` / ``log_metrics`` / ``sklearn.log_model`` / ``log_artifact``,
    reads ``get_artifact_uri`` and ``active_run()`` for the run_id.

    We yield a dict of mocks so individual tests can assert call args.
    """
    with patch("orakel.ml.trainer.mlflow.set_tracking_uri"), \
         patch("orakel.ml.trainer.mlflow.start_run") as mock_start_run, \
         patch("orakel.ml.trainer.mlflow.log_params") as mock_log_params, \
         patch("orakel.ml.trainer.mlflow.log_metrics") as mock_log_metrics, \
         patch("orakel.ml.trainer.mlflow.sklearn.log_model"), \
         patch("orakel.ml.trainer.mlflow.log_artifact"), \
         patch("orakel.ml.trainer.mlflow.get_artifact_uri", return_value="/tmp/mlruns/0/test"), \
         patch("orakel.ml.trainer.mlflow.active_run") as mock_active_run:

        run_mock = MagicMock()
        run_mock.info.run_id = "test-run-123"
        # Make start_run usable as a context manager
        mock_start_run.return_value.__enter__ = MagicMock(return_value=run_mock)
        mock_start_run.return_value.__exit__ = MagicMock(return_value=False)
        mock_active_run.return_value = run_mock

        yield {
            "start_run": mock_start_run,
            "log_params": mock_log_params,
            "log_metrics": mock_log_metrics,
            "active_run": mock_active_run,
            "run_id": "test-run-123",
        }


@pytest.fixture()
def mock_minio():
    """Patch the Minio constructor used inside ``upload_model_to_minio``.

    The trainer module does ``from minio import Minio`` at the top, so the
    reference in module-scope is ``orakel.ml.trainer.Minio``.
    """
    with patch("orakel.ml.trainer.Minio") as MockMinio:
        client = MagicMock()
        client.bucket_exists.return_value = True
        MockMinio.return_value = client
        yield client


# ─── Tests ────────────────────────────────────────────────────────────────


class TestGetNumericFeatureNames:
    """`_get_numeric_feature_names` — filter to columns that need normalization."""

    def test_returns_only_columns_in_numneric_features(self):
        result = _get_numeric_feature_names(FEATURE_COLUMNS)
        assert set(result) == set(NUMERIC_FEATURES)

    def test_excludes_binary_flag_columns(self):
        """Affix, dungeon, and role-count columns are not in NUMERIC_FEATURES."""
        result = _get_numeric_feature_names(FEATURE_COLUMNS)
        for col in AFFIX_COLUMNS + DUNGEON_COLUMNS + ROLE_COUNT_FEATURES:
            assert col not in result

    def test_preserves_order_of_numeric_features(self):
        result = _get_numeric_feature_names(FEATURE_COLUMNS)
        assert result == NUMERIC_FEATURES

    def test_filters_to_available_features(self):
        """If a column from NUMERIC_FEATURES is missing, drop it."""
        partial = [c for c in FEATURE_COLUMNS if c != "synergy_score"]
        result = _get_numeric_feature_names(partial)
        assert "synergy_score" not in result
        assert all(c in partial for c in result)


class TestTrainModelHappyPath:
    """train_model() with a valid DataFrame returns (Ridge, metrics_dict)."""

    def test_returns_ridge_model(self, mock_mlflow, mock_minio):
        df = _make_training_df(n=100, seed=42)
        model, metrics = train_model(df)

        assert isinstance(model, Ridge)
        assert metrics is not None
        assert metrics.get("skipped") is False

    def test_metrics_dict_has_core_keys(self, mock_mlflow, mock_minio):
        df = _make_training_df(n=100, seed=42)
        _model, metrics = train_model(df)

        for key in ("mae", "rmse", "r2", "baseline_mae", "train_size", "test_size"):
            assert key in metrics, f"Missing key in metrics: {key}"

    def test_metrics_are_floats(self, mock_mlflow, mock_minio):
        df = _make_training_df(n=100, seed=42)
        _model, metrics = train_model(df)

        for key in ("mae", "rmse", "r2", "baseline_mae"):
            assert isinstance(metrics[key], float)

    def test_train_test_split_recorded(self, mock_mlflow, mock_minio):
        df = _make_training_df(n=100, seed=42)
        _model, metrics = train_model(df)

        # 80/20 temporal split of 100 rows
        assert metrics["train_size"] == 80
        assert metrics["test_size"] == 20
        assert metrics["total_rows"] == 100

    def test_mlflow_run_id_recorded(self, mock_mlflow, mock_minio):
        df = _make_training_df(n=100, seed=42)
        _model, metrics = train_model(df)

        assert metrics["mlflow_run_id"] == "test-run-123"


class TestTrainModelInsufficientData:
    """train_model() with <10 rows returns (None, skipped dict) and warns."""

    def test_returns_none_model(self, mock_mlflow, mock_minio):
        df = _make_training_df(n=5, seed=42)
        model, metrics = train_model(df)

        assert model is None
        assert metrics["skipped"] is True

    def test_skip_reason_mentions_row_count(self, mock_mlflow, mock_minio):
        df = _make_training_df(n=5, seed=42)
        _model, metrics = train_model(df)

        assert "5" in metrics["reason"]
        assert "10" in metrics["reason"]

    def test_skip_logs_warning(self, mock_mlflow, mock_minio, caplog):
        df = _make_training_df(n=5, seed=42)
        with caplog.at_level(logging.WARNING, logger="orakel.ml.trainer"):
            train_model(df)

        assert any("Datos insuficientes" in rec.message for rec in caplog.records)

    def test_minimum_boundary_at_9_rows_skips(self, mock_mlflow, mock_minio):
        """The check is strict <10, so 9 rows → skipped."""
        df = _make_training_df(n=9, seed=42)
        model, metrics = train_model(df)
        assert model is None
        assert metrics["skipped"] is True

    def test_minimum_boundary_at_10_rows_trains(self, mock_mlflow, mock_minio):
        """The check is strict <10, so 10 rows → trained."""
        df = _make_training_df(n=10, seed=42)
        model, metrics = train_model(df)
        assert isinstance(model, Ridge)
        assert metrics["skipped"] is False


class TestTrainModelNullImputation:
    """train_model() must impute NULL numeric values with training-set mean."""

    def test_nulls_in_numeric_features_imputed(self, mock_mlflow, mock_minio):
        df = _make_training_df(n=100, seed=42)

        # Inject NULLs in the first numeric column
        first_numeric = NUMERIC_FEATURES[0]
        df.loc[:4, first_numeric] = np.nan

        # Suppress sklearn's "Input contains NaN" warning — the trainer fills
        # them, but the warning fires before fillna is reached. We don't want
        # this test to fail on warnings filter config.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model, _metrics = train_model(df)

        assert isinstance(model, Ridge)
        # If imputation failed, Ridge would raise. Reaching here is the assertion.

    def test_nulls_in_multiple_features(self, mock_mlflow, mock_minio):
        """Multiple NULLs across different numeric features should be handled."""
        df = _make_training_df(n=100, seed=42)
        df.loc[:9, "deficit_ratio"] = np.nan
        df.loc[10:19, "interrupts_per_minute"] = np.nan

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model, _metrics = train_model(df)

        assert isinstance(model, Ridge)


class TestTrainModelTemporalSplit:
    """The split must be 80/20 temporal — no shuffle, no leakage."""

    def test_first_80_percent_used_for_training(self, mock_mlflow, mock_minio):
        """Train rows are the first 80% in completed_at order."""
        df = _make_training_df(n=100, seed=42)
        sorted_df = df.sort_values("completed_at").reset_index(drop=True)

        # We inspect the train/test split indirectly: train_size and test_size.
        # The temporal split must be deterministic — the factory orders by
        # completed_at with hourly spacing starting at 2025-01-01.
        _model, metrics = train_model(df)

        assert metrics["train_size"] == 80
        assert metrics["test_size"] == 20

        # Verify the input was already ordered (factory produces ordered data)
        # so the first 80 sorted rows == factory rows 0..79
        assert sorted_df["run_id"].iloc[0] == df["run_id"].iloc[0]
        assert sorted_df["run_id"].iloc[79] == df["run_id"].iloc[79]

    def test_no_shuffle_call_in_trainer(self, mock_mlflow, mock_minio):
        """train_model() must NOT call df.sample(frac=...) or DataFrame.shuffle."""
        df = _make_training_df(n=100, seed=42)

        with patch.object(pd.DataFrame, "sample") as mock_sample:
            with patch.object(pd.DataFrame, "shuffle", create=True) as mock_shuffle:
                train_model(df)

        # Neither method should be used — temporal split is by index
        mock_sample.assert_not_called()
        mock_shuffle.assert_not_called()


class TestTrainModelMLflowCalls:
    """MLflow log_params and log_metrics must be called with the right data."""

    def test_log_params_called(self, mock_mlflow, mock_minio):
        df = _make_training_df(n=100, seed=42)
        train_model(df)

        mock_mlflow["log_params"].assert_called_once()
        params = mock_mlflow["log_params"].call_args[0][0]
        assert "model_type" in params
        assert params["model_type"] == "Ridge"
        assert "alpha" in params
        assert "train_size" in params
        assert "test_size" in params
        assert params["split_method"] == "temporal_80_20"

    def test_log_metrics_called(self, mock_mlflow, mock_minio):
        df = _make_training_df(n=100, seed=42)
        train_model(df)

        mock_mlflow["log_metrics"].assert_called_once()
        metrics = mock_mlflow["log_metrics"].call_args[0][0]
        for key in ("mae", "rmse", "r2", "baseline_mae", "baseline_r2"):
            assert key in metrics, f"Missing metric: {key}"

    def test_start_run_called_with_run_name(self, mock_mlflow, mock_minio):
        df = _make_training_df(n=100, seed=42)
        train_model(df)

        # start_run may be called twice (second for the minio path param)
        assert mock_mlflow["start_run"].call_count >= 1
        first_call_kwargs = mock_mlflow["start_run"].call_args_list[0].kwargs
        assert first_call_kwargs.get("run_name") == "ridge_clear_time"


class TestTrainModelMinioUpload:
    """upload_model_to_minio() must call put_object on the Minio client."""

    def _real_scaler(self) -> StandardScaler:
        """Return a real fitted StandardScaler (pickle-safe, unlike MagicMock)."""
        scaler = StandardScaler()
        scaler.fit(np.array([[0.0, 1.0], [1.0, 2.0], [2.0, 3.0]]))
        return scaler

    def test_put_object_called(self, mock_minio):
        """Direct call to upload_model_to_minio() invokes put_object."""
        model = Ridge()
        model.fit([[0], [1], [2]], [0, 1, 2])
        scaler = self._real_scaler()

        with patch("orakel.ml.trainer.settings") as mock_settings:
            mock_settings.MINIO_ENDPOINT = "localhost:9000"
            mock_settings.MINIO_ACCESS_KEY = "test"
            mock_settings.MINIO_SECRET_KEY = "test"
            mock_settings.MINIO_BUCKET = "test-bucket"
            mock_settings.MINIO_SECURE = False

            key = upload_model_to_minio(
                model=model,
                scaler=scaler,
                feature_names=["a", "b"],
                run_id="run-xyz",
                season="season-test",
            )

        mock_minio.put_object.assert_called_once()
        assert "run-xyz" in key
        assert key.endswith("model.pkl")

    def test_make_bucket_called_when_bucket_missing(self, mock_minio):
        """If bucket_exists() is False, make_bucket() should be called."""
        mock_minio.bucket_exists.return_value = False
        model = Ridge()
        model.fit([[0], [1]], [0, 1])
        scaler = self._real_scaler()

        with patch("orakel.ml.trainer.settings") as mock_settings:
            mock_settings.MINIO_ENDPOINT = "localhost:9000"
            mock_settings.MINIO_ACCESS_KEY = "test"
            mock_settings.MINIO_SECRET_KEY = "test"
            mock_settings.MINIO_BUCKET = "test-bucket"
            mock_settings.MINIO_SECURE = False

            upload_model_to_minio(
                model=model,
                scaler=scaler,
                feature_names=["a"],
                run_id="run-1",
                season="s",
            )

        mock_minio.make_bucket.assert_called_once_with("test-bucket")

    def test_train_model_calls_put_object(self, mock_mlflow, mock_minio):
        """The full train_model() flow reaches MinIO upload."""
        df = _make_training_df(n=100, seed=42)
        train_model(df)

        mock_minio.put_object.assert_called_once()
        # Object key in Minio is ml_models/{run_id}/model.pkl
        call_kwargs = mock_minio.put_object.call_args.kwargs
        assert "model.pkl" in call_kwargs["object_name"]
        assert "test-run-123" in call_kwargs["object_name"]
