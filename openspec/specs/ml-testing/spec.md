# ML Testing Specification

## Purpose

Unit tests for the ML module (`orakel/ml/`) covering schemas, training, prediction, and feature engineering. Tests prevent regressions in the prediction pipeline that forecasts Mythic+ `clear_time_seconds`. Two phases: Phase 1 (Tier 1, pure pytest) for schemas, trainer, predict; Phase 2 (Tier 2, Spark) for feature engineering.

## Requirements

### MLT-1: Schema Tests

The test suite MUST validate `orakel/ml/schemas.py` constants and column definitions.

#### Scenario: Column definitions match expectations

- GIVEN `FEATURE_COLUMNS`, `CATEGORICAL_FEATURES`, and `NUMERIC_FEATURES` from `schemas.py`
- WHEN each list is inspected
- THEN `FEATURE_COLUMNS` SHALL be the union of `NUMERIC_FEATURES` and `CATEGORICAL_FEATURES`
- AND all lists SHALL be non-empty

#### Scenario: Feature list sizes are correct

- GIVEN the affix/dungeon feature lists
- WHEN counted
- THEN `AFFIX_FEATURES` SHALL have exactly 16 entries
- AND `DUNGEON_FEATURES` SHALL have exactly 8 entries

#### Scenario: Target column is defined

- GIVEN `TARGET_COLUMN`
- WHEN accessed
- THEN it SHALL be `"clear_time_seconds"`
- AND it SHALL appear in `FEATURE_COLUMNS`

### MLT-2: Trainer Tests

The trainer MUST handle training, edge cases, and external dependencies correctly.

#### Scenario: Train with synthetic DataFrame

- GIVEN a synthetic pandas DataFrame with valid features and target
- WHEN `train_model(df)` is called with `mlflow.start_run` mocked via `unittest.patch`
- THEN a trained Ridge model SHALL be returned
- AND metrics dict SHALL contain `mae`, `rmse`, `r2`, `baseline_mae`

#### Scenario: Fewer than 10 rows skips training

- GIVEN a DataFrame with < 10 rows
- WHEN `train_model(df)` is called
- THEN `None` SHALL be returned
- AND a warning SHALL be logged

#### Scenario: NULL feature values are imputed

- GIVEN a DataFrame where some numeric features contain NULL
- WHEN `train_model(df)` is called
- THEN training SHALL proceed without error
- AND NULL values SHALL be imputed with column mean

#### Scenario: Temporal split respects chronological order

- GIVEN a DataFrame with explicit `completed_at` timestamps spanning 30 days
- WHEN the temporal split runs
- THEN the first 80% by time SHALL be training rows
- AND the last 20% SHALL be test rows
- AND no test row SHALL have an earlier timestamp than any train row

#### Scenario: MLflow logs parameters and metrics

- GIVEN `mlflow.start_run` is mocked via `unittest.patch`
- WHEN `train_model(df)` is called
- THEN `mlflow.log_params` SHALL be called with features, train_size, test_size
- AND `mlflow.log_metrics` SHALL be called with MAE, RMSE, R², baseline_MAE

#### Scenario: MinIO upload is called

- GIVEN `minio.Minio` constructor and `.put_object()` are mocked
- WHEN `train_model(df)` is called
- THEN the serialized model SHALL be uploaded via MinIO

### MLT-3: Predict Tests

Prediction MUST handle valid input, missing data, and model loading.

#### Scenario: Predict with trained Ridge model

- GIVEN a trained Ridge model and a DataFrame with valid features
- WHEN `predict(model, df)` is called
- THEN a DataFrame with `predicted_clear_time_seconds` SHALL be returned
- AND all predictions SHALL be positive floats

#### Scenario: Missing columns are silently handled

- GIVEN a DataFrame missing one or more required feature columns
- WHEN `predict(model, df)` is called
- THEN the function SHALL filter to only available feature columns
- AND missing columns SHALL be silently filled with NaN → 0
- AND prediction SHALL proceed without raising an error
- NOTE: This matches actual code behavior (filters to `available_features`, fills NaN with 0). The spec was previously incorrect stating a ValueError would be raised.

#### Scenario: Empty DataFrame raises ValueError

- GIVEN an empty DataFrame with correct columns
- WHEN `predict(model, df)` is called
- THEN a `ValueError` SHALL be raised (sklearn requires ≥1 sample)
- NOTE: This matches actual code behavior. The spec was previously incorrect stating an empty output would be returned.

### MLT-4: Feature Engineering Tests (Phase 2, Spark)

Feature engineering MUST correctly compute derived features from Gold KPI DataFrames using mocked Spark DataFrameReader.

#### Scenario: Role counts from comp_signature

- GIVEN a Spark DataFrame with `comp_signature` strings (e.g., "1-1-3", "2-2-1")
- WHEN `build_feature_view(spark, season)` is called
- THEN `num_tanks`, `num_healers`, `num_dps` SHALL be correctly parsed

#### Scenario: Affix flags are binary

- GIVEN a run with `affix_ids = [9, 10, 123, 124]`
- WHEN feature engineering runs
- THEN the 4 corresponding `affix_{id}` flags SHALL be 1
- AND all other affix flags SHALL be 0
- NOTE: Flags are integer 0/1 (not float 0.0/1.0), matching the actual `F.when(array_contains(...), 1).otherwise(0)` expression used in the code.

#### Scenario: Log1p applied to death_clock_seconds

- GIVEN a row with `death_clock_seconds = 10.0`
- WHEN the feature vector is built
- THEN `log1p_death_clock_seconds` SHALL be `log(1 + 10.0) ≈ 2.398`

#### Scenario: NULL KPI columns pass through (imputation deferred to trainer)

- GIVEN a run with `deficit_ratio = NULL` and `interrupts_per_minute = NULL`
- WHEN feature engineering runs
- THEN those columns SHALL remain NULL in the feature view output
- AND the run SHALL still produce a valid feature row
- NOTE: The feature view does NOT impute NULLs — that is deferred to `trainer.train_model()` which fills with column mean. The spec was previously incorrect stating NULLs would be imputed to 0.0 in the feature view.

#### Scenario: Join with dungeon_runs and player_performance

- GIVEN a `dungeon_runs` DataFrame with `key_level`
- AND a `player_performance` DataFrame with KPI columns
- WHEN the feature view is built
- THEN the output SHALL contain one row per dungeon run
- AND each row SHALL have KPI aggregates averaged per player role
