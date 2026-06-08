# Spec: agregar-tests — Hybrid Test Suite

## Purpose

Define the test requirements for Orakel's Medallion pipeline (Bronze → Silver → Gold).
Tests prevent regressions, document expected behavior, and enable safe refactoring.
Two tiers: pure-pytest units (no Spark) and chispa+SparkSession pipeline tests.

---

## Requirement 1: Dev Dependencies

The project MUST declare these dev dependencies in `pyproject.toml` under `[dependency-groups]`:

| Package | Version | Purpose |
|---------|---------|---------|
| `pytest` | >=8 | Test runner |
| `pytest-cov` | >=6 | Coverage reporting |
| `responses` | >=0.25 | HTTP request mocking |
| `chispa` | >=0.12 | PySpark DataFrame assertions |

### Scenario: uv sync installs test deps

- GIVEN a fresh clone with `pyproject.toml` containing the test group
- WHEN running `uv sync --group test`
- THEN all four packages SHALL be installed without error

---

## Requirement 2: Test Infrastructure

### Test directory structure

The `tests/` directory MUST mirror the `orakel/` package layout:

```
tests/
├── conftest.py              # Shared fixtures (SparkSession, mock clients)
├── pytest.ini               # Markers, asyncio mode, test discovery
├── test_models/
│   ├── test_kpi.py          # Tier 1 — pytest
│   └── test_schemas.py      # Tier 1 — schema constructors
├── test_utils/
│   ├── test_rate_limiter.py # Tier 1 — pytest (mock time)
│   └── test_minio.py        # Tier 2 — SparkSession fixture
├── test_clients/
│   ├── test_raiderio.py     # Tier 1 — responses mock
│   └── test_warcraftlogs.py # Tier 1 — responses/mock OAuth
└── test_pipeline/
    ├── test_bronze.py       # Tier 1 (_run_to_row) + Tier 2 (ingest_raiderio_runs)
    ├── test_silver.py       # Tier 2 — chispa + SparkSession
    └── test_gold.py         # Tier 2 — chispa + SparkSession + UDFs
```

### Markers

- `spark` — Tier 2 tests that require a SparkSession. Tests without this marker MUST NOT start a SparkSession.
- Configuration in `pytest.ini` or `pyproject.toml`:

  ```ini
  [pytest]
  markers =
      spark: marks tests that require a PySpark SparkSession (local mode)
  ```

### conftest.py fixtures

The root `conftest.py` MUST provide:

1. `spark_session` — session-scoped `SparkSession` builder: `local[1]`, 4g driver memory, `spark.sql.session.timeZone=UTC`
2. `mock_raiderio_session` — pre-configured `responses` mock for Raider.IO API
3. `mock_wcl_session` — pre-configured `responses` mock for WarcraftLogs OAuth + GraphQL

### Scenario: Tier 1 tests pass without Spark

- GIVEN a test without the `spark` marker
- WHEN running `uv run pytest tests/ -m "not spark"`
- THEN all tests SHALL pass
- AND no SparkSession SHALL be created

### Scenario: SparkSession fixture creates once

- GIVEN two Tier 2 tests requesting the `spark_session` fixture
- WHEN both tests run
- THEN the same SparkSession instance SHALL be reused (session scope)
- AND SparkSession SHALL be local[1] with UTC time zone

---

## Requirement 3: kpi.py — Pure Unit Tests (Tier 1)

### Function: `compute_death_clock(dtps, hps_on_tank, max_hp)`

| Scenario | Input | Expected |
|----------|-------|----------|
| Safe — healer out-heals damage | dtps=100, hps=200, max_hp=600000 | (-1.0, "safe") |
| Safe — death clock > 15s | dtps=100, hps=50, max_hp=600000 | (~12000.0, "safe") |
| Moderate — 5–15s window | dtps=100000, hps=20000, max_hp=600000 | (7.5, "moderate") |
| Critical — < 5s | dtps=200000, hps=20000, max_hp=600000 | (3.33, "critical") |
| Zero DTPS | dtps=0, hps=0, max_hp=600000 | (-1.0, "safe") |
| Negative DTPS | dtps=-50, hps=0, max_hp=600000 | (-1.0, "safe") |

### Function: `compute_healer_deficit(tank_dtps, healer_hps)`

| Scenario | Input | Expected |
|----------|-------|----------|
| Comfortable — ratio < 1.0 | dtps=100, hps=150 | (0.6667, "comfortable") |
| Moderate — ratio 1.0–1.2 | dtps=100, hps=90 | (1.1111, "moderate") |
| Critical — ratio > 1.2 | dtps=150, hps=100 | (1.5, "critical") |
| Zero DTPS | dtps=0, hps=100 | (None, None) |
| Zero HPS | dtps=100, hps=0 | (None, None) |

### Function: `compute_synergy_score(comp_avg, overall_avg)`

| Scenario | Input | Expected |
|----------|-------|----------|
| Synergy — comp clears faster | 180, 200 | 0.9 |
| Anti-synergy — comp slower | 220, 200 | 1.1 |
| Neutral — same average | 200, 200 | 1.0 |
| None comp_avg | None, 200 | None |
| None overall_avg | 200, None | None |
| Zero overall_avg | 200, 0 | None |

---

## Requirement 4: bronze.py::_run_to_row — Dict Transform (Tier 1)

### Process a full API response

- GIVEN a complete Raider.IO run dict with roster, dungeon, score, rank, and completed_at
- WHEN `_run_to_row(run, "season-tww-3")` is called
- THEN the output dict SHALL match `bronze_raiderio_schema` field types
- AND `source` SHALL be `"raiderio"`
- AND `roster` SHALL be a list with flattened player dicts (name, class, spec, role, realm, region)

### Handle missing optional fields

- GIVEN a run dict with no `score`, no `rank`, and no `completed_at`
- WHEN `_run_to_row(run, "season-tww-3")` is called
- THEN `score` SHALL be `None`
- AND `rank` SHALL be `None`
- AND `completed_at` SHALL be `None`

### Handle empty roster

- GIVEN a run dict with `"roster": []`
- WHEN `_run_to_row(run, "season-tww-3")` is called
- THEN `roster` SHALL be an empty list

### Handle null roster fields

- GIVEN a run with a roster entry where `character` is `None`
- WHEN `_run_to_row` processes that entry
- THEN `name`, `class`, `spec`, `role`, `realm`, `region` SHALL be `None`

### Parse weekly_modifiers from mixed types

- GIVEN `weekly_modifiers` contains both dicts with `{"id": 9}` and plain ints `[10]`
- WHEN `_run_to_row` processes them
- THEN `weekly_modifiers` SHALL be `[9, 10]`

---

## Requirement 5: config.py — Env Var Loading (Tier 1)

### All vars set

- GIVEN env vars `RAIDERIO_API_KEY=test-key`, `SEASON=season-test-1`, `MINIO_ENDPOINT=minio:9000`
- WHEN `Settings()` is constructed
- THEN `settings.RAIDERIO_API_KEY == "test-key"`
- AND `settings.SEASON == "season-test-1"`
- AND `settings.MINIO_ENDPOINT == "minio:9000"`

### Missing vars get defaults

- GIVEN no env vars set
- WHEN `Settings()` is constructed
- THEN `RAIDERIO_API_KEY` SHALL be `""`
- AND `RAIDERIO_BASE_URL` SHALL be `"https://raider.io/api/v1"`
- AND `MINIO_BUCKET` SHALL be `"orakel"`
- AND `SEASON` SHALL be `"season-tww-3"`

### MINIO_SECURE boolean parsing

- GIVEN `MINIO_SECURE=true`
- WHEN `Settings()` is constructed
- THEN `MINIO_SECURE == True`

- GIVEN `MINIO_SECURE=false` (or unset)
- THEN `MINIO_SECURE == False`

---

## Requirement 6: rate_limiter.py — Token Bucket (Tier 1)

### Normal consumption within budget

- GIVEN a `WCLRateLimiter` with default budget and threshold
- WHEN `record_usage(cost=100, remaining=3500)` is called
- THEN `points_remaining == 3500`
- AND `total_spent == 100`
- AND `wait_if_needed(estimated_cost=50) == True`

### Rate limit hit — wait triggers

- GIVEN `points_remaining` is below threshold after estimated cost
- AND `points_reset_at` is in the future
- WHEN `wait_if_needed(estimated_cost=50)` is called
- THEN it SHALL sleep until `points_reset_at`
- AND return `True` after budget resets

### Budget exhausted after retries

- GIVEN `points_remaining` is near zero
- AND `max_retries=3`
- WHEN `wait_if_needed(estimated_cost=100)` is called 4 times
- THEN it SHALL return `False` on the 4th attempt

### Edge: reset_at in milliseconds vs seconds

- GIVEN `record_usage(cost=100, remaining=500, reset_at=1717000000)` (seconds)
- AND another call with `reset_at=1717000000000` (milliseconds)
- THEN both SHALL correctly set `_points_reset_at` to `1717000000.0`

### Edge: no reset_at — estimate from session start

- GIVEN `record_usage(cost=100)` with no `reset_at`
- AND budget is below threshold
- WHEN `wait_if_needed` is called
- THEN it SHALL estimate reset from `_session_start + 3600`

---

## Requirement 7: clients/raiderio.py — REST Client (Tier 1, `responses`)

### Successful request returns runs

- GIVEN `responses` mock returning a 200 with `{"rankings": [{...}]}`
- WHEN `client.fetch_runs("season-tww-3", page=1)` is called
- THEN a list of run dicts SHALL be returned, each containing `score` and `rank`

### Empty page returns empty list

- GIVEN mock returns 200 with `{"rankings": []}`
- THEN `fetch_runs` returns `[]`

### 429 triggers retry then success

- GIVEN mock returns 429 twice, then 200
- WHEN `fetch_runs` is called with `max_retries=3`
- THEN all 3 requests SHALL be made (2 retries)
- AND runs SHALL be returned successfully

### 429 exhausts retries → RateLimitError

- GIVEN mock returns 429 4 times
- AND `max_retries=3`
- WHEN `fetch_runs` is called
- THEN `RateLimitError` SHALL be raised

### Retry-After header respected

- GIVEN mock returns 429 with `Retry-After: 5`
- WHEN retrying
- THEN `time.sleep(5)` SHALL be called (verified via monkeypatch)

### Network error propagates

- GIVEN mock raises `requests.ConnectionError`
- WHEN `fetch_runs` is called
- THEN the exception SHALL propagate

---

## Requirement 8: clients/warcraftlogs.py — GraphQL Client (Tier 1, `responses`)

### OAuth token acquisition succeeds

- GIVEN `responses` mock for token endpoint returning 200 with `{"access_token": "tok", "expires_in": 3600}`
- WHEN `client.authenticate()` is called
- THEN `"tok"` SHALL be returned
- AND the internal `_token` SHALL be cached

### OAuth token acquisition fails

- GIVEN mock returns 401 for token endpoint
- WHEN `client.authenticate()` is called
- THEN `WCLAuthError` SHALL be raised

### Missing credentials raises WCLAuthError

- GIVEN `client_id=""` and `client_secret=""`
- WHEN `client.authenticate()` is called
- THEN `WCLAuthError` SHALL be raised with a message about missing .env config

### GraphQL query succeeds

- GIVEN OAuth mock returns valid token
- AND GraphQL mock returns 200 with `{"data": {...}}`
- WHEN `client.query("{...}", variables={})` is called
- THEN the parsed JSON response SHALL be returned

### 401 triggers token refresh

- GIVEN the first GraphQL request returns 401
- AND the retry after refresh returns 200
- WHEN `client.query(...)` is called
- THEN the token SHALL be refreshed once
- AND the query SHALL succeed on retry

### 429 triggers wait-and-retry

- GIVEN GraphQL mock returns 429 once, then 200
- WHEN `client.query(...)` is called
- THEN `time.sleep` SHALL be called (Retry-After header or 60s default)
- AND the second request SHALL succeed

### Rate limit data tracked

- GIVEN GraphQL response includes `rateLimitData: {cost: 10, pointsRemaining: 3500, pointsReset: 1717000000}`
- WHEN `client.query(...)` is called
- THEN `client.rate_limiter.total_spent >= 10`

---

## Requirement 9: pipeline/bronze.py — Spark Ingest (Tier 2, chispa)

### _run_to_row (Tier 1, same as Req 4 — no Spark needed)

### ingest_raiderio_runs builds correct DataFrame

- GIVEN a `RaiderIOClient` mock that returns sample run dicts
- AND a `spark_session` fixture
- WHEN `ingest_raiderio_runs(client, spark, "season-tww-3", limit=1)` is called
- THEN the resulting DataFrame SHALL match `bronze_raiderio_schema`
- AND each row SHALL have `source="raiderio"`, `season="season-tww-3"`

### Empty API response returns 0

- GIVEN `client.fetch_runs` returns `[]`
- WHEN `ingest_raiderio_runs(client, spark, "season-tww-3")` is called
- THEN `0` SHALL be returned (no writes)

---

## Requirement 10: pipeline/silver.py — Spark Transforms (Tier 2, chispa)

### clean_raiderio: dedup by keystone_run_id

- GIVEN a small Bronze DataFrame with two rows for the same `keystone_run_id` (different `ingested_at`)
- WHEN `SilverPipeline.clean_raiderio(spark, "season-tww-3")` is called (reading from in-memory temp view)
- THEN the output SHALL have exactly 1 row for that run_id
- AND keep the row with the latest `ingested_at`

### clean_raiderio: struct flattening

- GIVEN a Bronze DataFrame where `roster[n].realm` is a struct `{slug: "azjol-nerub", ...}`
- AND `roster[n].region` is a struct `{short_name: "us", ...}`
- WHEN the flatten transformation runs
- THEN `roster[n].realm` SHALL be `"azjol-nerub"`
- AND `roster[n].region` SHALL be `"us"`

### clean_raiderio: idempotent flatten

- GIVEN a DataFrame where `realm` and `region` are already strings
- WHEN flatten runs
- THEN the output SHALL be identical to the input (no-op)

### apply_fuzzy_join: join with matches

- GIVEN a sample Raider.IO runs DataFrame and a match manifest with matching `keystone_run_id`s
- WHEN `SilverPipeline.apply_fuzzy_join(spark, "season-tww-3")` is called
- THEN `dungeon_runs` SHALL contain `wcl_report_code` and `wcl_fight_id` from the manifest
- AND `player_performance` SHALL contain combat stats (damage, healing, interrupts)

### apply_fuzzy_join: empty manifest fallback

- GIVEN no match manifest exists (Parquet path raises `AnalysisException`)
- WHEN `apply_fuzzy_join` runs
- THEN `dungeon_runs` SHALL have `match_method="rio_only"` for all rows
- AND `player_performance` SHALL be an empty DataFrame with correct schema

### apply_fuzzy_join: empty match list fallback

- GIVEN match manifest exists but has 0 rows
- THEN same behavior as missing manifest: `rio_only` fallback

---

## Requirement 11: pipeline/gold.py — KPI Computations (Tier 2, chispa)

### compute_kpi_death_clock: tank with data

- GIVEN a `player_performance` DataFrame with a tank row (role="tank", damage_taken=500000, healing_received=200000, fight_duration_ms=300000, max_hp=600000)
- AND a `dungeon_runs` DataFrame with matching `run_id`
- WHEN `GoldPipeline.compute_kpi_death_clock(spark, "season-tww-3")` is called
- THEN the output SHALL have `death_clock_seconds > 0`
- AND `death_clock_category` SHALL be one of "safe", "moderate", or "critical" based on the computed value

### compute_kpi_healer_deficit: tank + healer joined

- GIVEN `player_performance` with one tank and one healer in the same run
- WHEN `compute_kpi_healer_deficit` is called
- THEN the output SHALL contain the healer's name and class
- AND `deficit_ratio` SHALL be non-null
- AND `deficit_category` SHALL be one of "comfortable", "moderate", "critical"

### compute_kpi_healer_deficit: no tanks

- GIVEN `player_performance` has no tank rows
- WHEN `compute_kpi_healer_deficit` is called
- THEN it SHALL fall back to `_compute_healer_deficit_from_raiderio`
- AND produce placeholder rows with null combat stats

### compute_kpi_interrupt_rate: normalizes by duration

- GIVEN `player_performance` with `interrupts_count=12`, `fight_duration_ms=300000` (5 min)
- WHEN `compute_kpi_interrupt_rate` is called
- THEN `interrupts_per_minute` SHALL be 2.4

### compute_kpi_synergy: computes score

- GIVEN a `raiderio_runs` DataFrame with 3 runs for dungeon A (key=10, same affixes), 2 with same comp and 1 with different comp
- WHEN `compute_kpi_synergy(spark, "season-tww-3")` is called
- THEN the comp with 2 samples SHALL have a non-null `synergy_score`
- AND the comp with 1 sample SHALL have `synergy_score = None` (insufficient samples)

### build_dim_dungeon: dedup unique dungeons

- GIVEN a Silver DataFrame with multiple runs for the same dungeon
- WHEN `GoldPipeline.build_dim_dungeon(spark, "season-tww-3")` is called
- THEN the output SHALL have one row per unique `(dungeon_id, dungeon_name)` pair
- AND `slug` and `keystone_timer_ms` SHALL be populated from hardcoded data

### build_dim_player: explodes roster

- GIVEN a Silver DataFrame with one run having a 5-player roster
- WHEN `GoldPipeline.build_dim_player(spark, "season-tww-3")` is called
- THEN the output SHALL have 5 rows (one per player)
- AND each row SHALL have `class_id` populated from the hardcoded mapping

### build_dim_affix: returns hardcoded data

- GIVEN any season
- WHEN `GoldPipeline.build_dim_affix(spark, "season-tww-3")` is called
- THEN the output SHALL have rows matching `dim_affix_schema`
- AND contain affix entries for TWW Season 3

### build_dim_spec: returns mapping

- GIVEN any state
- WHEN `GoldPipeline.build_dim_spec(spark)` is called
- THEN the output SHALL have 38 rows (one per WoW spec)

---

## Non-Functional Requirements

1. **SparkSession config**: local[1], `spark.driver.memory=4g`, `spark.sql.session.timeZone=UTC`
2. **No network access**: All external HTTP calls MUST be mocked via `responses` or `unittest.mock`
3. **Coverage target**: ≥80% on pure modules (`kpi.py`, `config.py`, `bronze.py::_run_to_row`, `rate_limiter.py`) when running Tier 1
4. **Total test count**: ≥150 tests across both tiers
5. **Cleanup**: SparkSession SHALL be stopped in a finalizer via `pytest` session finish hook
