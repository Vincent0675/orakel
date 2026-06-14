# Tasks: Verification Dagster Orchestration

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | ~1100–1400 total |
| 400-line budget risk | High |
| Chained PRs recommended | Yes |
| Suggested split | PR 1 (foundation) → PR 2 (cross-layer checks) |

Decision needed before apply: No
Chained PRs recommended: Yes
Chain strategy: stacked-to-main
400-line budget risk: High

### Suggested Work Units

| Unit | Goal | Likely PR | Notes |
|------|------|-----------|-------|
| 1 | Config + 3 core check functions + 5 per-asset checks + tests | PR 1 | ~500–600 lines; base for PR 2 |
| 2 | RI + completeness + schema drift wrappers + tests | PR 2 | ~500–700 lines; depends on PR 1 |

## Phase 1: Investigation & Config (PR 1 Foundation)

- [x] 1.1 Investigate `dungeon_id` type consistency: read `orakel/models/schemas.py`, compare `dungeon_id` type across `bronze_raiderio_schema`, `silver_dungeon_runs_schema`, `gold_kpi_tank_death_clock_schema`, `gold_kpi_composition_synergy_schema`, `dim_dungeon_schema`. Confirm all use `IntegerType()`. Document findings in code comment or engram.
- [x] 1.2 Add to `orakel/config.py` — add to `Settings` dataclass:
  - `CHECK_COMPLETENESS_THRESHOLD: float = 0.5`
  - `CHECK_RI_SAMPLE_SIZE: int = 0` (0 = no sampling, full scan)
  - `CHECK_RI_ENABLED: bool = True`

## Phase 2: Core Check Functions (PR 1 Foundation)

- [x] 2.1 Add `check_referential_integrity()` to `orakel/pipeline/assets/checks.py` — generic function accepting `spark, upstream_path, downstream_path, join_key, season, sample_size`. Use left-anti join. Return `AssetCheckResult` with `orphan_count`, `total_downstream`, `sample_size` metadata. Handle missing upstream/downstream paths gracefully.
- [x] 2.2 Add `check_completeness_ratio()` to `orakel/pipeline/assets/checks.py` — generic function accepting `spark, upstream_path, downstream_path, season, min_ratio`. Guard against zero division. Return `passed, ratio, upstream_count, downstream_count, min_ratio` metadata.
- [x] 2.3 Add `check_schema_drift()` to `orakel/pipeline/assets/checks.py` — generic function accepting `spark, path, expected_schema, season, mode` ("exact" | "superset"). Compute fingerprint from Parquet metadata (no full scan). Return `drift_detected`, `differences` metadata.

## Phase 3: New Per-Asset Checks (PR 1 Foundation)

- [x] 3.1 Add `@asset_check` for `bronze_wcl` — `bronze_wcl_checks()`: row_count >= 0 (graceful), metadata `error: "Bronze WCL data not found"` on path-not-found.
- [x] 3.2 Add `@asset_check` for `match_manifest` — `match_manifest_checks()`: row_count >= 0 (graceful), metadata `error: "Match manifest not found"` on path-not-found.
- [x] 3.3 Add `@asset_check` for `gold_dim_dungeon` — `gold_dim_dungeon_checks()`: row_count >= 1, metadata `error: "Empty dim_dungeon"` on empty.
- [x] 3.4 Add `@asset_check` for `gold_dim_player` — `gold_dim_player_checks()`: row_count >= 1, metadata `error: "Empty dim_player"` on empty.
- [x] 3.5 Add `@asset_check` for `gold_dim_affix` — `gold_dim_affix_checks()`: row_count >= 1, metadata `error: "Empty dim_affix"` on empty.
- [x] 3.6 Add `@asset_check` for `gold_dim_spec` — `gold_dim_spec_checks()`: row_count >= 1, metadata `error: "Empty dim_spec"` on empty.

## Phase 4: Tests for Core Functions (PR 1 Foundation)

- [x] 4.1 Unit tests for `check_referential_integrity`: mock Spark DataFrames, test orphan detection (pass/fail), sampling behavior, missing upstream path, missing downstream path, empty downstream.
- [x] 4.2 Unit tests for `check_completeness_ratio`: mock row counts, test threshold pass/fail, zero-division guard (upstream=0), upstream_empty warning.
- [x] 4.3 Unit tests for `check_schema_drift`: mock StructType comparisons, test exact mode, superset mode, missing columns, extra columns, type mismatches, path not found.
- [x] 4.4 Integration test for new per-asset checks: verify each check produces expected metadata shape.

## Phase 5: Update Definitions (PR 1 Foundation)

- [x] 5.1 Update `orakel/pipeline/definitions.py` — add imports for new per-asset checks: `bronze_wcl_checks`, `match_manifest_checks`, `gold_dim_dungeon_checks`, `gold_dim_player_checks`, `gold_dim_affix_checks`, `gold_dim_spec_checks`. Add to `asset_checks=` list.

## Phase 6: Referential Integrity Checks (PR 2 Cross-Layer)

- [ ] 6.1 Add `@asset_check` for `silver_dungeon_runs`: `ri_silver_to_dungeon_runs_check()` — call `check_referential_integrity` with `upstream=silver/raiderio_runs`, `downstream=silver/dungeon_runs`, `join_key=keystone_run_id`. Full scan (Bronze small data).
- [ ] 6.2 Add `@asset_check` for `gold_kpi_death_clock`: `ri_death_clock_check()` — call `check_referential_integrity` with `upstream=silver/dungeon_runs`, `downstream=gold/kpi_tank_death_clock`, `join_key=run_id`. Use `CHECK_RI_SAMPLE_SIZE` for sampling (Gold large data).
- [ ] 6.3 Add `@asset_check` for `gold_kpi_healer_deficit`: `ri_healer_deficit_check()` — call `check_referential_integrity` with `upstream=silver/player_performance`, `downstream=gold/kpi_healer_deficit`, `join_key=run_id`.
- [ ] 6.4 Add `@asset_check` for `gold_features`: `ri_features_check()` — call `check_referential_integrity` with `upstream=silver/dungeon_runs`, `downstream=gold/features`, `join_key=run_id`. Note: includes `gold_features` per confirmed design decision (pipeline integrity, not ML validation).
- [ ] 6.5 Add `@asset_check` for `gold_dim_dungeon`: `ri_dim_dungeon_check()` — call `check_referential_integrity` with `upstream=silver/raiderio_runs`, `downstream=gold/dim_dungeon`, `join_key=dungeon_id`. Broadcast join for dim table.

## Phase 7: Completeness Ratio Checks (PR 2 Cross-Layer)

- [ ] 7.1 Add `@asset_check` for `silver_raiderio`: `cr_bronze_to_silver_rio_check()` — call `check_completeness_ratio` with `upstream=bronze/raiderio/runs`, `downstream=silver/raiderio_runs`, `min_ratio=0.5`.
- [ ] 7.2 Add `@asset_check` for `silver_dungeon_runs`: `cr_bronze_to_silver_dungeon_check()` — call `check_completeness_ratio` with `upstream=bronze/raiderio/runs`, `downstream=silver/dungeon_runs`, `min_ratio=0.3`.
- [ ] 7.3 Add `@asset_check` for `gold_kpi_death_clock`: `cr_silver_to_gold_death_clock_check()` — call `check_completeness_ratio` with `upstream=silver/dungeon_runs`, `downstream=gold/kpi_tank_death_clock`, `min_ratio=0.8`.
- [ ] 7.4 Add `@asset_check` for `gold_kpi_healer_deficit`: `cr_silver_to_gold_healer_check()` — call `check_completeness_ratio` with `upstream=silver/player_performance`, `downstream=gold/kpi_healer_deficit`, `min_ratio=0.8`.
- [ ] 7.5 Add `@asset_check` for `gold_features`: `cr_silver_to_gold_features_check()` — call `check_completeness_ratio` with `upstream=silver/dungeon_runs`, `downstream=gold/features`, `min_ratio=0.8`.

## Phase 8: Schema Drift Checks (PR 2 Cross-Layer)

- [ ] 8.1 Add `@asset_check` for `bronze_rio`: `sd_bronze_rio_check()` — call `check_schema_drift` with `path=bronze/raiderio/runs`, `expected_schema=bronze_raiderio_schema`, `mode=superset`.
- [ ] 8.2 Add `@asset_check` for `silver_dungeon_runs`: `sd_silver_dungeon_runs_check()` — call `check_schema_drift` with `path=silver/dungeon_runs`, `expected_schema=silver_dungeon_runs_schema`, `mode=superset`.
- [ ] 8.3 Add `@asset_check` for `silver_player_performance`: `sd_silver_player_perf_check()` — call `check_schema_drift` with `path=silver/player_performance`, `expected_schema=silver_player_performance_schema`, `mode=superset`.
- [ ] 8.4 Add `@asset_check` for `gold_kpi_death_clock`: `sd_gold_death_clock_check()` — call `check_schema_drift` with `path=gold/kpi_tank_death_clock`, `expected_schema=gold_kpi_tank_death_clock_schema`, `mode=superset`.
- [ ] 8.5 Add `@asset_check` for `gold_kpi_healer_deficit`: `sd_gold_healer_deficit_check()` — call `check_schema_drift` with `path=gold/kpi_healer_deficit`, `expected_schema=gold_kpi_healer_deficit_schema`, `mode=superset`.
- [ ] 8.6 Add `@asset_check` for `gold_kpi_interrupt_rate`: `sd_gold_interrupt_rate_check()` — call `check_schema_drift` with `path=gold/kpi_interrupt_rate`, `expected_schema=gold_kpi_interrupt_rate_schema`, `mode=superset`.
- [ ] 8.7 Add `@asset_check` for `gold_kpi_synergy`: `sd_gold_synergy_check()` — call `check_schema_drift` with `path=gold/kpi_composition_synergy`, `expected_schema=gold_kpi_composition_synergy_schema`, `mode=superset`.

## Phase 9: Tests for Cross-Layer Checks (PR 2 Cross-Layer)

- [ ] 9.1 Unit tests for all 5 RI check wrappers: mock SparkSession and verify correct paths/join keys are passed.
- [ ] 9.2 Unit tests for all 5 completeness ratio check wrappers: mock SparkSession row counts, verify threshold enforcement.
- [ ] 9.3 Unit tests for all 7 schema drift check wrappers: mock Parquet schema, verify fingerprint comparison logic.

## Phase 10: Update Definitions (PR 2 Cross-Layer)

- [ ] 10.1 Update `orakel/pipeline/definitions.py` — add imports for all RI, completeness, and schema drift checks. Add all to `asset_checks=` list.

## Dependencies

```
Phase 1 (Investigation) → Phase 2 (Core functions) → Phase 3 (Per-asset checks)
                                                           ↓
Phase 4 (Tests for core) ←────────────────────────────── Phase 3
       ↓
Phase 5 (Definitions update PR1) ← Phase 4 (Tests complete)
       ↓
PR1 MERGED TO MAIN
       ↓
Phase 6 (RI checks PR2) → Phase 7 (Completeness PR2) → Phase 8 (Schema drift PR2)
                                                           ↓
Phase 9 (Tests PR2) ←────────────────────────────────── Phase 8
       ↓
Phase 10 (Definitions update PR2)
```

## Implementation Order

**PR 1 — Foundation** (~500–600 lines):
1. Task 1.1: Investigate `dungeon_id` consistency (can be done in parallel with 1.2)
2. Task 1.2: Config additions
3. Task 2.1–2.3: Core check functions
4. Task 3.1–3.6: New per-asset checks
5. Task 4.1–4.4: Unit tests
6. Task 5.1: Update definitions.py

**PR 2 — Cross-Layer Checks** (~500–700 lines):
7. Task 6.1–6.5: RI check wrappers
8. Task 7.1–7.5: Completeness ratio wrappers
9. Task 8.1–8.7: Schema drift wrappers
10. Task 9.1–9.3: Unit tests for cross-layer checks
11. Task 10.1: Update definitions.py

## Per-PR Review Budget

| PR | Est. Lines | Budget | Status |
|----|-----------|--------|--------|
| PR 1 | ~500–600 | 800 | ✅ Under budget |
| PR 2 | ~500–700 | 800 | ✅ Under budget |

## Verification Criteria

| Task | Pass Criteria |
|------|--------------|
| 1.1 dungeon_id investigation | Code comment or engram entry confirming IntegerType consistency |
| 2.1 `check_referential_integrity` | Left-anti join returns 0 orphans for clean data, >0 for dirty |
| 2.2 `check_completeness_ratio` | Ratio computed correctly, zero-division guarded |
| 2.3 `check_schema_drift` | Fingerprint mismatch detected for added/missing/wrong-type columns |
| 3.1–3.6 per-asset checks | Each produces `AssetCheckResult` with `row_count` metadata |
| 4.1–4.3 unit tests | All mock tests pass: pass case, fail case, error case per function |
| 6.1–6.5 RI checks | Each calls correct upstream/downstream paths and join keys |
| 7.1–7.5 completeness checks | Each enforces correct min_ratio threshold |
| 8.1–8.7 schema drift checks | Each imports correct schema from `orakel.models.schemas` |
| 9.1–9.3 unit tests | All mock tests pass for cross-layer wrappers |
| 5.1, 10.1 definitions | All checks appear in `asset_checks=` list, Dagster loads without import errors |