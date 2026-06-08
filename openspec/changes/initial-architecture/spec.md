# initial-architecture — Full System Specification

> Full specification for the Orakel MVP pipeline: WoW Mythic+ dungeon predictor.
> No existing specs — this is the initial system definition.

---

## 1. Requirements Overview

### 1.1 Functional Requirements

#### Requirement: Pipeline Execution

The pipeline **MUST** execute all mandatory phases in sequence: 2 data source ingest, Bronze storage, Silver clean+join, Spark aggregation, Gold storage, ML training, and BI dashboard consumption.

#### Requirement: KPI Computation

The system **MUST** compute exactly 4 KPIs: Tank Death Clock, Healer Deficit, Interrupt Success Rate, and Composition Synergy Score.

#### Requirement: ML Model

The system **MUST** produce a scikit-learn regression model predicting `clear_time_ms` and **MAY** produce a binary classifier for `timed`/`not_timed`.

#### Requirement: BI Dashboard

The system **SHALL** expose a BI dashboard connected to the Gold layer with at least 4 KPI charts and 1 filter/segmentation control.

#### Scenario: Full pipeline run

- GIVEN Raider.IO API key and WarcraftLogs OAuth credentials are configured
- WHEN the pipeline is invoked for season `season-tww-3`
- THEN Bronze Parquet is written to MinIO under `bronze/raiderio/runs/` and `bronze/warcraftlogs/reports/`
- AND Silver Parquet is written under `silver/dungeon_runs/` with fuzzy-joined matches
- AND Gold Parquet is written under `gold/kpi_*` folders with computed KPI values
- AND an ML model artifact is saved under `ml_models/dungeon_predictor/`

#### Scenario: Missing credentials

- GIVEN no API credentials are configured
- WHEN the pipeline is invoked
- THEN the system **MUST** fail with a clear error message identifying the missing credential
- AND **MUST NOT** write partial data to MinIO

### 1.2 Non-Functional Requirements

#### Requirement: Rate Limit Compliance

All API calls **MUST** respect documented rate limits: Raider.IO 100 pages unauthenticated / 1000 with key; WarcraftLogs ~3600 points/hour.

#### Requirement: Data Volume

The system **MUST** handle at least 1 season of data (~50 MB Raider.IO + ~500 MB WCL events) without OOM or pipeline failure.

#### Scenario: Rate limit backoff

- GIVEN WarcraftLogs returns HTTP 429 with `Retry-After` header
- WHEN a retry occurs
- THEN the system **MUST** wait the specified duration before retrying
- AND **MUST** eventually succeed or abort after 3 retries with a logged error

---

## 2. Data Source Specifications

### 2.1 Raider.IO API

#### Requirement: Raider.IO Ingestion

The system **MUST** call the Raider.IO REST API endpoint `GET /mythic-plus/runs` with parameters `season`, `page`, and optional `access_key`.

| Endpoint | Method | Auth | Rate Limit |
|----------|--------|------|------------|
| `/mythic-plus/runs` | GET | `access_key` query param (optional) | 100 pages (no key), 1000 (with key) |
| `/mythic-plus/static-data` | GET | None | N/A (single call) |
| `/characters/profile` | GET | `access_key` query param | Shared with run quota |

**Key response fields — runs endpoint:**

| Field | Type | Description |
|-------|------|-------------|
| `keystone_run_id` | int | Unique run identifier |
| `mythic_level` | int | Key difficulty level (2–30+) |
| `clear_time_ms` | int | Completion time in milliseconds |
| `keystone_time_ms` | int | Par time for the dungeon |
| `completed_at` | ISO 8601 | Run completion timestamp |
| `weekly_modifiers[].id` | int | Affix ID |
| `weekly_modifiers[].name` | string | Affix name (e.g. "Tyrannical") |
| `roster[].character.name` | string | Player character name |
| `roster[].character.class` | string | Class name |
| `roster[].character.spec` | string | Specialization name |
| `roster[].role` | string | Role: `tank`/`healer`/`dps` |
| `roster[].character.realm` | string | Realm name |
| `roster[].region` | string | Region: `eu`/`us`/`kr`/`tw` |
| `dungeon.id` | int | Dungeon ID |
| `dungeon.name` | string | Dungeon display name |
| `score` | float | M+ rating for this run |
| `rank` | int | Leaderboard position |

#### Scenario: Paginated Raider.IO fetch

- GIVEN a season parameter `season-tww-3`
- WHEN fetching runs page by page
- THEN each page **MUST** be processed before fetching the next
- AND ingestion **MUST** stop when a page returns fewer results than the page size (end of data)

### 2.2 WarcraftLogs API

#### Requirement: WarcraftLogs Authentication

The system **MUST** authenticate via OAuth 2.0 Client Credentials flow against `https://www.warcraftlogs.com/oauth/token`.

#### Requirement: WarcraftLogs Ingestion

The system **MUST** query the GraphQL endpoint `https://www.warcraftlogs.com/api/v2/client` for combat reports, fights, and events.

| Query | Description | Cost (points) |
|-------|-------------|---------------|
| `reportData.report(code).fights` | List fights in a report | ~10 |
| `reportData.report(code).masterData` | Player/actor metadata | ~10 |
| `reportData.report(code).table(DamageTaken)` | Damage summary per fight | ~50 |
| `reportData.report(code).table(Healing)` | Healing summary per fight | ~50 |
| `reportData.report(code).events(Interrupts)` | Interrupt events | ~150 |
| `character.recentReports` | Recent reports for a character | ~15 |

**Key fields — fights response:**

| Field | Type | Description |
|-------|------|-------------|
| `id` | int | Fight ID (relative to report) |
| `encounterID` | int | Dungeon/encounter ID |
| `keystoneLevel` | int | Mythic+ key level |
| `keystoneAffixes[].id` | int | Affix ID |
| `keystoneTime` | int | Completion time (ms) |
| `kill` | boolean | True if timed/completed |
| `startTime` | int | Relative start (ms from report start) |
| `endTime` | int | Relative end (ms from report start) |

#### Scenario: WCL report fetch with point budget

- GIVEN a report code `ABC123` with 12 fights
- WHEN fetching events for each fight
- THEN the system **MUST** track cumulative point usage via `rateLimitData`
- AND **MUST** pause and resume when approaching the hourly point limit

---

## 3. Bronze Layer Specs (Raw Ingestion)

### 3.1 Raider.IO Runs — Bronze Schema

#### Requirement: Bronze Raider.IO Schema

The system **MUST** store Raider.IO runs with the following schema as Parquet, partitioned by `season/year/month`.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `source` | string | NO | Always `"raiderio"` |
| `keystone_run_id` | long | NO | Natural key |
| `dungeon_id` | int | NO | Raider.IO internal map ID |
| `challenge_mode_id` | int | NO | **ID usado para fuzzy join con WCL** — equivale a `encounterID` en WCL |
| `dungeon_name` | string | YES | |
| `mythic_level` | int | NO | |
| `clear_time_ms` | long | YES | |
| `keystone_time_ms` | long | YES | |
| `completed_at` | timestamp | YES | ISO 8601 parsed |
| `weekly_modifiers` | array\<int\> | YES | Affix ID list |
| `roster` | array\<struct\> | YES | 5 player entries |
| `score` | double | YES | |
| `rank` | int | YES | |
| `season` | string | NO | Partition column |
| `ingested_at` | timestamp | NO | Pipeline ingest time |

#### Scenario: Bronze Raider.IO write

- GIVEN Raider.IO API returns valid JSON for page 1
- WHEN the ingest script converts to Parquet
- THEN each JSON run **MUST** map to one Parquet row
- AND the file **MUST** be written to `bronze/raiderio/runs/season={s}/year={y}/month={m}/`

### 3.2 WarcraftLogs Reports — Bronze Schema

#### Requirement: Bronze WCL Reports Schema

The system **MUST** store WarcraftLogs report summaries with the following schema.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `source` | string | NO | Always `"warcraftlogs"` |
| `report_code` | string | NO | Natural key |
| `report_title` | string | YES | |
| `zone_id` | int | YES | |
| `start_time` | long | YES | Epoch ms |
| `end_time` | long | YES | Epoch ms |
| `owner_name` | string | YES | Report owner |
| `guild_name` | string | YES | |
| `visibility` | string | YES | `public`/`private` |
| `fights` | array\<struct\> | YES | Fight list |
| `ingested_at` | timestamp | NO | |

### 3.3 WarcraftLogs Events — Bronze Schema

#### Requirement: Bronze WCL Events Schema

The system **MUST** store combat events for matched runs (DamageTaken, HealingDone, Interrupts) as separate Parquet files under `bronze/warcraftlogs/events/`.

**DamageTaken events:**
| Column | Type | Nullable |
|--------|------|----------|
| `timestamp` | long | NO |
| `actor_id` | int | NO |
| `source_id` | int | YES |
| `ability_id` | int | YES |
| `ability_name` | string | YES |
| `damage_amount` | long | YES |
| `damage_type` | string | YES |
| `fight_id` | int | NO |
| `report_code` | string | NO |

#### Scenario: Bronze events append-only

- GIVEN a WCL report has 5 M+ fights
- WHEN DamageTaken events are ingested for each fight
- THEN each event row **MUST** include `report_code` and `fight_id`
- AND existing data **MUST NOT** be overwritten (append-only)

---

## 4. Silver Layer Specs (Clean + Integration)

### 4.1 Deduplication

#### Requirement: Silver Deduplication

The system **MUST** deduplicate Bronze records by natural key before writing to Silver:
- Raider.IO runs: `keystone_run_id`
- WCL reports: `report_code`
- WCL events: `(report_code, fight_id, actor_id, timestamp, ability_id)`

#### Scenario: Dedup preserves latest

- GIVEN Bronze contains 2 records with the same `keystone_run_id`
- WHEN the Silver layer processes them
- THEN only the record with the most recent `ingested_at` **MUST** be written to Silver

### 4.2 Type Casting & Null Handling

#### Requirement: Silver Type Enforcement

All Silver columns **MUST** conform to the typed schemas below. Nullable fields **MAY** be null; non-nullable fields with null values **MUST** cause the row to be quarantined to a `_errors/` path.

### 4.3 Fuzzy Join Algorithm

#### Requirement: Fuzzy Join Raider.IO ↔ WCL

The system **MUST** match Raider.IO dungeon runs to WarcraftLogs fights using a 3-layer approach with confidence scoring.

**Layer 1 — Exact match:** `challenge_mode_id + keystone_level + keystone_affixes`
```
challenge_mode_id (Raider.IO) ↔ encounterID (WCL)   ← ⚠️ NO es dungeon_id
mythic_level ↔ keystoneLevel
weekly_modifiers (sorted) ↔ keystoneAffixes (sorted)
```

> **Nota**: Raider.IO expone `dungeon.id` (map ID interno) y `challenge_mode_id` (Challenge Mode ID) como campos separados. WCL usa `encounterID` que equivale al `challenge_mode_id` de Raider.IO, NO al `dungeon.id`. Ver diseño para tabla completa de TWW Season 3.

**Layer 2 — Temporal window:**
```
|completed_at - wcl_report_start_time - fight_end_time| ≤ 30 seconds
```

**Layer 3 — Roster verification:**
```
overlap_count ≥ 3 of 5 players match by (character_name, realm)
```

**Confidence score:**
```
confidence = (layer3_match_count / 5) * 0.5 + (layer2_window_fraction) * 0.3 + 0.2 (for layer 1)
Base: 0.2 if layer 1 matches, 0 otherwise
Window fraction: 1.0 - (|time_diff| / 30.0)
```

#### Scenario: Fuzzy join — full 3-layer match

- GIVEN Raider.IO run for dungeon "Mists" at key 12 with affixes [Tyrannical, Fortified, Bursting]
- AND a WCL fight with matching encounterID, keystoneLevel, keystoneAffixes
- AND the timestamps are within 15 seconds
- AND 4 of 5 roster names match
- WHEN fuzzy join executes
- THEN a match record **MUST** be written to `silver/dungeon_runs/`
- AND the confidence score **MUST** be ≥ 0.8

#### Scenario: Fuzzy join — layer 1 mismatch

- GIVEN a Raider.IO run and a WCL fight with different encounter IDs
- WHEN fuzzy join executes
- THEN **MUST NOT** produce a match record
- AND the run **MUST** remain unmatched in the Silver layer

### 4.4 Silver Output Schemas

#### Requirement: Silver `dungeon_runs` Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `run_id` | string | NO | UUID generated at join time |
| `rio_run_id` | long | YES | Raider.IO run ID (null if WCL-only) |
| `wcl_report_code` | string | YES | WCL report code (null if Raider.IO-only) |
| `wcl_fight_id` | int | YES | |
| `dungeon_id` | int | NO | |
| `key_level` | int | NO | |
| `affix_ids` | array\<int\> | YES | |
| `clear_time_ms` | long | YES | |
| `completed_at` | timestamp | YES | |
| `confidence` | double | YES | 0–1 fuzzy match score |
| `match_method` | string | YES | `"full_3_layer"`, `"rio_only"`, `"wcl_only"` |
| `roster` | array\<struct\> | YES | 5 players with resolved names |
| `season` | string | NO | |
| `matched_at` | timestamp | YES | |

#### Requirement: Silver `player_performance` Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `run_id` | string | NO | FK to dungeon_runs |
| `player_name` | string | NO | |
| `realm` | string | YES | |
| `region` | string | YES | |
| `class_name` | string | YES | |
| `spec_name` | string | YES | |
| `role` | string | YES | `tank`/`healer`/`dps` |
| `total_damage_taken` | long | YES | |
| `total_healing_received` | long | YES | |
| `interrupts_cast` | int | YES | |
| `interrupts_successful` | int | YES | |
| `max_hp` | long | YES | |
| `fight_duration_ms` | long | YES | |
| `season` | string | NO | |

---

## 5. Gold Layer Specs (KPIs + Dimensions)

### 5.1 KPI 1 — Tank Death Clock

#### Requirement: Tank Death Clock Computation

The system **MUST** compute Death Clock per run as:
```
DeathClock(s) = EHP / (DTPS - HPS_on_tank)
```

Where:
- `DTPS` = `total_damage_taken / fight_duration_seconds`
- `HPS_on_tank` = `total_healing_received / fight_duration_seconds`
- `EHP` = `max_hp` (MVP approximation; full mitigation EHP is future)

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | string | |
| `dungeon_id` | int | |
| `key_level` | int | |
| `tank_name` | string | |
| `tank_class` | string | |
| `tank_spec` | string | |
| `dtps` | double | |
| `hps_on_tank` | double | |
| `ehp_estimate` | long | |
| `death_clock_seconds` | double | `0` if DTPS ≤ HPS (infinite survival) |
| `death_clock_category` | string | `critical` / `moderate` / `safe` |
| `fight_duration_ms` | long | |
| `affix_ids` | array\<int\> | |

#### Scenario: Death Clock computation

- GIVEN a tank took 1,200,000 damage over 120 seconds (DTPS = 10,000)
- AND received 600,000 healing over 120 seconds (HPS_on_tank = 5,000)
- AND has max_hp = 600,000
- WHEN Death Clock is computed
- THEN `death_clock_seconds` = `600000 / (10000 - 5000)` = 120 seconds (category: `safe`)

#### Scenario: Death Clock — healer out-heals damage

- GIVEN DTPS = 8,000 and HPS_on_tank = 12,000 (healer out-heals damage)
- WHEN Death Clock is computed
- THEN `death_clock_seconds` **MUST** be set to a sentinel value (e.g., `-1` or `Infinity`)
- AND category **MUST** be `safe`

### 5.2 KPI 2 — Healer Deficit

#### Requirement: Healer Deficit Computation

The system **MUST** compute Healer Deficit per run as:
```
Deficit = Tank_DTPS / Healer_HPS_on_tank
```

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | string | |
| `healer_name` | string | |
| `healer_class` | string | |
| `healer_spec` | string | |
| `tank_dtps` | double | |
| `healer_hps_on_tank` | double | |
| `deficit_ratio` | double | |
| `deficit_category` | string | `< 1.0 = comfortable`, `1.0–1.2 = moderate`, `> 1.2 = critical` |
| `affix_ids` | array\<int\> | |

#### Scenario: Healer Deficit — critical threshold

- GIVEN Tank_DTPS = 15,000 and Healer_HPS_on_tank = 10,000
- WHEN deficit is computed
- THEN `deficit_ratio` = 1.5 (category: `critical`)

### 5.3 KPI 3 — Interrupt Success Rate

#### Requirement: Interrupt Success Rate Computation

The system **MUST** compute Interrupt Success Rate per player per run as:
```
ISR = Successful_Interrupts / Total_Interrupt_Casts
```

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | string | |
| `player_name` | string | |
| `player_class` | string | |
| `player_spec` | string | |
| `player_role` | string | |
| `total_interrupt_casts` | int | |
| `successful_interrupts` | int | |
| `interrupt_success_rate` | double | 0–1 |
| `dangerous_enemy_casts` | int | Total dangerous casts in fight |
| `interrupt_coverage` | double | successful / dangerous_casts |

#### Scenario: ISR edge case — zero casts

- GIVEN a player cast 0 interrupts in a fight
- WHEN ISR is computed
- THEN `interrupt_success_rate` **MUST** be set to `NULL` (not 0 — the player didn't attempt, not failed)
- AND the row **MUST** still be written

### 5.4 KPI 4 — Composition Synergy Score

#### Requirement: Composition Synergy Score Computation

The system **MUST** compute synergy per dungeon+key+affix+comp combination as:
```
Synergy(comp) = avg_clear_time(comp) / avg_clear_time(all_comps)
```
Grouped by `(dungeon_id, key_level, affix_ids)`.

| Column | Type | Description |
|--------|------|-------------|
| `dungeon_id` | int | |
| `dungeon_name` | string | |
| `key_level` | int | |
| `affix_ids` | array\<int\> | |
| `comp_signature` | string | e.g. `paladin-holy_healer:warrior-protection_tank:...` |
| `avg_clear_time_ms` | double | Mean for this comp |
| `overall_avg_clear_time_ms` | double | Mean for all comps |
| `synergy_score` | double | ratio; < 1.0 = above average |
| `sample_count` | int | Number of runs for this comp |

#### Scenario: Synergy — minimum sample size

- GIVEN a composition appears only 1 time for a given dungeon+key+affix
- WHEN synergy is computed
- THEN `synergy_score` **MUST** be set to `NULL` (insufficient sample)
- AND `sample_count` **MUST** be 1

### 5.5 Dimension Tables

#### Requirement: Dimension Tables

The system **MUST** maintain 4 dimension tables in `gold/` with the following schemas.

**`dim_dungeon`:**
| Column | Type | Description |
|--------|------|-------------|
| `dungeon_id` | int | PK |
| `dungeon_name` | string | |
| `slug` | string | URL-friendly name |
| `keystone_timer_ms` | long | Par time |
| `season` | string | |

**`dim_player`:**
| Column | Type | Description |
|--------|------|-------------|
| `player_name` | string | |
| `realm` | string | |
| `region` | string | |
| `class_id` | int | |
| `class_name` | string | |
| `spec_name` | string | |
| `role` | string | |

**`dim_affix`:**
| Column | Type | Description |
|--------|------|-------------|
| `affix_id` | int | PK |
| `affix_name` | string | |
| `affix_description` | string | |
| `affix_icon` | string | URL |
| `season` | string | |

**`dim_spec`:**
| Column | Type | Description |
|--------|------|-------------|
| `class_id` | int | |
| `class_name` | string | |
| `spec_name` | string | |
| `role` | string | |
| `is_healer` | boolean | |
| `is_tank` | boolean | |
| `is_dps` | boolean | |

---

## 6. ML Feature Spec (Preliminary)

#### Requirement: ML Feature Assembly

The system **MUST** assemble a feature table for ML training from Gold KPI and dimension data.

**Target variable:**
- Primary: `clear_time_ms` (regression — continuous)
- Alternative: `timed` (classification — boolean, clear_time ≤ keystone_time)

**Features:**

| Feature | Source | Type |
|---------|--------|------|
| `avg_mplus_score` | Raider.IO — avg score per roster | double |
| `composition_synergy_score` | Gold KPI 4 | double |
| `avg_tank_death_clock` | Gold KPI 1 — avg per roster | double |
| `avg_healer_deficit` | Gold KPI 2 — avg per roster | double |
| `avg_interrupt_rate` | Gold KPI 3 — avg per roster | double |
| `key_level` | Bronze | int |
| `affix_ids` | Bronze | one-hot encoded |
| `dungeon_id` | Bronze | categorical |
| `roster_diversity` | computed — count unique classes | int |

#### Requirement: Train/Test Split

The system **MUST** split data 80/20 by `completed_at` timestamp (chronological split, not random).

#### Scenario: ML training runs

- GIVEN Gold layer has at least 100 runs with all 4 KPIs computed
- WHEN the ML pipeline executes
- THEN a regression model **MUST** be trained and saved to `ml_models/dungeon_predictor/`
- AND evaluation metrics (RMSE, MAE, R²) **MUST** be logged
- AND if classification is enabled, accuracy and F1 **MUST** also be logged

---

## 7. BI Dashboard Spec (Preliminary)

#### Requirement: Dashboard Chart Coverage

The dashboard **MUST** display at least 4 charts covering all 4 KPIs:

| Chart | KPI | Type |
|-------|-----|------|
| Tank Death Clock distribution | 1 | Histogram, faceted by key level |
| Healer Deficit scatter | 2 | Scatter: deficit_ratio vs key_level, colored by dungeon |
| Interrupt Success by role | 3 | Bar chart per dungeon, grouped by role |
| Comp Synergy heatmap | 4 | Heatmap: comps × dungeons, colored by synergy_score |

#### Requirement: Dashboard Filters

The dashboard **SHALL** support at least the following interactive filters:
- Key level range (slider)
- Dungeon (multi-select)
- Affix combination (multi-select)

---

## 8. Validation Scenarios

### Scenario V1: Bronze ingestion smoke test

- GIVEN Raider.IO API is reachable
- WHEN the ingest script runs for a single season
- THEN Bronze Parquet files **MUST** exist in MinIO under the expected paths
- AND the row count **SHOULD** match the API page count

### Scenario V2: Fuzzy join manual audit

- GIVEN 10 Raider.IO runs and their potential WCL matches
- WHEN fuzzy join executes
- THEN a human **MUST** verify that layers 1, 2, and 3 logic produced correct matches
- AND confidence scores **MUST** be within expected ranges per layer combination

### Scenario V3: Death Clock manual verification

- GIVEN a known fight where a tank died with a healer present
- WHEN KPI 1 computes Death Clock for that run
- THEN `death_clock_seconds` **MUST** be < 5 (category: `critical`)
- AND for a separate tank that never dipped below 80% HP, `death_clock_seconds` **MUST** be > 15 (category: `safe`)

### Scenario V4: ML output range check

- GIVEN a trained regression model on historical data
- WHEN the model predicts `clear_time_ms` for new run features
- THEN predictions **MUST** fall within `[dungeon_min_time * 0.5, dungeon_max_time * 2]`
- AND the prediction **MUST NOT** be negative
