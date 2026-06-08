# Exploration: WoW Mythic+ Dungeon Predictor System

## Project Context
- **Project**: orakel — Python 3.13 scaffold
- **Goal**: MVP of a WoW Mythic+ dungeon predictor (university assignment: Big Data + BI + Processing + ML + Orchestration)
- **Pipeline phases**: 2 data sources → Ingest → Clean → Integrate → Spark Processing → Final Storage → BI → ML/Analytics
- **Tech stack target**: MinIO S3 (Bronze/Silver/Gold), Apache Spark, Python, Airflow (optional)

---

## 1. Raider.IO API

### Authentication
- **No API key required** for basic access (100 pages max)
- **Free API key** available at https://raider.io/settings/apikey (requires login)
- With API key: up to **1000 pages** per request, higher rate limits
- No OAuth — simple `access_key` query parameter

### Base URL
```
https://raider.io/api/v1/
```

### Key Endpoints

| Endpoint | Description | Example URL |
|----------|-------------|-------------|
| `GET /mythic-plus/runs` | Timed M+ runs with rankings | `https://raider.io/api/v1/mythic-plus/runs?season=season-tww-3&page=1` |
| `GET /mythic-plus/static-data` | Season/dungeon metadata | `https://raider.io/api/v1/mythic-plus/static-data?expansion_id=11` |
| `GET /characters/profile` | Character profile & score | `https://raider.io/api/v1/characters/profile?region=eu&realm=area52&name=charName&fields=mythic_plus_scores` |

### Rate Limits (from production code analysis)
- **Unauthenticated**: ~100 pages per scan, reasonable per-second window
- **Authenticated**: Up to 1000 pages, higher limits
- **429 response**: Implement exponential backoff (Retry-After header)
- Source: [`mythic-plus-meta-scanner`](https://github.com/triat/mythic-plus-meta-scanner) uses 8 parallel workers with backoff

### Response Format
- **JSON** format
- Paginated results (page parameter)
- Each run includes:
  - `keystone_run_id`, `mythic_level` (key level), `clear_time_ms`, `keystone_time_ms` (par time)
  - `completed_at` (ISO 8601), `num_chests`, `time_remaining_ms`
  - `weekly_modifiers[]` — affixes (Tyrannical, Fortified, Xal'atath's Guile, etc.)
  - `roster[]` — 5 players: each with `character.name`, `character.class`, `character.spec`, `role`, `realm`, `region`
  - `dungeon` — id, name, slug, short_name, keystone_timer_ms
  - `score` — total M+ score for that run
  - `rank` — position on leaderboard

### Data Quality Notes
- Runs can be filtered by `season`, `region`, `page`
- Only timed runs typically fetchable
- Roster includes role assignment (tank/healer/dps)
- Character data includes class/spec IDs and names
- Season slugs pattern: `season-mn-1`, `season-tww-3` (3 segments = main season)

---

## 2. WarcraftLogs API

### Authentication
- **OAuth 2.0** required (no anonymous access)
- **Client Credentials Flow** for public data (v2/client endpoint):
  - POST to `https://www.warcraftlogs.com/oauth/token` with `grant_type=client_credentials`
  - Requires `client_id` + `client_secret` from https://www.warcraftlogs.com/api/clients/
  - Returns Bearer token
- **Authorization Code Flow** for private/user data (v2/user endpoint):
  - Full OAuth redirect flow
  - Requires user authorization
- **PKCE Flow** for browser-based apps

### Base URLs
```
Public API:  https://www.warcraftlogs.com/api/v2/client
User API:    https://www.warcraftlogs.com/api/v2/user
Token URL:   https://www.warcraftlogs.com/oauth/token
```

### API Paradigm
- **GraphQL** (not REST)
- Query schema available at https://www.warcraftlogs.com/v2-api-docs/warcraft/
- Rate limit info available via `rateLimitData` query

### Key GraphQL Queries

| Query | Description |
|-------|-------------|
| `characterData.character(name:, server:, region:)` | Get character by name/server/region |
| `character.recentReports(limit:, page:)` | Recent combat logs for a character |
| `reportData.report(code:)` | Access a specific report by code |
| `report.fights(killType:)` | List fights in a report (kills/wipes) |
| `report.events(dataType:, ...)` | Combat events with filtering (damage, heals, casts, deaths, interrupts) |
| `report.table(dataType:, ...)` | Summary tables (damage done, healing, etc.) |
| `report.playerDetails()` | Player specs, talents, gear |
| `character.zoneRankings(zoneID:, metric:)` | Rankings per zone/encounter |
| `character.encounterRankings(encounterID:)` | Rankings for specific boss |

### Available Event DataTypes
- `DamageDone`, `DamageTaken`, `HealingDone`, `HealingTaken`
- `Casts`, `BuffApplications`, `BuffRemovals`
- `Deaths`, `Summons`, `Dispels`, `Interrupts`
- `AuraApplied`, `AuraRemoved`, `AuraRefresh`

### Rate Limits
- **Point-based system** — each query costs points
- `rateLimitData` query shows remaining points
- Client credentials flow has lower limits than authorized user flow
- Points reset periodically
- Heavy queries (events, tables) cost more

### Response Format
- **JSON** (GraphQL response)
- Fight data includes: `id`, `boss`, `kill`, `startTime`, `endTime`, `difficulty`, `size`, `bossPercentage`
- Report data includes: `code`, `title`, `zone`, `owner`, `startTime`, `endTime`, `visibility`, `fights[]`
- Player data includes: `name`, `id`, `type` (Player/NPC), `server`, `classID`, `specID`

---

## 3. Medallion Architecture (Bronze → Silver → Gold) on MinIO S3

### Proposed Directory Structure

```
s3://orakel/
├── bronze/                          # Raw data as ingested (no transformation)
│   ├── raiderio/
│   │   ├── runs/
│   │   │   └── season={season}/
│   │   │       └── year={yyyy}/month={mm}/day={dd}/
│   │   │           └── runs_{timestamp}.parquet
│   │   ├── characters/
│   │   │   └── ...
│   │   └── affixes/
│   │       └── ...
│   ├── warcraftlogs/
│   │   ├── reports/
│   │   │   └── ...
│   │   └── events/
│   │       └── ...
│   └── ...
├── silver/                          # Cleaned, deduplicated, validated
│   ├── dungeon_runs/                # Unified runs across sources
│   │   └── season={season}/year={year}/month={month}/
│   │       └── dungeon_runs.parquet
│   ├── player_performance/           # Per-player per-fight metrics
│   │   └── season={season}/year={year}/month={month}/
│   │       └── player_performance.parquet
│   ├── affix_schedule/              # Weekly affix rotation
│   │   └── season={season}/
│   │       └── affixes.parquet
│   └── ...
├── gold/                            # Aggregated, business-ready
│   ├── kpi_tank_death_clock/        # Tank survival time prediction
│   │   └── season={season}/
│   │       └── tank_death_clock.parquet
│   ├── kpi_healer_deficit/          # Healer efficiency metrics
│   │   └── season={season}/
│   │       └── healer_deficit.parquet
│   ├── kpi_interrupt_success/       # Interrupt success rates
│   │   └── season={season}/
│   │       └── interrupt_success.parquet
│   ├── kpi_dps_rotation/            # DPS rotation quality
│   │   └── season={season}/
│   │       └── dps_rotation.parquet
│   ├── kpi_avoidance/               # Avoidance efficiency
│   │   └── season={season}/
│   │       └── avoidance.parquet
│   ├── dungeon_predictions/         # ML model predictions
│   │   └── ...
│   └── dimensional/                 # Dimension tables
│       ├── dim_dungeon.parquet
│       ├── dim_player.parquet
│       ├── dim_affix.parquet
│       ├── dim_spec.parquet
│       └── dim_date.parquet
└── ml_models/                       # Trained model artifacts
    └── dungeon_predictor/
        └── ...
```

### Key Conventions
- **File format**: Parquet (columnar, compressed, splittable)
- **Partition strategy**: By `season` (high-level filter) + date (`year/month/day` or `year/month`)
- **Bronze**: Append-only, raw JSON → Parquet conversion only
- **Silver**: Deduplication by natural keys, schema enforcement, null handling, type casting
- **Gold**: Pre-aggregated KPIs, dimension tables, feature tables for ML, ready for BI tools
- **Naming**: `snake_case.parquet` with descriptive prefixes

### Why Medallion for This Project
- Bronze holds raw API payloads (JSON → Parquet) — preserves evidence
- Silver joins Raider.IO runs with WarcraftLogs combat data — the integration layer
- Gold computes the 5+ KPIs and ML features — ready for dashboarding and prediction
- Partition by season aligns with WoW content cadence (seasons last ~6 months)

---

## 4. Domain Knowledge — WoW Mythic+

### Core Concepts

| Concept | Description |
|---------|-------------|
| **Mythic+ Key** | Scaling difficulty: key level 2-30+. Higher = more enemy HP/damage |
| **Timer** | Each dungeon has a par time (~20-35 min). Finish under = chest upgrade |
| **Affixes** | Weekly rotating modifiers: Tyrannical (bosses harder), Fortified (trash harder), seasonal affix, and 4th slot at key 10+ |
| **Role Comp** | 1 Tank + 1 Healer + 3 DPS (exactly 5 players) |
| **Chests/Rating** | Timed = +1/+2 key levels. M+ rating (RIO) tracks overall performance |
| **Season** | ~6 month cadence with rotating dungeon pool + seasonal affix |

### Key Metrics Per Role

**Tank** (`tank`):
- **DTPS**: Damage Taken Per Second — how much incoming damage
- **EHP**: Effective HP (HP + absorbs + armor + defensive cooldowns)
- **KTB**: Kite/Tank Bust mechanics avoidance
- **Self-healing**: Varies by class (Blood DK, Prot Pal, etc.)
- **Interrupts**: Number of successful interrupts

**Healer** (`healer`):
- **HPS**: Healing Per Second — raw output
- **Overheal %**: Healing wasted on full-HP targets
- **External CD usage**: Cooldowns used on tank/party
- **DPS contribution**: Some healers contribute damage
- **Mana management**: Key constraint for certain healers

**DPS** (`dps`):
- **DPS**: Damage Per Second — raw damage output
- **AoE vs ST split**: Cleave vs single-target priority
- **Interrupts**: Key survival utility
- **Avoidable damage taken**: Measures player skill
- **CC usage**: Crowd control application

### Affixes Impact
Affixes dramatically alter playstyle and metrics:
- **Tyrannical**: Bosses hit harder → higher tank DTPS, need more healing
- **Fortified**: Trash hits harder → DPS interrupts more critical
- **Bursting**: Enemies explode on death → healer HPS required
- **Sanguine**: Enemies leave healing pools → tank movement required
- **Grievous**: Wounds reduce healing received → healer dispel/purge
- **Raging**: Enemies enrage at low HP → interrupts required

---

## 5. KPI Definitions

### KPI 1: Tank Death Clock (Predictor)
**Goal**: Estimate how long a tank can survive without healing, given current damage intake and EHP.

**Formula**:
```
Tank Death Clock (seconds) = Tank EHP / (DTPS - HPS_on_tank)
```
Where:
- `EHP` = max HP + shields + armor mitigation factor + active defensive mitigation
- `DTPS` = Damage Taken Per Second (from tank logs)
- `HPS_on_tank` = Healing received from healer per second

**Inputs needed**:
- Raider.IO: Tank class/spec, key level, dungeon, affixes (affect damage scaling)
- WarcraftLogs: DamageTaken events, HealingDone events targeting tank, tank HP history

**Interpretation**:
- > 15s: Safe — healer has room to DPS
- 5-15s: Moderate — healer should focus tank
- < 5s: Critical — imminent death without external CDs

### KPI 2: Healer Deficit
**Goal**: Measure how well a healer can keep up with tank damage intake.

**Formula**:
```
Healer Deficit (HPS gap) = Tank DTPS - Healer HPS_on_tank
Healer Deficit Ratio = Tank DTPS / Healer HPS_on_tank
```

**Inputs needed**:
- WarcraftLogs: HealingDone events by healer → tank, DamageTaken events by tank
- Raider.IO: Affixes (Grievous, Bursting increase healer load)

**Interpretation**:
- Ratio < 1.0: Healer comfortably covering tank damage
- Ratio 1.0-1.2: Healer barely keeping up
- Ratio > 1.2: Tank is taking more damage than healer can output — death likely

### KPI 3: Successful Interrupt Rate
**Goal**: Track interrupt efficiency per player and per dungeon.

**Formula**:
```
Interrupt Success Rate = Successful Interrupts / Total Interrupt Casts
Interrupts per Threatening Cast = Successful Interrupts / Dangerous Enemy Casts
```

**Inputs needed**:
- WarcraftLogs: Casts events with `ability_id` = interrupt spells, `interrupts` event type
- Raider.IO: Dungeon roster to map character → player

**Interpretation**:
- > 90%: Excellent interrupt coverage
- 70-90%: Good, room for improvement
- < 70%: Gap in interrupt rotation

### KPI 4 (Suggested): DPS Rotation Quality
**Goal**: Evaluate how cleanly a DPS player executes their rotation (active time, cooldown usage, GCD uptime).

**Rationale**: DPS is affected by gear and key level — rotation quality is a more normalized measure of player skill.

**Formula**:
```
Rotation Quality = Active GCDs / Total Possible GCDs
Cooldown Utilization = CD uses / Expected CD uses during fight
```

**Inputs needed**: WarcraftLogs Cast events, fight duration, spec-specific cooldowns

### KPI 5 (Suggested): Avoidance Efficiency
**Goal**: Measure how much avoidable damage a player takes as a fraction of total damage.

**Rationale**: Low avoidable damage is the strongest predictor of high keys. Separates skilled players from carried ones.

**Formula**:
```
Avoidance Efficiency = 1 - (Avoidable Damage Taken / Total Damage Taken)
```

**Inputs needed**: WarcraftLogs DamageTaken events, dungeon-specific dangerous ability IDs

### KPI 6 (Suggested): CC (Crowd Control) Utilization
**Goal**: Track how effectively the group uses crowd control on dangerous trash packs.

**Rationale**: In high keys (25+), CC chain is essential for survival.

**Formula**:
```
CC Score = (Stuns + Roots + Disorients + Fears + Slows on dangerous targets) / Dangerous Trash Packs
```

**Inputs needed**: WarcraftLogs AuraApplied events with CC ability classifications

---

## 6. ML Approach

Based on the project requirements (regression, classification, clustering, or anomaly detection):

### Recommended: Regression (Dungeon Timer Prediction)
- **Target variable**: `clear_time_ms` — can the group time the key?
- **Features**:
  - Average player M+ score
  - Comp position (tank/healer/dps specs)
  - Key level
  - Affixes active
  - Dungeon ID
  - Historical clear rates for comp
- **Why**: Directly useful, interpretable, clear business value

### Alternative: Classification (Timer Outcome)
- **Target**: `num_chests` ∈ {0, 1, 2} (failed, +1, +2)
- **Features**: Same as regression
- **Why**: Simpler evaluation, aligns with player mental model

### Feature Engineering Notes
- Aggregate player stats from WarcraftLogs into per-player-per-season averages
- Encode affix combos as categorical features
- Create interaction features: (key_level × affix_strength)
- Normalize by dungeon (different dungeons have different timer lengths)

---

## 7. Python Libraries Discovered

| Library | Description | Usage |
|---------|-------------|-------|
| `requests` | HTTP client | Raider.IO REST API calls |
| `gql` / `gql-query-builder` | GraphQL client | WarcraftLogs GraphQL queries |
| `pyarrow` / `fastparquet` | Parquet support | Bronze/Silver/Gold storage |
| `pandas` | Data manipulation | Silver layer transformations |
| `pyspark` | Distributed processing | Required by assignment — transformations, aggregations |
| `scikit-learn` | ML library | Regression/classification models |
| `minio` | S3-compatible client | MinIO storage operations |
| `click` / `typer` | CLI framework | Pipeline orchestration |
| `apache-airflow` | Orchestration (optional) | DAG pipeline automation |
| `python-dotenv` | Environment config | API keys management |

### Useful Open-Source References
1. **mythic-plus-meta-scanner** (https://github.com/triat/mythic-plus-meta-scanner)
   - Pure Python, uses Raider.IO API directly via `requests`
   - Pattern: parallel HTTP fetches with ThreadPoolExecutor
   - Rate-limit handling with retry-backoff on 429

2. **ApplicantScout-Companion** (https://github.com/Antrakt92/ApplicantScout-Companion)
   - Production-quality WarcraftLogs API client in Python
   - OAuth token management, caching, retry logic
   - Combines Raider.IO + WarcraftLogs data
   - Good reference for OAuth flow and GraphQL query patterns

3. **ReunionLog** (https://github.com/nickgismokato/ReunionLog)
   - Python package for WarcraftLogs data extraction
   - Uses `requests` + `gql-query-builder`
   - Exports to CSV

---

## 8. Pipeline Architecture (Preliminary)

```
┌─────────────────────────────────────────────────────────────────┐
│                         Airflow DAG                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌───────────┐    │
│  │ Ingest   │   │ Ingest   │   │ Silver   │   │ Gold      │    │
│  │ RaiderIO │──▶│ WCLogs   │──▶│ Clean/   │──▶│ Aggregate │───▶│
│  │ ──▶Bronze│   │ ──▶Bronze│   │ Join     │   │ + KPIs    │    │
│  └──────────┘   └──────────┘   │ ──▶Silver │   │ ──▶Gold   │    │
│                                                  │           │    │
│                                                ┌─────────┐   │    │
│                                                │ ML      │   │    │
│                                                │ Train   │───▶│    │
│                                                │ ──▶Model│   │    │
│                                                └─────────┘   │    │
│                                                                  │
│  Parallel: Ingest RaiderIO + Ingest WCLogs                       │
│  Sequential: Ingest → Silver → Gold → ML                         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                          ┌──────────────────┐
                          │   Superset /     │
                          │  BI Dashboard    │
                          │  (5+ KPI charts) │
                          └──────────────────┘
```

### Critical Tasks
1. **Ingest Raider.IO** — fetch run data, character profiles, season/dungeon metadata
2. **Ingest WarcraftLogs** — fetch combat reports linked to Raider.IO runs
3. **Bronze → Silver** — type casting, schema enforcement, dedup, join on character+run
4. **Silver → Gold** — KPI computation (Spark aggregation)
5. **ML Training** — feature engineering, model training, evaluation
6. **BI Dashboard** — Superset or similar connected to Gold layer

### Parallel Tasks
- Raider.IO + WarcraftLogs ingestion (independent sources)
- Silver transformations for different domains (runs, players, affixes)
- KPI computations (independent — each KPI is its own aggregation)

---

## 9. Risks & Considerations

| Risk | Impact | Mitigation |
|------|--------|------------|
| WarcraftLogs OAuth setup complexity | High | Use Client Credentials flow (simpler); document setup clearly |
| Rate limits on WarcraftLogs API | Medium | Implement caching, batch processing, point budgeting |
| Large data volume from combat events | Medium | Sample events, use efficient filters, limit report depth |
| Mythic+ season changing during development | Low | Parameterize season slug; test with current + historical data |
| Linking Raider.IO runs to WarcraftLogs reports | High | No direct cross-reference; match by dungeon + key level + timestamp ± window |
| MinIO setup for local dev | Low | Docker Compose with MinIO container; well-documented pattern |
| Spark vs. pandas complexity | Medium | Use Spark for the mandatory transformations; pandas for lightweight prep |

---

## 10. Ready for Proposal

**Status**: YES — sufficient information gathered for proposal, spec, and design phases.

### Recommended Next Steps
1. Create SDD proposal with three approaches:
   - **Approach A (Minimal)**: Raider.IO only, pandas processing, Parquet on local FS, simple regression
   - **Approach B (Full)**: Raider.IO + WarcraftLogs, Spark + MinIO, Medallion architecture, 5+ KPIs, ML
   - **Approach C (Extended)**: Full + Airflow orchestration + Superset dashboard
2. The exploration reveals Approach B is the recommended baseline — it satisfies all university requirements
3. Additional KPIs (DPS Rotation Quality, Avoidance Efficiency, CC Utilization) are technically feasible with WarcraftLogs event data
4. Critical design decision: how to link Raider.IO runs to WarcraftLogs reports (no stable join key exists)
