# Design: initial-architecture

## Technical Approach

Medallion pipeline on MinIO S3 with PySpark processing. Two API sources (Raider.IO REST, WCL GraphQL+OAuth) ingest to Bronze Parquet. Silver layer cleans, deduplicates, and fuzzy-joins via a **match manifest** (Python script, not pure Spark). Gold computes 4 KPIs + dimensions. ML trains scikit-learn on Gold features with chronological split. All orchestrated by CLI scripts via `uv run`.

## Architecture Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Fuzzy join pattern | Python script → match manifest → Spark reads manifest | Requires sequential WCL API calls (rate-limited); manifest is tiny (KB); decouples API calls from data processing; checkpoint: re-run Silver without re-calling WCL |
| Tank sampling | **Stratified by tank class** (Warrior/Paladin/DK/Monk/Druid/DH) | Prevents ML bias toward overrepresented classes. Ensures balanced training data across all 6 tank specs |
| Parquet I/O | PySpark exclusively (no pandas/PyArrow for transforms) | Assignment mandates non-trivial Spark processing |
| Package manager | `uv` only (no pip/poetry) | Project constraint (VISION.md) |
| ML split | Chronological 80/20 by `completed_at` | Prevents data leakage (random split would predict past from future) |
| Spark mode | `local[*]` + Docker MinIO | Single-machine MVP; scale = change one config |
| Deployment | Docker Compose (MinIO) + Python scripts | No cluster needed for ~500 MB total |

## Data Flow

```
Raider.IO API ──▶ Bronze/raiderio/ ──┐
                                      ├─▶ Silver (match manifest join) ──▶ Gold (KPIs+dims) ──▶ ML
WCL API (via manifest) ──▶ Bronze/wcl/┘                                                          └─▶ BI
```

**Sequence**: `docker-compose up` → `ingest_raiderio.py` (Bronze) → `match_reports.py` (stratify tanks by class → WCL calls per tank → `silver/matches/`) → `ingest_warcraftlogs.py` (Bronze) → `bronze_to_silver.py` (Spark) → `silver_to_gold.py` (Spark) → `train_model.py` (ML)

**Stratified sampling detail in `match_reports.py`:**
```
1. Load Raider.IO runs → group by tank.class (Warrior, Paladin, DK, Monk, Druid, DH)
2. Sample top N per class:
   - Class con menos presencia → sample completo (priorizar variedad)
   - Class con más presencia → sample proporcional (no saturar)
3. Peso por clase inversamente proporcional a su popularidad
4. Recién entonces → llamadas WCL por cada tank del sample
```

## File Changes

| File | Action | Description |
|------|--------|-------------|
| `pyproject.toml` | Modify | Add pyspark, minio, requests, python-dotenv, scikit-learn |
| `docker-compose.yml` | Create | MinIO service (ports 9000/9001) |
| `.env` | Create | API keys, MinIO config, season (gitignored) |
| `orakel/config.py` | Create | python-dotenv env loading |
| `orakel/clients/raiderio.py` | Create | REST client with pagination + 429 backoff |
| `orakel/clients/warcraftlogs.py` | Create | GraphQL+OAuth client with point tracking |
| `orakel/models/schemas.py` | Create | PySpark StructType for Bronze/Silver/Gold |
| `orakel/models/kpi.py` | Create | Pure Python KPI functions (UDF-compatible) |
| `orakel/pipeline/bronze.py` | Create | API → Spark DataFrame → Parquet on MinIO |
| `orakel/pipeline/silver.py` | Create | Clean/dedup + join via match manifest |
| `orakel/pipeline/gold.py` | Create | KPI aggregations + dimension tables |
| `orakel/utils/minio.py` | Create | Spark session factory (S3A config, local mode) |
| `orakel/utils/rate_limiter.py` | Create | Token-bucket limiter for WCL |
| `scripts/ingest_raiderio.py` | Create | CLI: runs → Bronze |
| `scripts/ingest_warcraftlogs.py` | Create | CLI: events (using manifest) → Bronze |
| `scripts/match_reports.py` | Create | CLI: fuzzy-join engine → match manifest |
| `scripts/bronze_to_silver.py` | Create | CLI: Spark Bronze → Silver |
| `scripts/silver_to_gold.py` | Create | CLI: Spark Silver → Gold |
| `scripts/train_model.py` | Create | CLI: ML training |
| `scripts/run_pipeline.py` | Create | CLI orchestrator (`--steps`, `--season`, `--limit`) |
| `main.py` | Delete | Replaced by package structure |

## Interfaces / Contracts

**Spark Session** (`orakel/utils/minio.py`): Returns `SparkSession` with `local[*]`, S3A config pointing to MinIO, `hadoop-aws` JAR, 4g driver memory.

**KPI Functions** (`orakel/models/kpi.py`): `compute_death_clock(dtps, hps_on_tank, max_hp) → (seconds, category)`, `compute_healer_deficit(tank_dtps, healer_hps) → (ratio, category)`, `compute_interrupt_rate(successful, total) → (rate|null, coverage)`, `compute_synergy_score(comp_avg, overall_avg) → score|null`. All pure Python, usable in Spark UDFs.

**Match Manifest** (`silver/matches/`): Parquet with columns `rio_run_id, wcl_report_code, wcl_fight_id, confidence, match_method`.

**CLI** (`scripts/run_pipeline.py`): `--steps bronze,silver,gold,ml,all` | `--season season-tww-3` | `--limit 100`.

## Testing Strategy

| Layer | What | Approach |
|-------|------|----------|
| Unit | KPI functions, fuzzy join layers | `pytest` with known I/O fixtures |
| Unit | API clients (pagination, rate limit) | `responses` mock HTTP |
| Integration | Spark ↔ MinIO read/write | Local Spark + Docker MinIO |
| E2E | Full pipeline `--limit 10` | Small real-data subset |

## Migration / Rollout

No migration — greenfield. Phased: Phase 1 (skeleton + Raider.IO→Bronze) → Phase 2 (Silver clean + Synergy KPI) → Phase 3 (WCL + fuzzy join + KPIs 1-3) → Phase 4 (ML + BI).

## ID Mapping: Raider.IO ↔ WarcraftLogs (TWW Season 3)

**Regla**: Raider.IO `challenge_mode_id` = WCL `encounterID`. NO usar `dungeon.id`.

| Mazmorra | `dungeon.id` (Raider.IO) | `challenge_mode_id` (Raider.IO) = `encounterID` (WCL) | Short Name |
|----------|:------------------------:|:------------------------------------------------------:|:----------:|
| Ara-Kara, City of Echoes | 15093 | **503** | ARAK |
| Eco-Dome Al'dani | 16104 | **542** | EDA |
| Halls of Atonement | 12831 | **378** | HOA |
| Operation: Floodgate | 15452 | **525** | FLOOD |
| Priory of the Sacred Flame | 14954 | **499** | PSF |
| Tazavesh: So'leah's Gambit | 1000001 | **392** | GMBT |
| Tazavesh: Streets of Wonder | 1000000 | **391** | STRT |
| The Dawnbreaker | 14971 | **505** | DAWN |

El `challenge_mode_id` viene incluido en la respuesta de `/mythic-plus/static-data` de Raider.IO. Se almacena en Bronze junto al `dungeon_id` para referencia.

## Open Questions

- [ ] PyArrow transitive dependency: PySpark 4.x may require pyarrow for Spark SQL — need to verify at install
- [ ] WCL point budget per season: validate that one season fits in one batch (~3600 pts/hr)