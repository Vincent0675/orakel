# Proposal: initial-architecture

## 1. Intent & Motivation

**Context**: University assignment requiring a complete Big Data pipeline: 2 data sources → Ingest → Clean → Integrate → Spark Processing → Final Storage → BI → ML/Analytics. Orakel is the implementation vehicle.

**Why WoW dungeon predictor**: Mythic+ dungeons provide rich, public data (Raider.IO + WarcraftLogs) with natural complexity — fuzzy joins, time-series events, multi-source integration — ideal for demonstrating all pipeline phases.

**MVP Philosophy**: Build the minimum that satisfies every assignment phase, designed for scale. Medallion architecture on MinIO S3 allows incremental growth without re-architecting.

## 2. Scope

### In Scope (MVP)
- Full pipeline: Ingest → Bronze → Silver → Gold → BI + ML
- 4 KPIs: Tank Death Clock, Healer Deficit, Interrupt Success Rate, Composition Synergy Score
- Two data sources: Raider.IO (REST) + WarcraftLogs (GraphQL, OAuth 2.0)
- PySpark for ALL Parquet transformations (no PyArrow)
- Medallion architecture on MinIO S3 (Bronze/Silver/Gold)
- scikit-learn ML model (timer prediction)
- BI dashboard connected to Gold layer
- **Package management**: `uv` exclusively — no pip/pip3/poetry commands

### Out of Scope (MVP)
- Real-time streaming
- Web frontend
- Multi-season historical backfill
- Airflow orchestration (deferred, optional)
- Additional KPIs beyond the 4 confirmed

## 3. KPI Definitions

### KPI 1: Tank Death Clock
**Source**: WarcraftLogs (DamageTaken + HealingDone events)
**Formula**: `DeathClock(s) = EHP / (DTPS - HPS_on_tank)`
- EHP: max HP + shields + armor mitigation + active defensives
- DTPS: damage taken per second by tank
- HPS_on_tank: healing received by tank per second
- Interpretation: >15s safe, 5-15s moderate, <5s critical

### KPI 2: Healer Deficit
**Source**: WarcraftLogs (HealingDone + DamageTaken events)
**Formula**: `Deficit = Tank_DTPS / Healer_HPS_on_tank`
- < 1.0: healer covers tank damage
- 1.0-1.2: barely keeping up
- > 1.2: death likely

### KPI 3: Interrupt Success Rate
**Source**: WarcraftLogs (Casts + Interrupts events)
**Formula**: `ISR = Successful_Interrupts / Total_Interrupt_Casts`
- >90% excellent, 70-90% good, <70% gap

### KPI 4: Composition Synergy Score
**Source**: Raider.IO (runs with roster)
**Formula**: `Synergy(comp) = avg_clear_time(comp) / avg_clear_time(all_comps)`
- Normalized per dungeon + affix + key level
- < 1.0: above-average comp performance

## 4. Approach Comparison

| Aspect | A (Minimal) | B (Complete — recommended) | C (Extended) |
|--------|------------|--------------------------|-------------|
| Sources | Raider.IO only | Raider.IO + WCL | Raider.IO + WCL |
| Processing | pandas | PySpark + MinIO | PySpark + MinIO + Airflow |
| Storage | Local FS | Medallion on MinIO | Medallion on MinIO |
| KPIs | 3 | 4 | 5+ |
| ML | Simple regression | Regression + feature eng | Full pipeline + tuning |
| BI | Static plots | Dashboard | Superset dashboard |
| Risk | Low | Medium | Medium-High |

## 5. Recommended Approach Justification

Approach B satisfies ALL mandatory assignment requirements (2 data sources, Spark processing, Medallion storage, 4+ KPIs, ML, BI) while remaining achievable as an MVP. The primary engineering challenge is the fuzzy join between Raider.IO runs and WarcraftLogs combat reports — matched by dungeon + key level + timestamp window. Approach A fails the "2 data sources" requirement. Approach C adds Airflow and Superset but these are optional per the assignment brief.

## 6. Key Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| PySpark install from source (sdist) | Low | Java 11+ JDK required; already available locally. No Python 3.13 incompatibility — PySpark 4.1.2 officially supports 3.13 |
| Fuzzy join Raider.IO ↔ WCL | High | Timestamp + dungeon + key level matching; validate |
| WarcraftLogs OAuth 2.0 setup | Medium | Client Credentials flow (simplest) |
| Large combat event volume | Medium | Server-side filtering; sample; limit depth |
| No cross-reference key | High | Accept approximate matching; document precision |

## 7. High-Level Pipeline DAG

```
Raider.IO API ──▶ Bronze (Raw Parquet)
WarcraftLogs API ──▶ Bronze (Raw Parquet)
        │
        ▼
Silver (Clean + Dedup + Fuzzy Join Raider.IO ↔ WCL)
        │
        ▼
Gold (4 KPI Aggregations + Dimension Tables)
        │
        ├──▶ BI Dashboard (4 KPI charts)
        └──▶ ML Model (Timer Prediction)
```

## 8. ML Strategy

**Target**: `clear_time_ms` (regression) — predicts dungeon completion time. Alternative: binary `timed` / `not_timed` (classification).

**Features**: avg M+ score per roster, composition synergy score (KPI 4), key level, affixes (encoded), dungeon ID, historical per-player KPI values.

**Models**: Linear Regression (interpretability baseline) → Random Forest (accuracy, feature importance).

**Evaluation**: RMSE, MAE, R² for regression; accuracy + F1 for classification.
