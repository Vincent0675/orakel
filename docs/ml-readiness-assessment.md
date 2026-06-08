# ML Readiness Assessment — Orakel

> **Date**: 2026-06-08
> **Pipeline state**: Bronze → Silver complete (2.000 runs). Match WCL in progress (rate-limited).
> **Author**: SDD Orchestrator (automated assessment)

---

## 1. Current Data State

| Layer | Data | Status |
|-------|------|--------|
| 🟤 Bronze | 2.000 runs Raider.IO | ✅ Complete |
| ⚪ Silver | 2.000 runs cleaned + dedup | ✅ Complete |
| 🔗 Match manifest | Stratified tank sampling → WCL | ⏳ In progress (70/386 tanks) |
| 🔵 WCL Bronze events | Damage, healing, interrupts | ⏳ Pending match manifest |
| 🟡 Gold KPIs | Death clock, healer deficit, interrupt rate, synergy | ⏳ Pending WCL data |

## 2. Dataset Profile

### Sample
| Metric | Value |
|--------|-------|
| Total runs | 2.000 |
| Dungeons | 8/8 (TWW Season 3) |
| Mythic level range | 22 — 24 |
| Mean mythic level | 22.7 |
| Time period | 2025-11-13 — 2026-02-26 |
| Players in roster | 10.000 (5 per run) |

### Class Distribution
| Class | Players | % |
|-------|---------|:-:|
| Druid | 1.415 | 14.1% |
| Death Knight | 1.398 | 14.0% |
| Mage | 1.330 | 13.3% |
| Warrior | 1.299 | 13.0% |
| Demon Hunter | 1.178 | 11.8% |
| Paladin | 1.067 | 10.7% |
| Hunter | 868 | 8.7% |
| Shaman | 455 | 4.5% |
| Monk | 363 | 3.6% |
| Warlock | 264 | 2.6% |
| Evoker | 188 | 1.9% |
| Priest | 138 | 1.4% |
| Rogue | 37 | 0.4% |

**13/13 classes represented.** Coefficient of variation: 67.1% (moderately unbalanced — reflects real M+ meta).

### Specializations
**35 of 38** possible specializations detected across all runs. Missing specs are likely niche (e.g., specific tank/healer variants with low representation at high keys).

### Roles (per run)
- 1 Tank ✅
- 1 Healer ✅
- 3 DPS ✅

### Data Quality
| Column | Nulls | Quality |
|--------|:-----:|:-------:|
| `mythic_level` | 0 (0%) | ✅ |
| `clear_time_ms` | 0 (0%) | ✅ |
| `score` | 0 (0%) | ✅ |
| `rank` | 0 (0%) | ✅ |
| `dungeon_id` | 0 (0%) | ✅ |

## 3. Features Available

### Current Features (from Silver)
| Feature | Type | Description |
|---------|------|-------------|
| `mythic_level` | Numeric | Keystone level (22-24) |
| `clear_time_ms` | Numeric | Time to complete dungeon (target variable) |
| `score` | Numeric | Raider.IO run score |
| `rank` | Numeric | Ranking position |
| `dungeon_id` | Categorical | Dungeon identifier |
| `dungeon_name` | Categorical | Dungeon name |
| `weekly_modifiers` | Array[Int] | Affix IDs for the week |
| `year`, `month` | Temporal | Time components |
| `completed_at` | Temporal | Timestamp |
| `roster` | Structured | 5 players with class/spec/role |

### Pending Features (Gold KPIs — after WCL match)
| Feature | Type | Description |
|---------|------|-------------|
| `death_clock` | Numeric | Tank survival time (seconds) |
| `healer_deficit` | Numeric | Healer throughput ratio |
| `interrupt_rate` | Numeric | Interrupts per minute |
| `synergy_score` | Numeric | Composition synergy bonus |
| `damage_taken` | Numeric | Per-player damage received |
| `healing_received` | Numeric | Per-player healing received |
| `interrupts_count` | Numeric | Per-player interrupt count |

## 4. Target Variable

**Primary target**: `clear_time_ms` (dungeon completion time)

| Statistic | Value |
|-----------|-------|
| Min | 17.2 min |
| P25 | 26.3 min |
| Median | **28.3 min** |
| P75 | 30.3 min |
| Max | 35.0 min |
| Mean | 28.4 min |
| Std | 2.7 min |

**Observation**: Tight distribution at high keys (22-24). A wider key range (2-24) would provide more variance for robust model training.

## 5. ML Readiness Assessment

### ✅ Ready
- Sample size sufficient for MVP (2000 runs, ~10K player observations)
- Zero nulls in critical columns
- All 13 classes and 35 specs represented
- Clean feature engineering pipeline in place (Spark → Pandas)
- Stratified sampling strategy implemented to prevent class bias

### ⚠️ Requires Attention
- **Class imbalance** (CV 67.1%): Rare specs (Rogue, Priest) will need class weights or oversampling
- **Narrow key range** (22-24): Model will specialize in high-end M+ only. Expand ingestion for lower keys.
- **Homogeneous target**: 28.3±2.7 min — predictions will have limited resolution

### ⏳ Pending
- Gold KPIs (4 features) — waiting on WCL match completion
- WCL combat stats (3 features) — waiting on WCL data ingestion
- Full feature set of ~14 features expected

## 6. Recommendations

1. **Complete the pipeline**: Let match_reports finish rate limit, then process WCL events → Silver fuzzy join → Gold KPIs
2. **Expand key range**: Ingest runs from all mythic levels (2-24) for better generalization
3. **Weighted training**: Use class weights or stratified sampling during ML training to handle class imbalance
4. **Start with regression**: Predict `clear_time_ms` as a regression task; evaluate with MAE/RMSE
5. **Future multiclass**: Once enough data, classify by outcome category (fast/medium/slow)

## 7. Visualizations

Generated exploration plots are available at:

| Plot | Path |
|------|------|
| Class & role distribution | `/tmp/orakel_viz/class_distribution.png` |
| Dungeon & key level distribution | `/tmp/orakel_viz/dungeon_distribution.png` |
| Temporal & clear time distribution | `/tmp/orakel_viz/temporal_distribution.png` |
| Mythic level vs clear time | `/tmp/orakel_viz/mythic_vs_time.png` |

---

*Assessment generated automatically from Silver Raider.IO data. Update when Gold KPIs and WCL combat data are available.*
