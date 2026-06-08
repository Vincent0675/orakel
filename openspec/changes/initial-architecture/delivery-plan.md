# Delivery Plan — initial-architecture

> Estrategia: **Stacked-to-main** con PRs encadenados
> Cada PR mergea directo a `main` en orden secuencial.

---

## 📊 Resumen

| PR | Fase | Tareas | Archivos | Líneas estimadas | Depende de |
|----|------|--------|----------|-----------------|------------|
| #1 | Infraestructura + Bronze | 1.1 → 1.8 | 10 nuevos | ~800 | Docker + API keys |
| #2 | Silver + KPI 4 | 2.1 → 2.7 | 6 nuevos | ~600 | PR #1 |
| #3 | WCL + KPIs 1-3 | 3.1 → 3.10 | 6 nuevos + 3 modificados | ~1000 | PR #2 + WCL keys |
| #4 | ML + BI + Orquestador | 4.1 → 4.5 | 3 nuevos + 1 modificado | ~600 | PR #3 |
| | **Total** | **30 tareas** | **~25 nuevos + 4 modificados** | **~3000** | |

---

## PR #1 — Infraestructura + Bronze

**Objetivo**: Tener el pipeline base funcionando: MinIO, Spark, y datos de Raider.IO en Bronze.

### Tareas

| ID | Tarea | Archivos | Esfuerzo |
|----|-------|----------|----------|
| 1.1 | Inicializar `pyproject.toml` con `uv add pyspark minio requests python-dotenv scikit-learn` | `pyproject.toml` | S |
| 1.2 | Crear `docker-compose.yml` con MinIO | `docker-compose.yml` | S |
| 1.3 | Crear `.env` template + `orakel/config.py` + `.gitignore` | `.env` `.gitignore` `orakel/__init__.py` `orakel/config.py` | S |
| 1.4 | Crear `orakel/utils/minio.py` — SparkSession factory (S3A, local[*]) | `orakel/utils/__init__.py` `orakel/utils/minio.py` | M |
| 1.5 | Crear `orakel/models/schemas.py` — StructType de Bronze/Silver/Gold | `orakel/models/__init__.py` `orakel/models/schemas.py` | M |
| 1.6 | Crear `orakel/clients/raiderio.py` — REST con paginación + 429 backoff | `orakel/clients/__init__.py` `orakel/clients/raiderio.py` | M |
| 1.7 | Crear `scripts/ingest_raiderio.py` — fetch runs → Bronze Parquet | `scripts/ingest_raiderio.py` | M |
| 1.8 | Smoke test: `--limit 5` → verificar archivos en MinIO | — | S |

### Estructura final después del PR

```
orakel/
├── pyproject.toml
├── docker-compose.yml
├── .env
├── .gitignore
├── orakel/
│   ├── __init__.py
│   ├── config.py
│   ├── clients/
│   │   ├── __init__.py
│   │   └── raiderio.py
│   ├── models/
│   │   ├── __init__.py
│   │   └── schemas.py
│   └── utils/
│       ├── __init__.py
│       └── minio.py
└── scripts/
    └── ingest_raiderio.py
```

### Hito de verificación pre-merge
- [ ] `uv run pyspark --version` funciona
- [ ] `docker compose up -d` → MinIO accesible en localhost:9001
- [ ] `uv run python scripts/ingest_raiderio.py --season season-tww-3 --limit 5` escribe 5 runs en Bronze
- [ ] `spark.read.parquet("s3a://orakel/bronze/raiderio/runs/")` devuelve DataFrame con schema correcto

---

## PR #2 — Silver + KPI 4 (Synergy Score)

**Objetivo**: Pipeline de limpieza + primer KPI funcional (Composition Synergy) solo con Raider.IO.

### Tareas

| ID | Tarea | Archivos | Esfuerzo |
|----|-------|----------|----------|
| 2.1 | Crear `orakel/pipeline/bronze.py` — pipeline module | `orakel/pipeline/__init__.py` `orakel/pipeline/bronze.py` | M |
| 2.2 | Crear `orakel/pipeline/silver.py` — clean + dedup + type casting | `orakel/pipeline/silver.py` | M |
| 2.3 | Crear `scripts/bronze_to_silver.py` — Spark job | `scripts/bronze_to_silver.py` | S |
| 2.4 | Crear `orakel/models/kpi.py` — funciones de KPIs (los 4) | `orakel/models/kpi.py` | M |
| 2.5 | Crear `orakel/pipeline/gold.py` — KPI aggregations + dims | `orakel/pipeline/gold.py` | L |
| 2.6 | Crear `scripts/silver_to_gold.py` — Spark job | `scripts/silver_to_gold.py` | S |
| 2.7 | Verificar KPI 4: synergy_score con datos reales | — | S |

### Estructura nueva

```
orakel/
├── orakel/
│   ├── models/
│   │   └── kpi.py              ← NUEVO
│   └── pipeline/
│       ├── __init__.py         ← NUEVO
│       ├── bronze.py           ← NUEVO
│       ├── silver.py           ← NUEVO
│       └── gold.py             ← NUEVO
└── scripts/
    ├── bronze_to_silver.py     ← NUEVO
    └── silver_to_gold.py       ← NUEVO
```

### Hito de verificación pre-merge
- [ ] `bronze_to_silver.py` produce `silver/raiderio_runs/` deduplicado
- [ ] `compute_death_clock(10000, 5000, 600000)` → 120s (safe)
- [ ] `compute_interrupt_rate(0, 0)` → NULL (no zero casts)
- [ ] `silver_to_gold.py` produce `gold/kpi_composition_synergy/` con datos
- [ ] synergy_score NULL para comps con sample_count=1

---

## PR #3 — WCL Integration + KPIs 1-3

**Objetivo**: Fuzzy join + los 3 KPIs que dependen de datos de combate.

### Tareas

| ID | Tarea | Archivos | Esfuerzo |
|----|-------|----------|----------|
| 3.1 | Crear `orakel/clients/warcraftlogs.py` — GraphQL+OAuth + point tracking | `orakel/clients/warcraftlogs.py` | L |
| 3.2 | Crear `orakel/utils/rate_limiter.py` — token-bucket | `orakel/utils/rate_limiter.py` | S |
| 3.3 | Crear `scripts/match_reports.py` — fuzzy join engine + stratified sampling | `scripts/match_reports.py` | XL |
| 3.4 | Ejecutar match_reports → verificar matches | — | M |
| 3.5 | Crear `scripts/ingest_warcraftlogs.py` — fetch eventos WCL | `scripts/ingest_warcraftlogs.py` | L |
| 3.6 | Modificar `orakel/pipeline/silver.py` — fuzzy join + match manifest | `orakel/pipeline/silver.py` (mod) | M |
| 3.7 | Modificar `scripts/bronze_to_silver.py` — procesar WCL | `scripts/bronze_to_silver.py` (mod) | S |
| 3.8 | Modificar `orakel/pipeline/gold.py` — KPIs 1-3 | `orakel/pipeline/gold.py` (mod) | L |
| 3.9 | Pipeline completo → verificar 4 KPIs | — | M |
| 3.10 | Auditoría manual: verificar 10 fuzzy join matches | — | S |

### Archivos modificados respecto a PR anterior
- `orakel/pipeline/silver.py` (se agrega `apply_fuzzy_join()`)
- `scripts/bronze_to_silver.py` (se agrega soporte WCL)
- `orakel/pipeline/gold.py` (se agregan KPIs 1-3)

### Hito de verificación pre-merge
- [ ] `WarcraftLogsClient.get_token()` retorna token válido
- [ ] `match_reports.py` produce `silver/matches/` con confianza > 0.5
- [ ] `ingest_warcraftlogs.py` escribe eventos en Bronze
- [ ] Los 4 KPIs existen en Gold: `kpi_tank_death_clock`, `kpi_healer_deficit`, `kpi_interrupt_success`, `kpi_composition_synergy`
- [ ] 10 matches auditados manualmente pasan Layer 1 + 2 + 3

---

## PR #4 — ML + BI + Orquestador

**Objetivo**: Modelo predictivo, dashboard, y script que orquesta todo.

### Tareas

| ID | Tarea | Archivos | Esfuerzo |
|----|-------|----------|----------|
| 4.1 | Crear `scripts/train_model.py` — features + split + scikit-learn | `scripts/train_model.py` | L |
| 4.2 | Entrenar → verificar métricas (RMSE, MAE, R²) | — | S |
| 4.3 | Dashboard/notebook BI conectado a Gold | `notebooks/bi_dashboard.ipynb` | M |
| 4.4 | Crear `scripts/run_pipeline.py` — CLI orquestador | `scripts/run_pipeline.py` | M |
| 4.5 | Smoke test E2E con `--limit 10` | — | S |

### Estructura nueva

```
orakel/
├── scripts/
│   ├── train_model.py          ← NUEVO
│   └── run_pipeline.py         ← NUEVO
└── notebooks/
    └── bi_dashboard.ipynb      ← NUEVO
```

### Hito de verificación pre-merge
- [ ] Modelo entrenado guardado en `ml_models/dungeon_predictor/`
- [ ] R² > 0 (mejor que línea de base)
- [ ] Dashboard con 4 charts + 3 filtros funcionales
- [ ] `run_pipeline.py --steps all --limit 10` corre de principio a fin
- [ ] Pipeline completo en < 15 min (limit 10)

---

## 📐 Principios de ejecución

### Commits
Cada PR se compone de commits por unidad de trabajo:
- `feat: add RaiderIO client with pagination`
- `feat: add Bronze ingestion pipeline`
- `feat: add Silver clean + dedup layer`
- etc.

### Stacked-to-main workflow
```
main ← PR #1 (infra)
main ← PR #2 (silver + kpi4)
main ← PR #3 (wcl + kpis 1-3)
main ← PR #4 (ml + bi)
```

Cada PR mergea directo a `main`. El siguiente PR se rebasea sobre `main` después del merge anterior.

### Dependencias externas
| PR | Requiere | Cómo verificarlo |
|----|----------|-----------------|
| #1 | Docker instalado | `docker --version` |
| #1 | API key de Raider.IO | Configurada en `.env` |
| #3 | WCL Client ID + Secret | Configurado en `.env` |
| #3 | Verificación del mapping challenge_mode_id ↔ encounterID | Validar con llamada real a WCL |

---

## 📁 Documentos relacionados

| Documento | Ruta |
|-----------|------|
| Visión del proyecto | `.openspec/VISION.md` |
| Especificaciones | `.openspec/changes/initial-architecture/spec.md` |
| Diseño técnico | `.openspec/changes/initial-architecture/design.md` |
| Desglose de tareas | `.openspec/changes/initial-architecture/tasks.md` |
| **Plan de entrega** | **`.openspec/changes/initial-architecture/delivery-plan.md`** ← estás acá |
