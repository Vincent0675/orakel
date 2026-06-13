# Orakel — Análisis Predictivo de Mazmorras WoW Mythic+

> **Proyecto Integrador — Big Data + BI + Procesamiento + ML + Orquestación**

[![Tests](https://img.shields.io/badge/tests-146_passing-brightgreen)]()
[![Coverage ML](https://img.shields.io/badge/coverage%20ML-93%25-brightgreen)]()
[![Python 3.13](https://img.shields.io/badge/python-3.13-blue)]()
[![Dagster](https://img.shields.io/badge/orquestación-Dagster-orange)]()

## 📋 Descripción

Pipeline completo de ingeniería y analítica de datos sobre la temporada 3 de The War Within (WoW Mythic+). Integra datos de **Raider.IO** y **WarcraftLogs** mediante arquitectura Medallion (Bronce → Plata → Oro) con PySpark, almacenamiento en MinIO (S3-compatible), orquestación con **Dagster**, tracking de modelos con **MLflow**, y un dashboard BI con **Streamlit**.

Incluye un módulo de **Machine Learning** (Ridge regression) que predice `clear_time_seconds` a partir de los KPIs calculados en la capa Gold.

---

## 🏗️ Arquitectura del Pipeline

```
Raider.IO ──► Bronze ──► Silver ──► Match ──► WCL Events ──► Gold ──► ML ──► Predicción
   API         Parquet     Clean +     3-Layer      DamageTaken   4 KPIs    Ridge   clear_time_seconds
                           Dedup       Fuzzy Join   Healing       4 Dims    MLflow
                                                     Interrupts
```

### Capas implementadas

| Capa | Estado | Descripción |
|------|:------:|-------------|
| ① Fuente A (Raider.IO) | ✅ | 2.000 runs de Mythic+, temporada TWW S3 |
| ② Fuente B (WarcraftLogs) | ✅ | 286 reports con datos de combate vía GraphQL |
| ③ Ingesta | ✅ | Scripts con checkpointing incremental |
| ④ Limpieza | ✅ | Dedup por keystone_run_id, tipado forzado |
| ⑤ Integración | ✅ | Fuzzy join 3-capas (nivel+afijos, ventana temporal, solapamiento de roster) |
| ⑥ Procesamiento Spark | ✅ | PySpark: agregaciones, Window functions, UDFs |
| ⑦ Almacenamiento final | ✅ | Gold layer en MinIO (Parquet) |
| ⑧ Visualización BI | ✅ | Streamlit dashboard con 5 páginas, filtros, plotly |
| ⑨ ML / Analítica | ✅ | Ridge regression con MLflow tracking y MinIO persistence |
| ⑩ Orquestación | ✅ | Dagster con 17 software-defined assets |

---

## 🧠 Módulo ML

| Componente | Descripción |
|------------|-------------|
| **Algoritmo** | Ridge regression con z-score normalization |
| **Features** | key_level, role counts, affix flags (16), dungeon one-hot (8), death_clock_log1p, 4 KPIs |
| **Tracking** | MLflow — params, metrics, model artifacts |
| **Persistencia** | MinIO — modelo serializado con StandardScaler |
| **Train/Test split** | Temporal 80/20 (sin data leakage) |
| **Edge case** | Skip training si n < 10 filas |
| **Cobertura tests** | **93%** (75 tests: 68 Tier 1 pytest + 7 Tier 2 Spark) |

### Estructura del módulo

```
orakel/ml/
├── __init__.py
├── schemas.py        # Constantes: FEATURE_COLUMNS, TARGET_COLUMN, AFFIX_COLUMNS, etc.
├── features.py       # Spark feature engineering: build_feature_view()
├── trainer.py        # train_model() con MLflow + MinIO upload
└── predict.py        # predict() y load_model_from_mlflow()
```

---

## 📊 Datos

### Volumen

| Capa | Tabla | Filas |
|------|-------|------:|
| Bronze | WCL DamageTaken | 28.072 |
| Bronze | WCL Healing | 108.496 |
| Bronze | WCL Interrupts | 378.339 |
| Silver | Raider.IO runs | 2.000 |
| Silver | dungeon_runs | 5.936 |
| Silver | player_performance | 29.680 |
| Gold | KPI 1 — Death Clock | 5.936 |
| Gold | KPI 2 — Healer Deficit | 5.936 |
| Gold | KPI 3 — Interrupt Rate | 29.680 |
| Gold | KPI 4 — Comp Synergy | 1.310 |
| Gold | Dimensiones (4) | 8–3.079 |

---

## 🛠️ Stack Tecnológico

| Componente | Tecnología |
|------------|-----------|
| **Lenguaje** | Python 3.13 |
| **Procesamiento** | PySpark 4.x (Hadoop S3A) |
| **Almacenamiento** | MinIO (S3-compatible) |
| **Ingesta API** | Raider.IO REST + WarcraftLogs GraphQL |
| **Orquestación** | Dagster 1.10+ (17 SDAs) |
| **ML** | scikit-learn 1.9+ (Ridge) + MLflow 2.20+ |
| **Dashboard** | Streamlit + Plotly Express |
| **Tests** | pytest 9 + chispa (Tier 2) |
| **Rate Limiting** | Token bucket (WCL: ~3.600 pts/hora) |
| **Checkpointing** | Checkpoint Parquet en MinIO por reporte/tanque |
| **Dependencias** | uv (100% Python) |

---

## 🚀 Quick Start

### Requisitos
- Python 3.13+
- [uv](https://docs.astral.sh/uv/) (gestor de dependencias)
- Docker + Docker Compose (para MinIO)

### 1. Setup
```bash
make setup       # Crea .env desde .env.example, instala deps
```

### 2. Levantar servicios (MinIO, MLflow, Dagster)
```bash
make run
```

Esto inicia en background:
- **MinIO** (S3-compatible storage) → `localhost:9000` (API), `localhost:9001` (console, `orakel` / `orakel123`)
- **MLflow** tracking server → `localhost:5000`
- **Dagster** webserver → `localhost:3000`

### 3. Ver logs / detener
```bash
make logs        # Ver logs de los servicios
make stop        # Detener todos los servicios
```

---

## 📜 Comandos disponibles (Makefile)

| Target | Descripción |
|--------|-------------|
| `make setup` | Instala dependencias, crea `.env` desde `.env.example` |
| `make run` | Levanta MinIO + MLflow + Dagster en background |
| `make stop` | Detiene todos los servicios |
| `make logs` | Muestra logs en vivo de los servicios |
| `make test` | Corre la suite de tests (146 tests) |
| `make test-fast` | Solo Tier 1 (sin Spark) |
| `make coverage` | Reporte de cobertura HTML |
| `make pipeline` | Ejecuta el pipeline completo (Dagster) |
| `make dashboard` | Levanta el dashboard Streamlit en `localhost:8501` |
| `make clean` | Borra caches Python y datos locales |

---

## 🔬 Ejecución manual del pipeline

Si querés correr el pipeline paso a paso sin Dagster:

```bash
# Ingesta Bronze (Raider.IO + WCL)
uv run python scripts/ingest_raiderio.py --season season-tww-3
uv run python scripts/ingest_warcraftlogs.py --season season-tww-3 --limit 10

# Match WCL con checkpointing
uv run python scripts/match_reports.py --season season-tww-3 --limit-tanks 30

# Bronze → Silver
uv run python scripts/bronze_to_silver.py --season season-tww-3

# Silver → Gold
uv run python scripts/silver_to_gold.py --season season-tww-3

# ML: entrenar modelo (MLflow + MinIO)
uv run python -c "from orakel.ml.trainer import train_model; ..."
```

O con Dagster:
```bash
uv run dagster dev -m orakel.pipeline.definitions
# → http://localhost:3000
```

### Dashboard
```bash
uv run streamlit run dashboard/app.py
# → http://localhost:8501
```

---

## 📁 Estructura del proyecto

```
orakel/
├── clients/
│   ├── raiderio.py           # Cliente API Raider.IO
│   └── warcraftlogs.py       # Cliente GraphQL WCL + OAuth2
├── models/
│   ├── kpi.py                # Lógica de KPIs
│   └── schemas.py            # Schemas PySpark
├── pipeline/
│   ├── bronze.py             # Ingesta a Bronze
│   ├── silver.py             # Limpieza + Fuzzy Join
│   ├── gold.py               # KPIs + Dimensiones
│   ├── io_managers.py        # IO managers para MinIO
│   ├── definitions.py        # Dagster Definitions (17 SDAs)
│   └── assets/               # SDAs de Dagster
│       ├── bronze.py
│       ├── silver.py
│       ├── gold.py
│       └── checks.py
├── ml/
│   ├── schemas.py            # FEATURE_COLUMNS, TARGET_COLUMN
│   ├── features.py           # build_feature_view() — Spark
│   ├── trainer.py            # train_model() — Ridge + MLflow + MinIO
│   └── predict.py            # predict() + load_model_from_mlflow()
├── utils/
│   ├── minio.py              # SparkSession con S3A
│   └── rate_limiter.py       # Token bucket
├── config.py                 # Settings desde .env
├── dashboard/
│   └── app.py                # Streamlit BI dashboard
├── tests/                    # 146 tests (Tier 1 + Tier 2)
├── scripts/                  # Scripts de ingesta
├── openspec/                 # Documentación SDD (2 cambios archivados)
├── docker-compose.yml        # MinIO service
├── Makefile                  # Comandos del proyecto
├── pyproject.toml
└── README.md
```

---

## 📈 Dashboard — Páginas

| Página | Contenido | Filtros |
|--------|-----------|---------|
| **Resumen General** | Métricas globales, cobertura de datos | — |
| **KPI 1 — Reloj de Muerte** | Distribución por categoría, scatter DTPS vs death clock | key_level, tank_class |
| **KPI 2 — Déficit del Sanador** | Distribución, scatter con líneas de referencia 1.0/1.2 | healer_class |
| **KPI 3 — Tasa de Interrupciones** | Interrupciones por min y rol, top 20 | player_role, player_class |
| **KPI 4 — Sinergia de Composición** | Mejores/peores composiciones, fiabilidad vs muestras | dungeon, key_level |

---

## 🔄 Checkpointing

Los scripts de ingesta implementan checkpointing incremental en MinIO:

- **`match_reports.py`**: Checkpoint por tanque (nombre+reino+región). Resume automáticamente.
- **`ingest_warcraftlogs.py`**: Checkpoint por report_code. Respeta `--skip-*` flags. Prioridad por timestamp.

Flags disponibles: `--resume` (default), `--no-resume`, `--limit N`.

---

## 🧪 Testing

```bash
# Todos los tests
make test
# o: uv run pytest tests/ -v
# → 146 tests passed (68 Tier 1 + 7 Tier 2 ML + 71 pipeline)

# Solo Tier 1 (sin Spark, más rápido)
make test-fast
# o: uv run pytest tests/ -m "not spark" -v
# → 139 tests passed

# Cobertura
make coverage
# o: uv run pytest --cov=orakel --cov-report=html
```

### Cobertura por módulo

| Módulo | Cobertura |
|--------|:---------:|
| `orakel/ml/` (ML module) | **93%** |
| `orakel/clients/` | ~95% |
| `orakel/pipeline/` | ~80% |
| `orakel/models/` | ~85% |
| `orakel/utils/` | ~90% |

---

## 📦 Configuración

Variables de entorno (`.env` desde `.env.example`):

| Variable | Descripción | Default |
|----------|-------------|---------|
| `WCL_CLIENT_ID` | OAuth client ID de WarcraftLogs | — |
| `WCL_CLIENT_SECRET` | OAuth client secret de WCL | — |
| `RAIDERIO_API_KEY` | API key de Raider.IO (opcional) | — |
| `MINIO_ENDPOINT` | Endpoint de MinIO | `localhost:9000` |
| `MINIO_ACCESS_KEY` | Usuario MinIO | `orakel` |
| `MINIO_SECRET_KEY` | Password MinIO | `orakel123` |
| `MINIO_BUCKET` | Bucket para datos | `orakel` |
| `SEASON` | Temporada activa | `season-tww-3` |
| `MLFLOW_TRACKING_URI` | URI de tracking MLflow | `file:./mlruns` |

---

## 🔭 Roadmap

- [x] Pipeline completo Bronze → Silver → Gold
- [x] ML module con Ridge regression
- [x] Orquestación con Dagster
- [x] Test suite (146 tests, 93% cobertura ML)
- [x] README + Makefile
- [ ] **Dockerización completa** — Dockerfile + compose con MinIO + MLflow + Dagster
- [ ] **CI/CD** — GitHub Actions para tests automáticos
- [ ] **Documento técnico** — Memoria del proyecto integrador
- [ ] **Airflow DAG** (opcional) — alternativa a Dagster
- [ ] **Estimación de escalado** (opcional) — Coste mensual en producción

---

## 👤 Autor

Proyecto integrador — Vinculación de Big Data, BI, Procesamiento Distribuido y ML.
