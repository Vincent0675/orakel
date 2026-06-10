# Orakel — Análisis Predictivo de Mazmorras WoW Mythic+

> **Proyecto Integrador — Big Data + BI + Procesamiento + ML + Orquestación**

## 📋 Descripción

Pipeline completo de ingeniería y analítica de datos sobre la temporada 3 de The War Within (WoW Mythic+). Integra datos de **Raider.IO** y **WarcraftLogs** mediante arquitectura Medallion (Bronce → Plata → Oro) con PySpark, almacenamiento en MinIO (S3-compatible), y visualización BI con Streamlit.

---

## 🏗️ Arquitectura del Pipeline

```
Raider.IO ──► Bronze ──► Silver ──► Match ──► WCL Events ──► Gold ──► Dashboard
   API         Parquet     Clean +     3-Layer      DamageTaken   4 KPIs    Streamlit
                          Dedup       Fuzzy Join   Healing       4 Dims
                                                    Interrupts
```

### Fases implementadas

| Fase | Estado | Descripción |
|------|--------|-------------|
| ① Fuente A (Raider.IO) | ✅ | 2.000 runs de Mythic+, temporada TWW S3 |
| ② Fuente B (WarcraftLogs) | ✅ | 286 reports con datos de combate vía GraphQL |
| ③ Ingesta | ✅ | Scripts con checkpointing incremental |
| ④ Limpieza | ✅ | Dedup por keystone_run_id, tipado forzado |
| ⑤ Integración | ✅ | Fuzzy join 3-capas (nivel+afijos, ventana temporal, solapamiento de roster) |
| ⑥ Procesamiento Spark | ✅ | PySpark: agregaciones, Window functions, UDFs |
| ⑦ Almacenamiento final | ✅ | Gold layer en MinIO (Parquet) |
| ⑧ Visualización BI | ✅ | Streamlit dashboard con 5 páginas, filtros, plotly |
| ⑨ ML / Analítica | ⬜ Pendiente | Regresión para clear_time_ms |

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

### KPIs calculados

| KPI | Fórmula | Datos reales |
|-----|---------|:------------:|
| **Death Clock** | EHP / (DTPS - HPS_on_tank) | 815 filas (55 crítico, 8 moderado, 752 safe) |
| **Healer Deficit** | Tank_DTPS / Healer_HPS_on_tank | 2.814 filas (56 crítico, 778 moderado, 1.980 confortable) |
| **Interrupt Rate** | Interrupts / minuto por jugador | 5.747 filas con interrupts > 0 |
| **Comp Synergy** | avg_clear_time(comp) / avg_clear_time(all) | 283 grupos con ≥ 2 muestras |

---

## 🛠️ Stack Tecnológico

| Componente | Tecnología |
|------------|-----------|
| **Lenguaje** | Python 3.13 |
| **Procesamiento** | PySpark 4.x (Hadoop S3A) |
| **Almacenamiento** | MinIO (S3-compatible) |
| **Ingesta API** | Raider.IO REST + WarcraftLogs GraphQL |
| **Dashboard** | Streamlit + Plotly Express |
| **Rate Limiting** | Token bucket (WCL: ~3.600 pts/hora) |
| **Checkpointing** | Checkpoint Parquet en MinIO por reporte/tanque |
| **Dependencias** | uv (100% Python) |

---

## 🚀 Cómo ejecutar

### Requisitos
- Docker (para MinIO)
- Python 3.13+
- uv

### 1. Iniciar MinIO
```bash
docker run -d --name orakel-minio \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=orakel \
  -e MINIO_ROOT_PASSWORD=orakel123 \
  minio/minio server /data --console-address ":9001"
```

### 2. Configurar entorno
```bash
cp .env.example .env  # Configurar WCL_CLIENT_ID, WCL_CLIENT_SECRET
uv sync
```

### 3. Ingesta de datos
```bash
# Raider.IO → Bronze → Silver
uv run python scripts/ingest_raiderio.py --season season-tww-3
uv run python scripts/bronze_to_silver.py --season season-tww-3

# Match WCL (con checkpointing)
uv run python scripts/match_reports.py --season season-tww-3 --limit-tanks 30

# Eventos WCL → Bronze (con checkpointing)
uv run python scripts/ingest_warcraftlogs.py --season season-tww-3 --limit 10

# Pipeline completo: Bronze → Silver → Gold
uv run python scripts/bronze_to_silver.py --season season-tww-3 --with-wcl
uv run python scripts/silver_to_gold.py --season season-tww-3
```

### 4. Dashboard
```bash
uv run streamlit run dashboard/app.py
# → http://localhost:8501
```

---

## 📁 Estructura del proyecto

```
orakel/
├── dashboard/
│   └── app.py                    # Streamlit BI dashboard
├── docs/
│   ├── images/                   # Visualizaciones exploratorias
│   └── ml-readiness-assessment.md
├── orakel/
│   ├── clients/
│   │   ├── raiderio.py           # Cliente API Raider.IO
│   │   └── warcraftlogs.py       # Cliente GraphQL WCL + OAuth2
│   ├── models/
│   │   ├── kpi.py                # Lógica de KPIs
│   │   └── schemas.py            # Schemas PySpark
│   ├── pipeline/
│   │   ├── bronze.py             # Ingesta a Bronze
│   │   ├── silver.py             # Limpieza + Fuzzy Join
│   │   └── gold.py               # KPIs + Dimensiones
│   ├── utils/
│   │   ├── minio.py              # SparkSession con S3A
│   │   └── rate_limiter.py       # Token bucket para WCL
│   └── config.py                 # Settings desde .env
├── scripts/
│   ├── ingest_raiderio.py        # Ingesta Raider.IO
│   ├── ingest_warcraftlogs.py    # Ingesta WCL (con checkpointing)
│   ├── match_reports.py          # Fuzzy join (con checkpointing)
│   ├── bronze_to_silver.py       # Bronze → Silver
│   └── silver_to_gold.py         # Silver → Gold
├── tests/                        # 71 tests (Tier 1 + Tier 2)
├── openspec/                     # Documentación SDD
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
uv run pytest tests/ -v
# 71 tests (52 Tier 1 sin Spark + 19 Tier 2 con SparkSession)
```

---

## 📝 Pendiente

- [ ] **ML**: Regresión para clear_time_ms con features de Gold KPIs
- [ ] **Diseño del pipeline**: Diagrama DAG con dependencias y tareas paralelas
- [ ] **Documento técnico**: Memoria del proyecto
- [ ] **Airflow** (opcional): DAG funcional con 3+ tareas
- [ ] **Estimación de escalado** (opcional): Coste mensual en producción

---

## 👤 Autor

Proyecto integrador — Vinculación de Big Data, BI, Procesamiento Distribuido y ML.
