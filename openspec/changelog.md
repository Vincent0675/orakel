# Changelog — orakel

## PR #1 — Infraestructura + Bronze (2026-06-05)

### Archivos creados
- `pyproject.toml` — Dependencias: pyspark, minio, requests, python-dotenv, scikit-learn
- `docker-compose.yml` — MinIO (puertos 9000/9001)
- `.env` — Template de variables de entorno (gitignored)
- `.gitignore` — Excluye `.env`, `__pycache__`, `data/`, `jars/`
- `orakel/config.py` — Carga de config desde `.env` vía python-dotenv
- `orakel/utils/minio.py` — SparkSession factory con S3A apuntando a MinIO
- `orakel/models/schemas.py` — StructType definitions para todas las capas (Bronze/Silver/Gold)
- `orakel/clients/raiderio.py` — Cliente REST con paginación y backoff 429
- `scripts/ingest_raiderio.py` — Script CLI de ingesta a Bronze
- `scripts/setup_jars.sh` — Descarga JARs Hadoop AWS para S3A

### Fixes post-creación
- [`schemas.py`] **realm/region**: Cambiados de `StringType` a `StructType` anidado (realm_struct con 7 campos, region_struct con 3 campos). Zero cambios en ingesta — `run_to_row()` ya pasaba los dicts completos.
- [`raiderio.py` + `bronze.py` + `ingest_raiderio.py`] **score/rank**: El API devuelve `{rankings: [{rank, score, run{...}}]}`. El cliente descartaba rank/score al extraer el run. Fix: mergear rank/score dentro del run dict antes de retornar. Además, castear score a `float` para compatibilidad con schema `DoubleType`.

### Dependencias externas
- Java 17 instalado vía SDKMAN (requerido por PySpark 4.1.2)
- JARs Hadoop AWS (613MB) descargados a `jars/`
- Docker + MinIO corriendo

---

## PR #2 — Silver + KPI 4 (2026-06-07)

### Archivos creados
- `orakel/pipeline/__init__.py` + `bronze.py` — Pipeline de ingesta reusable
- `orakel/pipeline/silver.py` — SilverPipeline: dedup por `keystone_run_id` + flatten de structs anidados
- `orakel/pipeline/gold.py` — GoldPipeline: KPI 4 (Composition Synergy Score) + 4 dimension tables
- `orakel/models/kpi.py` — 4 funciones puras de KPIs: death_clock, healer_deficit, interrupt_rate, synergy_score
- `scripts/bronze_to_silver.py` — CLI: Bronze → Silver con `--season`
- `scripts/silver_to_gold.py` — CLI: Silver → Gold con verificación inline de KPIs

### Fixes post-creación
- [`gold.py`] **affix_ids sin ordenar**: `weekly_modifiers` podía venir como `[9, 10, 147]` o `[10, 9, 147]`. Sin `array_sort()`, Spark los trataba como grupos distintos, dividiendo artificialmente los sample_count y calculando overall_avg incorrecto. Fix: `F.array_sort(F.col("weekly_modifiers"))` en ambos groupBy.

### Verificaciones
- Silver: 100 rows, realm/region flatten a slug/short_name ✅
- Gold synergy: 72 rows, 20 con synergy_score válido (≥2 samples) ✅
- Synergy scores rango: 0.93 – 1.04 (realista) ✅
- NULL synergy para sample_count < 2: verificado ✅
- score/rank: ahora poblados (media 547.5, rango 545.9–550.5) ✅

### KPIs verificados
| Función | Input | Output esperado | Output real |
|---------|-------|----------------|-------------|
| `compute_death_clock(10000, 5000, 600000)` | DTPS=10k, HPS=5k | 120s safe | ✅ 120s safe |
| `compute_healer_deficit(15000, 10000)` | DTPS=15k, HPS=10k | 1.5 critical | ✅ 1.5 critical |
| `compute_interrupt_rate(0, 0)` | Sin intentos | None | ✅ None |
| `compute_synergy_score(1800000, 2000000)` | Comp 1.8M, avg 2.0M | 0.9 | ✅ 0.9 |

---

## PR #3 — WCL Integration + KPIs 1-3 (2026-06-07)

### Archivos creados
- `orakel/clients/warcraftlogs.py` — Cliente GraphQL con OAuth 2.0, point tracking, 7 métodos
- `orakel/utils/rate_limiter.py` — Token-bucket rate limiter (3600 pts/hr)
- `scripts/match_reports.py` — Motor fuzzy join 3 capas + muestreo estratificado

### Archivos modificados
- `orakel/pipeline/silver.py` — `apply_fuzzy_join()` con match manifest + WCL data
- `orakel/pipeline/gold.py` — KPIs 1-3 (Death Clock, Healer Deficit, Interrupt Rate)
- `orakel/models/schemas.py` — `player_name` agregado a Bronze WCL events
- `scripts/bronze_to_silver.py` — Flag `--with-wcl`
- `scripts/ingest_warcraftlogs.py` — Ingesta de eventos WCL

### Bugs resueltos
- [`warcraftlogs.py`] **recentReports anidado en `data {}`**: La API de WCL envuelve los reportes en un pagination wrapper. Fix: acceder a `reports_data["data"]`.
- [`warcraftlogs.py`] **Query complexity excedida (81K > 50K)**: Reducir limit a 20 por página y agregar paginación automática (hasta 5 páginas).
- [`warcraftlogs.py`] **events query sin sub-selección**: El tipo `ReportEventPaginator.data` es `JSON` y no acepta sub-campos. Fix: `{ data }` sin sub-fields.
- [`match_reports.py`] **encounterID ≠ challenge_mode_id**: WCL usa IDs de encuentro internos, distintos a los de Raider.IO. Fix: Layer 1 matchea por key_level + affixes sin encounterID.
- [`match_reports.py`] **Layer 3 nombres con sufijos numéricos**: Raider.IO a veces agrega IDs únicos (Name-123456). Fix: `re.sub(r'-\d+$', '', name)`.
- [`match_reports.py`] **Layer 3 realm matching**: WCL usaba formatos distintos para realms (slug vs display name). Fix: `_normalize_realm()` con 5 variantes.
- [`match_reports.py`] **Spark Row sin `.get()`**: Los `Row` de Spark no tienen método `.get()`. Fix: convertir a dict con `asDict()`.
- [`silver.py`] **Struct field ordering con `withField()`**: PySpark 4.x reordena los campos del struct al usar `withField`. Fix: leer datos planos desde Bronze con `F.expr()`.

### Bugs resueltos (segunda ronda)
- [`ingest_warcraftlogs.py`] **Interrupt Rate NULL**: Los eventos de interrupt se almacenaban sin `player_name`. Fix: obtener masterData del reporte (actor_id → player_name) y resolver sourceID antes de almacenar.
- [`ingest_warcraftlogs.py`] **Interrupt fight_id=0**: Al usar `fight_ids[0]` en vez de iterar por fight, todos los eventos tenían fight_id=0 y no se vinculaban. Fix: iterar `for fid in fight_ids`.
- [`silver.py`] **Interrupt join fallaba por capitalización**: Los nombres de jugadores de WCL no coincidían con los de Raider.IO por diferencias de mayúsculas. Fix: `F.lower()` en ambos lados del join.
