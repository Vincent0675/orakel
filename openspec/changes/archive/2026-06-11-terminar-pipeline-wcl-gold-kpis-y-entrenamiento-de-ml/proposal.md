# Propuesta: Terminar Pipeline WCL → Gold KPIs y Entrenamiento de ML

## Intención

Completar el pipeline faltante — orquestación, procesamiento incremental, y entrenamiento ML — sobre el código existente de ingesta WCL y KPIs Gold. Hoy los scripts se ejecutan manualmente y no hay modelo predictivo.

## Alcance

### Incluido
- Orquestación Dagster (asset graph + schedule diario)
- Entrenamiento ML (Random Forest regresión, features desde Gold)
- Procesamiento incremental (merge en vez de overwrite)
- Hardening: dead-letter para fallos WCL, paginación de eventos, lookup tables desde Parquet
- Data quality gates post-capa
- Asset de verificación de MinIO

### Excluido
- Nuevas fuentes de datos
- Soporte multi-season
- Rediseño del dashboard Streamlit
- Airflow / Prefect (solo Dagster)

## Capacidades

### Nuevas
- `pipeline-orchestration`: Definitions Dagster con schedule diario + manual trigger
- `ml-training`: Feature engineering + entrenamiento + evaluación de modelo
- `data-quality`: Gates por capa (row count, nulos, schema)

### Modificadas
- Ninguna (el pipeline manual sigue funcionando sin cambios)

## Enfoque

**Orquestación**: Dagster con asset graph modelando cada capa: `bronze_rio`, `bronze_wcl`, `match_manifest` → `silver` → `gold_kpis` → `ml_features` → `ml_model`. Schedule diario nocturno. Cada asset reporta métricas de calidad via `AssetCheck`.

**Incremental**: Reemplazar `mode("overwrite")` por merge con `run_id` como key. Match manifest con checkpoint incremental — solo nuevos runs no procesados.

**ML**: Random Forest Regressor. Features: death_clock, healer_deficit, interrupt_rate, synergy_score, key_level, affixes. Target: clear_time_seconds. Evaluación: MAE, RMSE, R² con hold-out 80/20. Baseline: mean predictor. Serialización: MLflow local.

**Hardening**: WCL `get_events()` con paginación via `nextPageTimestamp`. Fallos WCL → dead-letter Parquet + asset continúa. Datos hardcodeados (timers, specs) → lookup tables en bronze.

**Riesgos**:
- Rate limit WCL (3600 pts/h): mitigado con batching nocturno + dead-letter
- MinIO sin datos: asset `check_minio_state` con error claro antes del pipeline
- Season lock-in: timers/maps como lookup Parquet, no hardcode

## Áreas Afectadas

| Área | Impacto | Cambio |
|------|---------|--------|
| `orakel/pipeline/orchestrator.py` | Nuevo | Asset definitions Dagster |
| `orakel/ml/` | Nuevo | Feature engineering + trainer |
| `orakel/pipeline/gold.py` | Modificado | Merge en vez de overwrite |
| `orakel/clients/warcraftlogs.py` | Modificado | Paginación eventos |
| `scripts/` | Modificado | Refactor a funciones reusables |
| `pyproject.toml` | Modificado | dagster, mlflow, sklearn |

## Riesgos

| Riesgo | Prob. | Mitigación |
|--------|-------|------------|
| WCL rate limit bloquea refrescos | Alta | Batching nocturno + dead-letter |
| MinIO sin datos | Media | Asset de verificación inicial |
| Dagster overhead setup | Baja | `dagster dev` local, sin deployment |

## Plan de Rollback

Por fase: revertir el PR individual. Los scripts originales en `scripts/` siguen funcionando sin cambios — no hay breaking change en pipeline manual.

## Dependencias

- `dagster`, `dagster-webserver` en pyproject.toml
- `scikit-learn`, `mlflow`, `pandas` para ML
- MinIO accesible desde Dagster (misma red/compose)

## Criterios de Éxito

- [ ] Dagster ejecuta pipeline completo: Bronze → Silver → Gold → ML sin errores
- [ ] Modelo ML produce MAE < 60s en hold-out set (vs ~120s baseline)
- [ ] Reprocesamiento incremental mergea sin duplicados ni pérdida
- [ ] Fallo WCL no detiene el pipeline (dead-letter + continúa)
- [ ] `dagster dev` levanta UI local en < 30s
