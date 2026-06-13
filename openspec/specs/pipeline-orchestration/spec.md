# Pipeline Orchestration — Especificación

## Propósito

Orquestar el pipeline completo Bronze → Silver → Gold → ML mediante Dagster, con ejecución incremental, schedule nocturno, gates de calidad por capa, y tolerancia a fallos parciales de WCL.

## Requerimientos

| ID | Descripción | Keyword |
|----|-------------|---------|
| PO-1 | Definir software-defined assets en Dagster para: `bronze_rio`, `bronze_wcl`, `match_manifest`, `silver_runs`, `silver_performance`, `kpi_death_clock`, `kpi_healer_deficit`, `kpi_interrupt_rate`, `kpi_synergy`, `dim_dungeon`, `dim_player`, `dim_affix`, `dim_spec`, `ml_features`, `ml_model` | MUST |
| PO-2 | Assets ejecutan en orden de dependencia: bronze → silver → gold → ml_features → ml_model. Un asset fallido no bloquea siblings independientes | MUST |
| PO-3 | Schedule diario nocturno configurable (default 02:00). El usuario SHALL poder trigger manual desde Dagster UI | MUST |
| PO-4 | Reemplazar `mode("overwrite")` con merge por clave natural: `run_id` en silver, `(dungeon_id, key_level, affix_ids, comp_signature)` en synergy, `run_id` en demás KPIs | MUST |
| PO-5 | Cada asset silver y gold tiene AssetCheck: row count > 0, columnas críticas (`run_id`, `dungeon_id`) no nulas | MUST |
| PO-6 | AssetCheck SHALL verificar conformidad de esquema (columnas esperadas vs reales) por capa | SHOULD |
| PO-7 | Asset `check_minio_state` verifica que el bucket MinIO existe y es escribible ANTES de cualquier otro asset | MUST |
| PO-8 | Pipeline NO MUST fallar si WCL está incompleto. Si `bronze_wcl` o `match_manifest` faltan, silver produce datos Raider.IO-only | MUST |
| PO-9 | Si `check_minio_state` falla, ningún downstream asset se ejecuta. Error claro: "MinIO bucket {name} no accesible" | MUST |

## Escenarios

### Happy path — datos completos

- GIVEN Bronze Raider.IO y WCL existen en MinIO
- WHEN el schedule nocturno dispara el pipeline
- THEN los 15 assets se materializan en orden de dependencia
- AND todos los AssetChecks pasan
- AND Gold KPIs contienen métricas enrichadas con WCL

### Reprocesamiento incremental

- GIVEN el pipeline ya se ejecutó y hay nuevos datos en Bronze
- WHEN se ejecuta nuevamente
- THEN los registros existentes se mergean por clave natural
- AND no aparecen `run_id` duplicados en silver ni gold

### WCL faltante — pipeline degradado

- GIVEN Bronze WCL está vacío o el directorio no existe
- WHEN el pipeline se ejecuta
- THEN `silver_runs` tiene `match_method="rio_only"`
- AND KPIs 1-3 contienen NULLs en métricas de combate
- AND el pipeline no lanza excepción

### MinIO no disponible

- GIVEN MinIO no responde o credenciales inválidas
- WHEN `check_minio_state` se ejecuta
- THEN el asset falla con mensaje claro: "MinIO bucket {name} no accesible"
- AND ningún downstream se ejecuta

### Fallo en asset intermedio

- GIVEN silver está disponible pero `kpi_death_clock` falla por error de datos
- WHEN el pipeline se ejecuta
- THEN `kpi_death_clock` queda en estado `failed` en Dagster
- AND `kpi_synergy` (dependencia diferente) se ejecuta sin bloqueo
