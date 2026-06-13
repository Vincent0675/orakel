# Operational Hardening — Especificación

## Propósito

Hardenizar el pipeline contra fallos de WCL (dead-letter queue), soportar paginación completa de eventos, y reemplazar datos hardcodeados con lookup tables en Bronze para facilitar mantenimiento multi-season.

## Requerimientos

| ID | Descripción | Keyword |
|----|-------------|---------|
| OH-1 | Fallos de match WCL (rate limit, error GraphQL, timeout) se escriben a `silver/dead_letter/wcl_matches/` como Parquet | MUST |
| OH-2 | Fallos de ingesta de eventos WCL se escriben a `silver/dead_letter/wcl_events/` como Parquet | MUST |
| OH-3 | Formato DLQ: `entity_type`, `entity_key`, `error_type`, `error_message`, `occurred_at`, `payload_snapshot` (última request, truncada a 1KB), `retried`, `season` | MUST |
| OH-4 | `WarcraftLogsClient.get_events()` itera via `nextPageTimestamp` hasta que la página devuelva `nextPageTimestamp: null` | MUST |
| OH-5 | Datos hardcodeados reemplazados por lookup Parquet en `bronze/lookups/`: `dungeon_timers/`, `affixes/`, `spec_roles/` | MUST |
| OH-6 | Schemas de lookup: dungeon_timers (`dungeon_id`, `dungeon_name`, `slug`, `keystone_timer_ms`, `season`), affixes (`affix_id`, `affix_name`, `affix_description`, `season`), spec_roles (`class_id`, `class_name`, `spec_name`, `role`, `is_tank`, `is_healer`, `is_dps`) | MUST |
| OH-7 | Cuando una lookup table no existe, el pipeline SHALL usar los valores hardcodeados como fallback — sin crash | MUST |
| OH-8 | Pipeline logga un `WARNING` por cada registro en dead-letter, con conteo total al final: "DLQ: N registros en source={name}" | MUST |
| OH-9 | Lookup tables SHALL poblarse via script dedicado `scripts/seed_lookup_tables.py` que extrae de datos Bronze existentes + hardcode | SHOULD |

## Escenarios

### WCL rate limit — dead-letter registrado

- GIVEN WCL retorna HTTP 429 y el retry agota el budget de puntos
- WHEN `match_reports` o `ingest_warcraftlogs` fallan
- THEN el match/reporte fallido se escribe a `silver/dead_letter/wcl_matches/`
- AND el pipeline continúa con el siguiente tank/reporte sin abortar
- AND al final del pipeline se loggea "DLQ: 5 registros en source=wcl_matches"

### Paginación completa de interrupts

- GIVEN un reporte WCL tiene > 3000 eventos de interrupt
- WHEN `get_events(dataType="Interrupts")` se ejecuta
- THEN la primera página retorna `data` + `nextPageTimestamp`
- AND el cliente itera páginas hasta `nextPageTimestamp: null`
- AND todos los eventos de interrupt se persisten en Bronze

### Lookup faltante — fallback a hardcode

- GIVEN `bronze/lookups/dungeon_timers/` no existe
- WHEN GoldPipeline.build_dim_dungeon se ejecuta
- THEN usa los valores hardcodeados de `_TWW3_DUNGEONS` como fallback
- AND se loggea "Lookup table dungeon_timers no encontrada, usando fallback hardcodeado"
- AND la pipeline no falla

### Dead-letter consultable

- GIVEN hay registros en `silver/dead_letter/wcl_matches/`
- WHEN un operador ejecuta `spark.read.parquet("silver/dead_letter/wcl_matches/")`
- THEN obtiene un DataFrame con schema consistente
- AND puede filtrar por `error_type` o `occurred_at`
