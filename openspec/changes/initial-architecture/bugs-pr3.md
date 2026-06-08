# PR #3 — Bugs Pendientes

> Generado por sdd-verify el 2026-06-07
> Para resolver antes de comenzar PR #4 (ML + BI + Orquestador)

## 🔴 Críticos

### 1. Healer Deficit usa sanación total del healer, no la del tank
**Archivo**: `orakel/pipeline/silver.py:247-252`
**Problema**: `total_healing_received` se obtiene de la tabla `Healing` de WCL, que devuelve la sanación **hecha por** cada jugador, no la **recibida por**. Al unir por `player_name` del tank, se obtiene la auto-sanación del tank (si tiene), o 0. El deficit siempre es Infinity porque `healer_hps_on_tank` no captura la sanación del healer hacia el tank.

**Fix propuesto**: Usar eventos crudos de Healing con `targetID`, filtrar por `targetID == tank_actor_id` (obtenido de masterData). O aproximar: sumar sanación del healer donde target sea el tank.

### 2. Layer 2 timestamps: ¿relativo o absoluto?
**Archivo**: `scripts/match_reports.py:277`
**Problema**: Se asume que `fight.endTime` es relativo al `report.startTime`, pero WCL podría devolver timestamps absolutos. Si son absolutos, la suma `report.startTime + fight.endTime` duplica el offset y Layer 2 falla siempre.

**Fix propuesto**: Verificar con datos reales. Si WCL devuelve absolutos, cambiar a `wcl_absolute_time_ms = fight.endTime` sin sumar.

### 3. Interrupts: `interrupts_cast == interrupts_successful` siempre
**Archivo**: `orakel/pipeline/silver.py:270-272`
**Problema**: Ambas columnas se computan como `F.count("*")` sobre los mismos eventos de `Interrupts`. WCL solo devuelve eventos de interrupción exitosa, no intentos fallidos.

**Fix propuesto**: Renombrar a `interrupts_count` o aceptar que ISR = 100% siempre (y documentarlo). Alternativa: buscar datos de "enemy casts" para calcular cobertura.

### 4. Division by zero → NaN
**Archivo**: `orakel/models/kpi.py:89`
**Problema**: Si `tank_dtps = 0` y `healer_hps = 0`, la división `0/0 = NaN`.

**Fix propuesto**: Agregar guarda `if tank_dtps <= 0 or healer_hps <= 0: return (None, None)`.

### 5. Silver falla al re-ejecutarse
**Archivo**: `orakel/pipeline/silver.py:76-84`
**Problema**: `dropFields` + `withField` asume que `realm` sigue siendo struct. En segunda ejecución ya es string.

**Fix propuesto**: Verificar tipo con `p.getField("realm").getType().typeName()` antes de aplicar flattening.

### 6. dim_spec booleanos como strings
**Archivo**: `orakel/models/schemas.py:307-309`
**Problema**: `is_healer`, `is_tank`, `is_dps` definidos como `StringType` pero poblados con Python `True`/`False`.

**Fix propuesto**: Cambiar schema a `BooleanType()`.

## 🟡 Warnings

| # | Archivo | Problema |
|---|---------|----------|
| 7 | `warcraftlogs.py:303` vs `match_reports.py:545` | Inconsistencia en zonas aceptadas (43 vs 45/47) |
| 8 | `warcraftlogs.py:59` | `requests.Session()` nunca se cierra |
| 9 | `warcraftlogs.py:81` | Auth usa `requests.post()` no `self._session.post()` |
| 10 | `ingest_warcraftlogs.py:117` | Healing en columna `damage_amount` (confuso) |
| 11 | `match_reports.py:491` | `collect()` sin límite → OOM |
| 12 | `gold.py:692` | N+1 Spark actions con `.count()` |
| 13 | `match_reports.py:626` | `wcl_fight_id = 0` por defecto |
| 14 | `silver.py:153,376`, `gold.py:474,660,822` | `except Exception` muy amplio |
| 15 | `warcraftlogs.py:285`, `match_reports.py:308` | Doble stripping de sufijos numéricos |

## 🔵 Sugerencias

| # | Archivo | Sugerencia |
|---|---------|-----------|
| 16 | — | No hay tests — crear `tests/` con pytest para `kpi.py` |
| 17 | `gold.py` | IDs de zona/mazmorra hardcodeados — parametrizar |
| 18 | `match_reports.py:188` | Comentario de "50 por clase" engañoso |
| 19 | `gold.py:554` | EHP siempre 600k — debería depender de clase/spec |
| 20 | — | ISR solo 43/585 filas — mejorar con más datos |
| 21 | `silver.py:223-226` | `createDataFrame(RDD, schema)` silencia errores |

---

## Prioridad para próxima sesión

1. 🔴 Bug #1 — Healer Deficit (KPI 2 incorrecto)
2. 🔴 Bug #3 — Interrupts siempre 100% (KPI 3 incorrecto)  
3. 🔴 Bug #2 — Verificar timestamps Layer 2
4. 🔴 Bug #4 — Division by zero
5. 🟡 Bugs menores
6. PR #4 — ML + BI + Orquestador
