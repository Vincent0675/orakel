# Orakel — Visión del Proyecto

> Documento vivo. Refleja las decisiones de diseño tomadas hasta ahora.
> Última actualización: 2026-06-05

---

## 🎯 Propósito

Pipeline completo de datos para **predecir performance en calabozos Mythic+ de World of Warcraft**, usando datos de Raider.IO + WarcraftLogs. Proyecto integrador universitario que cubre: ingesta, limpieza, integración, procesamiento Spark, almacenamiento, ML y BI.

**Filosofía:** MVP que cumpla todas las fases obligatorias de la asignación, diseñado para escalar.

---

## 📊 KPIs (4 definidos)

| # | KPI | Fórmula | Fuente Principal |
|---|-----|---------|-----------------|
| 1 | 🛡️ **Tank Death Clock** | `EHP / (DTPS - HPS_on_tank)` | WarcraftLogs |
| 2 | 💚 **Healer Deficit** | `Tank_DTPS / Healer_HPS_on_tank` | WarcraftLogs |
| 3 | ⛔ **Interrupt Success Rate** | `Successful_Interrupts / Total_Interrupt_Casts` | WarcraftLogs |
| 4 | 🧩 **Composition Synergy Score** | `avg_clear_time(comp) / avg_clear_time(all_comps)` | Raider.IO |

### Interpretación rápida

| KPI | Bueno | Regular | Malo |
|-----|-------|---------|------|
| Tank Death Clock | > 15s | 5-15s | < 5s |
| Healer Deficit (ratio) | < 1.0 | 1.0-1.2 | > 1.2 |
| Interrupt Success Rate | > 90% | 70-90% | < 70% |
| Composition Synergy | < 0.90 | 0.90-1.10 | > 1.10 |

---

## 🏗️ Pipeline

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
        ├──▶ BI Dashboard (4 charts, filtros)
        └──▶ ML Model (predicción de timer)
```

### Fases del pipeline (obligatorias)

1. **Fuente A** — Raider.IO (REST API)
2. **Fuente B** — WarcraftLogs (GraphQL + OAuth 2.0)
3. **Ingesta** — Scripts Python → Bronze en MinIO
4. **Limpieza** — Silver: dedup, schema enforcement, type casting
5. **Integración** — Fuzzy join entre Raider.IO y WCL (por mazmorra + nivel + timestamp)
6. **Procesamiento Spark** — Transformaciones y agregaciones distribuidas
7. **Almacenamiento final** — Gold en MinIO S3 (Parquet particionado)
8. **Visualización BI** — Dashboard conectado a Gold
9. **ML** — Regresión para predicción de timer

---

## 🛠️ Stack Tecnológico

| Componente | Elección | Notas |
|-----------|----------|-------|
| Lenguaje | **Python 3.13** | |
| Gestor paquetes | **uv** | ⚠️ Exclusivo. No usar pip/pip3/poetry |
| Procesamiento | **PySpark 4.x** | Spark maneja TODO el Parquet. Sin PyArrow |
| Almacenamiento | **MinIO S3** | Arquitectura Medallion |
| ML | **scikit-learn** | Regresión + Random Forest |
| Orquestación | Pendiente (opcional) | Airflow como bonus |

### Arquitectura Medallion en MinIO

```
orakel/
├── bronze/      # Datos crudos tal cual llegan (JSON→Parquet)
├── silver/      # Limpios, deduplicados, unidos (clean+join)
├── gold/        # KPIs agregados + dimensiones + features
└── ml_models/   # Artefactos de modelos entrenados
```

Particionado por: `season={season}/year={yyyy}/month={mm}/`

---

## 🔗 Fuzzy Join (3 capas)

```
Capa 1: challenge_mode_id + keyLevel + affixes  (match exacto)
Capa 2: |completed_at - (report.start + fight.end)| ≤ 30s  (ventana temporal)
Capa 3: roster overlap ≥ 3/5 jugadores  (confirmación)
```

**Estrategia**: Match manifest (script Python → Parquet → Spark lo lee).  
**Muestreo**: Estratificado por clase del tank para evitar sesgo en ML.  
**Match rate esperado**: 85-95% con tank primary + guild fallback.

---

## 📦 Plan de Entrega: 4 PRs Stacked-to-Main

```
main ← PR #1: Infraestructura + Bronze Raider.IO    ~800 líneas
main ← PR #2: Silver + KPI 4 (Synergy Score)        ~600 líneas
main ← PR #3: WCL Integration + KPIs 1-3            ~1000 líneas
main ← PR #4: ML + BI + Orquestador                 ~600 líneas
```

Cada PR mergea a `main`. El siguiente se rebasea. Cada uno tiene checklist de verificación pre-merge.

---

## ⚠️ Riesgos Clave

| Riesgo | Severidad | Mitigación |
|--------|-----------|------------|
| **Fuzzy join** Raider.IO ↔ WCL (sin cross-reference key) | 🔴 Alto | 3 capas (IDs + timestamp + roster). Match manifest. |
| **WarcraftLogs OAuth 2.0** setup | 🟡 Medio | Client Credentials flow. Ya creadas las credenciales. |
| **Volumen de datos** de eventos de combate | 🟡 Medio | Usar WCL Tables (agregados) en vez de Events crudos. |
| **PySpark sdist** (sin wheel precompilado) | 🟢 Bajo | Java 11 ya instalado. Build lento la primera vez. |
| **Sesgo por clase de tank en ML** | 🟡 Medio | Muestreo estratificado por clase (6 clases, pesos inversos). |

---

## 📐 Decisiones de Diseño

- **MVP primero**, escalar después — no over-engineering
- Sin PyArrow — Spark maneja todo el I/O de Parquet
- Las transformaciones Spark DEBEN ser no-triviales (requisito de la asignación)
- El modelo ML DEBE tener sentido de negocio e interpretación (requisito)
- Airflow es bonus, no requisito
- Los KPIs pueden cambiar mientras haya mínimo 3
- `uv` exclusivo para gestión de paquetes
- IDs: `challenge_mode_id` (Raider.IO) = `encounterID` (WCL) — NO usar `dungeon.id`

---

## 📁 Documentos Relacionados

| Documento | Ruta |
|-----------|------|
| Visión del proyecto (este doc) | `.openspec/VISION.md` |
| Exploración APIs | `.openspec/changes/initial-architecture/exploration.md` |
| Propuesta formal | `.openspec/changes/initial-architecture/proposal.md` |
| Especificaciones detalladas | `.openspec/changes/initial-architecture/spec.md` |
| Diseño técnico | `.openspec/changes/initial-architecture/design.md` |
| Desglose de tareas | `.openspec/changes/initial-architecture/tasks.md` |
| **Plan de entrega** | **`.openspec/changes/initial-architecture/delivery-plan.md`** |
| Asignación proyecto (cátedra) | `Asignación de Proyecto Integrador.md` |
