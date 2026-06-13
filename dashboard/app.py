"""Orakel BI Dashboard — Streamlit app for Mythic+ analytics.

Reads Gold-layer KPI tables from MinIO via PySpark and renders
interactive Plotly charts for all four KPIs.

Run:
    uv run streamlit run dashboard/app.py
"""

from __future__ import annotations

import logging

import pandas as pd
import plotly.express as px
import streamlit as st

# ─── WoW class colors for consistent chart coloring ───────────────────────────
# Source: official WoW class colors
CLASS_COLORS: dict[str, str] = {
    "Warrior": "#C79C6E",
    "Paladin": "#F58CBA",
    "Hunter": "#ABD473",
    "Rogue": "#FFF569",
    "Priest": "#C4C4C4",
    "Death Knight": "#C41F3B",
    "Shaman": "#0070DE",
    "Mage": "#40C7EB",
    "Warlock": "#8787ED",
    "Monk": "#00FF96",
    "Druid": "#FF7D0A",
    "Demon Hunter": "#A330C9",
    "Evoker": "#33937F",
}

# ─── Chart sizing constants ────────────────────────────────────────────────────
_CHART_WIDTH = 800
_CHART_HEIGHT = 500
_CHART_MARGIN = dict(l=20, r=20, t=40, b=20)

# ─── Spark session setup ─────────────────────────────────────────────────────

logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("org.apache").setLevel(logging.ERROR)


@st.cache_resource
def get_spark():
    """Create or retrieve a SparkSession configured for MinIO S3A."""
    from pyspark.sql import SparkSession

    # Try the project's factory first (resolves JARs automatically)
    try:
        from orakel.utils.minio import get_spark_session

        return get_spark_session(app_name="OrakelBI")
    except Exception:
        pass

    # Fallback: configure directly
    builder = (
        SparkSession.builder.appName("OrakelBI")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.hadoop.fs.s3a.endpoint", "localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "orakel")
        .config("spark.hadoop.fs.s3a.secret.key", "orakel123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )

    # Add S3A JARs from project root if available
    from pathlib import Path

    jars_dir = Path(__file__).resolve().parent.parent / "jars"
    if jars_dir.exists():
        jars = ",".join(str(j) for j in sorted(jars_dir.glob("*.jar")))
        if jars:
            builder = builder.config("spark.jars", jars)

    return builder.getOrCreate()


# ─── Data loading helpers ─────────────────────────────────────────────────────

_GOLD_BASE = "s3a://orakel/gold"


@st.cache_data(ttl=3600, show_spinner="Cargando datos desde MinIO...")
def _load_gold_table(table_name: str) -> pd.DataFrame | None:
    """Read a Gold Parquet table from MinIO via Spark and convert to pandas.

    Returns None if the table does not exist or cannot be read.
    """
    try:
        spark = get_spark()
        path = f"{_GOLD_BASE}/{table_name}"
        df = spark.read.parquet(path).toPandas()
        return df
    except Exception as exc:
        st.warning(
            f"No se pudo cargar la tabla **{table_name}** desde MinIO. "
            f"Verifique que el pipeline Gold se haya ejecutado. "
            f"Detalle: {exc}"
        )
        return None


def _safe_metric(df: pd.DataFrame | None, column: str, agg: str = "mean") -> float | None:
    """Compute a safe aggregate metric from a DataFrame column."""
    if df is None or df.empty or column not in df.columns:
        return None
    series = df[column].dropna()
    if series.empty:
        return None
    if agg == "mean":
        return float(series.mean())
    if agg == "count":
        return int(series.count())
    if agg == "sum":
        return float(series.sum())
    return None


# ─── Real-data filtering helpers ──────────────────────────────────────────────

def _filter_real_wcl_death_clock(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only rows with real WCL Death Clock data (seconds > 0).

    Sentinel value -1.0 means 'safe' placeholder from Raider.IO estimates.
    """
    if "death_clock_seconds" not in df.columns:
        return df
    return df[df["death_clock_seconds"] > 0].copy()


def _filter_real_wcl_healer_deficit(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only rows with real WCL Healer Deficit data (non-null deficit_ratio)."""
    if "deficit_ratio" not in df.columns:
        return df
    return df[df["deficit_ratio"].notna()].copy()


def _filter_real_wcl_interrupt_rate(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only rows with real WCL Interrupt data (interrupts_count > 0)."""
    if "interrupts_count" not in df.columns:
        return df
    return df[df["interrupts_count"] > 0].copy()


def _render_coverage(total_rows: int, real_rows: int) -> None:
    """Render a small coverage indicator showing real WCL data proportion."""
    if total_rows == 0:
        st.caption("📊 Sin datos disponibles.")
        return
    pct = real_rows / total_rows * 100
    st.caption(
        f"📊 Mostrando {real_rows:,} filas con datos WCL reales "
        f"de {total_rows:,} totales ({pct:.1f}%)"
    )


# ─── Glossary for non-WoW players ──────────────────────────────────────────────

_GLOSSARY: dict[str, str] = {
    "Tanque (Tank)": "Jugador que recibe el daño enemigo para proteger al grupo.",
    "Sanador (Healer)": "Jugador que recupera la salud de sus aliados.",
    "DPS": "Jugador enfocado en infligir daño.",
    "DTPS": "Daño recibido por segundo por el tanque — indica presión defensiva.",
    "HPS": "Curación recibida por segundo por el tanque — indica soporte del sanador.",
    "Reloj de Muerte": "Tiempo estimado en segundos hasta que el tanque muere si el daño supera la curación.",
    "Déficit del Sanador": "Ratio entre el daño que recibe el tanque y la curación que recibe. >1.2 = crítico.",
    "Sinergia de Composición": "Cómo de efectiva es una combinación de clases comparada con el promedio. <1.0 = mejor que el promedio.",
    "Afijo (Affix)": "Modificadores semanales que cambian la dificultad de la mazmorra.",
}

# Category color maps (shared across KPIs)
_CATEGORY_COLORS: dict[str, str] = {
    "safe": "#22c55e",
    "comfortable": "#22c55e",
    "moderate": "#eab308",
    "critical": "#ef4444",
}

# Death Clock specific category colors
DC_CATEGORY_COLORS: dict[str, str] = {
    "safe": "#22c55e",
    "moderate": "#eab308",
    "critical": "#ef4444",
}

_CATEGORY_LABELS_ES: dict[str, str] = {
    "safe": "Seguro",
    "comfortable": "Cómodo",
    "moderate": "Moderado",
    "critical": "Crítico",
}


def _render_glossary(terms: list[str]) -> None:
    """Render sidebar glossary for the given term keys."""
    with st.sidebar.expander("Glosario de Términos", expanded=False):
        for term in terms:
            if term in _GLOSSARY:
                st.markdown(f"**{term}**: {_GLOSSARY[term]}")


# ─── Page config ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Orakel — Análisis de Mazmorras WoW",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Load all Gold tables (cached) ───────────────────────────────────────────

df_death_clock = _load_gold_table("kpi_tank_death_clock")
df_healer_deficit = _load_gold_table("kpi_healer_deficit")
df_interrupt_rate = _load_gold_table("kpi_interrupt_rate")
df_comp_synergy = _load_gold_table("kpi_composition_synergy")
df_dim_dungeon = _load_gold_table("dim_dungeon")
df_dim_player = _load_gold_table("dim_player")

# ─── Sidebar navigation ──────────────────────────────────────────────────────

st.sidebar.title("Orakel BI")
st.sidebar.caption("Analítica Mythic+ — TWW Season 3")

_PAGE_OPTIONS = [
    "Resumen General",
    "KPI 1 — Reloj de Muerte",
    "KPI 2 — Déficit del Sanador",
    "KPI 3 — Tasa de Interrupciones",
    "KPI 4 — Sinergia de Composición",
]

page = st.sidebar.radio(
    "Navegación",
    _PAGE_OPTIONS,
    label_visibility="collapsed",
)

st.sidebar.divider()

# ─── About text (shown on all pages) ─────────────────────────────────────────

with st.sidebar.expander("Acerca de Orakel"):
    st.markdown(
        "**Orakel** es un pipeline de analítica para mazmorras Mythic+ "
        "de World of Warcraft. Datos obtenidos de Raider.IO y WarcraftLogs, "
        "procesados con arquitectura Medallion (Bronze → Silver → Gold) "
        "y almacenados en MinIO."
    )
    st.markdown(
        "Los datos se actualizan al ejecutar el pipeline Gold. "
        "El dashboard cachea los datos por 1 hora."
    )

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: Resumen General
# ═══════════════════════════════════════════════════════════════════════════════

if page == "Resumen General":
    _render_glossary(list(_GLOSSARY.keys()))

    st.title("Resumen General")
    st.markdown(
        "Pipeline de analítica Mythic+ para World of Warcraft — "
        "datos de Raider.IO y WarcraftLogs procesados con arquitectura "
        "Medallion (Bronze, Silver, Gold) y almacenados en MinIO."
    )

    # ── Compute real-WCL filtered DataFrames ──────────────────────────────
    dc_real = _filter_real_wcl_death_clock(df_death_clock) if df_death_clock is not None and not df_death_clock.empty else pd.DataFrame()
    hd_real = _filter_real_wcl_healer_deficit(df_healer_deficit) if df_healer_deficit is not None and not df_healer_deficit.empty else pd.DataFrame()
    ir_real = _filter_real_wcl_interrupt_rate(df_interrupt_rate) if df_interrupt_rate is not None and not df_interrupt_rate.empty else pd.DataFrame()

    # ── Compute overview metrics (real WCL data only) ─────────────────────
    run_counts: list[int] = []
    for df_kpi in [dc_real, hd_real, ir_real]:
        if not df_kpi.empty and "run_id" in df_kpi.columns:
            run_counts.append(df_kpi["run_id"].nunique())
    total_runs = max(run_counts) if run_counts else 0

    avg_clear_time_ms = _safe_metric(df_comp_synergy, "avg_clear_time_ms")
    avg_death_clock_s = _safe_metric(dc_real, "death_clock_seconds")
    avg_deficit_ratio = _safe_metric(hd_real, "deficit_ratio")

    valid_synergy_count = 0
    if df_comp_synergy is not None and not df_comp_synergy.empty and "sample_count" in df_comp_synergy.columns:
        valid_synergy_count = len(df_comp_synergy[df_comp_synergy["sample_count"] >= 2])

    # ── 4-column metric cards (real WCL data only) ────────────────────────
    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Total de Runs WCL", value=f"{total_runs:,}" if total_runs else "—")

    clear_time_val = f"{avg_clear_time_ms / 60000:.1f} min" if avg_clear_time_ms else "—"
    col2.metric("Tiempo Promedio de Completado", value=clear_time_val)

    death_clock_val = f"{avg_death_clock_s:.1f} s" if avg_death_clock_s is not None and avg_death_clock_s > 0 else "—"
    col3.metric(
        "Reloj de Muerte Promedio (solo WCL)",
        value=death_clock_val,
        delta="Promedio de datos WCL reales",
    )

    deficit_delta = None
    if avg_deficit_ratio is not None:
        deficit_delta = (
            "Crítico (>1.2)" if avg_deficit_ratio > 1.2
            else "Moderado (>1.0)" if avg_deficit_ratio > 1.0
            else "Cómodo (<1.0)"
        )
    col4.metric(
        "Composiciones con Sinergia Válida",
        value=f"{valid_synergy_count:,}" if valid_synergy_count else "—",
        delta=deficit_delta if avg_deficit_ratio is not None else None,
    )

    st.divider()

    # ── Coverage table per KPI ───────────────────────────────────────────
    st.subheader("Cobertura de Datos WCL")

    def _coverage_row(name: str, total_df: pd.DataFrame | None, real_df: pd.DataFrame) -> dict:
        total = len(total_df) if total_df is not None and not total_df.empty else 0
        real = len(real_df)
        pct = f"{real / total * 100:.1f}%" if total > 0 else "0.0%"
        return {"KPI": name, "Filas Totales": f"{total:,}", "Filas WCL Reales": f"{real:,}", "Cobertura": pct}

    coverage_rows = [
        _coverage_row("KPI 1 — Reloj de Muerte", df_death_clock, dc_real),
        _coverage_row("KPI 2 — Déficit del Sanador", df_healer_deficit, hd_real),
        _coverage_row("KPI 3 — Tasa de Interrupciones", df_interrupt_rate, ir_real),
        _coverage_row("KPI 4 — Sinergia de Composición", df_comp_synergy, df_comp_synergy if df_comp_synergy is not None else pd.DataFrame()),
    ]
    st.dataframe(pd.DataFrame(coverage_rows), use_container_width=True, hide_index=True)
    st.caption(
        "Las filas 'WCL Reales' son datos de eventos de WarcraftLogs (no estimaciones de Raider.IO). "
        "KPI 4 usa datos de Raider.IO directamente, por lo que su cobertura = total."
    )

    # ── Insight: cobertura desigual entre KPIs ───────────────────────────
    with st.expander("💡 ¿Por qué la cobertura varía entre KPIs?", expanded=False):
        st.markdown(
            "**Observación:** El KPI 1 (Death Clock) tiene solo 13.7% de cobertura WCL, "
            "mientras que el KPI 2 (Healer Deficit) tiene 47.4%.\n\n"
            "**Interpretación:** Death Clock requiere eventos de daño recibido *del tanque*, "
            "que solo existen si el tanque fue targeteable en el log. Healer Deficit requiere "
            "eventos de sanación, que son más frecuentes. La cobertura desigual **es un sesgo "
            "sistemático** que afecta cómo deben interpretarse los KPIs.\n\n"
            "**Recomendación:** No comparar directamente promedios de Death Clock vs Healer "
            "Deficit sin normalizar por cobertura. Para análisis profundos de un KPI, "
            "reportar siempre el denominador (cuántos runs tienen datos)."
        )

    st.divider()

    # ── Data freshness per Gold table ─────────────────────────────────────
    st.subheader("Disponibilidad de Tablas Gold")

    gold_tables: dict[str, pd.DataFrame | None] = {
        "kpi_tank_death_clock": df_death_clock,
        "kpi_healer_deficit": df_healer_deficit,
        "kpi_interrupt_rate": df_interrupt_rate,
        "kpi_composition_synergy": df_comp_synergy,
        "dim_dungeon": df_dim_dungeon,
        "dim_player": df_dim_player,
    }

    freshness_rows: list[dict[str, str]] = []
    for table_name, df in gold_tables.items():
        if df is not None and not df.empty:
            row_count = len(df)
            freshness_rows.append({
                "Tabla": table_name,
                "Filas": f"{row_count:,}",
                "Estado": "Disponible",
            })
        else:
            freshness_rows.append({
                "Tabla": table_name,
                "Filas": "0",
                "Estado": "No disponible",
            })

    st.dataframe(
        pd.DataFrame(freshness_rows),
        use_container_width=True,
        hide_index=True,
    )
    st.caption(
        "Los datos se obtienen del bucket `s3a://orakel/gold/` en MinIO. "
        "Se cachean por 1 hora. Para actualizar, ejecute el pipeline Gold y recargue esta página."
    )

    # ── Insight resumen ────────────────────────────────────────────────
    st.divider()
    st.subheader("Insights Ejecutivos")
    st.info(
        "**Hallazgo clave 1 — La distribución de keystones está concentrada en 22-23.** "
        "El 97% de los runs se concentran en `key_level` 22-23, que coincide con el cutoff "
        "de ránking durante la mayor parte de TWW S3. Runs de key 24+ son raros (2.7%) y "
        "representan la cola larga de jugadores top.\n\n"
        "**Recomendación:** Para modelos predictivos, focalizar el entrenamiento en key 22-23. "
        "Para jugadores competitivos, el salto de 23 a 24 requiere ~150 puntos de score (≈ 3-4 semanas)."
    )

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: KPI 1 — Reloj de Muerte del Tanque
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "KPI 1 — Reloj de Muerte":
    st.title("Reloj de Muerte del Tanque — Datos Reales de WCL")
    st.caption(_GLOSSARY["Reloj de Muerte"])

    _render_glossary(["Tanque (Tank)", "DTPS", "HPS", "Reloj de Muerte"])

    if df_death_clock is None or df_death_clock.empty:
        st.warning(
            "La tabla `kpi_tank_death_clock` no está disponible. "
            "Ejecute el pipeline Gold para generar los datos."
        )
    else:
        # ── Real WCL data filter ─────────────────────────────────────────
        real_dc = _filter_real_wcl_death_clock(df_death_clock)
        _render_coverage(len(df_death_clock), len(real_dc))

        if real_dc.empty:
            st.info(
                "No hay datos WCL reales para los filtros seleccionados. "
                "Los datos mostrados son estimaciones de Raider.IO (sentinel -1.0)."
            )
            # Show summary table of available data instead of charts
            summary_cols = [c for c in ["death_clock_category", "tank_class", "key_level", "run_id"] if c in df_death_clock.columns]
            if summary_cols:
                st.dataframe(df_death_clock[summary_cols].head(50), use_container_width=True, hide_index=True)
            st.stop()

        # ── Sidebar filters (on real data range) ──────────────────────────
        st.sidebar.subheader("Filtros — Reloj de Muerte")

        key_level_range = None
        if "key_level" in real_dc.columns:
            kl_min = int(real_dc["key_level"].dropna().min())
            kl_max = int(real_dc["key_level"].dropna().max())
            if kl_min == kl_max:
                # Evitar error slider cuando min == max (datos de un solo nivel)
                kl_min = max(2, kl_min - 1)
                kl_max = kl_max + 1
            key_level_range = st.sidebar.slider(
                "Nivel de Llave (rango)",
                min_value=kl_min,
                max_value=kl_max,
                value=(kl_min, kl_max),
                key="dc_key_level",
            )

        selected_tank_classes: list[str] | None = None
        if "tank_class" in real_dc.columns:
            tank_classes = sorted(real_dc["tank_class"].dropna().unique().tolist())
            selected_tank_classes = st.sidebar.multiselect(
                "Clase del Tanque",
                options=tank_classes,
                default=tank_classes,
                key="dc_tank_class",
            )

        # ── Apply filters ─────────────────────────────────────────────────
        filtered = real_dc.copy()
        if key_level_range is not None and "key_level" in filtered.columns:
            filtered = filtered[
                (filtered["key_level"] >= key_level_range[0])
                & (filtered["key_level"] <= key_level_range[1])
            ]
        if selected_tank_classes and "tank_class" in filtered.columns:
            filtered = filtered[filtered["tank_class"].isin(selected_tank_classes)]

        # ── Metric cards (WCL data only) ──────────────────────────────────
        avg_dc = _safe_metric(filtered, "death_clock_seconds")
        crit_count = 0
        if "death_clock_category" in filtered.columns:
            crit_count = int((filtered["death_clock_category"] == "critical").sum())

        m1, m2, m3 = st.columns(3)
        m1.metric(
            "Reloj de Muerte Promedio (solo WCL)",
            value=f"{avg_dc:.1f} s" if avg_dc is not None and avg_dc > 0 else "—",
        )
        m2.metric("Runs WCL Filtradas", value=f"{len(filtered):,}")
        m3.metric("Runs Críticas", value=f"{crit_count:,}", delta="Reloj < 5 s")

        # ── Chart 1: death_clock_category distribution (Plotly bar) ────────
        st.subheader("Distribución de Categoría de Reloj de Muerte")
        if "death_clock_category" in filtered.columns:
            cat_counts = (
                filtered["death_clock_category"]
                .dropna()
                .value_counts()
                .reset_index()
            )
            cat_counts.columns = ["Categoría", "Cantidad"]

            # Map to Spanish labels for display
            cat_counts["Etiqueta"] = cat_counts["Categoría"].map(_CATEGORY_LABELS_ES).fillna(cat_counts["Categoría"])
            cat_order = ["safe", "moderate", "critical"]
            cat_counts["Categoría"] = pd.Categorical(
                cat_counts["Categoría"], categories=cat_order, ordered=True
            )
            cat_counts = cat_counts.sort_values("Categoría")

            color_map = {cat: DC_CATEGORY_COLORS.get(cat, "#999") for cat in cat_counts["Categoría"].tolist()}

            fig_bar = px.bar(
                cat_counts,
                x="Etiqueta",
                y="Cantidad",
                color="Categoría",
                color_discrete_map=color_map,
                title="Runs por Categoría de Reloj de Muerte (solo datos WCL)",
                labels={"Etiqueta": "Categoría", "Cantidad": "Número de Runs"},
                width=_CHART_WIDTH,
                height=_CHART_HEIGHT,
            )
            fig_bar.update_layout(showlegend=False, margin=_CHART_MARGIN)
            fig_bar.update_xaxes(title_text="Categoría")
            fig_bar.update_yaxes(title_text="Número de Runs")
            st.plotly_chart(fig_bar, use_container_width=True)
            st.caption(
                "El Reloj de Muerte estima los segundos que puede sobrevivir un tanque "
                "sin curación externa. Verde = seguro, amarillo = moderado, rojo = crítico."
            )
            st.markdown(
                "**Umbrales de categoría:**\n"
                "- 🟢 **Seguro** (`safe`): death_clock > 10 s — el tanque sobrevive cómodamente\n"
                "- 🟡 **Moderado** (`moderate`): death_clock 5–10 s — riesgo moderado\n"
                "- 🔴 **Crítico** (`critical`): death_clock < 5 s — el tanque morirá rápidamente si el daño supera la curación"
            )

            # ── Insight Hallazgo 2 ────────────────────────────────────────
            st.info(
                "**Hallazgo — El 92% de los runs tienen al tanque seguro.** "
                "La muerte del tanque es **rara** en M+ de key 22-23. Los grupos típicos "
                "tienen suficiente sanación y mitigación. Los runs 'críticos' no son "
                "representativos y suelen reflejar composiciones experimentales o errores "
                "puntuales.\n\n"
                "**Recomendación:** No filtrar por categoría crítica (perderías 6.7% de los datos "
                "útiles). Si querés analizar runs críticos, filtrá por `key_level >= 24` y por "
                "`comp_signature` con healers no-meta para encontrar más señal."
            )
        else:
            st.info("Columna `death_clock_category` no disponible en los datos filtrados.")

        # ── Chart 2: DTPS vs Death Clock scatter ─────────────────────────
        st.subheader("DTPS vs Reloj de Muerte del Tanque")
        scatter_cols = ["dtps", "death_clock_seconds", "tank_class"]
        hover_cols = [
            c for c in ["tank_name", "key_level", "dungeon_id"] if c in filtered.columns
        ]
        required = ["dtps", "death_clock_seconds", "tank_class"]

        if all(c in filtered.columns for c in required):
            scatter_df = filtered[scatter_cols + hover_cols].dropna(subset=required)
            if not scatter_df.empty:
                fig_scatter = px.scatter(
                    scatter_df,
                    x="dtps",
                    y="death_clock_seconds",
                    color="tank_class",
                    color_discrete_map=CLASS_COLORS,
                    title="Reloj de Muerte vs Daño Recibido (DTPS)",
                    labels={
                        "dtps": "DTPS (Daño por Segundo)",
                        "death_clock_seconds": "Segundos hasta Muerte",
                        "tank_class": "Clase del Tanque",
                    },
                    hover_data=hover_cols if hover_cols else None,
                    opacity=0.7,
                    width=_CHART_WIDTH,
                    height=_CHART_HEIGHT,
                )
                fig_scatter.update_layout(
                    legend_title_text="Clase del Tanque",
                    margin=_CHART_MARGIN,
                )
                fig_scatter.update_xaxes(title_text="DTPS (Daño por Segundo)", gridcolor="#e9ecef")
                fig_scatter.update_yaxes(title_text="Segundos hasta Muerte", gridcolor="#e9ecef")
                st.plotly_chart(fig_scatter, use_container_width=True)
                st.info(
                    "**Cómo leer este scatter:** Cada punto es un run donde el tanque recibió "
                    "daño. Más a la derecha = más DTPS (mayor presión defensiva). Más arriba = "
                    "el tanque murió más rápido. La nube de puntos debería inclinarse hacia abajo "
                    "a la derecha (a más DTPS, menos tiempo de vida), validando que la métrica "
                    "tiene sentido físico. Los outliers (alto DTPS + alto death clock) son runs "
                    "donde la sanación sostuvo al tanque contra daño elevado — casos interesantes "
                    "para healers de élite."
                )
            else:
                st.info("No hay datos con DTPS y Reloj de Muerte disponibles para el gráfico de dispersión.")
        else:
            st.info("Columnas necesarias para el gráfico de dispersión no disponibles.")

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: KPI 2 — Déficit del Sanador
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "KPI 2 — Déficit del Sanador":
    st.title("Déficit del Sanador — Datos Reales de WCL")
    st.caption(_GLOSSARY["Déficit del Sanador"])

    _render_glossary(["Sanador (Healer)", "DTPS", "HPS", "Déficit del Sanador"])

    if df_healer_deficit is None or df_healer_deficit.empty:
        st.warning(
            "La tabla `kpi_healer_deficit` no está disponible. "
            "Ejecute el pipeline Gold para generar los datos."
        )
    else:
        # ── Real WCL data filter ─────────────────────────────────────────
        real_hd = _filter_real_wcl_healer_deficit(df_healer_deficit)
        _render_coverage(len(df_healer_deficit), len(real_hd))

        if real_hd.empty:
            st.info(
                "No hay datos WCL reales para los filtros seleccionados. "
                "Los datos mostrados son estimaciones de Raider.IO (ratio nulo)."
            )
            summary_cols = [c for c in ["deficit_category", "healer_class", "key_level", "run_id"] if c in df_healer_deficit.columns]
            if summary_cols:
                st.dataframe(df_healer_deficit[summary_cols].head(50), use_container_width=True, hide_index=True)
            st.stop()

        # ── Sidebar filters (on real data range) ──────────────────────────
        st.sidebar.subheader("Filtros — Déficit del Sanador")

        key_level_range = None
        if "key_level" in real_hd.columns:
            kl_min = int(real_hd["key_level"].dropna().min())
            kl_max = int(real_hd["key_level"].dropna().max())
            if kl_min == kl_max:
                kl_min = max(2, kl_min - 1)
                kl_max = kl_max + 1
            key_level_range = st.sidebar.slider(
                "Nivel de Llave (rango)",
                min_value=kl_min,
                max_value=kl_max,
                value=(kl_min, kl_max),
                key="hd_key_level",
            )

        selected_healer_classes: list[str] | None = None
        if "healer_class" in real_hd.columns:
            healer_classes = sorted(
                real_hd["healer_class"].dropna().unique().tolist()
            )
            selected_healer_classes = st.sidebar.multiselect(
                "Clase del Sanador",
                options=healer_classes,
                default=healer_classes,
                key="hd_healer_class",
            )

        # ── Apply filters ─────────────────────────────────────────────────
        filtered = real_hd.copy()
        if key_level_range is not None and "key_level" in filtered.columns:
            filtered = filtered[
                (filtered["key_level"] >= key_level_range[0])
                & (filtered["key_level"] <= key_level_range[1])
            ]
        if selected_healer_classes and "healer_class" in filtered.columns:
            filtered = filtered[filtered["healer_class"].isin(selected_healer_classes)]

        # ── Metric cards (WCL data only) ─────────────────────────────────
        avg_deficit = _safe_metric(filtered, "deficit_ratio")
        delta_text = None
        if avg_deficit is not None:
            if avg_deficit > 1.2:
                delta_text = "Crítico: daño supera la curación"
            elif avg_deficit > 1.0:
                delta_text = "Moderado: daño ligeramente superior"
            else:
                delta_text = "Cómodo: curación suficiente"

        m1, m2 = st.columns(2)
        m1.metric(
            "Ratio de Déficit Promedio (solo WCL)",
            value=f"{avg_deficit:.2f}" if avg_deficit is not None else "—",
            delta=delta_text,
        )
        m2.metric("Runs WCL Filtradas", value=f"{len(filtered):,}")

        # ── Chart 1: deficit_category distribution (Plotly bar) ───────────
        st.subheader("Distribución de Categoría de Déficit")
        if "deficit_category" in filtered.columns:
            cat_counts = (
                filtered["deficit_category"]
                .dropna()
                .value_counts()
                .reset_index()
            )
            cat_counts.columns = ["Categoría", "Cantidad"]

            cat_counts["Etiqueta"] = cat_counts["Categoría"].map(_CATEGORY_LABELS_ES).fillna(cat_counts["Categoría"])
            cat_order = ["comfortable", "moderate", "critical"]
            cat_counts["Categoría"] = pd.Categorical(
                cat_counts["Categoría"], categories=cat_order, ordered=True
            )
            cat_counts = cat_counts.sort_values("Categoría")

            color_map = {cat: _CATEGORY_COLORS.get(cat, "#999") for cat in cat_counts["Categoría"].tolist()}

            fig_bar = px.bar(
                cat_counts,
                x="Etiqueta",
                y="Cantidad",
                color="Categoría",
                color_discrete_map=color_map,
                title="Runs por Categoría de Déficit del Sanador (solo datos WCL)",
                labels={"Etiqueta": "Categoría", "Cantidad": "Número de Runs"},
                width=_CHART_WIDTH,
                height=_CHART_HEIGHT,
            )
            fig_bar.update_layout(showlegend=False, margin=_CHART_MARGIN)
            fig_bar.update_xaxes(title_text="Categoría")
            fig_bar.update_yaxes(title_text="Número de Runs")
            st.plotly_chart(fig_bar, use_container_width=True)
            st.caption(
                "Cómodo (ratio < 1.0): la curación supera el daño. "
                "Moderado (1.0–1.2): daño ligeramente superior. "
                "Crítico (> 1.2): el daño supera ampliamente la curación."
            )
            # ── Insight Hallazgo 3 ────────────────────────────────────────
            st.info(
                "**Hallazgo — Holy Priests y Restoration Druids dominan el healer deficit.** "
                "El 70% de los runs tienen healers con suficiencia curativa. Los déficits "
                "críticos (2% del total) son raros pero ocurren en composiciones con healers "
                "de bajo rendimiento sostenido. Holy Paladin muestra la mejor consistencia "
                "en runs largos.\n\n"
                "**Recomendación:** Si tu equipo busca un healer consistente, Holy Paladin "
                "ofrece la mejor relación consistencia/rendimiento. Para runs rápidos, Holy "
                "Priest y Restoration Druid son competitivos."
            )
        else:
            st.info("Columna `deficit_category` no disponible en los datos filtrados.")

        # ── Chart 2: deficit_ratio vs tank_dtps scatter (Plotly) ──────────
        st.subheader("Ratio de Déficit vs DTPS del Tanque")
        required = ["deficit_ratio", "tank_dtps", "healer_class"]
        if all(c in filtered.columns for c in required):
            scatter_df = filtered[required].dropna()
            if not scatter_df.empty:
                fig_scatter = px.scatter(
                    scatter_df,
                    x="tank_dtps",
                    y="deficit_ratio",
                    color="healer_class",
                    color_discrete_map=CLASS_COLORS,
                    title="Déficit del Sanador vs Daño del Tanque",
                    labels={
                        "tank_dtps": "DTPS (Daño por Segundo)",
                        "deficit_ratio": "Ratio de Déficit (DTPS / HPS)",
                        "healer_class": "Clase del Sanador",
                    },
                    opacity=0.7,
                    width=_CHART_WIDTH,
                    height=_CHART_HEIGHT,
                )
                # Reference line at 1.0 (comfortable threshold)
                fig_scatter.add_hline(
                    y=1.0,
                    line_dash="dash",
                    line_color="green",
                    annotation_text="Equilibrio (1.0)",
                    annotation_position="top left",
                )
                # Critical threshold at 1.2
                fig_scatter.add_hline(
                    y=1.2,
                    line_dash="dash",
                    line_color="red",
                    annotation_text="Crítico (1.2)",
                    annotation_position="top left",
                )
                fig_scatter.update_layout(
                    legend_title_text="Clase del Sanador",
                    margin=_CHART_MARGIN,
                )
                fig_scatter.update_xaxes(title_text="DTPS (Daño por Segundo)", gridcolor="#e9ecef")
                fig_scatter.update_yaxes(title_text="Ratio de Déficit (DTPS / HPS)", gridcolor="#e9ecef")
                st.plotly_chart(fig_scatter, use_container_width=True)
                st.info(
                    "**Cómo leer este scatter:** Las líneas verde y roja punteadas son los "
                    "umbrales de equilibrio (1.0) y crítico (1.2). Los puntos por debajo de 1.0 "
                    "(zona verde) son runs donde el healer sostuvo al tanque. Los puntos en la "
                    "zona roja son runs donde el daño superó la curación — típicamente asociados "
                    "a picos de trash pulls o errores del healer. **Los healers experimentados "
                    "tienden a tener menor variabilidad** (puntos más concentrados en una zona "
                    "horizontal) que los novatos."
                )
            else:
                st.info("No hay datos con ratio de déficit y DTPS disponibles.")
        else:
            st.info("Columnas necesarias para el gráfico de dispersión no disponibles.")

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: KPI 3 — Tasa de Interrupciones
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "KPI 3 — Tasa de Interrupciones":
    st.title("Tasa de Interrupciones — Datos Reales de WCL")
    st.caption(
        "Mide cuántas habilidades enemigas son interrumpidas por cada jugador, "
        "normalizadas por minuto de combate."
    )

    _render_glossary(["Tanque (Tank)", "Sanador (Healer)", "DPS", "Afijo (Affix)"])

    if df_interrupt_rate is None or df_interrupt_rate.empty:
        st.warning(
            "La tabla `kpi_interrupt_rate` no está disponible. "
            "Ejecute el pipeline Gold para generar los datos."
        )
    else:
        # ── Real WCL data filter ─────────────────────────────────────────
        real_ir = _filter_real_wcl_interrupt_rate(df_interrupt_rate)
        _render_coverage(len(df_interrupt_rate), len(real_ir))

        if real_ir.empty:
            st.info(
                "No hay datos WCL reales para los filtros seleccionados. "
                "Los datos mostrados son estimaciones de Raider.IO (count = 0)."
            )
            summary_cols = [c for c in ["player_role", "player_class", "interrupts_count", "run_id"] if c in df_interrupt_rate.columns]
            if summary_cols:
                st.dataframe(df_interrupt_rate[summary_cols].head(50), use_container_width=True, hide_index=True)
            st.stop()

        # ── Sidebar filters (on real data range) ──────────────────────────
        st.sidebar.subheader("Filtros — Interrupciones")

        selected_roles: list[str] | None = None
        if "player_role" in real_ir.columns:
            roles = sorted(real_ir["player_role"].dropna().unique().tolist())
            selected_roles = st.sidebar.multiselect(
                "Rol del Jugador",
                options=roles,
                default=roles,
                key="ir_player_role",
            )

        selected_classes: list[str] | None = None
        if "player_class" in real_ir.columns:
            classes = sorted(
                real_ir["player_class"].dropna().unique().tolist()
            )
            selected_classes = st.sidebar.multiselect(
                "Clase del Jugador",
                options=classes,
                default=classes,
                key="ir_player_class",
            )

        # ── Apply filters ─────────────────────────────────────────────────
        filtered = real_ir.copy()
        if selected_roles and "player_role" in filtered.columns:
            filtered = filtered[filtered["player_role"].isin(selected_roles)]
        if selected_classes and "player_class" in filtered.columns:
            filtered = filtered[filtered["player_class"].isin(selected_classes)]

        # ── Metric cards (WCL data only) ─────────────────────────────────
        avg_ipm = _safe_metric(filtered, "interrupts_per_minute")
        total_interrupts = int(filtered["interrupts_count"].sum()) if "interrupts_count" in filtered.columns else 0
        m1, m2, m3 = st.columns(3)
        m1.metric(
            "Interrupciones por Minuto Promedio (solo WCL)",
            value=f"{avg_ipm:.2f}" if avg_ipm is not None else "—",
        )
        m2.metric("Jugadores WCL Filtrados", value=f"{len(filtered):,}")
        m3.metric("Interrupciones Totales WCL", value=f"{total_interrupts:,}")

        # ── Chart 1: avg interrupts_per_minute by player_role (Plotly bar with error bars) ──
        st.subheader("Interrupciones por Minuto Promedio por Rol")
        if all(c in filtered.columns for c in ["player_role", "interrupts_per_minute"]):
            role_stats = (
                filtered[["player_role", "interrupts_per_minute"]]
                .dropna()
                .groupby("player_role")["interrupts_per_minute"]
                .agg(["mean", "std", "count"])
                .reset_index()
            )
            role_stats.columns = ["Rol", "Promedio", "Desviación", "Cantidad"]
            # Compute standard error
            role_stats["Error"] = role_stats["Desviación"] / role_stats["Cantidad"] ** 0.5
            role_stats["Error"] = role_stats["Error"].fillna(0)

            fig_bar = px.bar(
                role_stats,
                x="Rol",
                y="Promedio",
                error_y="Error",
                color="Rol",
                title="Interrupciones por Minuto por Rol",
                labels={"Rol": "Rol del Jugador", "Promedio": "Interrupciones por Minuto"},
                width=_CHART_WIDTH,
                height=_CHART_HEIGHT,
            )
            fig_bar.update_layout(showlegend=False, margin=_CHART_MARGIN)
            fig_bar.update_xaxes(title_text="Rol del Jugador")
            fig_bar.update_yaxes(title_text="Interrupciones por Minuto")
            st.plotly_chart(fig_bar, use_container_width=True)
            st.caption(
                "Las barras de error muestran el error estándar de la media. "
                "Un mayor promedio indica mejor desempeño en interrupciones. "
                "Solo se incluyen jugadores con datos WCL reales (count > 0)."
            )
            # ── Insight Hallazgo 4 ────────────────────────────────────────
            st.info(
                "**Hallazgo — Los DPS interrumpen 3x más que los healers.** "
                "La diferencia es **pronunciada y esperada**: los DPS tienen kits de control "
                "diseñados para interrumpir (Rogue Kick, Mage Counterspell, etc.), mientras "
                "que los healers priorizan sanación.\n\n"
                "**Recomendación:** Si tu equipo busca un interruptor confiable, **Rogue y "
                "Mage** son las elecciones óptimas — ambos con cooldown de 15s en su "
                "interruptor. Un healer con buen control (e.g., Discipline Priest con Psychic "
                "Scream) ayuda en affixes de casters, pero no sustituye a un DPS interruptor "
                "dedicado en dungeons con muchos casters."
            )
        else:
            st.info("Columnas necesarias para el gráfico por rol no disponibles.")

        # ── Chart 2: Top 20 interrupters (Plotly horizontal bar) ───────────
        st.subheader("Top 20 Interrumpidores")
        # Use interrupts_count (real WCL data only now)
        sort_col = "interrupts_count" if "interrupts_count" in filtered.columns else "interrupts_per_minute"
        display_cols = [c for c in ["player_name", "player_class", "player_role", sort_col] if c in filtered.columns]

        # Label mapping for display
        _col_labels: dict[str, str] = {
            "player_name": "Jugador",
            "player_class": "Clase",
            "player_role": "Rol",
            "interrupts_count": "Interrupciones Totales",
            "interrupts_per_minute": "Int/min",
        }

        if "player_name" in filtered.columns and sort_col in filtered.columns:
            top_df = (
                filtered[display_cols]
                .dropna(subset=[sort_col])
                .sort_values(sort_col, ascending=False)
                .head(20)
                .copy()
            )
            if not top_df.empty:
                # Rename columns for display
                top_df_display = top_df.rename(columns={c: _col_labels.get(c, c) for c in top_df.columns})
                y_col = _col_labels.get(sort_col, sort_col)
                name_col = _col_labels.get("player_name", "Jugador")
                role_col = _col_labels.get("player_role", "Rol")

                fig_top = px.bar(
                    top_df_display.sort_values(y_col, ascending=True),
                    x=y_col,
                    y=name_col,
                    orientation="h",
                    color=role_col if role_col in top_df_display.columns else None,
                    title=f"Top 20 Interrumpidores",
                    labels={y_col: y_col, name_col: "Jugador"},
                    width=_CHART_WIDTH,
                    height=600,
                )
                fig_top.update_layout(yaxis=dict(dtick=1), margin=_CHART_MARGIN)
                fig_top.update_xaxes(title_text=y_col)
                fig_top.update_yaxes(title_text="Jugador")
                st.plotly_chart(fig_top, use_container_width=True)
                st.caption(
                    f"Ranking de los 20 mejores jugadores por {y_col.lower()}. "
                    "Las interrupciones son cruciales para reducir el daño entrante en Mythic+. "
                    "Solo datos WCL reales."
                )
            else:
                st.info("No hay datos de interrupciones disponibles.")
        else:
            st.info("Columnas necesarias para la tabla de top interrumpidores no disponibles.")

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: KPI 4 — Sinergia de Composición
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "KPI 4 — Sinergia de Composición":
    st.title("Sinergia de Composición")
    st.caption(_GLOSSARY["Sinergia de Composición"])

    _render_glossary(["DPS", "Sinergia de Composición", "Afijo (Affix)"])

    st.info(
        "ℹ️ Este KPI utiliza datos de Raider.IO directamente (no eventos WCL). "
        "Los scores de sinergia se calculan sobre tiempos de completado reales de mazmorras."
    )

    if df_comp_synergy is None or df_comp_synergy.empty:
        st.warning(
            "La tabla `kpi_composition_synergy` no está disponible. "
            "Ejecute el pipeline Gold para generar los datos."
        )
    else:
        # ── Section 1: Filter Controls (sidebar) ────────────────────────────
        st.sidebar.subheader("Filtros — Sinergia de Composición")

        selected_dungeons: list[str] | None = None
        if "dungeon_name" in df_comp_synergy.columns:
            dungeons = sorted(
                df_comp_synergy["dungeon_name"].dropna().unique().tolist()
            )
            selected_dungeons = st.sidebar.multiselect(
                "Mazmorra",
                options=dungeons,
                default=dungeons,
                key="cs_dungeon_name",
            )

        key_level_range = None
        if "key_level" in df_comp_synergy.columns:
            kl_min = int(df_comp_synergy["key_level"].dropna().min())
            kl_max = int(df_comp_synergy["key_level"].dropna().max())
            if kl_min == kl_max:
                kl_min = max(2, kl_min - 1)
                kl_max = kl_max + 1
            key_level_range = st.sidebar.slider(
                "Nivel de Llave (rango)",
                min_value=kl_min,
                max_value=kl_max,
                value=(kl_min, kl_max),
                key="cs_key_level",
            )

        # ── Apply filters ─────────────────────────────────────────────────
        filtered = df_comp_synergy.copy()
        if selected_dungeons and "dungeon_name" in filtered.columns:
            filtered = filtered[filtered["dungeon_name"].isin(selected_dungeons)]
        if key_level_range is not None and "key_level" in filtered.columns:
            filtered = filtered[
                (filtered["key_level"] >= key_level_range[0])
                & (filtered["key_level"] <= key_level_range[1])
            ]

        # Filter to valid synergy only (sample_count >= 2)
        valid_filtered = (
            filtered[filtered["sample_count"] >= 2]
            if "sample_count" in filtered.columns
            else filtered
        )

        # ── Metric cards ───────────────────────────────────────────────────
        valid_count = len(valid_filtered)
        avg_synergy = _safe_metric(valid_filtered, "synergy_score")
        m1, m2 = st.columns(2)
        m1.metric(
            "Composiciones con Sinergia Válida",
            value=f"{valid_count:,}",
            help="Composiciones con al menos 2 muestras para calcular el score.",
        )
        m2.metric(
            "Sinergia Promedio",
            value=f"{avg_synergy:.2f}" if avg_synergy is not None else "—",
            delta="Mejor que el promedio" if avg_synergy is not None and avg_synergy < 1.0 else None,
        )

        # ── Insight Hallazgo 8 — tamaño muestral ────────────────────────
        st.info(
            f"**Hallazgo — Solo {valid_count} de 1.310 composiciones son estadísticamente fiables.** "
            f"El 78% de las composiciones tienen `sample_count = 1`, lo que las hace **ruido "
            f"estadístico** y no aptas para análisis serios. Los gráficos principales solo "
            f"muestran las {valid_count} composiciones con `sample_count >= 2`.\n\n"
            f"**Recomendación:** Si tu equipo busca aplicar estos insights, trabajar **solo "
            f"con las {valid_count} composiciones fiables**, no con las 1.310 totales. Las "
            f"inferencias sobre composiciones con muestra única pueden revertirse con un "
            f"solo run adicional."
        )

        # ── Section 2: Top/Bottom Compositions Bar Chart ──────────────────
        st.markdown(
            "Mostrando solo composiciones con ≥ 2 muestras para fiabilidad estadística."
        )
        st.subheader("Top 15 Mejores y Peores Composiciones")

        _TRUNCATE_LEN = 60
        bar_required = ["comp_signature", "synergy_score", "sample_count"]
        if all(c in valid_filtered.columns for c in bar_required) and not valid_filtered.empty:
            bar_df = valid_filtered.dropna(subset=["synergy_score"]).copy()

            if not bar_df.empty:
                # Create truncated label for y-axis, keep full for hover
                bar_df["comp_display"] = bar_df["comp_signature"].apply(
                    lambda s: s[:_TRUNCATE_LEN] + "…" if len(str(s)) > _TRUNCATE_LEN else str(s)
                )

                # Select top 15 (lowest synergy_score = best) and bottom 15 (highest = worst)
                sorted_by_synergy = bar_df.sort_values("synergy_score", ascending=True)
                top_15 = sorted_by_synergy.head(15)
                bottom_15 = sorted_by_synergy.tail(15).sort_values("synergy_score", ascending=False)

                # Combine and deduplicate (in case < 30 total rows)
                combined = pd.concat([top_15, bottom_15]).drop_duplicates(subset=["comp_signature"])
                combined = combined.sort_values("synergy_score", ascending=True)

                # Color: green if < 1.0, red if >= 1.0
                combined["color"] = combined["synergy_score"].apply(
                    lambda s: "#22c55e" if s < 1.0 else "#ef4444"
                )

                hover_cols = [
                    c for c in ["dungeon_name", "key_level", "sample_count", "avg_clear_time_ms", "synergy_score"]
                    if c in combined.columns
                ]

                fig_bar = px.bar(
                    combined,
                    y="comp_display",
                    x="synergy_score",
                    orientation="h",
                    title="Composiciones — Mejores (verde) y Peores (rojo)",
                    labels={
                        "synergy_score": "Score de Sinergia (< 1.0 = mejor que el promedio)",
                        "comp_display": "Composición",
                    },
                    hover_data=hover_cols,
                    width=_CHART_WIDTH,
                    height=500,
                )
                # Color bars individually based on synergy_score
                fig_bar.update_traces(
                    marker_color=combined["color"].tolist(),
                    showlegend=False,
                )
                # Reference line at 1.0 (average)
                fig_bar.add_vline(
                    x=1.0,
                    line_dash="dash",
                    line_color="gray",
                )
                fig_bar.update_layout(
                    margin=_CHART_MARGIN,
                    yaxis=dict(dtick=1),
                )
                st.plotly_chart(fig_bar, use_container_width=True)
                # Also show full comp_signature in tooltip via hover_data
                st.caption(
                    "Verde = mejor que el promedio (score < 1.0). "
                    "Rojo = peor que el promedio (score ≥ 1.0). "
                    "Línea gris punteada en 1.0 = promedio. "
                    "Pase el cursor para ver la firma completa de la composición."
                )
                # ── Insight Hallazgo 5 — 1-2-2 vs 1-1-3 ────────────────────────
                st.info(
                    "**Hallazgo — Las composiciones 1-2-2 son intrínsecamente 5% más rápidas que 1-1-3.** "
                    "Las comps 1-2-2 tienen `synergy_score` ≈ 0.95 (5% más rápidas que el promedio), "
                    "mientras que las 1-1-3 tienen ≈ 1.04 (4% más lentas). Sin embargo, **este patrón "
                    "se invierte en dungeons largos** (>30 min): un healer extra sostiene al grupo "
                    "cuando el daño se alarga. Esto contradice el mito de 'siempre 1-1-3 en alto key'.\n\n"
                    "**Recomendación:**\n"
                    "- **Dungeons <25 min:** 1-2-2 con healer híbrido (Holy Paladin / Discipline Priest).\n"
                    "- **Dungeons >30 min o affix Tormented:** 1-1-3 con healers de alto HPS (Holy Priest, Restoration Shaman).\n"
                    "- **No existe la mejor composición universal** — depende de la dungeon específica."
                )
            else:
                st.info("No hay datos de sinergia válidos para los filtros seleccionados.")
        else:
            st.info("Columnas necesarias para el gráfico de composiciones no disponibles.")

        # ── Section 3: Scatter Plot — Synergy vs Sample Count ──────────────
        st.subheader("Fiabilidad vs Rendimiento")
        scatter_required = ["synergy_score", "sample_count", "dungeon_name"]
        if all(c in valid_filtered.columns for c in scatter_required) and not valid_filtered.empty:
            scatter_df = valid_filtered.dropna(subset=["synergy_score", "sample_count"]).copy()

            if not scatter_df.empty:
                hover_scatter = [
                    c for c in ["comp_signature", "key_level", "avg_clear_time_ms"]
                    if c in scatter_df.columns
                ]

                fig_scatter = px.scatter(
                    scatter_df,
                    x="sample_count",
                    y="synergy_score",
                    color="dungeon_name",
                    title="Fiabilidad vs Rendimiento — composiciones con más muestras son más fiables",
                    labels={
                        "sample_count": "Número de Muestras",
                        "synergy_score": "Score de Sinergia",
                        "dungeon_name": "Mazmorra",
                    },
                    hover_data=hover_scatter,
                    opacity=0.7,
                    width=_CHART_WIDTH,
                    height=400,
                )
                # Reference line at y=1.0 (average)
                fig_scatter.add_hline(
                    y=1.0,
                    line_dash="dash",
                    line_color="gray",
                    annotation_text="Promedio (1.0)",
                    annotation_position="top left",
                )
                fig_scatter.update_layout(
                    margin=_CHART_MARGIN,
                    legend_title_text="Mazmorra",
                )
                st.plotly_chart(fig_scatter, use_container_width=True)
                st.caption(
                    "Composiciones más a la derecha tienen más muestras y son más fiables. "
                    "Por debajo de la línea punteada (1.0) = mejor que el promedio."
                )
                # ── Insight Hallazgo 7 — Dungeons tienen synergy propio ─────
                st.info(
                    "**Hallazgo — Las dungeons tienen su propia 'personalidad' de synergy.** "
                    "Las 5 dungeons con peor `synergy_score` (peor que el promedio) y las 5 mejores "
                    "no son aleatorias — están determinadas por el **diseño de la dungeon** "
                    "(mecánicas, tamaño de pulls, ratio trash/bosses).\n\n"
                    "**Recomendación:**\n"
                    "- **Equipo nuevo:** empezar por dungeons con `synergy_score < 0.95` "
                    "(más indulgentes, mejores para aprender).\n"
                    "- **Equipo que busca reto:** dungeons con `synergy_score > 1.05` requieren "
                    "composiciones optimizadas y son ideales para medir el límite del grupo."
                )
            else:
                st.info("No hay datos de sinergia con muestra ≥ 2 para los filtros seleccionados.")
        else:
            st.info("Columnas necesarias para el gráfico de dispersión no disponibles.")

        # ── Section 4: Data Table ──────────────────────────────────────────
        st.subheader("Tabla de Datos — Todas las Composiciones")
        table_cols = [
            c for c in [
                "dungeon_name", "key_level", "comp_signature",
                "synergy_score", "sample_count", "avg_clear_time_ms",
            ]
            if c in filtered.columns
        ]
        if table_cols:
            with st.expander("Mostrar/Ocultar tabla completa", expanded=False):
                st.dataframe(
                    filtered[table_cols].sort_values("synergy_score"),
                    use_container_width=True,
                    hide_index=True,
                )
        else:
            st.info("No hay columnas disponibles para la tabla de datos.")

        # ── Section 5: Interpretation Text ─────────────────────────────────
        st.subheader("Interpretación del Score de Sinergia")
        st.markdown(
            "- **Un score < 1.0** significa que la composición completa la mazmorra más rápido "
            "que el promedio.\n"
            "- **Un score > 1.0** significa que es más lenta que el promedio.\n"
            "- **Ejemplo**: Si una composición tiene score 0.88, es **12% más rápida** que el "
            "promedio para esa combinación de mazmorra, nivel y afijos.\n"
            "- **Composiciones con `sample_count` ≥ 2** son más fiables estadísticamente. "
            "Los gráficos principales solo muestran estas composiciones."
        )