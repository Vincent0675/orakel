## Exploration: Mejora de visualizaciones Streamlit y posibilidad de otra herramienta de visualización

### Current State

The project uses **Streamlit 1.58.0** with **Plotly 6.8.0** for a single-page BI dashboard at `dashboard/app.py` (1157 lines, single file). The dashboard reads pre-computed Gold-layer KPI tables from MinIO via PySpark, converts to pandas, and renders Plotly charts inside Streamlit components.

**Existing Pages (5):**
| Page | Content | Charts |
|------|---------|--------|
| Resumen General | Overview metrics, coverage table, Gold table availability | Metric cards (4 columns), data tables |
| KPI 1 — Reloj de Muerte | Death Clock category distribution, DTPS vs Death Clock scatter | Plotly bar + scatter |
| KPI 2 — Déficit del Sanador | Deficit category distribution, Deficit vs DTPS scatter with reference lines | Plotly bar + scatter |
| KPI 3 — Tasa de Interrupciones | Interrupts/min by role, Top 20 interrupters | Plotly bar with error bars, horizontal bar |
| KPI 4 — Sinergia de Composición | Top/bottom compositions, reliability vs performance | Plotly bar + scatter + data table |

**Dashboard architecture:**
- Single `app.py` file with all pages in one `if/elif` chain
- PySpark SparkSession cached via `@st.cache_resource`
- Gold table loading cached via `@st.cache_data(ttl=3600)`
- Sidebar filters per KPI (key_level ranges, class selectors)
- WCL real-data filtering helpers (`_filter_real_wcl_*`)
- Glossary in Spanish (non-WoW players friendly)
- All charting via Plotly Express (`px.bar`, `px.scatter`)
- No custom CSS, no theming, no `.streamlit/config.toml`

**Strengths of current implementation:**
- Well-structured single-file app with clear sections and Spanish UX
- Plotly charts are interactive (hover, zoom, pan)
- Sidebar navigation works, filters per KPI are functional
- WCL data coverage indicators are thoughtful
- Caching layer prevents repeated Spark reads

**Weaknesses / Improvement opportunities:**
- **Monolithic single file**: 1157 lines, hard to maintain, no separation of pages into modules
- **No custom theming**: Default Streamlit white theme, no brand identity
- **No .streamlit/config.toml**: Server config, theme, settings all at defaults
- **No tests**: Zero tests for dashboard code (dashboard logic is untested)
- **No error boundaries**: If MinIO/Spark is down, the app shows raw Python tracebacks to users
- **No real-time or auto-refresh**: User must manually refresh to see new data (1h cache)
- **No mobile optimization**: Streamlit is desktop-first, mobile experience is poor
- **Performance**: Spark → pandas conversion for every page load (even with cache, first load is slow — 4g driver memory)
- **Limited chart types**: Only `px.bar` and `px.scatter` — no heatmaps, time series, network graphs, or advanced Plotly
- **No export/download**: Users cannot download charts as images or data as CSV
- **No multi-user**: Streamlit runs per-user session; each user gets their own Python process

---

### Project Structure (relevant files)

```
orakel/
├── dashboard/
│   ├── __init__.py              # Empty
│   └── app.py                   # Streamlit dashboard (1157 lines, all logic)
├── orakel/
│   ├── __init__.py
│   ├── config.py                # Settings from .env (MinIO, API keys, season)
│   ├── clients/
│   │   ├── raiderio.py          # REST API client for Raider.IO
│   │   └── warcraftlogs.py      # GraphQL client for WarcraftLogs
│   ├── models/
│   │   ├── kpi.py               # Pure Python KPI functions (pure logic)
│   │   └── schemas.py           # PySpark StructType schemas for all layers
│   ├── pipeline/
│   │   ├── bronze.py            # Raw data ingestion
│   │   ├── silver.py            # Clean + dedup + fuzzy join
│   │   └── gold.py              # KPI aggregations + dimension tables
│   └── utils/
│       ├── minio.py             # SparkSession with S3A MinIO configuration
│       └── rate_limiter.py      # Token-bucket rate limiter
├── scripts/                     # 8 CLI scripts for pipeline execution
├── tests/                       # 71 tests (Tier 1 pure pytest, Tier 2 with Spark)
├── openspec/                    # SDD documentation
├── pyproject.toml               # Python project config
├── docker-compose.yml           # MinIO service
├── .python-version              # 3.13
└── README.md                    # Project documentation
```

---

### Dependencies

**Production deps:**
- `pyspark>=4.1.2` — Spark processing (heavy, requires Java 17+)
- `minio>=7.2.20` — S3-compatible storage client
- `python-dotenv>=1.2.2` — .env loading
- `requests>=2.34.2` — HTTP client
- `scikit-learn>=1.9.0` — ML (future use)

**Dev deps (for dashboard):**
- `streamlit>=1.58.0` — Dashboard framework
- `plotly>=6.8.0` — Charting library

**Test deps:**
- `pytest>=8`, `pytest-cov>=6`, `responses>=0.25`, `chispa>=0.12`

**Infrastructure:**
- Docker + MinIO (local S3)
- Java 17 (for PySpark)
- Python 3.13 via uv (exclusive package manager)

**Notable:**
- No `.streamlit/config.toml` exists — server config is fully default
- No CI/CD pipeline (no `.github/` directory)
- No database — all data is in MinIO Parquet files

---

### Opportunities for Improvement (Current Streamlit)

1. **Split monolithic app into modules**: Extract each KPI page into its own file under `dashboard/pages/` using Streamlit's multipage pattern, or use a proper module structure
2. **Add .streamlit/config.toml**: Configure theme (colors, font), server settings, browser tab icon
3. **Add custom CSS/styling**: Improve visual polish — better spacing, branded headers, responsive layout
4. **Add chart export buttons**: Plotly supports `config` with `modeBarButtons` for download as PNG/SVG
5. **Add data export**: Download buttons for underlying DataFrames (CSV)
6. **Add tests**: pytest with mocked Streamlit session state for each page
7. **Add error boundaries**: Graceful error handling with user-friendly messages instead of tracebacks
8. **Add auto-refresh**: `st.autorefresh` or manual "Refresh Data" button
9. **Add more chart types**: Heatmaps, box plots for class comparison, trend lines over time
10. **Performance optimization**: Move Spark-to-pandas conversion earlier, use column subset selection
11. **Session state**: Use `st.session_state` to persist filter selections across page navigations
12. **Internationalization**: Currently Spanish-only; could parameterize language

---

### Alternatives Analysis

| Feature | Streamlit (current) | Gradio | Panel (HoloViz) | Dash (Plotly) | NiceGUI |
|---------|-------------------|--------|-----------------|---------------|---------|
| **Type** | Dashboard framework | ML demo tool | Dashboard framework | Enterprise dashboard | Python UI framework |
| **Charts** | Plotly (manual) | Built-in + Plotly | HoloViews + Bokeh + Plotly | Plotly (native) | Matplotlib + Plotly |
| **Multi-page** | Pages directory | Tabs/blocks | Templates + panes | Multi-page app | Routes |
| **State mgmt** | Session state | gr.State | Param + Reactive | Callbacks | Reactive properties |
| **Theming** | Basic (config.toml) | Built-in themes | Bootstrap + Fast | Dash Bootstrap | Tailwind + Quasar |
| **Mobile** | Poor | Good | Fair | Fair | Good |
| **Performance** | Per-user process | Lightweight | Proactor pattern | Flask/Gunicorn | Async Uvicorn |
| **Auth** | None built-in | Hugging Face SSO | None | Flask-Login | OAuth + JWT |
| **Learning curve** | Low | Very Low | Medium | Medium-High | Medium |
| **Community** | Very Large | Very Large | Medium | Large | Growing |
| **Deployment** | `streamlit run` | `gradio launch` | `panel serve` | Gunicorn/WSGI | `uvicorn` |
| **File size** | 1.58 GB (PySpark dep) | 60 MB | 150 MB + Bokeh | 200 MB + Flask | 50 MB |
| **Spark integration** | Manual (app.py does it) | Manual | Manual | Manual through Dash callbacks | Manual |
| **Current usage** | ✅ Single 1157-line app | ❌ | ❌ | ❌ | ❌ |

#### 1. **Gradio** (v5+)
- **Pros**: Simpler API than Streamlit, built-in theming, better mobile support, Hugging Face Spaces deploy, lighter weight
- **Cons**: Less flexible for complex dashboards (tabs vs pages), no built-in multipage, smaller charting ecosystem
- **Verdict**: Good for ML demos/dashboards but too simple for 4-KPI analytical BI with filters

#### 2. **Panel** (HoloViz ecosystem)
- **Pros**: Native HoloViews/Bokeh/Plotly support, reactive programming model (Param), multipage templates, good for data-heavy dashboards, async support, works with Jupyter
- **Cons**: Steeper learning curve (Param + reactive pattern), smaller community, Bokeh output is less polished than Plotly for interactive charts
- **Verdict**: Viable alternative, especially if moving toward more reactive/analytical dashboards. Good fit for data pipeline projects.

#### 3. **Dash** (Plotly Enterprise)
- **Pros**: Native Plotly (no manual `st.plotly_chart` wrapping), extensive component library (Dash Bootstrap, AG Grid, DataTable), proper backend/frontend separation, production-grade deployment (Gunicorn + Celery), maturity
- **Cons**: Requires Flask/WSGI deployment (more ops overhead), callback-based (can get complex for many interdependent filters), heavier learning curve
- **Verdict**: Best migration target for a production dashboard. Overkill for MVP unless planning to scale significantly.

#### 4. **NiceGUI**
- **Pros**: Python-native reactive UI (like Streamlit but with more control), Tailwind/Quasar styling, async, routes, built-in auth, auto-refresh, mobile-friendly
- **Cons**: Newer (less battle-tested), smaller ecosystem, fewer dashboard-specific components
- **Verdict**: Interesting middle ground between Streamlit and full web frameworks. Good for projects needing more custom UI without leaving Python.

#### 5. **Custom Flask/FastAPI + HTMX**
- **Pros**: Full control, lightweight, any chart library, proper separation of concerns
- **Cons**: Requires frontend knowledge (HTML/JS/HTMX), more boilerplate, no pre-built dashboard components
- **Verdict**: Overkill for MVP. Only if needing a public-facing production app.

---

### Recommendation

**Keep Streamlit for the MVP dashboard, but significantly improve the implementation.** The project is a university pipeline project (not a production SaaS), and the current Streamlit + Plotly combination works well. The real issue is code quality and UX polish, not the framework.

**Short term (MVP — this change):**
1. **Refactor `app.py` into modules** using Streamlit's multipage pattern (`dashboard/pages/01_*.py`):
   - `dashboard/main.py` — Shared components (Spark session, data loading, glossary)
   - `dashboard/pages/01_overview.py` — Resumen General
   - `dashboard/pages/02_death_clock.py` — KPI 1
   - `dashboard/pages/03_healer_deficit.py` — KPI 2
   - `dashboard/pages/04_interrupt_rate.py` — KPI 3
   - `dashboard/pages/05_composition_synergy.py` — KPI 4
2. **Add `.streamlit/config.toml`** with custom theme (dark mode, branded colors matching WoW class colors)
3. **Add custom CSS** for visual polish
4. **Add chart export/download buttons** (Plotly's `config` + CSV download)
5. **Add error boundaries** (try/except with user-friendly st.error messages)
6. **Add data refresh button** using `st.button` to bust cache
7. **Persist sidebar filter state** across page changes via `st.session_state`
8. **Add more chart variations**: Box plots per class, heatmaps for affix combinations, trend charts per dungeon

**Medium term (next phase):**
- If the dashboard needs to be public-facing or multi-user, consider migrating to **Dash** for production-grade deployment with proper auth and scalability.
- Alternatively, **Panel** is worth exploring if the project moves toward more analytical/reactive dashboards with HoloViews integration.
- Add **Grafana** or **Superset** as an alternative if the team prefers SQL-based dashboards connected directly to MinIO (via Trino/Presto).

**Long term (not for MVP):**
- A custom **FastAPI + HTMX + Plotly** frontend if the project evolves into a full product with user accounts, sessions, and complex interactions.

---

### Risks

1. **Refactoring effort is non-trivial**: Splitting 1157 lines into pages requires careful extraction of shared state (SparkSession, data loading, filters). Risk of breaking existing functionality.
2. **Streamlit multipage caveat**: Pages share the same Python process, so cache is shared. But each page reload re-runs the script — need to ensure data loading is lazy.
3. **PySpark memory overhead**: SparkSession uses 4g driver memory. Running Streamlit + PySpark in the same process is heavy. Consider lazy-loading Spark only when needed.
4. **No dashboard testing**: Currently zero tests for UI code. Adding visual regression testing is complex (requires Playwright/Selenium).
5. **Mobile experience**: Streamlit has inherent mobile limitations. If mobile access is required, consider Gradio or a native app instead.
6. **Dependency weight**: Adding more visualization libraries (e.g., if migrating to Panel or Dash) increases `uv sync` time and deployment size.
7. **Dark vs light theme**: WoW has a dark fantasy aesthetic — users might expect a dark theme. Streamlit's theming is limited to basic colors.

### Ready for Proposal

Yes. The exploration is thorough enough to drive a proposal. The orchestrator should tell the user:
- The dashboard is functional but monolithic (1157 lines, single file)
- Short-term recommendation is to **refactor + improve existing Streamlit (not replace it)**
- Medium-term options (Dash/Panel) are documented with tradeoffs
- The proposal will focus on modularization, theming, export features, and UX improvements
