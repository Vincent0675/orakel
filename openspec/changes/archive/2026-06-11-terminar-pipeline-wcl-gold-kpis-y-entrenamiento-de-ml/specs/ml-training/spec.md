# ML Training — Especificación

## Propósito

Entrenar un modelo de regresión lineal múltiple que prediga `clear_time_seconds` de una run Mythic+ usando features derivadas de Gold KPIs, con tracking en MLflow, evaluación contra baseline, y serialización para inferencia.

## Requerimientos

| ID | Descripción | Keyword |
|----|-------------|---------|
| ML-1 | Feature engineering: one-hot encode `comp_signature` (top-20 signatures, resto como "other"), binary flags por `affix_id`, normalizar features numéricas (z-score) | MUST |
| ML-2 | Features numéricas: `death_clock_seconds`, `deficit_ratio`, `interrupts_per_minute`, `synergy_score`, `key_level`. Si una KPI es NULL, imputar con la media del training set | MUST |
| ML-3 | Modelo: `sklearn.linear_model.LinearRegression` (regresión lineal múltiple) | MUST |
| ML-4 | Train/test split temporal: ordenar por `completed_at`, 80% train (más antiguo), 20% test (más reciente). Sin data leakage — no mezclar temporalmente | MUST |
| ML-5 | Target: `clear_time_seconds` = `clear_time_ms` / 1000.0 | MUST |
| ML-6 | Métricas de evaluación: MAE (Mean Absolute Error), RMSE (Root Mean Squared Error), R² (coeficiente de determinación) en hold-out test set | MUST |
| ML-7 | Baseline: mean predictor (predecir la media de `clear_time_seconds` del training set para todas las muestras de test). MAE_baseline y R²_baseline se reportan junto a las métricas del modelo | MUST |
| ML-8 | MLflow tracking: loggear parámetros (features usadas, train_size, test_size), métricas (MAE, RMSE, R², baseline_MAE), modelo serializado, y archivo `feature_importance.csv` con coeficientes | MUST |
| ML-9 | Feature importance: análisis de coeficientes del modelo. Reportar top-5 features positivas (aumentan clear_time) y top-5 negativas (lo reducen) | MUST |
| ML-10 | Modelo serializado se escribe a MinIO `ml_models/{run_id}/` como artifact de MLflow. Path registrado en metadata Parquet | MUST |

## Escenarios

### Happy path — modelo entrenado con métricas válidas

- GIVEN Gold KPIs existen con ≥ 100 runs
- WHEN `ml_model` asset se materializa
- THEN feature engineering produce matriz con : Muestras × (N features)
- THEN train/test split respeta orden temporal
- THEN LinearRegression se entrena sin error
- THEN MAE < 60s y R² > 0.3 (mejor que baseline)
- THEN MLflow registry contiene parámetros, métricas, y artifact del modelo

### Baseline supera al modelo

- GIVEN los datos tienen alta varianza y pocas samples (< 50 runs)
- WHEN el modelo se entrena
- THEN R² puede ser negativo (modelo peor que baseline)
- THEN MAE_model > MAE_baseline es aceptable
- THEN se loggea "WARNING: modelo no supera baseline — considerar más datos o mejor feature engineering"

### Features nulas por WCL faltante

- GIVEN Gold KPIs 1-3 tienen NULLs (WCL no disponible)
- WHEN feature engineering se ejecuta
- THEN las features nulas se imputan con la media del training set
- THEN el modelo se entrena solo con features disponibles (`key_level`, `synergy_score`)
- THEN métricas reflejan menor poder predictivo

### Split temporal insuficiente

- GIVEN hay < 10 runs en total
- WHEN el asset `ml_model` se materializa
- THEN NO se entrena modelo
- THEN el asset queda en `skipped` con mensaje: "Datos insuficientes: N < 10 runs"
