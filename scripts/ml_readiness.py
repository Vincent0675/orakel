"""Evaluate Silver data readiness for ML training."""

from __future__ import annotations

from collections import Counter

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from orakel.config import settings


def main():
    spark = _get_spark()
    
    path = f"s3a://{settings.MINIO_BUCKET}/silver/raiderio_runs"
    df = spark.read.parquet(path).filter(F.col("season") == settings.SEASON)
    pdf = df.toPandas()
    
    print("=" * 62)
    print("  EVALUACION DE DATOS PARA ML — ORAKEL")
    print("=" * 62)
    
    # 1. Sample size
    print(f"\n{'─' * 62}")
    print("  TAMANO DE MUESTRA")
    print(f"{'─' * 62}")
    print(f"  Runs totales:              {len(pdf)}")
    print(f"  Mazmorras unicas:          {pdf['dungeon_id'].nunique()}")
    print(f"  Rango mythic levels:       {int(pdf['mythic_level'].min())} - {int(pdf['mythic_level'].max())}")
    print(f"  Periodo:                   {pdf['completed_at'].min()} a {pdf['completed_at'].max()}")
    
    # 2. Class balance
    all_players = []
    for _, run in pdf.iterrows():
        for p in (run.get("roster") or []):
            if p:
                d = dict(p.asDict()) if hasattr(p, "asDict") else p
                all_players.append(d)
    
    classes = Counter(p.get("class", "?") for p in all_players)
    total_p = sum(classes.values())
    vals = list(classes.values())
    cv = float(np.std(vals) / np.mean(vals) * 100)
    
    print(f"\n{'─' * 62}")
    print("  BALANCE DE CLASES")
    print(f"{'─' * 62}")
    print(f"  Clases: {len(classes)}/13 representadas")
    print(f"  Coef. variacion entre clases: {cv:.1f}% ", end="")
    print("OK" if cv < 50 else "DESBALANCEADO")
    for cls, cnt in classes.most_common():
        print(f"    {cls:25s} {cnt:>5} ({cnt/total_p*100:5.1f}%)")
    
    # 3. Feature inventory
    print(f"\n{'─' * 62}")
    print("  INVENTARIO DE FEATURES")
    print(f"{'─' * 62}")
    
    current = {
        "Numericas": ["mythic_level", "clear_time_ms", "score", "rank"],
        "Temporales": ["year", "month", "completed_at"],
        "Categoricas": ["dungeon_id", "dungeon_name", "weekly_modifiers"],
        "Estructuradas": ["roster (5 jugadores con clase/spec/role)"],
    }
    future = {
        "Gold KPIs": ["death_clock", "healer_deficit", "interrupt_rate", "synergy_score"],
        "WCL combat": ["damage_taken", "healing_received", "interrupts_count"],
        "Composicion": ["comp_signature", "tank_class", "healer_class", "dps_classes"],
    }
    
    for cat, feats in current.items():
        print(f"  {cat}:")
        for f in feats:
            ok = f in pdf.columns or f in pdf.columns
            print(f"    {'+' if ok else '-'} {f}")
    
    print(f"  (Gold KPIs - pendientes del match WCL):")
    for f in future["Gold KPIs"]:
        print(f"    ~ {f}")
    print(f"  (WCL combat stats - pendientes):")
    for f in future["WCL combat"]:
        print(f"    ~ {f}")
    
    # 4. Data quality
    print(f"\n{'─' * 62}")
    print("  CALIDAD DE DATOS")
    print(f"{'─' * 62}")
    for col in ["mythic_level", "clear_time_ms", "score", "rank", "dungeon_id"]:
        nulls = int(pdf[col].isna().sum())
        print(f"  {col:25s}  {nulls:>4} nulos  ({nulls/len(pdf)*100:4.1f}%)")
    
    # 5. Target distribution
    print(f"\n{'─' * 62}")
    print("  DISTRIBUCION DE TARGET (clear_time_ms)")
    print(f"{'─' * 62}")
    q = pdf["clear_time_ms"].describe()
    for pct, label in [("min", "25%"), ("25%", "50%"), ("50%", "75%"), ("75%", "max")]:
        pass
    print(f"     Min:    {q['min']/60000:.1f} min")
    print(f"     P25:    {q['25%']/60000:.1f} min")
    print(f"     Mediana:{q['50%']/60000:.1f} min")
    print(f"     P75:    {q['75%']/60000:.1f} min")
    print(f"     Max:    {q['max']/60000:.1f} min")
    print(f"     Media:  {q['mean']/60000:.1f} min")
    print(f"     Std:    {q['std']/60000:.1f} min")
    
    # 6. ML verdict
    print(f"\n{'═' * 62}")
    print("  VEREDICTO ML")
    print(f"{'═' * 62}")
    
    checks = []
    checks.append(("2000+ runs", len(pdf) >= 2000, f"{len(pdf)} runs"))
    checks.append(("Balance clases OK", cv < 50, f"CV={cv:.1f}%"))
    checks.append(("Sin nulos criticos", pdf["mythic_level"].isna().sum() == 0, f"{pdf['mythic_level'].isna().sum()} nulos en mythic_level"))
    checks.append(("Features numericas", 4, "mythic_level, time, score, rank"))
    checks.append(("Gold KPIs pendientes", False, "Llegaran tras match + silver + gold"))
    checks.append(("WCL combat stats pendientes", False, "Llegaran tras ingesta WCL"))
    
    for label, ok, detail in checks:
        status = "OK" if ok else "PENDIENTE"
        print(f"  [{status:>9}] {label:25s} — {detail}")
    
    print()
    print(f"  Recomendacion: datos suficientes para feature engineering preliminar.")
    print(f"  Esperar a que complete el match WCL + Gold KPIs para entrenar.")
    
    spark.stop()


def _get_spark():
    from orakel.utils.minio import get_spark_session
    return get_spark_session("ml-readiness")


if __name__ == "__main__":
    main()
