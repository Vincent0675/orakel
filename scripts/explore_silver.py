"""Explore Silver Raider.IO data: distributions, stats, and visualizations.

Reads from MinIO, generates plots in /tmp/orakel_viz/.
Does NOT modify any pipeline code or config.
"""

from __future__ import annotations

import os
from collections import Counter

import matplotlib
matplotlib.use("Agg")  # Non-interactive backend
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from orakel.config import settings
from orakel.utils.minio import get_spark_session

sns.set_theme(style="whitegrid", palette="muted")
OUTPUT_DIR = "/tmp/orakel_viz"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_silver(spark: SparkSession) -> tuple:
    """Load Silver raidderio_runs from MinIO."""
    path = f"s3a://{settings.MINIO_BUCKET}/silver/raiderio_runs"
    df = spark.read.parquet(path).filter(F.col("season") == settings.SEASON)
    pdf = df.toPandas()
    print(f"📊 Silver raiderio_runs: {len(pdf)} rows, {len(pdf.columns)} columns")
    print(f"   Columnas: {list(pdf.columns)}")
    return df, pdf


def plot_class_distribution(pdf, path: str):
    """Distribution of tank classes (stratified sampling validation)."""
    # Extract all players from roster, filter tanks
    rows = []
    for _, run in pdf.iterrows():
        roster = run.get("roster") or []
        if roster and isinstance(roster[0], dict):
            roster_dicts = roster
        elif roster:
            roster_dicts = [dict(r.asDict()) for r in roster]
        else:
            roster_dicts = []
        for player in roster_dicts:
            rows.append({
                "class": player.get("class", "Unknown"),
                "spec": player.get("spec", "Unknown"),
                "role": player.get("role", "Unknown"),
                "dungeon_id": run.get("dungeon_id"),
                "mythic_level": run.get("mythic_level"),
            })
    
    import pandas as pd
    players = pd.DataFrame(rows)
    
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    # 1. Class distribution (all roles)
    class_counts = players["class"].value_counts()
    axes[0, 0].barh(class_counts.index, class_counts.values, color="steelblue")
    axes[0, 0].set_title("Distribución de Clases (todos los roles)")
    axes[0, 0].set_xlabel("Jugadores")
    for i, v in enumerate(class_counts.values):
        axes[0, 0].text(v + 2, i, str(v), va="center", fontsize=8)
    
    # 2. Role distribution
    role_counts = players["role"].value_counts()
    colors_roles = {"tank": "#e74c3c", "healer": "#2ecc71", "dps": "#3498db"}
    role_colors = [colors_roles.get(r, "#95a5a6") for r in role_counts.index]
    axes[0, 1].pie(role_counts.values, labels=role_counts.index, autopct="%1.1f%%",
                   colors=role_colors, startangle=90)
    axes[0, 1].set_title("Distribución de Roles")
    
    # 3. Tank classes specifically
    tanks = players[players["role"] == "tank"]
    tank_counts = tanks["class"].value_counts()
    axes[1, 0].barh(tank_counts.index, tank_counts.values, color="#e74c3c")
    axes[1, 0].set_title("Distribución de Tanks por Clase")
    axes[1, 0].set_xlabel("Tanks")
    for i, v in enumerate(tank_counts.values):
        axes[1, 0].text(v + 0.5, i, str(v), va="center", fontsize=8)
    
    # 4. Spec distribution (top 15)
    spec_counts = players["spec"].value_counts().head(15)
    axes[1, 1].barh(spec_counts.index[::-1], spec_counts.values[::-1], color="teal")
    axes[1, 1].set_title("Top 15 Especializaciones")
    axes[1, 1].set_xlabel("Jugadores")
    
    plt.tight_layout()
    filepath = os.path.join(path, "class_distribution.png")
    plt.savefig(filepath, dpi=120, bbox_inches="tight")
    plt.close()
    print(f"  ✅ {filepath}")


def plot_dungeon_distribution(pdf, path: str):
    """Distribution by dungeon."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # 1. Dungeon frequency
    dungeon_counts = pdf["dungeon_name"].value_counts()
    axes[0].barh(dungeon_counts.index, dungeon_counts.values, color="coral")
    axes[0].set_title("Runs por Mazmorra")
    axes[0].set_xlabel("Cantidad de runs")
    for i, v in enumerate(dungeon_counts.values):
        axes[0].text(v + 5, i, str(v), va="center", fontsize=8)
    
    # 2. Mythic level distribution
    level_counts = pdf["mythic_level"].value_counts().sort_index()
    axes[1].bar(level_counts.index.astype(str), level_counts.values, color="mediumseagreen")
    axes[1].set_title("Distribución por Nivel de Clave")
    axes[1].set_xlabel("Mythic Level")
    axes[1].set_ylabel("Runs")
    for i, v in enumerate(level_counts.values):
        axes[1].text(i, v + 2, str(v), ha="center", fontsize=8)
    
    plt.tight_layout()
    filepath = os.path.join(path, "dungeon_distribution.png")
    plt.savefig(filepath, dpi=120, bbox_inches="tight")
    plt.close()
    print(f"  ✅ {filepath}")


def plot_temporal_distribution(pdf, path: str):
    """Temporal distribution of runs."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # 1. By month
    pdf["completed_date"] = pdf["completed_at"].dt.date if hasattr(pdf["completed_at"], "dt") else None
    if pdf["completed_date"] is not None:
        monthly = pdf.groupby(pdf["completed_at"].dt.to_period("M")).size()
        axes[0].plot(monthly.index.astype(str), monthly.values, marker="o", color="purple", linewidth=2)
        axes[0].tick_params(axis="x", rotation=45)
        axes[0].set_title("Runs por Mes")
        axes[0].set_ylabel("Cantidad")
    
    # 2. Clear time distribution
    if "clear_time_ms" in pdf.columns:
        times = pdf["clear_time_ms"].dropna() / 1000 / 60  # convert to minutes
        axes[1].hist(times, bins=30, color="gold", edgecolor="black", alpha=0.7)
        axes[1].set_title("Distribución de Tiempo de Limpieza")
        axes[1].set_xlabel("Minutos")
        axes[1].set_ylabel("Runs")
    
    plt.tight_layout()
    filepath = os.path.join(path, "temporal_distribution.png")
    plt.savefig(filepath, dpi=120, bbox_inches="tight")
    plt.close()
    print(f"  ✅ {filepath}")


def plot_mythic_level_vs_time(pdf, path: str):
    """Mythic level vs clear time scatter."""
    fig, ax = plt.subplots(figsize=(10, 6))
    
    df = pdf.dropna(subset=["mythic_level", "clear_time_ms"])
    times_min = df["clear_time_ms"] / 1000 / 60
    
    scatter = ax.scatter(
        df["mythic_level"], times_min,
        c=df["mythic_level"], cmap="viridis", alpha=0.5, s=20
    )
    ax.set_xlabel("Mythic Level")
    ax.set_ylabel("Tiempo de limpieza (minutos)")
    ax.set_title("Tiempo de limpieza vs Nivel de Clave")
    plt.colorbar(scatter, ax=ax, label="Mythic Level")
    
    plt.tight_layout()
    filepath = os.path.join(path, "mythic_vs_time.png")
    plt.savefig(filepath, dpi=120, bbox_inches="tight")
    plt.close()
    print(f"  ✅ {filepath}")


def print_stats(pdf):
    """Print key statistics."""
    print()
    print("=" * 60)
    print("📈 ESTADÍSTICAS CLAVE")
    print("=" * 60)
    print(f"  Total runs:              {len(pdf)}")
    print(f"  Mazmorras únicas:        {pdf['dungeon_id'].nunique()}")
    print(f"  Rango mythic levels:     {int(pdf['mythic_level'].min())} - {int(pdf['mythic_level'].max())}")
    print(f"  Mythic level promedio:   {pdf['mythic_level'].mean():.1f}")
    print(f"  Runs completadas:        {pdf['completed_at'].notna().sum()} ({pdf['completed_at'].notna().mean()*100:.1f}%)")
    
    # Per-player stats from roster
    all_players = []
    for _, run in pdf.iterrows():
        roster = run.get("roster") or []
        if roster and isinstance(roster[0], dict):
            roster_dicts = roster
        elif roster:
            roster_dicts = [dict(r.asDict()) for r in roster]
        else:
            roster_dicts = []
        for player in roster_dicts:
            all_players.append(player)
    
    if all_players:
        from collections import Counter
        classes = Counter(p.get("class", "Unknown") for p in all_players)
        specs = Counter(p.get("spec", "Unknown") for p in all_players)
        roles = Counter(p.get("role", "Unknown") for p in all_players)
        
        print()
        print(f"  Jugadores en roster: {len(all_players)}")
        print(f"  Roles: {dict(roles)}")
        print(f"  Clases: {len(classes)} únicas")
        for cls, cnt in classes.most_common():
            print(f"    {cls:20s} {cnt:>4} ({cnt/len(all_players)*100:5.1f}%)")
        print()
        print(f"  Especializaciones: {len(specs)} únicas")
        for spec, cnt in specs.most_common(10):
            print(f"    {spec:25s} {cnt:>4} ({cnt/len(specs)*100 if specs else 0:5.1f}%)")


def main():
    spark = get_spark_session("explore-silver")
    
    print("🔍 Explorando Silver Raider.IO...")
    df, pdf = load_silver(spark)
    
    print_stats(pdf)
    
    print()
    print("📊 Generando visualizaciones...")
    plot_class_distribution(pdf, OUTPUT_DIR)
    plot_dungeon_distribution(pdf, OUTPUT_DIR)
    plot_temporal_distribution(pdf, OUTPUT_DIR)
    plot_mythic_level_vs_time(pdf, OUTPUT_DIR)
    
    print()
    print(f"🎯 Todas las gráficas en: {OUTPUT_DIR}/")
    print(f"   {len(os.listdir(OUTPUT_DIR))} archivos generados")
    
    spark.stop()


if __name__ == "__main__":
    main()
