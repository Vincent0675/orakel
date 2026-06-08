"""Deep exploration of Silver Raider.IO data."""

from __future__ import annotations

from collections import Counter, defaultdict

from pyspark.sql import functions as F
from orakel.config import settings
from orakel.utils.minio import get_spark_session


def main():
    spark = get_spark_session("explore-deep")
    path = f"s3a://{settings.MINIO_BUCKET}/silver/raiderio_runs"
    df = spark.read.parquet(path).filter(F.col("season") == settings.SEASON)
    pdf = df.toPandas()

    # Parse roster into structured data
    players = []
    for _, run in pdf.iterrows():
        roster = run.get("roster") or []
        for p in roster:
            if p:
                d = dict(p.asDict()) if hasattr(p, "asDict") else p
                d["run_id"] = run["keystone_run_id"]
                d["dungeon"] = run["dungeon_name"]
                d["mythic_level"] = run["mythic_level"]
                d["clear_time_ms"] = run["clear_time_ms"]
                d["region"] = d.get("region", "?")
                players.append(d)

    print("=" * 62)
    print("  EXPLORACION PROFUNDA — SILVER RAIDER.IO")
    print("=" * 62)

    # ── 1. REGIONAL DISTRIBUTION ──
    print(f"\n{'─' * 62}")
    print("  1. DISTRIBUCION REGIONAL")
    print(f"{'─' * 62}")
    regions = Counter(p.get("region", "?") for p in players)
    for reg, cnt in regions.most_common():
        print(f"    {reg:15s} {cnt:>6} jugadores ({cnt/len(players)*100:5.1f}%)")

    # Region per server (top realms)
    realms = Counter(p.get("realm", "?") for p in players)
    print(f"\n  Top 10 realms:")
    for realm, cnt in realms.most_common(10):
        print(f"    {realm:35s} {cnt:>4}")

    # ── 2. DUNGEON ANALYSIS ──
    print(f"\n{'─' * 62}")
    print("  2. ANALISIS POR MAZMORRA")
    print(f"{'─' * 62}")

    dungeon_stats = pdf.groupby("dungeon_name")["clear_time_ms"].agg(["mean", "std", "min", "max", "count"])
    dungeon_stats["mean_min"] = dungeon_stats["mean"] / 60000
    dungeon_stats["std_min"] = dungeon_stats["std"] / 60000
    dungeon_stats = dungeon_stats.sort_values("mean_min")

    for dg in dungeon_stats.itertuples():
        print(f"  {dg.Index:35s} {dg.count:>4} runs  "
              f"media={dg.mean_min:5.1f}min  "
              f"std={dg.std_min:4.1f}  "
              f"rang={dg.min/60000:.1f}-{dg.max/60000:.1f}")

    # Fastest and slowest runs
    fastest = pdf.loc[pdf["clear_time_ms"].idxmin()]
    slowest = pdf.loc[pdf["clear_time_ms"].idxmax()]
    print(f"\n  Run mas rapido: {fastest['dungeon_name']} "
          f"({fastest['clear_time_ms']/60000:.1f}min, "
          f"key {fastest['mythic_level']})")
    print(f"  Run mas lento:  {slowest['dungeon_name']} "
          f"({slowest['clear_time_ms']/60000:.1f}min, "
          f"key {slowest['mythic_level']})")

    # ── 3. AFFIX ANALYSIS ──
    print(f"\n{'─' * 62}")
    print("  3. ANALISIS DE AFIJOS")
    print(f"{'─' * 62}")

    affix_names = {
        2: "Bolstering", 3: "Raging", 4: "Sanguine", 5: "Spiteful",
        6: "Storming", 7: "Bursting", 8: "Volcanic", 9: "Tyrannical",
        10: "Fortified", 117: "Encrypted", 121: "Shrouded",
        123: "Thundering", 128: "Tyrannical", 129: "Fortified",
        130: "Entangling", 131: "Raging", 132: "Bursting",
        133: "Bolstering", 134: "Sanguine", 135: "Storming",
        136: "Spiteful", 137: "Volcanic", 138: "Incorporeal",
        139: "Afflicted", 140: "Entangling",
    }
    print("  Frecuencia de afijos:")
    affix_id_counter: Counter[int] = Counter()
    for mods in pdf["weekly_modifiers"]:
        if isinstance(mods, list):
            for m in mods:
                affix_id_counter[m] += 1
    for aff_id, cnt in affix_id_counter.most_common():
        name = affix_names.get(aff_id, f"desconocido-{aff_id}")
        print(f"    {name:25s} (id={aff_id:>3})  aparece en {cnt} de 2000 runs")

    # Group by affix combo
    combo_set = set()
    for m in pdf["weekly_modifiers"]:
        if m is not None and not (isinstance(m, float) and __import__("math").isnan(m)):
            combo_set.add(tuple(sorted(m)))
    print(f"\n  Combinaciones de afijos unicas: {len(combo_set)}")
    combo_counts = Counter()
    for m in pdf["weekly_modifiers"]:
        key = tuple(sorted(m)) if isinstance(m, list) else ()
        combo_counts[key] += 1
    for combo, cnt in combo_counts.most_common(5):
        names = [affix_names.get(a, f"?{a}") for a in combo]
        print(f"    {', '.join(names):45s} {cnt:>4} runs")

    # Impact of Tyrannical vs Fortified
    def has_affix(mods, aff_id):
        return isinstance(mods, list) and aff_id in mods
    tyrannical_runs = pdf[pdf["weekly_modifiers"].apply(lambda m: has_affix(m, 9))]
    fortified_runs = pdf[pdf["weekly_modifiers"].apply(lambda m: has_affix(m, 10))]
    if len(tyrannical_runs) > 0 and len(fortified_runs) > 0:
        tyr_mean = tyrannical_runs["clear_time_ms"].mean() / 60000
        fort_mean = fortified_runs["clear_time_ms"].mean() / 60000
        print(f"\n  Tyrannical: {len(tyrannical_runs)} runs, media {tyr_mean:.1f}min")
        print(f"  Fortified:  {len(fortified_runs)} runs, media {fort_mean:.1f}min")
        diff = ((tyr_mean - fort_mean) / fort_mean) * 100
        print(f"  Diferencia: {diff:+.1f}% ({'Tyran mas lento' if diff > 0 else 'Fort mas lento'})")

    # ── 4. TEAM COMPOSITIONS ──
    print(f"\n{'─' * 62}")
    print("  4. COMPOSICIONES DE EQUIPO")
    print(f"{'─' * 62}")

    # Build comps per run
    comps = defaultdict(lambda: {"tank": "", "healer": "", "dps": []})
    for p in players:
        role = p.get("role", "?")
        cls = p.get("class", "?")
        spec = p.get("spec", "?")
        run_id = p["run_id"]
        key = (p["dungeon"], p["mythic_level"], run_id)
        if role == "tank":
            comps[key]["tank"] = f"{cls}-{spec}"
        elif role == "healer":
            comps[key]["healer"] = f"{cls}-{spec}"
        elif role == "dps":
            comps[key]["dps"].append(f"{cls}-{spec}")

    # Top tank classes
    tank_classes = Counter()
    for c in comps.values():
        if c["tank"]:
            tank_classes[c["tank"]] += 1
    print("  Top 10 tanks (clase-spec):")
    for t, cnt in tank_classes.most_common(10):
        print(f"    {t:30s} {cnt:>4} runs ({cnt/len(comps)*100:.1f}%)")

    # Top healer specs
    healer_classes = Counter()
    for c in comps.values():
        if c["healer"]:
            healer_classes[c["healer"]] += 1
    print("\n  Top 10 healers (clase-spec):")
    for h, cnt in healer_classes.most_common(10):
        print(f"    {h:30s} {cnt:>4} runs ({cnt/len(comps)*100:.1f}%)")

    # Full 5-player comps
    full_comps = Counter()
    for c in comps.values():
        if c["tank"] and c["healer"] and len(c["dps"]) == 3:
            dps_sorted = tuple(sorted(c["dps"]))
            comp_str = f"{c['tank']} + {c['healer']} + {', '.join(dps_sorted)}"
            full_comps[comp_str] += 1

    print("\n  Top 10 composiciones completas (tank + healer + 3dps):")
    for comp_str, cnt in full_comps.most_common(10):
        print(f"    {cnt:>3} runs  {comp_str}")

    # ── 5. CORRELATIONS ──
    print(f"\n{'─' * 62}")
    print("  5. CORRELACIONES RAPIDAS")
    print(f"{'─' * 62}")
    print(f"  mythic_level vs clear_time: {pdf['mythic_level'].corr(pdf['clear_time_ms']):+.3f}")
    print(f"  score vs clear_time:        {pdf['score'].corr(pdf['clear_time_ms']):+.3f}")
    print(f"  rank vs clear_time:         {pdf['rank'].corr(pdf['clear_time_ms']):+.3f}")

    spark.stop()
    print("\n✅ Exploracion completa")


if __name__ == "__main__":
    main()
