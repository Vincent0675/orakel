"""Pure Python KPI computation functions — no Spark dependencies.

All functions are pure (no I/O) and can be used directly or as Spark UDFs.
They follow the specification in the initial-architecture spec:
    - KPI 1: Tank Death Clock
    - KPI 2: Healer Deficit
    - KPI 3: Interrupt Success Rate
    - KPI 4: Composition Synergy Score
"""

from __future__ import annotations


# ─── KPI 1: Tank Death Clock ─────────────────────────────────────────────────

def compute_death_clock(
    dtps: float,
    hps_on_tank: float,
    max_hp: float,
) -> tuple[float, str]:
    """Compute how many seconds a tank can survive without healing.

    Formula: ``DeathClock = max_hp / (dtps - hps_on_tank)``

    If DTPS <= HPS_on_tank (healer out-heals damage), the tank survives
    indefinitely → returns ``(-1.0, "safe")``.

    Categories:
        - ``safe``      → Death Clock > 15s
        - ``moderate``  → Death Clock 5-15s
        - ``critical``  → Death Clock < 5s

    Args:
        dtps: Damage Taken Per Second on the tank.
        hps_on_tank: Healing Per Second received by the tank.
        max_hp: Tank's max HP (EHP estimate for MVP).

    Returns:
        Tuple of (death_clock_seconds, category).
        Sentinel value (-1.0, "safe") when DTPS <= HPS_on_tank.
    """
    if dtps <= 0:
        return (-1.0, "safe")

    net_dps = dtps - hps_on_tank

    # Healer out-heals damage → infinite survival
    if net_dps <= 0:
        return (-1.0, "safe")

    seconds = max_hp / net_dps

    if seconds > 15:
        category = "safe"
    elif seconds >= 5:
        category = "moderate"
    else:
        category = "critical"

    return (round(seconds, 2), category)


# ─── KPI 2: Healer Deficit ───────────────────────────────────────────────────

def compute_healer_deficit(
    tank_dtps: float,
    healer_hps: float,
) -> tuple[float, str]:
    """Compute the healer deficit ratio.

    Formula: ``Deficit = tank_dtps / healer_hps``

    Categories:
        - ``comfortable`` → ratio < 1.0 (healer can keep up)
        - ``moderate``    → ratio 1.0-1.2 (healer barely managing)
        - ``critical``   → ratio > 1.2 (healer overwhelmed)

    Args:
        tank_dtps: Damage Taken Per Second on the tank.
        healer_hps: Healer's HPS on the tank.

    Returns:
        Tuple of (deficit_ratio, category).
    """
    if tank_dtps <= 0 or healer_hps <= 0:
        # Zero or negative DTPS/HPS produces NaN or inf — return None instead
        return (None, None)

    ratio = tank_dtps / healer_hps

    if ratio < 1.0:
        category = "comfortable"
    elif ratio <= 1.2:
        category = "moderate"
    else:
        category = "critical"

    return (round(ratio, 4), category)


# ─── KPI 3: Interrupt Success Rate ────────────────────────────────────────────

def compute_interrupt_rate(successful: int, total: int) -> float | None:
    """Compute the interrupt success rate.

    Formula: ``ISR = successful / total``

    Edge case: if ``total`` is 0, returns ``None`` (player didn't attempt
    any interrupts — not 0, which would imply they failed).

    Args:
        successful: Number of successful interrupts.
        total: Total number of interrupt casts attempted.

    Returns:
        Float rate (0.0-1.0) or None if total is 0.
    """
    if total <= 0:
        return None

    rate = successful / total
    # Clamp to [0, 1] in case of edge-case data
    return round(min(max(rate, 0.0), 1.0), 4)


# ─── KPI 4: Composition Synergy Score ─────────────────────────────────────────

def compute_synergy_score(
    comp_avg: float,
    overall_avg: float,
) -> float | None:
    """Compute the composition synergy score.

    Formula: ``Synergy(comp) = avg_clear_time(comp) / avg_clear_time(all_comps)``

    A score < 1.0 means the comp clears faster than average (synergy).
    A score > 1.0 means the comp clears slower than average.

    Returns None if either input is non-positive or zero (insufficient sample).

    Args:
        comp_avg: Average clear time for this specific composition.
        overall_avg: Average clear time across all compositions in the
            same (dungeon, key_level, affix_ids) group.

    Returns:
        Rounded synergy score or None if insufficient data.
    """
    if comp_avg is None or overall_avg is None:
        return None

    if overall_avg <= 0:
        return None

    return round(comp_avg / overall_avg, 4)