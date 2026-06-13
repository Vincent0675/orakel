"""ML feature and target column definitions.

Defines the canonical column lists used by feature engineering (features.py),
model training (trainer.py), and prediction (predict.py).

Key design decisions:
    - death_clock_seconds is log1p-transformed in the feature view (skewed)
    - comp_signature uses role count encoding (num_tanks, num_healers, num_dps)
      instead of one-hot, per user decision
    - dungeon_id uses one-hot encoding (TWW Season 3: 8 dungeons)
    - affix_ids uses multi-hot binary flags (TWW Season 3: 16 affixes)
    - Z-score normalization is applied at training time (not in feature view)
      to avoid data leakage from the test set
"""

from __future__ import annotations

# ─── Target ──────────────────────────────────────────────────────────────────────

TARGET_COLUMN = "clear_time_seconds"

# ─── Numeric Features ────────────────────────────────────────────────────────────

# Raw numeric features available in the feature view (pre-normalization)
RAW_NUMERIC_FEATURES = [
    "key_level",
    "death_clock_seconds",
    "deficit_ratio",
    "interrupts_per_minute",
    "synergy_score",
]

# Features that receive a log1p transform in the feature view (skewed distributions)
LOG1P_FEATURES = ["death_clock_seconds"]

# Final numeric feature names after transforms (used by trainer and predict)
# death_clock_seconds → death_clock_seconds_log1p
NUMERIC_FEATURES = [
    "key_level",
    "death_clock_seconds_log1p",
    "deficit_ratio",
    "interrupts_per_minute",
    "synergy_score",
]

# ─── Role Count Features (count encoding by role combo) ──────────────────────────

ROLE_COUNT_FEATURES = ["num_tanks", "num_healers", "num_dps"]

# ─── Affix Binary Flags ──────────────────────────────────────────────────────────

# TWW Season 3 affix IDs (from gold.py _TWW3_AFFIXES)
AFFIX_IDS = [
    1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 122, 123, 124,
]

# Column names for affix binary flags
AFFIX_COLUMNS = [f"affix_{aid}" for aid in AFFIX_IDS]

# ─── Dungeon One-Hot Encoding ────────────────────────────────────────────────────

# TWW Season 3 dungeon IDs (from gold.py _TWW3_DUNGEONS)
DUNGEON_IDS = [15093, 16104, 12831, 15452, 14954, 1000001, 1000000, 14971]

# Column names for dungeon one-hot flags
DUNGEON_COLUMNS = [f"dungeon_{did}" for did in DUNGEON_IDS]

# ─── Complete Feature List ───────────────────────────────────────────────────────

# All feature columns in canonical order (used by trainer and predict)
FEATURE_COLUMNS = NUMERIC_FEATURES + ROLE_COUNT_FEATURES + AFFIX_COLUMNS + DUNGEON_COLUMNS

# ─── Metadata Columns ───────────────────────────────────────────────────────────

# Columns present in the feature view but NOT model features
METADATA_COLUMNS = ["run_id", "season", "key_level", "completed_at"]