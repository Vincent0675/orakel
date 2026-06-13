"""Tests for orakel.ml.schemas — column constants and feature lists (Tier 1).

Validates that the canonical column definitions match the project's
TWW Season 3 design (8 dungeons, 16 affixes, role count encoding).

References:
    - orakel/ml/schemas.py — actual column constants
    - orakel/ml/trainer.py — uses FEATURE_COLUMNS
    - orakel/ml/predict.py — uses FEATURE_COLUMNS
    - orakel/ml/features.py — builds the feature view from these constants
"""

from __future__ import annotations

from orakel.ml.schemas import (
    AFFIX_COLUMNS,
    AFFIX_IDS,
    DUNGEON_COLUMNS,
    DUNGEON_IDS,
    FEATURE_COLUMNS,
    LOG1P_FEATURES,
    METADATA_COLUMNS,
    NUMERIC_FEATURES,
    RAW_NUMERIC_FEATURES,
    ROLE_COUNT_FEATURES,
    TARGET_COLUMN,
)


class TestTargetColumn:
    """TARGET_COLUMN must be the regression target used by trainer/predict."""

    def test_target_column_is_clear_time_seconds(self):
        assert TARGET_COLUMN == "clear_time_seconds"

    def test_target_column_not_in_feature_columns(self):
        """Target is the label, not a feature — trainer excludes it from X."""
        assert TARGET_COLUMN not in FEATURE_COLUMNS


class TestNumericFeatures:
    """NUMERIC_FEATURES lists the columns that get z-score normalized."""

    def test_numeric_features_is_non_empty(self):
        assert len(NUMERIC_FEATURES) > 0

    def test_numeric_features_all_strings(self):
        assert all(isinstance(name, str) for name in NUMERIC_FEATURES)

    def test_numeric_features_subset_of_feature_columns(self):
        assert set(NUMERIC_FEATURES).issubset(set(FEATURE_COLUMNS))

    def test_raw_numeric_features_contains_log1p_source(self):
        """RAW_NUMERIC_FEATURES is the pre-transform list — death_clock_seconds is the
        source for the log1p transform."""
        assert "death_clock_seconds" in RAW_NUMERIC_FEATURES

    def test_log1p_features_source_in_raw_numeric(self):
        """Every column in LOG1P_FEATURES must come from RAW_NUMERIC_FEATURES."""
        for col in LOG1P_FEATURES:
            assert col in RAW_NUMERIC_FEATURES

    def test_numeric_features_contains_log1p_result(self):
        """After the log1p transform, the new column name appears in NUMERIC_FEATURES."""
        log1p_results = [f"{c}_log1p" for c in LOG1P_FEATURES]
        for col in log1p_results:
            assert col in NUMERIC_FEATURES


class TestRoleCountFeatures:
    """ROLE_COUNT_FEATURES — count encoding for comp_signature."""

    def test_role_count_features_count(self):
        assert len(ROLE_COUNT_FEATURES) == 3

    def test_role_count_features_names(self):
        assert set(ROLE_COUNT_FEATURES) == {"num_tanks", "num_healers", "num_dps"}

    def test_role_count_features_subset_of_feature_columns(self):
        assert set(ROLE_COUNT_FEATURES).issubset(set(FEATURE_COLUMNS))


class TestAffixColumns:
    """AFFIX_COLUMNS — TWW Season 3 has 16 affixes, one-hot encoded as binary flags."""

    def test_affix_columns_count_is_16(self):
        assert len(AFFIX_COLUMNS) == 16

    def test_affix_ids_count_matches_columns(self):
        assert len(AFFIX_IDS) == len(AFFIX_COLUMNS)

    def test_affix_columns_names_match_ids(self):
        """Each affix_id must produce a column name 'affix_{id}'."""
        assert AFFIX_COLUMNS == [f"affix_{aid}" for aid in AFFIX_IDS]

    def test_affix_columns_subset_of_feature_columns(self):
        assert set(AFFIX_COLUMNS).issubset(set(FEATURE_COLUMNS))

    def test_affix_ids_unique(self):
        assert len(set(AFFIX_IDS)) == len(AFFIX_IDS)


class TestDungeonColumns:
    """DUNGEON_COLUMNS — TWW Season 3 has 8 dungeons, one-hot encoded."""

    def test_dungeon_columns_count_is_8(self):
        assert len(DUNGEON_COLUMNS) == 8

    def test_dungeon_ids_count_matches_columns(self):
        assert len(DUNGEON_IDS) == len(DUNGEON_COLUMNS)

    def test_dungeon_columns_names_match_ids(self):
        """Each dungeon_id must produce a column name 'dungeon_{id}'."""
        assert DUNGEON_COLUMNS == [f"dungeon_{did}" for did in DUNGEON_IDS]

    def test_dungeon_columns_subset_of_feature_columns(self):
        assert set(DUNGEON_COLUMNS).issubset(set(FEATURE_COLUMNS))

    def test_dungeon_ids_unique(self):
        assert len(set(DUNGEON_IDS)) == len(DUNGEON_IDS)


class TestFeatureColumns:
    """FEATURE_COLUMNS — the canonical ordered list passed to trainer and predict."""

    def test_feature_columns_is_union(self):
        """FEATURE_COLUMNS = NUMERIC + ROLE_COUNT + AFFIX + DUNGEON (in that order)."""
        expected = (
            NUMERIC_FEATURES
            + ROLE_COUNT_FEATURES
            + AFFIX_COLUMNS
            + DUNGEON_COLUMNS
        )
        assert FEATURE_COLUMNS == expected

    def test_feature_columns_count(self):
        """Total: 5 numeric + 3 role + 16 affix + 8 dungeon = 32."""
        assert len(FEATURE_COLUMNS) == 32

    def test_feature_columns_no_duplicates(self):
        assert len(set(FEATURE_COLUMNS)) == len(FEATURE_COLUMNS)


class TestMetadataColumns:
    """METADATA_COLUMNS — columns in the feature view that are NOT model features.

    Note: ``key_level`` is BOTH a metadata column AND a numeric feature.
    That is intentional — it carries business meaning (key difficulty) but
    is also z-score normalized for the model.
    """

    def test_metadata_columns_defined(self):
        assert len(METADATA_COLUMNS) > 0

    def test_run_id_is_metadata(self):
        """run_id identifies the run, not used as a model feature."""
        assert "run_id" in METADATA_COLUMNS

    def test_completed_at_is_metadata(self):
        """completed_at is used for temporal splitting, not as a feature."""
        assert "completed_at" in METADATA_COLUMNS

    def test_season_is_metadata(self):
        assert "season" in METADATA_COLUMNS

    def test_metadata_includes_run_id_and_completed_at(self):
        """These two are the minimum — splitter and result identifier."""
        assert "run_id" in METADATA_COLUMNS
        assert "completed_at" in METADATA_COLUMNS
