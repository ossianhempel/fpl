import pytest
import pandas as pd

from src.etl_pipeline.components.bronze_to_silver_utils import (
    determine_season,
    add_season_column,
    validate_expected_columns,
    validate_important_columns,
    validate_key_columns,
    remove_dupes,
    assert_strictly_sequential_values,
    assert_continuous_sequential_values,
    assert_accepted_ranges,
    merge_dataframes,
)


@pytest.fixture
def fake_dataframe() -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "date": [
                "2024-03-01",
                "2024-03-09",
                "2024-05-10",
                "2023-05-11",
                "2024-03-14",
            ],
            "gw": [1, 2, 9, 23, 2],
            "player_id": [1, 2, 3, 4, 1],
            "season": ["2024-25", "2024-25", "2024-25", "2023-24", "2024-25"],
            "score": [1, 3, 9, 3, 2],
            "row_id": [1, 2, 3, 4, 5],
        }
    )

    return df


@pytest.fixture
def fake_dataframe_with_nulls() -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "date": [
                "2024-03-01",
                "2024-03-09",
                "2024-05-10",
                "2023-05-11",
                "2024-03-14",
            ],
            "gw": [1, 2, pd.NA, 23, 2],
            "player_id": [1, 2, 3, 4, 1],
            "season": ["2024-25", "2024-25", "2024-25", "2023-24", "2024-25"],
            "score": [1, 3, 9, 3, 2],
            "row_id": [1, 2, 3, 4, pd.NA],
        }
    )

    return df


@pytest.fixture
def fake_dataframe_with_dupes() -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "date": [
                "2024-03-01",
                "2024-03-09",
                "2024-03-09",
                "2023-05-11",
                "2024-03-14",
            ],
            "gw": [1, 2, 2, 23, 2],
            "player_id": [1, 2, 2, 4, 1],
            "season": ["2024-25", "2024-25", "2024-25", "2023-24", "2024-25"],
            "score": [1, 3, 3, 3, 2],
            "row_id": [1, 2, 2, 4, 5],
        }
    )

    return df


def test_validate_expected_columns(fake_dataframe: pd.DataFrame) -> None:
    expected_cols_1 = ["date", "gw", "season"]
    assert (
        validate_expected_columns(
            dataframe=fake_dataframe, expected_columns=expected_cols_1
        )
        is True
    )

    expected_cols_2 = ["gw", "season", "column_not_in_df"]
    assert (
        validate_expected_columns(
            dataframe=fake_dataframe, expected_columns=expected_cols_2
        )
        is False
    )

    expected_cols_3 = ["gw", "date", "season"]
    assert (
        validate_expected_columns(
            dataframe=fake_dataframe, expected_columns=expected_cols_3
        )
        is True
    )


def test_validate_important_columns(
    fake_dataframe: pd.DataFrame, fake_dataframe_with_nulls: pd.DataFrame
) -> None:
    assert (
        validate_important_columns(dataframe=fake_dataframe, important_columns=["date"])
        is True
    )
    assert (
        validate_important_columns(
            dataframe=fake_dataframe_with_nulls, important_columns=["row_id"]
        )
        is False
    )


def test_validate_key_columns(fake_dataframe: pd.DataFrame) -> None:
    key_cols_1 = ["date", "player_id"]
    assert (
        validate_key_columns(
            dataframe=fake_dataframe, key_columns=key_cols_1, composite_key=True
        )
        is True
    )

    key_cols_2 = ["player_id", "gw"]
    assert (
        validate_key_columns(
            dataframe=fake_dataframe, key_columns=key_cols_2, composite_key=True
        )
        is True
    )

    key_cols_3 = ["row_id"]
    assert (
        validate_key_columns(
            dataframe=fake_dataframe, key_columns=key_cols_3, composite_key=False
        )
        is True
    )

    key_cols_4 = ["player_id"]
    assert (
        validate_key_columns(dataframe=fake_dataframe, key_columns=key_cols_4) is False
    )


def test_remove_dupes(fake_dataframe_with_dupes: pd.DataFrame) -> None:
    deduped = remove_dupes(fake_dataframe_with_dupes)
    assert (
        deduped.duplicated().sum() == 0
    ), "Found duplicates in supposedly deduped dataframe"


def test_assert_strictly_sequential_values(fake_dataframe: pd.DataFrame) -> None:
    assert assert_strictly_sequential_values(fake_dataframe, "row_id", 1) is True
    assert assert_strictly_sequential_values(fake_dataframe, "gw", 1) is False


def test_assert_continuous_sequential_values(fake_dataframe: pd.DataFrame) -> None:
    assert assert_continuous_sequential_values(fake_dataframe, "score") is False
    assert assert_continuous_sequential_values(fake_dataframe, "row_id") is True


def test_assert_accepted_ranges(fake_dataframe: pd.DataFrame) -> None:
    assert (
        assert_accepted_ranges(fake_dataframe, "gw", 39, 1) is True
    ), "Expected True but got False, for GW"
    assert (
        assert_accepted_ranges(fake_dataframe, "row_id", 3, 3) is False
    ), "row_id test expected false but got true"
    assert (
        assert_accepted_ranges(fake_dataframe, "jkkjökjö", 3, 3) is False
    ), "Column didn't exist in df but still returned True"


def test_merge_dataframes(
    fake_dataframe: pd.DataFrame,
    fake_dataframe_with_nulls: pd.DataFrame,
    fake_dataframe_with_dupes: pd.DataFrame,
) -> None:
    test_dict = {
        "fake_df": fake_dataframe,
        "fake_df_nulls": fake_dataframe_with_nulls,
        "fake_df_dupes": fake_dataframe_with_dupes,
    }

    merged_df = merge_dataframes(test_dict)

    assert isinstance(merged_df, pd.DataFrame)
    assert merged_df.isnull().sum().sum() > 0
    assert merged_df.duplicated().sum() > 0


class TestDetermineSeason:
    """Test cases for the determine_season function"""

    def test_determine_season_summer_date(self) -> None:
        """test that a summer date (july onwards) returns current year season"""
        # july 15, 2024 should return "2024-25"
        date = pd.Timestamp("2024-07-15")
        result = determine_season(date)
        assert result == "2024-25"

    def test_determine_season_autumn_date(self) -> None:
        """test that an autumn date returns current year season"""
        # november 10, 2024 should return "2024-25"
        date = pd.Timestamp("2024-11-10")
        result = determine_season(date)
        assert result == "2024-25"

    def test_determine_season_winter_date(self) -> None:
        """test that a winter date returns previous year season"""
        # january 15, 2024 should return "2023-24"
        date = pd.Timestamp("2024-01-15")
        result = determine_season(date)
        assert result == "2023-24"

    def test_determine_season_spring_date(self) -> None:
        """test that a spring date returns previous year season"""
        # april 20, 2024 should return "2023-24"
        date = pd.Timestamp("2024-04-20")
        result = determine_season(date)
        assert result == "2023-24"

    def test_determine_season_july_first(self) -> None:
        """test edge case of july 1st (start of season)"""
        # july 1, 2024 should return "2024-25"
        date = pd.Timestamp("2024-07-01")
        result = determine_season(date)
        assert result == "2024-25"

    def test_determine_season_june_thirtieth(self) -> None:
        """test edge case of june 30th (end of previous season)"""
        # june 30, 2024 should return "2023-24"
        date = pd.Timestamp("2024-06-30")
        result = determine_season(date)
        assert result == "2023-24"

    def test_determine_season_december_date(self) -> None:
        """test that december date returns current year season"""
        # december 31, 2024 should return "2024-25"
        date = pd.Timestamp("2024-12-31")
        result = determine_season(date)
        assert result == "2024-25"

    def test_determine_season_with_null_date(self) -> None:
        """test that null/na date returns none"""
        result = determine_season(pd.NaT)
        assert result is None

    def test_determine_season_different_year(self) -> None:
        """test with a different year to ensure year calculation is correct"""
        # august 15, 2023 should return "2023-24"
        date = pd.Timestamp("2023-08-15")
        result = determine_season(date)
        assert result == "2023-24"

        # march 10, 2023 should return "2022-23"
        date = pd.Timestamp("2023-03-10")
        result = determine_season(date)
        assert result == "2022-23"


class TestAddSeasonColumn:
    """Test cases for the add_season_column function"""

    @pytest.fixture
    def sample_dataframe(self) -> pd.DataFrame:
        """create a sample dataframe with kickoff times"""
        return pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "kickoff_time": [
                    pd.Timestamp("2024-08-17"),  # summer - should be 2024-25
                    pd.Timestamp("2024-01-15"),  # winter - should be 2023-24
                    pd.Timestamp("2024-11-10"),  # autumn - should be 2024-25
                    pd.Timestamp("2024-05-20"),  # spring - should be 2023-24
                    pd.NaT,  # null - should be none
                ],
                "team_h": [1, 2, 3, 4, 5],
                "team_a": [6, 7, 8, 9, 10],
            }
        )

    def test_add_season_column_default_params(
        self, sample_dataframe: pd.DataFrame
    ) -> None:
        """test adding season column with default parameters"""
        result_df = add_season_column(sample_dataframe.copy())

        # check that season column was added
        assert "season" in result_df.columns

        # check expected season values
        expected_seasons = ["2024-25", "2023-24", "2024-25", "2023-24", None]
        assert result_df["season"].tolist() == expected_seasons

    def test_add_season_column_custom_column_names(
        self, sample_dataframe: pd.DataFrame
    ) -> None:
        """test adding season column with custom column names"""
        df = sample_dataframe.copy()
        df = df.rename(columns={"kickoff_time": "match_date"})

        result_df = add_season_column(
            df, season_column_name="season_year", date_column_name="match_date"
        )

        # check that custom season column was added
        assert "season_year" in result_df.columns
        assert "season" not in result_df.columns

        # check expected season values
        expected_seasons = ["2024-25", "2023-24", "2024-25", "2023-24", None]
        assert result_df["season_year"].tolist() == expected_seasons

    def test_add_season_column_preserves_original_data(
        self, sample_dataframe: pd.DataFrame
    ) -> None:
        """test that original dataframe columns are preserved"""
        original_df = sample_dataframe.copy()
        result_df = add_season_column(original_df)

        # check that all original columns are still there
        for col in sample_dataframe.columns:
            assert col in result_df.columns
            # check that original data is unchanged
            pd.testing.assert_series_equal(
                sample_dataframe[col], result_df[col], check_names=True
            )

    def test_add_season_column_all_summer_dates(self) -> None:
        """test with dataframe containing only summer/autumn dates"""
        df = pd.DataFrame(
            {
                "kickoff_time": [
                    pd.Timestamp("2024-07-01"),
                    pd.Timestamp("2024-08-15"),
                    pd.Timestamp("2024-12-25"),
                ],
                "match_id": [1, 2, 3],
            }
        )

        result_df = add_season_column(df)
        expected_seasons = ["2024-25", "2024-25", "2024-25"]
        assert result_df["season"].tolist() == expected_seasons

    def test_add_season_column_all_winter_spring_dates(self) -> None:
        """test with dataframe containing only winter/spring dates"""
        df = pd.DataFrame(
            {
                "kickoff_time": [
                    pd.Timestamp("2024-01-01"),
                    pd.Timestamp("2024-03-15"),
                    pd.Timestamp("2024-06-30"),
                ],
                "match_id": [1, 2, 3],
            }
        )

        result_df = add_season_column(df)
        expected_seasons = ["2023-24", "2023-24", "2023-24"]
        assert result_df["season"].tolist() == expected_seasons

    def test_add_season_column_empty_dataframe(self) -> None:
        """test with empty dataframe"""
        df = pd.DataFrame(columns=["kickoff_time", "match_id"])
        result_df = add_season_column(df)

        assert "season" in result_df.columns
        assert len(result_df) == 0

    def test_add_season_column_single_row(self) -> None:
        """test with single row dataframe"""
        df = pd.DataFrame(
            {
                "kickoff_time": [pd.Timestamp("2024-09-15")],
                "match_id": [1],
            }
        )

        result_df = add_season_column(df)
        assert result_df["season"].tolist() == ["2024-25"]
