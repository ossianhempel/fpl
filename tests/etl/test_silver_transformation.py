import pytest
import pandas as pd

from src.etl_pipeline.components.silver_transformation import (
    validate_expected_columns,
    validate_important_columns,
    validate_key_columns,
    remove_dupes,
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


def test_assert_strictly_sequential_values():
    pass


def test_assert_continuous_sequential_values():
    pass


def test_assert_accepted_ranges():
    pass


def test_merge_dataframes():
    pass
