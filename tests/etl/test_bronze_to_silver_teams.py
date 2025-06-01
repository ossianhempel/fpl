import pytest
import pandas as pd

from src.etl_pipeline.components.bronze_to_silver_teams import (
    transform_teams,
    rename_teams_columns,
)


@pytest.fixture
def test_teams_dataframe() -> pd.DataFrame:
    # this data is based on the first 3 rows of data/teams/teams_2023_24.csv (see file_context_1)
    # the 'season' column is manually added here because the transformation functions
    # expect it to be present (it's typically added during or after fetch_bronze_data).
    data = {
        "code": [3, 7, 91],  # original column, will be renamed to team_code
        "draw": [0, 0, 0],
        "form": ["", "", ""],  # csv file shows empty strings for these initial records
        "id": [1, 2, 3],  # original column, will be renamed to seasonal_team_id
        "loss": [0, 0, 0],
        "name": [
            "Arsenal",
            "Aston Villa",
            "Bournemouth",
        ],  # original column, will be renamed to team_name
        "played": [0, 0, 0],
        "points": [0, 0, 0],
        "position": [
            0,
            0,
            0,
        ],  # original column, will be renamed to league_table_position
        "short_name": ["ARS", "AVL", "BOU"],
        "strength": [5, 4, 3],
        "team_division": ["", "", ""],  # csv file shows empty strings
        "unavailable": [False, False, False],
        "win": [0, 0, 0],
        "strength_overall_home": [1350, 1160, 1100],
        "strength_overall_away": [1365, 1285, 1100],
        "strength_attack_home": [1370, 1140, 1055],
        "strength_attack_away": [1370, 1220, 1130],
        "strength_defence_home": [1330, 1180, 1145],
        "strength_defence_away": [1360, 1350, 1075],
        "pulse_id": [1, 2, 127],
        "season": [
            "2023-24",
            "2023-24",
            "2023-24",
        ],  # added column, crucial for transformations and partitioning
    }
    df = pd.DataFrame(data)
    return df


@pytest.fixture
def test_transformed_teams_dataframe(
    test_teams_dataframe: pd.DataFrame,
) -> pd.DataFrame:
    return transform_teams(test_teams_dataframe)


def test_rename_teams_columns(test_teams_dataframe: pd.DataFrame) -> None:
    renamed_df = rename_teams_columns(test_teams_dataframe)
    expected_columns = {
        "team_code",
        "team_name",
        "league_table_position",
        "seasonal_team_id",
    }
    # check that original names are not present and new names are
    assert "code" not in renamed_df.columns
    assert "name" not in renamed_df.columns
    assert "position" not in renamed_df.columns
    assert "id" not in renamed_df.columns
    assert expected_columns.issubset(renamed_df.columns)


def test_transform_teams_output_columns(
    test_transformed_teams_dataframe: pd.DataFrame,
) -> None:
    # this test checks the columns after all transformations
    # including renaming, dropping unnecessary, and removing duplicate columns.
    # 'modified' should be dropped.
    # original names like 'code', 'id', 'name', 'position' should be replaced
    transformed_df = test_transformed_teams_dataframe
    assert "modified" not in transformed_df.columns
    assert "code" not in transformed_df.columns
    assert "id" not in transformed_df.columns

    expected_final_columns = [
        "team_code",
        "draw",
        "form",
        "seasonal_team_id",
        "loss",
        "team_name",
        "played",
        "points",
        "league_table_position",
        "short_name",
        "strength",
        "team_division",
        "unavailable",
        "win",
        "strength_overall_home",
        "strength_overall_away",
        "strength_attack_home",
        "strength_attack_away",
        "strength_defence_home",
        "strength_defence_away",
        "pulse_id",
        "season",
    ]
    assert all(col in transformed_df.columns for col in expected_final_columns)
    assert len(transformed_df.columns) == len(expected_final_columns)


def test_transform_teams_critical_column_types(
    test_transformed_teams_dataframe: pd.DataFrame,
) -> None:
    df = test_transformed_teams_dataframe
    assert df["seasonal_team_id"].dtype == pd.Int64Dtype()
    assert df["team_name"].dtype == "object"  # pandas uses 'object' for strings
    assert df["season"].dtype == "object"


def test_transform_teams_no_duplicates(
    test_transformed_teams_dataframe: pd.DataFrame,
) -> None:
    assert not test_transformed_teams_dataframe.duplicated().any()


def test_transform_teams_with_duplicates(
    test_teams_dataframe: pd.DataFrame,
) -> None:
    # create a dataframe with duplicate rows
    duplicated_df = pd.concat(
        [test_teams_dataframe, test_teams_dataframe.head(1)], ignore_index=True
    )
    assert duplicated_df.duplicated().any()  # ensure test setup is correct

    transformed_df = transform_teams(duplicated_df)
    assert not transformed_df.duplicated().any()
    # original df has 3 rows, duplicated_df has 4 (3 unique + 1 dupe)
    # transformed_df should have 3 rows after dupe removal
    assert len(transformed_df) == len(test_teams_dataframe)
