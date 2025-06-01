import pytest
import pandas as pd

from src.etl_pipeline.components.bronze_to_silver_fixtures import (
    transform_fixtures,
    map_team_names,
)
from src.etl_pipeline.components.bronze_to_silver_teams import (
    transform_teams,
)


@pytest.fixture
def test_fixtures_dataframe() -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "id": [2444564, 2444561, 2444570, 2444573, 2444578],
            "event": [10.0, 10.0, 11.0, 11.0, 11.0],
            "finished": [True, True, True, True, True],
            "finished_provisional": [True, True, True, True, True],
            "code": [95, 92, 101, 104, 109],
            "kickoff_time": [
                "2024-11-03T16:30:00Z",
                "2024-11-04T20:00:00Z",
                "2024-11-09T15:00:00Z",
                "2024-11-09T15:00:00Z",
                "2024-11-09T15:00:00Z",
            ],
            "minutes": [90, 90, 90, 90, 90],
            "provisional_start_time": [False, False, False, False, False],
            "started": [True, True, True, True, True],
            "team_a": [6, 4, 3, 9, 8],
            "team_a_score": [1.0, 1.0, 2.0, 2.0, 0.0],
            "team_h": [14, 9, 4, 7, 19],
            "team_h_score": [1.0, 2.0, 3.0, 0.0, 0.0],
            "stats": [
                "[{'identifier': 'goals_scored', 'a': [{'value': 1, 'element': 157}], 'h': [{'value': 1, 'element': 366}]}]",
                "[{'identifier': 'goals_scored', 'a': [{'value': 1, 'element': 94}], 'h': [{'value': 2, 'element': 259}]}]",
                "[{'identifier': 'goals_scored', 'a': [{'value': 1, 'element': 71}, {'value': 1, 'element': 617}], 'h': [{'value': 2, 'element': 110}, {'value': 1, 'element': 89}]}]",
                "[{'identifier': 'goals_scored', 'a': [{'value': 1, 'element': 19}, {'value': 1, 'element': 259}], 'h': []}]",
                "[{'identifier': 'goals_scored', 'a': [], 'h': []}]",
            ],
        }
    )

    return df


@pytest.fixture
def test_fixtures_dataframe_with_nulls() -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "id": [2444564, 2444561, 2444570, 2444573, 2444578, 2444579],
            "event": [10.0, 10.0, 11.0, 11.0, 11.0, 11.0],
            "finished": [True, True, True, True, True, True],
            "finished_provisional": [True, True, True, True, True, True],
            "code": [95, 92, 101, 104, 109, 110],
            "kickoff_time": [
                "2024-11-03T16:30:00Z",
                "2024-11-04T20:00:00Z",
                "2024-11-09T15:00:00Z",
                "2024-11-09T15:00:00Z",
                "2024-11-09T15:00:00Z",
                "2024-11-09T15:00:00Z",
            ],
            "minutes": [90, 90, 90, 90, 90, 90],
            "provisional_start_time": [False, False, False, False, False, False],
            "started": [True, True, True, True, True, True],
            "team_a": [6, 4, 3, 9, 8, 7],
            "team_a_score": [1.0, 1.0, 2.0, 2.0, 0.0, 1.0],
            "team_h": [14, 9, 4, 7, 19, 20],
            "team_h_score": [1.0, 2.0, 3.0, 0.0, 0.0, 2.0],
            "stats": [
                "[{'identifier': 'goals_scored', 'a': [{'value': 1, 'element': 157}], 'h': [{'value': 1, 'element': 366}]}]",
                "[{'identifier': 'goals_scored', 'a': [{'value': 1, 'element': 94}], 'h': [{'value': 2, 'element': 259}]}]",
                "[{'identifier': 'goals_scored', 'a': [{'value': 1, 'element': 71}, {'value': 1, 'element': 617}], 'h': [{'value': 2, 'element': 110}, {'value': 1, 'element': 89}]}]",
                "[{'identifier': 'goals_scored', 'a': [{'value': 1, 'element': 19}, {'value': 1, 'element': 259}], 'h': []}]",
                "[{'identifier': 'goals_scored', 'a': [], 'h': []}]",
                "[{'identifier': 'goals_scored', 'a': [{'value': 1, 'element': 100}], 'h': [{'value': 2, 'element': 200}]}]",
            ],
            "row_id": [1, 2, 3, 4, 5, pd.NA],
        }
    )

    return df


@pytest.fixture
def test_fixtures_dataframe_with_dupes() -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "id": [2444564, 2444561, 2444570, 2444573, 2444578, 2444578],
            "event": [10.0, 10.0, 11.0, 11.0, 11.0, 11.0],
            "finished": [True, True, True, True, True, True],
            "finished_provisional": [True, True, True, True, True, True],
            "code": [95, 92, 101, 104, 109, 109],
            "kickoff_time": [
                "2024-11-03T16:30:00Z",
                "2024-11-04T20:00:00Z",
                "2024-11-09T15:00:00Z",
                "2024-11-09T15:00:00Z",
                "2024-11-09T15:00:00Z",
                "2024-11-09T15:00:00Z",
            ],
            "minutes": [90, 90, 90, 90, 90, 90],
            "provisional_start_time": [False, False, False, False, False, False],
            "started": [True, True, True, True, True, True],
            "team_a": [6, 4, 3, 9, 8, 8],
            "team_a_score": [1.0, 1.0, 2.0, 2.0, 0.0, 0.0],
            "team_h": [14, 9, 4, 7, 19, 19],
            "team_h_score": [1.0, 2.0, 3.0, 0.0, 0.0, 0.0],
            "stats": [
                "[{'identifier': 'goals_scored', 'a': [{'value': 1, 'element': 157}], 'h': [{'value': 1, 'element': 366}]}]",
                "[{'identifier': 'goals_scored', 'a': [{'value': 1, 'element': 94}], 'h': [{'value': 2, 'element': 259}]}]",
                "[{'identifier': 'goals_scored', 'a': [{'value': 1, 'element': 71}, {'value': 1, 'element': 617}], 'h': [{'value': 2, 'element': 110}, {'value': 1, 'element': 89}]}]",
                "[{'identifier': 'goals_scored', 'a': [{'value': 1, 'element': 19}, {'value': 1, 'element': 259}], 'h': []}]",
                "[{'identifier': 'goals_scored', 'a': [], 'h': []}]",
                "[{'identifier': 'goals_scored', 'a': [], 'h': []}]",
            ],
            "row_id": [1, 2, 3, 4, 5, 6],
        }
    )

    return df


@pytest.fixture
def test_teams_dataframe() -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "code": [3, 7, 91, 94, 36, 90, 8, 31, 11, 54],
            "draw": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            "form": ["", "", "", "", "", "", "", "", "", ""],
            "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "loss": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            "name": [
                "Arsenal",
                "Aston Villa",
                "Bournemouth",
                "Brentford",
                "Brighton",
                "Burnley",
                "Chelsea",
                "Crystal Palace",
                "Everton",
                "Fulham",
            ],
            "played": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            "points": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            "position": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            "short_name": [
                "ARS",
                "AVL",
                "BOU",
                "BRE",
                "BHA",
                "BUR",
                "CHE",
                "CRY",
                "EVE",
                "FUL",
            ],
            "strength": [5, 4, 3, 3, 3, 2, 3, 3, 3, 3],
            "team_division": ["", "", "", "", "", "", "", "", "", ""],
            "unavailable": [
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
            ],
            "win": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            "strength_overall_home": [
                1350,
                1160,
                1100,
                1100,
                1100,
                1045,
                1125,
                1070,
                1095,
                1055,
            ],
            "strength_overall_away": [
                1365,
                1285,
                1100,
                1100,
                1210,
                1050,
                1190,
                1100,
                1100,
                1175,
            ],
            "strength_attack_home": [
                1370,
                1140,
                1055,
                1110,
                1070,
                1050,
                1080,
                1080,
                1125,
                1050,
            ],
            "strength_attack_away": [
                1370,
                1220,
                1130,
                1055,
                1180,
                1050,
                1150,
                1120,
                1125,
                1180,
            ],
            "strength_defence_home": [
                1330,
                1180,
                1145,
                1090,
                1135,
                1040,
                1170,
                1060,
                1065,
                1060,
            ],
            "strength_defence_away": [
                1360,
                1350,
                1075,
                1150,
                1240,
                1050,
                1230,
                1085,
                1080,
                1170,
            ],
            "pulse_id": [1, 2, 127, 130, 131, 43, 4, 6, 7, 34],
            "season": [
                "2024-25",
                "2024-25",
                "2024-25",
                "2024-25",
                "2024-25",
                "2024-25",
                "2024-25",
                "2024-25",
                "2024-25",
                "2024-25",
            ],
        }
    )

    return df


@pytest.fixture
def test_transformed_teams_dataframe(
    test_teams_dataframe: pd.DataFrame,
) -> pd.DataFrame:
    return transform_teams(test_teams_dataframe)


@pytest.fixture
def minimal_fixtures_for_mapping() -> pd.DataFrame:
    """fixtures data after column renaming and season addition for testing map_team_names"""
    return pd.DataFrame(
        {
            "gameweek": [10, 11, 12],
            "home_team_id": [3, 6, 9],  # some existing teams
            "away_team_id": [4, 7, 999],  # last one doesn't exist in teams
            "season": ["2024-25", "2024-25", "2024-25"],
            "kickoff_time": [
                "2024-11-03T16:30:00Z",
                "2024-11-04T20:00:00Z",
                "2024-11-05T15:00:00Z",
            ],
        }
    )


@pytest.fixture
def minimal_teams_for_mapping() -> pd.DataFrame:
    """teams data with required columns for mapping after transformation"""
    return pd.DataFrame(
        {
            "seasonal_team_id": [3, 4, 6, 7, 8, 9],
            "team_name": [
                "Bournemouth",
                "Brentford",
                "Burnley",
                "Chelsea",
                "Crystal Palace",
                "Everton",
            ],
            "season": [
                "2024-25",
                "2024-25",
                "2024-25",
                "2024-25",
                "2024-25",
                "2024-25",
            ],
        }
    )


@pytest.fixture
def teams_without_season_column() -> pd.DataFrame:
    """teams data missing the critical season column"""
    return pd.DataFrame(
        {
            "seasonal_team_id": [3, 4, 6, 7],
            "team_name": ["Bournemouth", "Brentford", "Burnley", "Chelsea"],
        }
    )


@pytest.fixture
def empty_teams_dataframe() -> pd.DataFrame:
    """empty teams dataframe"""
    return pd.DataFrame(columns=["seasonal_team_id", "team_name", "season"])


def test_map_team_names_successful_mapping(
    minimal_fixtures_for_mapping: pd.DataFrame, minimal_teams_for_mapping: pd.DataFrame
) -> None:
    """test successful team name mapping when teams data has matching ids and seasons"""
    result = map_team_names(minimal_fixtures_for_mapping, minimal_teams_for_mapping)

    # check that team name columns were added
    assert "home_team_name" in result.columns
    assert "away_team_name" in result.columns

    # check successful mappings for existing teams (first two rows)
    assert result.iloc[0]["home_team_name"] == "Bournemouth"  # team_id 3
    assert result.iloc[0]["away_team_name"] == "Brentford"  # team_id 4
    assert result.iloc[1]["home_team_name"] == "Burnley"  # team_id 6
    assert result.iloc[1]["away_team_name"] == "Chelsea"  # team_id 7

    # check that non-existent team (999) results in NA
    assert pd.isna(result.iloc[2]["away_team_name"])


def test_map_team_names_missing_season_column(
    minimal_fixtures_for_mapping: pd.DataFrame,
    teams_without_season_column: pd.DataFrame,
) -> None:
    """test handling when teams dataframe is missing the season column"""
    result = map_team_names(minimal_fixtures_for_mapping, teams_without_season_column)

    # should add team name columns but with all NA values
    assert "home_team_name" in result.columns
    assert "away_team_name" in result.columns
    assert result["home_team_name"].isna().all()
    assert result["away_team_name"].isna().all()


def test_map_team_names_empty_teams_data(
    minimal_fixtures_for_mapping: pd.DataFrame, empty_teams_dataframe: pd.DataFrame
) -> None:
    """test handling when teams dataframe is empty"""
    result = map_team_names(minimal_fixtures_for_mapping, empty_teams_dataframe)

    # should add team name columns but with all NA values
    assert "home_team_name" in result.columns
    assert "away_team_name" in result.columns
    assert result["home_team_name"].isna().all()
    assert result["away_team_name"].isna().all()


def test_map_team_names_preserves_fixture_data(
    minimal_fixtures_for_mapping: pd.DataFrame, minimal_teams_for_mapping: pd.DataFrame
) -> None:
    """test that original fixture data is preserved during mapping"""
    original_columns = set(minimal_fixtures_for_mapping.columns)
    result = map_team_names(minimal_fixtures_for_mapping, minimal_teams_for_mapping)

    # all original columns should still be present
    for col in original_columns:
        assert col in result.columns

    # original data should be unchanged (check a few key columns)
    assert result["gameweek"].equals(minimal_fixtures_for_mapping["gameweek"])
    assert result["home_team_id"].equals(minimal_fixtures_for_mapping["home_team_id"])
    assert result["away_team_id"].equals(minimal_fixtures_for_mapping["away_team_id"])


# TODO - add test that makes sure all teams are in master team list

# TODO - add test that makes sure there's never the same team in both home and away columns


def test_transform_fixtures(
    test_fixtures_dataframe: pd.DataFrame,
    test_transformed_teams_dataframe: pd.DataFrame,
) -> None:
    assert (
        transform_fixtures(test_fixtures_dataframe, test_transformed_teams_dataframe)
        is not None
    )


def test_transform_fixtures_with_nulls(
    test_fixtures_dataframe_with_nulls: pd.DataFrame,
    test_transformed_teams_dataframe: pd.DataFrame,
) -> None:
    assert (
        transform_fixtures(
            test_fixtures_dataframe_with_nulls, test_transformed_teams_dataframe
        )
        is not None
    )


def test_transform_fixtures_with_dupes(
    test_fixtures_dataframe_with_dupes: pd.DataFrame,
    test_transformed_teams_dataframe: pd.DataFrame,
) -> None:
    assert (
        transform_fixtures(
            test_fixtures_dataframe_with_dupes, test_transformed_teams_dataframe
        )
        is not None
    )
