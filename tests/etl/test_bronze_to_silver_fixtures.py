import pytest
import pandas as pd

from src.etl_pipeline.components.bronze_to_silver_fixtures import (
    transform_fixtures,
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


# TODO - add test for team mapping logic


# TODO - add test for season column
