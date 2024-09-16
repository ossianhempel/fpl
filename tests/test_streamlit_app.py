import pytest
from unittest.mock import patch, MagicMock
import polars as pl
import os
import sys
import streamlit as st
from streamlit.testing.v1 import AppTest
from datetime import datetime

# Add the project root directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(project_root)

# Mock data tailored to the app's expected schema
mock_data = [
    {
        "player_name": "Player1",
        "season": "2023/24",
        "gameweek": 1,
        "team": "Team1",
        "opponent_team": "Team2",
        "position": "FWD",
        "player_cost": 5.0,
        "total_points": 10,
        "goals_scored": 2,
        "assists": 1,
        "clean_sheets": True,
        "ict_index": 10.5,
        "minutes_played": 90,
        "kickoff_time": datetime.strptime("2023-08-11 20:00:00", "%Y-%m-%d %H:%M:%S"),
        "selected": 1000
    },
    {
        "player_name": "Player2",
        "season": "2023/24",
        "gameweek": 1,
        "team": "Team1",
        "opponent_team": "Team2",
        "position": "MID",
        "player_cost": 6.0,
        "total_points": 8,
        "goals_scored": 1,
        "assists": 2,
        "clean_sheets": False,
        "ict_index": 9.0,
        "minutes_played": 85,
        "kickoff_time": datetime.strptime("2023-08-11 20:00:00", "%Y-%m-%d %H:%M:%S"),
        "selected": 500
    },
]

# Define the schema based on the app's expectations
schema = {
    "player_name": pl.Utf8,
    "season": pl.Utf8,
    "gameweek": pl.Int64,
    "team": pl.Utf8,
    "opponent_team": pl.Utf8,
    "position": pl.Utf8,
    "player_cost": pl.Float64,
    "total_points": pl.Int64,
    "goals_scored": pl.Int64,
    "assists": pl.Int64,
    "clean_sheets": pl.Boolean,
    "ict_index": pl.Float64,
    "minutes_played": pl.Int64,
    "kickoff_time": pl.Datetime,
    "selected": pl.Int64
}


def test_app():
    at = AppTest.from_file("src/streamlit/fpl_dashboard.py")

    at.secrets["PG_DATABASE"] = "fpl"
    at.secrets["PG_HOST"] = "localhost"
    at.secrets["PG_USER"] = "postgres"
    at.secrets["PG_PASSWORD"] = "postgres"
    at.secrets["PG_PORT"] = "5432"

    at.run()

    assert not at.exception

    assert at.title[0].value == "Fantasy Premier League Dashboard"