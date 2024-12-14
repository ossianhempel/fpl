import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime
import sys
from pathlib import Path

# Add src to path for imports
src_path = str(Path(__file__).parent.parent)
if src_path not in sys.path:
    sys.path.append(src_path)

from src.components.data_ingestion_fixtures import DataIngestion as FixturesIngestion
from src.components.data_ingestion_gameweeks import DataIngestion as GameweeksIngestion

# Fixtures test data
@pytest.fixture
def sample_fixtures_df():
    return pd.DataFrame({
        'event': [1, 2],
        'id': [1001, 1002],
        'kickoff_time': ['2024-08-10 15:00:00', '2024-08-17 15:00:00'],
        'minutes': [90, 90],
        'team_a': [2, 4],
        'team_a_score': [1.0, 2.0],
        'team_h': [1, 3],
        'team_h_score': [2.0, 1.0],
        'team_h_difficulty': [3, 4],
        'team_a_difficulty': [4, 3],
        'pulse_id': [101, 102],
        'finished': [True, False],
        'finished_provisional': [True, False],
        'started': [True, False],
        'code': [1234, 1235]
    })

@pytest.fixture
def sample_teams_df():
    return pd.DataFrame({
        'id': [1, 2, 3, 4],
        'name': ['Arsenal', 'Chelsea', 'Liverpool', 'Man City'],
        'season': ['2024-25', '2024-25', '2024-25', '2024-25']
    })

@pytest.fixture
def sample_gameweeks_df():
    return pd.DataFrame({
        'name': ['Player1', 'Player2'],
        'GW': [1, 1],
        'kickoff_time': ['2024-08-10 15:00:00', '2024-08-10 15:00:00'],
        'team': ['Arsenal', 'Chelsea'],
        'fixture': [1001, 1001],
        'total_points': [5, 3],
        'minutes': [90, 85],
        'value': [55, 65],
        'position': ['MID', 'FWD'],
        'xP': [4.5, 3.8],
        'creativity': [25.0, 15.0],
        'expected_assists': [0.5, 0.3],
        'expected_goals': [0.3, 0.4],
        'was_home': [True, False],
        'round': [1, 1],
        'starts': [1, 1],
        'team_a_score': [1, 1],
        'team_h_score': [2, 2]
    })

# Fixtures Ingestion Tests
class TestFixturesIngestion:
    @patch('src.components.data_ingestion_fixtures.fetch_all_from_minio')
    def test_initiate_data_ingestion(self, mock_fetch, sample_fixtures_df, sample_teams_df):
        mock_fetch.side_effect = [
            {'test.csv': sample_fixtures_df},  # First call for fixtures
            {'teams.csv': sample_teams_df}     # Second call for teams
        ]
        
        ingestion = FixturesIngestion()
        df, teams_df = ingestion._initiate_data_ingestion()
        
        assert isinstance(df, pd.DataFrame)
        assert isinstance(teams_df, pd.DataFrame)
        assert len(df) == len(sample_fixtures_df)
        assert len(teams_df) == len(sample_teams_df)

    def test_transform_and_dedupe_data(self, sample_fixtures_df, sample_teams_df):
        ingestion = FixturesIngestion()
        transformed_df = ingestion._transform_and_dedupe_data(sample_fixtures_df, sample_teams_df)
        
        assert 'season' in transformed_df.columns
        assert 'gameweek' in transformed_df.columns
        assert 'team_h_name' in transformed_df.columns
        assert 'team_a_name' in transformed_df.columns
        assert len(transformed_df) == len(sample_fixtures_df)

    @patch('src.components.data_ingestion_fixtures.connect_to_postgres')
    def test_create_table_if_not_exists(self, mock_connect):
        mock_cursor = MagicMock()
        ingestion = FixturesIngestion()
        ingestion._create_table_if_not_exists(mock_cursor, "test_table")
        
        assert mock_cursor.execute.called

# Gameweeks Ingestion Tests
class TestGameweeksIngestion:
    @patch('src.components.data_ingestion_gameweeks.fetch_all_from_minio')
    def test_initiate_data_ingestion(self, mock_fetch, sample_gameweeks_df):
        mock_fetch.return_value = {'test.csv': sample_gameweeks_df}
        
        ingestion = GameweeksIngestion()
        df = ingestion._initiate_data_ingestion()
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == len(sample_gameweeks_df)

    def test_transform_and_dedupe_data(self, sample_gameweeks_df):
        ingestion = GameweeksIngestion()
        transformed_df = ingestion._transform_and_dedupe_data(sample_gameweeks_df)
        
        assert 'season' in transformed_df.columns
        assert 'gameweek' in transformed_df.columns
        assert 'player_name' in transformed_df.columns
        assert 'opponent_team' in transformed_df.columns
        assert len(transformed_df) == len(sample_gameweeks_df)

    @patch('src.components.data_ingestion_gameweeks.connect_to_postgres')
    def test_create_table_if_not_exists(self, mock_connect):
        mock_cursor = MagicMock()
        ingestion = GameweeksIngestion()
        ingestion._create_table_if_not_exists(mock_cursor, "test_table")
        
        assert mock_cursor.execute.called

# Integration Tests
class TestIngestionIntegration:
    @patch('src.components.data_ingestion_fixtures.connect_to_postgres')
    @patch('src.components.data_ingestion_fixtures.fetch_all_from_minio')
    def test_fixtures_full_ingestion(self, mock_fetch, mock_connect, sample_fixtures_df, sample_teams_df):
        mock_fetch.side_effect = [
            {'test.csv': sample_fixtures_df},
            {'teams.csv': sample_teams_df}
        ]
        mock_connect.return_value = MagicMock()
        
        ingestion = FixturesIngestion()
        ingestion.ingest_data()  # Should complete without errors

    @patch('src.components.data_ingestion_gameweeks.connect_to_postgres')
    @patch('src.components.data_ingestion_gameweeks.fetch_all_from_minio')
    def test_gameweeks_full_ingestion(self, mock_fetch, mock_connect, sample_gameweeks_df):
        mock_fetch.return_value = {'test.csv': sample_gameweeks_df}
        mock_connect.return_value = MagicMock()
        
        ingestion = GameweeksIngestion()
        ingestion.ingest_data()  # Should complete without errors 