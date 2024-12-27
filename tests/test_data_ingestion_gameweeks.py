import pytest
import os
import sys
import pandas as pd
from unittest.mock import MagicMock, patch, Mock
from datetime import datetime

from src.components.data_ingestion_gameweeks import DataIngestion, DataIngestionConfig

# @pytest.fixture
# def mock_minio_client():
#     """Mock MinIO client for testing."""
#     with patch("src.components.data_ingestion_gameweeks.create_minio_client") as mock_connect:
#         mock_client = Mock()
#         mock_connect.return_value = mock_client
#         yield mock_client

@pytest.fixture
def data_ingestion(mock_minio_client: MagicMock) -> DataIngestion:
    """DataIngestion instance for testing."""
    with patch.dict(os.environ, {
        "PG_DATABASE": "test_db",
        "PG_HOST": "localhost",
        "PG_USER": "test_user",
        "PG_PASSWORD": "test_pass",
        "PG_PORT": "5432",
        "PG_TABLE_NAME_GW": "stg_gameweeks",
        "MINIO_ENDPOINT": "test-endpoint",
        "MINIO_ACCESS_KEY": "test-key",
        "MINIO_SECRET_KEY": "test-secret"
    }):
        return DataIngestion()

def test_config_initialization() -> None:
    """Test configuration initialization."""
    config = DataIngestionConfig()
    assert config.postgres_database == "fpl"

def test_transform_and_dedupe_data(data_ingestion: DataIngestion, gameweeks_data: pd.DataFrame) -> None:
    """Test data transformation and deduplication."""
    transformed_df = data_ingestion._transform_and_dedupe_data(gameweeks_data)
    
    # Check basic properties
    assert not transformed_df.empty, "Transformed DataFrame should not be empty"
    
    # Check column names
    expected_columns = sorted([
        'player_name', 'player_cost', 'total_points', 'position', 'season',
        'gameweek', 'seasonal_fixture_id', 'team', 'opponent_team',
        'team_a_score', 'team_h_score', 'was_home', 'minutes_played',
        'xP', 'element', 'creativity', 'expected_assists', 'expected_goal_involvements',
        'expected_goals', 'expected_goals_conceded', 'ict_index', 'influence',
        'threat', 'kickoff_time', 'goals_scored', 'assists', 'clean_sheets',
        'goals_conceded', 'own_goals', 'penalties_saved', 'penalties_missed',
        'yellow_cards', 'red_cards', 'saves', 'bonus', 'bps', 'selected',
        'transfers_balance', 'transfers_in', 'transfers_out'
    ])

    # Add 'player_started' if it exists
    if 'player_started' in transformed_df.columns:
        expected_columns.append('player_started')
    expected_columns = sorted(expected_columns)
    
    # Print debugging information
    df_columns_sorted = sorted(transformed_df.columns)
    print("\nColumn comparison:")
    print("Missing columns:", [col for col in expected_columns if col not in df_columns_sorted])
    print("Extra columns:", [col for col in df_columns_sorted if col not in expected_columns])
    print("\nActual columns:", df_columns_sorted)
    print("Expected columns:", expected_columns)
    
    assert df_columns_sorted == expected_columns, "Incorrect columns in transformed data"
    
    # Check data types
    assert transformed_df["was_home"].dtype == bool, "was_home should be boolean"
    if 'player_started' in transformed_df.columns:
        assert transformed_df["player_started"].dtype == bool, "player_started should be boolean"
    assert pd.api.types.is_integer_dtype(transformed_df["gameweek"]), "gameweek should be integer type"
    
    # Check value ranges
    assert transformed_df["gameweek"].min() == 1, "gameweek should start at 1"
    assert transformed_df["gameweek"].max() <= 38, "gameweek should not exceed 38"
    assert transformed_df["team"].nunique() > 1, "should have multiple unique teams"
    assert transformed_df["opponent_team"].nunique() > 1, "should have multiple unique opponent teams"
    assert transformed_df["season"].nunique() == 2, "should have 2 seasons"
    assert transformed_df["season"].iloc[0] == "2020-21", "first season should be 2020-21"

def test_transform_with_missing_columns(data_ingestion: DataIngestion, gameweeks_data: pd.DataFrame) -> None:
    """Test transformation with missing columns."""
    df_gameweeks = gameweeks_data.drop(columns=["kickoff_time"])
    with pytest.raises(Exception) as excinfo:
        data_ingestion._transform_and_dedupe_data(df_gameweeks)
    assert "error transforming data" in str(excinfo.value).lower()

def test_transform_with_duplicates(data_ingestion: DataIngestion, gameweeks_data: pd.DataFrame) -> None:
    """Test transformation with duplicate data."""
    df_gameweeks = pd.concat([gameweeks_data, gameweeks_data])
    transformed_df = data_ingestion._transform_and_dedupe_data(df_gameweeks)
    
    # Get original transformed length for comparison
    original_transformed = data_ingestion._transform_and_dedupe_data(gameweeks_data)
    assert len(transformed_df) == len(original_transformed), "Duplicates should be removed"

def test_validate_data(data_ingestion: DataIngestion, gameweeks_data: pd.DataFrame) -> None:
    """Test data validation."""
    transformed_df = data_ingestion._transform_and_dedupe_data(gameweeks_data)
    data_ingestion._validate_data(transformed_df)  # Should not raise exceptions

@patch("src.components.data_ingestion_gameweeks.fetch_all_from_minio")
def test_initiate_data_ingestion(mock_fetch: MagicMock, data_ingestion: DataIngestion, gameweeks_data: pd.DataFrame) -> None:
    """Test data ingestion initiation."""
    mock_fetch.return_value = {"test_gameweeks.csv": gameweeks_data}
    
    df = data_ingestion._initiate_data_ingestion()
    assert not df.empty
    assert len(df) == len(gameweeks_data)

@patch("src.components.data_ingestion_gameweeks.connect_to_postgres")
@patch("src.components.data_ingestion_gameweeks.create_engine")
def test_complete_ingestion_process(
    mock_engine: Mock, 
    mock_postgres: Mock, 
    data_ingestion: DataIngestion, 
    gameweeks_data: pd.DataFrame
    ) -> None:
    """Test the complete ingestion process."""
    # Mock database connections
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_postgres.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    
    # Mock MinIO data fetch
    with patch("src.components.data_ingestion_gameweeks.fetch_all_from_minio") as mock_fetch:
        mock_fetch.return_value = {"test_gameweeks.csv": gameweeks_data}
        
        # Execute ingestion
        data_ingestion.ingest_data()
        
        # Verify database operations
        assert mock_cursor.execute.called
        assert mock_conn.commit.called
        assert mock_engine.called

def test_transform_with_incorrect_data_types(data_ingestion: DataIngestion) -> None:
    """Test transformation with incorrect data types."""
    # Create test data with invalid types
    data = {
        'GW': ['not_a_number', '2', '3'],
        'team': [1, 2, 3],  # Should be string
        'name': ['Player1', 'Player2', 'Player3'],
        'kickoff_time': ['2023-08-01', '2023-08-02', 'not_a_date'],
        'starts': ['not_a_bool', 'True', 'False']
    }
    df = pd.DataFrame(data)
    
    # Transform the data
    transformed_df = data_ingestion._transform_and_dedupe_data(df)
    
    # Should only have one valid row (row index 1)
    assert len(transformed_df) == 1, "Should only have one valid row after transformation"
    
    # Verify the types of the remaining row
    assert pd.api.types.is_integer_dtype(transformed_df['gameweek'].dtype), "gameweek should be integer, was " + str(transformed_df['gameweek'].dtype)
    assert pd.api.types.is_string_dtype(transformed_df['team'].dtype), "team should be string, was " + str(transformed_df['team'].dtype)
    assert pd.api.types.is_string_dtype(transformed_df['player_name'].dtype), "player_name should be string, was " + str(transformed_df['player_name'].dtype)
    assert pd.api.types.is_bool_dtype(transformed_df['player_started'].dtype), "player_started should be boolean, was " + str(transformed_df['player_started'].dtype)

if __name__ == "__main__":
    pytest.main(["-v", __file__]) 