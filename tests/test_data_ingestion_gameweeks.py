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
        'transfers_balance', 'transfers_in', 'transfers_out', 'modified'
    ])

    # Optional columns that may or may not be present
    optional_columns = ['player_started']

    # Add optional columns to expected columns if they exist in the transformed data
    for col in optional_columns:
        if col in transformed_df.columns:
            expected_columns.append(col)
    expected_columns = sorted(expected_columns)
    
    # Print debugging information
    df_columns_sorted = sorted(transformed_df.columns)
    print("\nColumn comparison:")
    print("Missing mandatory columns:", [col for col in expected_columns if col not in df_columns_sorted])
    print("Missing optional columns:", [col for col in optional_columns if col not in df_columns_sorted])
    print("Extra columns:", [col for col in df_columns_sorted if col not in expected_columns and col not in optional_columns])
    print("\nActual columns:", df_columns_sorted)
    print("Expected mandatory columns:", expected_columns)
    
    # Only assert for mandatory columns
    mandatory_columns_present = all(col in df_columns_sorted for col in expected_columns)
    assert mandatory_columns_present, "Missing mandatory columns in transformed data"
    
    # Check data types
    assert pd.api.types.is_bool_dtype(transformed_df["was_home"]), "was_home should be boolean"
    assert pd.api.types.is_bool_dtype(transformed_df["modified"]), "modified should be boolean"
    assert pd.api.types.is_bool_dtype(transformed_df["player_started"]), "player_started should be boolean"
    assert pd.api.types.is_integer_dtype(transformed_df["gameweek"]), "gameweek should be integer type"
    
    # Check value ranges
    assert transformed_df["gameweek"].min() == 1, "gameweek should start at 1"
    assert transformed_df["gameweek"].max() <= 38, "gameweek should not exceed 38"
    assert transformed_df["team"].nunique() > 1, "should have multiple unique teams"
    assert transformed_df["opponent_team"].nunique() > 1, "should have multiple unique opponent teams"

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
    
    # Create a mock SQLAlchemy engine that properly handles to_sql
    mock_engine_instance = Mock()
    mock_connection = Mock()
    mock_engine_instance.connect.return_value = mock_connection
    mock_connection.execute.return_value.fetchall.return_value = []
    mock_engine.return_value = mock_engine_instance
    
    # Mock pandas to_sql to avoid actual database operations
    with patch('pandas.DataFrame.to_sql') as mock_to_sql:
        # Mock MinIO data fetch
        with patch("src.components.data_ingestion_gameweeks.fetch_all_from_minio") as mock_fetch:
            mock_fetch.return_value = {"test_gameweeks.csv": gameweeks_data}
            
            # Execute ingestion
            data_ingestion.ingest_data()
            
            # Verify database operations
            assert mock_cursor.execute.called
            assert mock_conn.commit.called
            assert mock_engine.called
            assert mock_to_sql.called
            
            # Verify critical columns exist and have correct types
            transformed_df = data_ingestion._transform_and_dedupe_data(gameweeks_data)
            
            # Check required columns exist
            required_columns = [
                "player_name", "player_cost", "total_points", "position", "season",
                "gameweek", "seasonal_fixture_id", "team", "opponent_team", "kickoff_time",
                "was_home", "player_started", "modified"
            ]
            for col in required_columns:
                assert col in transformed_df.columns, f"Required column {col} missing"
            
            # Check data types
            assert pd.api.types.is_string_dtype(transformed_df["player_name"].dtype), "player_name should be string"
            assert pd.api.types.is_numeric_dtype(transformed_df["player_cost"].dtype), "player_cost should be numeric"
            assert pd.api.types.is_integer_dtype(transformed_df["total_points"].dtype), "total_points should be integer"
            assert pd.api.types.is_string_dtype(transformed_df["position"].dtype), "position should be string"
            assert pd.api.types.is_string_dtype(transformed_df["season"].dtype), "season should be string"
            assert pd.api.types.is_integer_dtype(transformed_df["gameweek"].dtype), "gameweek should be integer"
            assert pd.api.types.is_integer_dtype(transformed_df["seasonal_fixture_id"].dtype), "seasonal_fixture_id should be integer"
            assert pd.api.types.is_string_dtype(transformed_df["team"].dtype), "team should be string"
            assert pd.api.types.is_string_dtype(transformed_df["opponent_team"].dtype), "opponent_team should be string"
            assert pd.api.types.is_bool_dtype(transformed_df["was_home"].dtype), "was_home should be boolean"
            assert pd.api.types.is_bool_dtype(transformed_df["player_started"].dtype), "player_started should be boolean"
            assert pd.api.types.is_bool_dtype(transformed_df["modified"].dtype), "modified should be boolean"

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
    assert pd.api.types.is_integer_dtype(transformed_df['gameweek'].dtype), "gameweek should be integer"
    assert pd.api.types.is_string_dtype(transformed_df['team'].dtype), "team should be string"
    assert pd.api.types.is_string_dtype(transformed_df['player_name'].dtype), "player_name should be string"
    if 'player_started' in transformed_df.columns:
        assert pd.api.types.is_bool_dtype(transformed_df['player_started'].dtype), "player_started should be boolean"
    if 'modified' in transformed_df.columns:
        assert pd.api.types.is_bool_dtype(transformed_df['modified'].dtype), "modified should be boolean"

def test_modified_column_behavior(data_ingestion: DataIngestion) -> None:
    """Test that the modified column is handled correctly."""
    # Create test data with modified column
    data = {
        'GW': [1, 2, 3],
        'team': ['Team1', 'Team2', 'Team3'],
        'name': ['Player1', 'Player2', 'Player3'],
        'kickoff_time': ['2023-08-01', '2023-08-02', '2023-08-03'],
        'modified': [False, False, False]
    }
    df = pd.DataFrame(data)
    
    # Transform the data
    transformed_df = data_ingestion._transform_and_dedupe_data(df)
    
    # Verify the modified column
    assert 'modified' in transformed_df.columns, "modified column should be present"
    assert pd.api.types.is_bool_dtype(transformed_df['modified'].dtype), "modified should be boolean"
    assert not transformed_df['modified'].any(), "all modified values should be False"

def test_transform_with_different_schemas(data_ingestion: DataIngestion) -> None:
    """Test transformation with data from different seasons having different schemas."""
    # Create test data simulating different seasons
    data_old = {
        'GW': [1, 2],
        'team': ['Team1', 'Team2'],
        'name': ['Player1', 'Player2'],
        'kickoff_time': ['2023-08-01', '2023-08-02'],
        'starts': [True, False],  # old schema uses 'starts'
        'was_home': [True, False]
    }

    data_new = {
        'GW': [3, 4],
        'team': ['Team3', 'Team4'],
        'name': ['Player3', 'Player4'],
        'kickoff_time': ['2023-08-03', '2023-08-04'],
        'player_started': [True, False],  # new schema uses 'player_started'
        'was_home': [True, False],
        'modified': [False, False]  # new schema has 'modified'
    }

    # Create DataFrames
    df_old = pd.DataFrame(data_old)
    df_new = pd.DataFrame(data_new)
    
    # Combine the data
    combined_df = pd.concat([df_old, df_new], ignore_index=True)
    
    # Transform the data
    transformed_df = data_ingestion._transform_and_dedupe_data(combined_df)
    
    # Verify the transformation
    assert len(transformed_df) == 4, "Should preserve all rows"
    assert 'player_started' in transformed_df.columns, "Should have player_started column"
    assert 'modified' in transformed_df.columns, "Should have modified column"
    assert pd.api.types.is_bool_dtype(transformed_df['player_started']), "player_started should be boolean"
    assert pd.api.types.is_bool_dtype(transformed_df['modified']), "modified should be boolean"
    
    # Check that old 'starts' data was properly converted to 'player_started'
    assert transformed_df.iloc[0]['player_started'] == True, "First row should have player_started True"
    assert transformed_df.iloc[1]['player_started'] == False, "Second row should have player_started False"
    
    # Check that all rows have modified column with proper values
    assert not transformed_df['modified'].iloc[0:2].any(), "Old data should have modified=False"
    assert not transformed_df['modified'].iloc[2:4].any(), "New data should preserve modified=False"

if __name__ == "__main__":
    pytest.main(["-v", __file__]) 