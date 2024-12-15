import pytest
import os
import sys
import pandas as pd
from unittest.mock import Mock, patch
from datetime import datetime

# Add the project's root directory to the PYTHONPATH
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
sys.path.append(project_root)

from src.components.data_ingestion_fixtures import DataIngestion, DataIngestionConfig

@pytest.fixture
def fixtures_data():
    """Test fixture data for fixtures."""
    data = pd.read_csv(os.path.join(project_root, "tests", "test_data", "test_fixtures_19_20.csv"))
    return data

@pytest.fixture
def teams_data():
    """Test fixture data for teams."""
    data = pd.read_csv(os.path.join(project_root, "tests", "test_data", "test_teams_2019_20_with_season.csv"))
    return data

@pytest.fixture
def mock_minio_client():
    """Mock MinIO client for testing."""
    with patch("src.components.data_ingestion_fixtures.connect_to_minio") as mock_connect:
        mock_client = Mock()
        mock_connect.return_value = mock_client
        yield mock_client

@pytest.fixture
def data_ingestion(mock_minio_client):
    """DataIngestion instance for testing."""
    with patch.dict(os.environ, {
        "PG_DATABASE": "test_db",
        "PG_HOST": "localhost",
        "PG_USER": "test_user",
        "PG_PASSWORD": "test_pass",
        "PG_PORT": "5432",
        "PG_TABLE_NAME_FIXTURES": "stg_fixtures",
        "MINIO_ENDPOINT": "test-endpoint",
        "MINIO_ACCESS_KEY": "test-key",
        "MINIO_SECRET_KEY": "test-secret"
    }):
        return DataIngestion(testing=True)

def test_config_initialization():
    """Test configuration initialization."""
    config = DataIngestionConfig()
    assert config.testing is False
    
    config = DataIngestionConfig(testing=True)
    assert config.testing is True

def test_transform_and_dedupe_data(data_ingestion, fixtures_data, teams_data):
    """Test data transformation and deduplication."""
    transformed_df = data_ingestion._transform_and_dedupe_data(fixtures_data, teams_data)
    
    # Check basic properties
    assert not transformed_df.empty, "Transformed DataFrame should not be empty"
    assert len(transformed_df) == 380, "Should have 380 fixtures (20 teams * 19 home games)"
    
    # Check column names
    expected_columns = sorted([
        'seasonal_fixture_id', 
        'code',
        'gameweek', 
        'season',
        'finished',
        'finished_provisional',
        'kickoff_time',
        'minutes',
        'provisional_start_time',
        'started',
        'team_a',
        'team_a_name',
        'team_a_score',
        'team_h',
        'team_h_name',
        'team_h_score',
        'team_h_difficulty',
        'team_a_difficulty'
    ])
    
    # Print debugging information
    df_columns_sorted = sorted(transformed_df.columns)
    print("\nColumn comparison:")
    print("Missing columns:", [col for col in expected_columns if col not in df_columns_sorted])
    print("Extra columns:", [col for col in df_columns_sorted if col not in expected_columns])
    print("\nActual columns:", df_columns_sorted)
    print("Expected columns:", expected_columns)
    
    assert df_columns_sorted == expected_columns, "Incorrect columns in transformed data"
    
    # Check data types
    assert transformed_df["finished"].dtype == bool, "finished should be boolean"
    assert transformed_df["finished_provisional"].dtype == bool, "finished_provisional should be boolean"
    assert transformed_df["started"].dtype == bool, "started should be boolean"
    assert transformed_df["gameweek"].dtype == "Int64", "gameweek should be Int64"
    
    # Check unique constraints
    assert transformed_df["code"].nunique() == 380, "code should be unique"
    assert transformed_df["seasonal_fixture_id"].nunique() == 380, "seasonal_fixture_id should be unique"
    
    # Check value ranges
    assert transformed_df["gameweek"].min() == 1, "gameweek should start at 1"
    assert transformed_df["gameweek"].max() == 47, "gameweek should end at 47"  # Updated for 2019-20 season
    assert transformed_df["team_h_name"].nunique() == 20, "should have 20 unique home teams"
    assert transformed_df["team_a_name"].nunique() == 20, "should have 20 unique away teams"
    assert transformed_df["season"].iloc[0] == "2019-20", "season should be 2019-20"

def test_transform_with_missing_columns(data_ingestion, fixtures_data, teams_data):
    """Test transformation with missing columns."""
    df_fixtures = fixtures_data.drop(columns=["kickoff_time"])
    with pytest.raises(Exception) as excinfo:
        data_ingestion._transform_and_dedupe_data(df_fixtures, teams_data)
    assert "error transforming data" in str(excinfo.value).lower()

def test_transform_with_duplicates(data_ingestion, fixtures_data, teams_data):
    """Test transformation with duplicate data."""
    df_fixtures = pd.concat([fixtures_data, fixtures_data])
    transformed_df = data_ingestion._transform_and_dedupe_data(df_fixtures, teams_data)
    
    # Get original transformed length for comparison
    original_transformed = data_ingestion._transform_and_dedupe_data(fixtures_data, teams_data)
    assert len(transformed_df) == len(original_transformed), "Duplicates should be removed"

def test_validate_data(data_ingestion, fixtures_data, teams_data):
    """Test data validation."""
    transformed_df = data_ingestion._transform_and_dedupe_data(fixtures_data, teams_data)
    data_ingestion._validate_data(transformed_df)  # Should not raise exceptions

@patch("src.components.data_ingestion_fixtures.fetch_all_from_minio")
def test_initiate_data_ingestion(mock_fetch, data_ingestion, fixtures_data, teams_data):
    """Test data ingestion initiation."""
    mock_fetch.side_effect = [
        {"test_fixtures.csv": fixtures_data},
        {"test_teams.csv": teams_data}
    ]
    
    df_fixtures, df_teams = data_ingestion._initiate_data_ingestion()
    assert not df_fixtures.empty
    assert not df_teams.empty
    assert len(df_fixtures) == len(fixtures_data)
    assert len(df_teams) == len(teams_data)

@patch("src.components.data_ingestion_fixtures.connect_to_postgres")
@patch("src.components.data_ingestion_fixtures.create_engine")
def test_complete_ingestion_process(mock_engine, mock_postgres, data_ingestion, fixtures_data, teams_data):
    """Test the complete ingestion process."""
    # Mock database connections
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_postgres.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    
    # Mock MinIO data fetch
    with patch("src.components.data_ingestion_fixtures.fetch_all_from_minio") as mock_fetch:
        mock_fetch.side_effect = [
            {"test_fixtures.csv": fixtures_data},
            {"test_teams.csv": teams_data}
        ]
        
        # Execute ingestion
        data_ingestion.ingest_data()
        
        # Verify database operations
        assert mock_cursor.execute.called
        assert mock_conn.commit.called
        assert mock_engine.called

def test_transform_with_incorrect_data_types(data_ingestion, fixtures_data, teams_data):
    """Test transformation with incorrect data types."""
    # Create a copy with invalid data in critical columns
    df_fixtures = fixtures_data.copy()
    df_fixtures['event'] = 'invalid_int'
    df_fixtures['team_h'] = 'not_a_team_id'
    df_fixtures['team_a'] = 'not_a_team_id'
    df_fixtures['kickoff_time'] = 'not_a_date'
    
    # Also add some invalid data in non-critical columns
    df_fixtures['team_h_score'] = 'not_a_score'
    df_fixtures['team_a_score'] = 'not_a_score'
    
    # Transform the data
    transformed_df = data_ingestion._transform_and_dedupe_data(df_fixtures, teams_data)
    
    # Verify that the DataFrame is empty since all critical columns have invalid values
    assert len(transformed_df) == 0, "DataFrame should be empty after dropping rows with invalid critical columns"
    
def test_transform_with_partially_invalid_data(data_ingestion, fixtures_data, teams_data):
    """Test transformation with some invalid data in non-critical columns."""
    # Create a copy with invalid data only in non-critical columns
    df_fixtures = fixtures_data.copy()
    df_fixtures['team_h_score'] = 'not_a_score'
    df_fixtures['team_a_score'] = 'not_a_score'
    
    # Transform the data
    transformed_df = data_ingestion._transform_and_dedupe_data(df_fixtures, teams_data)
    
    # Verify that the DataFrame is not empty (critical columns are still valid)
    assert len(transformed_df) > 0, "DataFrame should not be empty when only non-critical columns have invalid values"
    
    # Verify that invalid values were coerced to NaN
    assert transformed_df['team_h_score'].isna().all(), "Invalid score values should be coerced to NaN"
    assert transformed_df['team_a_score'].isna().all(), "Invalid score values should be coerced to NaN"
    
    # Verify that critical columns are not null
    assert not transformed_df['gameweek'].isna().any(), "gameweek should not contain NaN values"
    assert not transformed_df['team_h'].isna().any(), "team_h should not contain NaN values"
    assert not transformed_df['team_a'].isna().any(), "team_a should not contain NaN values"
    assert not transformed_df['kickoff_time'].isna().any(), "kickoff_time should not contain NaN values"

if __name__ == "__main__":
    pytest.main(["-v", __file__]) 