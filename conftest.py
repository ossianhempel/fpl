import pytest
import os
from unittest.mock import patch, MagicMock
import pandas as pd
from typing import Generator

@pytest.fixture
def test_data_path() -> str:
    """Return the path to test data directory"""
    return os.path.join("tests", "test_data")

@pytest.fixture
def mock_minio_client() -> Generator[MagicMock, None, None]:
    """Create a mock MinIO client"""
    with patch('src.utils.minio_utils.Minio') as mock_minio:
        mock_client = MagicMock()
        mock_minio.return_value = mock_client
        yield mock_client

@pytest.fixture
def gameweeks_data() -> pd.DataFrame:
    """Test fixture data for gameweeks."""
    # Load and combine test data from both seasons
    data_20_21 = pd.read_csv(os.path.join("tests", "test_data", "test_merged_gw_20_21.csv"))
    data_23_24 = pd.read_csv(os.path.join("tests", "test_data", "test_merged_gw_23_24.csv"))
    data_24_25 = pd.read_csv(os.path.join("tests", "test_data", "test_merged_gw_24_25.csv"), on_bad_lines='skip')
    
    # Add 'modified' column with default False to older data if it doesn't exist
    for df in [data_20_21, data_23_24]:
        if 'modified' not in df.columns:
            df['modified'] = False
    
    # Combine all dataframes
    combined_df = pd.concat([data_20_21, data_23_24, data_24_25], ignore_index=True)
    return combined_df

@pytest.fixture
def fixtures_data() -> pd.DataFrame:
    """Test fixture data for fixtures."""
    data = pd.read_csv(os.path.join("tests", "test_data", "test_fixtures_19_20.csv"))
    return data

@pytest.fixture
def teams_data() -> pd.DataFrame:
    """Test fixture data for teams."""
    data = pd.read_csv(os.path.join("tests", "test_data", "test_teams_2019_20_with_season.csv"))
    return data