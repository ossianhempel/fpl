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
    data = pd.concat([data_20_21, data_23_24], ignore_index=True)
    return data