import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
import io
import os
from pathlib import Path
import sys

# Add src to path for imports
src_path = str(Path(__file__).parent.parent)
if src_path not in sys.path:
    sys.path.append(src_path)

from src.utils import fetch_all_from_minio

@pytest.fixture
def test_data_path():
    """Return the path to test data directory"""
    return os.path.join(os.path.dirname(__file__), "test_data")

@pytest.fixture
def mock_minio_client():
    """Create a mock MinIO client"""
    with patch('src.utils.Minio') as mock_minio:
        mock_client = MagicMock()
        mock_minio.return_value = mock_client
        yield mock_client

def test_fetch_all_from_minio_handles_malformed_csv(test_data_path):
    """Test that fetch_all_from_minio can handle CSV files with malformed data"""
    # Read the test file
    test_file_path = os.path.join(test_data_path, "test_merged_gw_24_25.csv")
    with open(test_file_path, 'rb') as f:
        test_data = f.read()
    
    # Set up mock
    with patch('src.utils.Minio') as mock_minio:
        mock_client = MagicMock()
        mock_minio.return_value = mock_client
        
        # Mock list_buckets for connection test
        mock_client.list_buckets.return_value = [MagicMock(name='test-bucket')]
        
        # Mock list_objects
        mock_object = MagicMock()
        mock_object.object_name = "test_merged_gw_24_25.csv"
        mock_client.list_objects.return_value = [mock_object]
        
        # Mock get_object
        mock_response = MagicMock()
        mock_response.read.return_value = test_data
        mock_response.release_conn = MagicMock()
        mock_client.get_object.return_value = mock_response
        
        # Call the function
        result = fetch_all_from_minio("test-endpoint", "test-key", "test-secret", "test-bucket")
        
        # Verify the result
        assert result is not None
        assert isinstance(result, dict)
        assert "test_merged_gw_24_25.csv" in result
        df = result["test_merged_gw_24_25.csv"]
        
        # Check that the DataFrame has the correct number of columns
        expected_columns = 41  # Based on the header of test_merged_gw_24_25.csv
        assert len(df.columns) == expected_columns
        
        # Check that problematic rows were handled
        # The row with Alex Scott that had misplaced 'False' should be skipped
        problematic_rows = df[
            (df['name'] == 'Alex Scott') & 
            (df['opponent_team'].astype(str).str.contains('False', na=False))
        ]
        assert len(problematic_rows) == 0
        
        # Verify that the DataFrame contains valid data
        assert not df.empty
        assert all(col in df.columns for col in ['name', 'position', 'team', 'GW'])
        
        # Check data types of key columns
        assert pd.api.types.is_numeric_dtype(df['GW'])
        assert pd.api.types.is_string_dtype(df['name'])
        assert pd.api.types.is_string_dtype(df['position'])
        
        # Verify no rows have incorrect number of fields
        assert all(df.notna().sum(axis=1) <= expected_columns)

def test_fetch_all_from_minio_handles_empty_files():
    """Test that fetch_all_from_minio handles empty files gracefully"""
    with patch('src.utils.Minio') as mock_minio:
        mock_client = MagicMock()
        mock_minio.return_value = mock_client
        
        # Mock list_buckets for connection test
        mock_client.list_buckets.return_value = [MagicMock(name='test-bucket')]
        
        # Mock list_objects
        mock_object = MagicMock()
        mock_object.object_name = "empty.csv"
        mock_client.list_objects.return_value = [mock_object]
        
        # Mock get_object with empty file
        mock_response = MagicMock()
        mock_response.read.return_value = b"name,position,team,GW\n"  # Just header
        mock_response.release_conn = MagicMock()
        mock_client.get_object.return_value = mock_response
        
        result = fetch_all_from_minio("test-endpoint", "test-key", "test-secret", "test-bucket")
        
        assert result is not None
        assert isinstance(result, dict)
        assert "empty.csv" in result
        assert len(result["empty.csv"]) == 0  # Should be empty DataFrame with headers

def test_fetch_all_from_minio_handles_connection_error():
    """Test that fetch_all_from_minio handles connection errors gracefully"""
    with patch('src.utils.Minio') as mock_minio:
        # Mock MinIO client to simulate connection error
        mock_minio.side_effect = Exception("Connection failed")
        
        result = fetch_all_from_minio("test-endpoint", "test-key", "test-secret", "test-bucket")
        assert result is None