import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
import io
import os
from pathlib import Path
import sys

from src.utils.minio_utils import fetch_all_from_minio

def test_fetch_all_from_minio_handles_real_gameweeks_data(test_data_path):
    """Test handling of real gameweeks data"""
    test_file_path = os.path.join(test_data_path, "test_merged_gw_24_25.csv")
    with open(test_file_path, 'rb') as f:
        test_data = f.read()
    
    with patch('src.utils.minio_utils.Minio') as mock_minio:
        mock_client = MagicMock()
        mock_minio.return_value = mock_client
        
        mock_client.list_buckets.return_value = [MagicMock(name='test-bucket')]
        
        mock_object = MagicMock()
        mock_object.object_name = "test_merged_gw_24_25.csv"
        mock_client.list_objects.return_value = [mock_object]
        
        mock_response = MagicMock()
        mock_response.read.return_value = test_data
        mock_response.release_conn = MagicMock()
        mock_client.get_object.return_value = mock_response
        
        result = fetch_all_from_minio("test-endpoint", "test-key", "test-secret", "gameweeks")
        
        assert result is not None
        assert isinstance(result, dict)
        assert "test_merged_gw_24_25.csv" in result
        df = result["test_merged_gw_24_25.csv"]
        
        # Verify critical columns have no nulls
        assert not df['GW'].isnull().any(), "GW column contains NULL values"
        assert not df['team'].isnull().any(), "team column contains NULL values"
        assert not df['name'].isnull().any(), "name column contains NULL values"
        
        # Verify data types
        assert pd.api.types.is_integer_dtype(df['GW']), "GW should be integer type"
        assert pd.api.types.is_string_dtype(df['team']), "team should be string type"
        assert pd.api.types.is_string_dtype(df['name']), "name should be string type"
        
        # Verify expected columns exist
        assert all(col in df.columns for col in ['name', 'position', 'team', 'GW'])

def test_fetch_all_from_minio_handles_real_teams_data(test_data_path):
    """Test handling of real teams data"""
    test_file_path = os.path.join(test_data_path, "test_teams_2019_20_with_season.csv")
    with open(test_file_path, 'rb') as f:
        test_data = f.read()
    
    with patch('src.utils.minio_utils.Minio') as mock_minio:
        mock_client = MagicMock()
        mock_minio.return_value = mock_client
        
        mock_client.list_buckets.return_value = [MagicMock(name='test-bucket')]
        
        mock_object = MagicMock()
        mock_object.object_name = "test_teams_2019_20_with_season.csv"
        mock_client.list_objects.return_value = [mock_object]
        
        mock_response = MagicMock()
        mock_response.read.return_value = test_data
        mock_response.release_conn = MagicMock()
        mock_client.get_object.return_value = mock_response
        
        result = fetch_all_from_minio("test-endpoint", "test-key", "test-secret", "teams")
        
        assert result is not None

def test_fetch_all_from_minio_handles_empty_files():
    """Test handling of empty files with headers"""
    # Create a test file with only headers
    test_data = b"name,team,GW,position\n"
    
    with patch('src.utils.minio_utils.Minio') as mock_minio:
        mock_client = MagicMock()
        mock_minio.return_value = mock_client
        
        mock_client.list_buckets.return_value = [MagicMock(name='test-bucket')]
        
        mock_object = MagicMock()
        mock_object.object_name = "empty.csv"
        mock_client.list_objects.return_value = [mock_object]
        
        mock_response = MagicMock()
        mock_response.read.return_value = test_data
        mock_response.release_conn = MagicMock()
        mock_client.get_object.return_value = mock_response
        
        result = fetch_all_from_minio("test-endpoint", "test-key", "test-secret", "gameweeks")
        
        assert result is not None
        assert isinstance(result, dict)
        assert "empty.csv" in result
        df = result["empty.csv"]
        assert len(df) == 0
        assert list(df.columns) == ["name", "team", "GW", "position"]

def test_fetch_all_from_minio_handles_completely_empty_file():
    """Test handling of completely empty files"""
    test_data = b""
    
    with patch('src.utils.minio_utils.Minio') as mock_minio:
        mock_client = MagicMock()
        mock_minio.return_value = mock_client
        
        mock_client.list_buckets.return_value = [MagicMock(name='test-bucket')]
        
        mock_object = MagicMock()
        mock_object.object_name = "completely_empty.csv"
        mock_client.list_objects.return_value = [mock_object]
        
        mock_response = MagicMock()
        mock_response.read.return_value = test_data
        mock_response.release_conn = MagicMock()
        mock_client.get_object.return_value = mock_response
        
        result = fetch_all_from_minio("test-endpoint", "test-key", "test-secret", "gameweeks")
        
        assert result is not None
        assert isinstance(result, dict)
        # A completely empty file should not be included in the results
        assert "completely_empty.csv" not in result

def test_fetch_all_from_minio_handles_connection_error():
    """Test handling of connection errors"""
    with patch('src.utils.minio_utils.Minio') as mock_minio:
        mock_minio.side_effect = Exception("Connection failed")
        
        result = fetch_all_from_minio("test-endpoint", "test-key", "test-secret", "test-bucket")
        assert result is None

# def test_fetch_all_from_minio_handles_malformed_data():
#     """Test handling of malformed data"""
#     test_data = (
#         "name,team,GW,position\n"
#         "Player1,TeamA,not_a_number,FWD\n"  # GW should be int
#         "Player2,TeamB,2,FWD\n"
#     ).encode('utf-8')
    
#     with patch('src.utils.Minio') as mock_minio:
#         mock_client = MagicMock()
#         mock_minio.return_value = mock_client
        
#         mock_client.list_buckets.return_value = [MagicMock(name='test-bucket')]
        
#         mock_object = MagicMock()
#         mock_object.object_name = "malformed.csv"
#         mock_client.list_objects.return_value = [mock_object]
        
#         mock_response = MagicMock()
#         mock_response.read.return_value = test_data
#         mock_response.release_conn = MagicMock()
#         mock_client.get_object.return_value = mock_response
        
#         result = fetch_all_from_minio("test-endpoint", "test-key", "test-secret", "gameweeks")
        
#         assert result is not None
#         assert "malformed.csv" in result
#         df = result["malformed.csv"]
        
#         # Only the valid row should remain
#         assert len(df) == 1
#         assert df.iloc[0]['name'] == 'Player2'
#         assert df.iloc[0]['GW'] == 2
