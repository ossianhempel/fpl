# tests/test_minio_utils.py
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
    # same as before, no changes
    test_file_path = os.path.join(test_data_path, "test_merged_gw_24_25.csv")
    with open(test_file_path, 'rb') as f:
        test_data = f.read()
    
    with patch('src.utils.Minio') as mock_minio:
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
        
        assert not df['GW'].isnull().any(), "GW column contains NULL values"
        assert not df['team'].isnull().any(), "team column contains NULL values"
        assert not df['name'].isnull().any(), "name column contains NULL values"
        
        assert not df.empty
        assert all(col in df.columns for col in ['name', 'position', 'team', 'GW'])

def test_fetch_all_from_minio_handles_null_in_critical_columns():
    # updated expectation: player1 and player5 remain
    test_data = (
        "name,team,GW,position\n"
        "Player1,TeamA,1,FWD\n"
        "Player2,,2,FWD\n"
        "Player3,TeamC,,MID\n"
        ",TeamD,4,DEF\n"
        "Player5,TeamE,5,MID\n"
    ).encode('utf-8')
    
    with patch('src.utils.Minio') as mock_minio:
        mock_client = MagicMock()
        mock_minio.return_value = mock_client
        
        mock_client.list_buckets.return_value = [MagicMock(name='test-bucket')]
        
        mock_object = MagicMock()
        mock_object.object_name = "test.csv"
        mock_client.list_objects.return_value = [mock_object]
        
        mock_response = MagicMock()
        mock_response.read.return_value = test_data
        mock_response.release_conn = MagicMock()
        mock_client.get_object.return_value = mock_response
        
        result = fetch_all_from_minio("test-endpoint", "test-key", "test-secret", "gameweeks")
        
        assert result is not None
        assert "test.csv" in result
        df = result["test.csv"]
        
        # since player1 and player5 rows are fully valid, we keep them both
        assert len(df) == 2
        assert set(df['name']) == {'Player1', 'Player5'}
        assert set(df['team']) == {'TeamA', 'TeamE'}
        assert set(df['GW']) == {1, 5}

def test_fetch_all_from_minio_handles_empty_files():
    # same as before, no changes
    with patch('src.utils.Minio') as mock_minio:
        mock_client = MagicMock()
        mock_minio.return_value = mock_client
        
        mock_client.list_buckets.return_value = [MagicMock(name='test-bucket')]
        
        mock_object = MagicMock()
        mock_object.object_name = "empty.csv"
        mock_client.list_objects.return_value = [mock_object]
        
        mock_response = MagicMock()
        mock_response.read.return_value = b"name,team,GW,position\n"
        mock_response.release_conn = MagicMock()
        mock_client.get_object.return_value = mock_response
        
        result = fetch_all_from_minio("test-endpoint", "test-key", "test-secret", "gameweeks")
        
        assert result is not None
        assert isinstance(result, dict)
        assert "empty.csv" in result
        assert len(result["empty.csv"]) == 0

def test_fetch_all_from_minio_handles_connection_error():
    # same as before
    with patch('src.utils.Minio') as mock_minio:
        mock_minio.side_effect = Exception("Connection failed")
        
        result = fetch_all_from_minio("test-endpoint", "test-key", "test-secret", "test-bucket")
        assert result is None

def test_fetch_all_from_minio_handles_teams_data(test_data_path):
    """Test that fetch_all_from_minio properly handles teams data with critical columns"""
    # Read the test teams file
    test_file_path = os.path.join(test_data_path, "test_teams_2019_20_with_season.csv")
    with open(test_file_path, 'rb') as f:
        test_data = f.read()
    
    with patch('src.utils.Minio') as mock_minio:
        mock_client = MagicMock()
        mock_minio.return_value = mock_client
        
        # Mock list_buckets for connection test
        mock_client.list_buckets.return_value = [MagicMock(name='test-bucket')]
        
        # Mock list_objects
        mock_object = MagicMock()
        mock_object.object_name = "test_teams_2019_20_with_season.csv"
        mock_client.list_objects.return_value = [mock_object]
        
        # Mock get_object
        mock_response = MagicMock()
        mock_response.read.return_value = test_data
        mock_response.release_conn = MagicMock()
        mock_client.get_object.return_value = mock_response
        
        # Call the function with teams bucket
        result = fetch_all_from_minio("test-endpoint", "test-key", "test-secret", "teams")
        
        assert result is not None
        assert "test_teams_2019_20_with_season.csv" in result
        df = result["test_teams_2019_20_with_season.csv"]
        
        # Check critical columns
        assert not df['name'].isnull().any(), "name column contains NULL values"
        assert not df['id'].isnull().any(), "id column contains NULL values"
        
        # Verify data integrity
        assert len(df) == 20  # Should have 20 teams
        assert 'Arsenal' in df['name'].values
        assert 'Man City' in df['name'].values
        assert all(df['id'].between(1, 20))  # IDs should be 1-20

def test_fetch_all_from_minio_handles_null_in_teams_critical_columns():
    """Test that fetch_all_from_minio properly handles NULL values in teams critical columns"""
    test_data = (
        "code,name,id,strength\n"
        "3,Arsenal,1,4\n"
        ",Man City,2,5\n"  # Missing code (non-critical) should be kept
        "7,,3,3\n"  # Missing name (critical) should be dropped
        "8,Chelsea,,4\n"  # Missing id (critical) should be dropped
        "1,Man Utd,5,4\n"
    ).encode('utf-8')
    
    with patch('src.utils.Minio') as mock_minio:
        mock_client = MagicMock()
        mock_minio.return_value = mock_client
        
        mock_client.list_buckets.return_value = [MagicMock(name='test-bucket')]
        
        mock_object = MagicMock()
        mock_object.object_name = "test.csv"
        mock_client.list_objects.return_value = [mock_object]
        
        mock_response = MagicMock()
        mock_response.read.return_value = test_data
        mock_response.release_conn = MagicMock()
        mock_client.get_object.return_value = mock_response
        
        result = fetch_all_from_minio("test-endpoint", "test-key", "test-secret", "teams")
        
        assert result is not None
        assert "test.csv" in result
        df = result["test.csv"]
        
        # Should only keep rows with non-null name and id
        assert len(df) == 3
        assert set(df['name']) == {'Arsenal', 'Man City', 'Man Utd'}
        assert set(df['id']) == {1, 2, 5}
        # Verify that missing code (non-critical) doesn't cause row to be dropped
        assert df[df['name'] == 'Man City']['code'].isnull().iloc[0]
