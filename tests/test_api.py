import os
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
import sys
from pathlib import Path

# Add src to path for imports
src_path = str(Path(__file__).parent.parent)
if src_path not in sys.path:
    sys.path.append(src_path)

from src.fastapi.api import app, DataSource

client = TestClient(app)

# Test data paths
TEST_DATA_DIR = Path(__file__).parent.parent / "data" / "teams"
TEST_FILES = {
    "teams_2019_20.csv": TEST_DATA_DIR / "teams_2019_20.csv",
    "teams_2020_21.csv": TEST_DATA_DIR / "teams_2020_21.csv",
    "teams_2021_22.csv": TEST_DATA_DIR / "teams_2021_22.csv",
    "teams_2022_23.csv": TEST_DATA_DIR / "teams_2022_23.csv",
    "teams_2023_24.csv": TEST_DATA_DIR / "teams_2023_24.csv",
    "teams_2024_25.csv": TEST_DATA_DIR / "teams_2024_25.csv",
}

def test_health_check():
    """Test the health check endpoint"""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}

@pytest.mark.parametrize("source", ["fixtures", "gameweeks", "teams"])
def test_upload_endpoint_validation(source):
    """Test that upload endpoint validates source parameter"""
    response = client.post(f"/upload/{source}")
    assert response.status_code == 422  # Validation error - missing file

@patch("src.fastapi.api.upload_to_minio")
def test_upload_teams_file(mock_upload):
    """Test uploading a teams file"""
    # Setup mock
    mock_upload.return_value = None
    
    # Test file path
    test_file_path = TEST_FILES["teams_2024_25.csv"]
    
    # Create test file upload
    with open(test_file_path, "rb") as f:
        files = {"file": ("teams_2024_25.csv", f, "text/csv")}
        response = client.post("/upload/teams", files=files)
    
    assert response.status_code == 200
    assert "Successfully uploaded" in response.json()["message"]
    mock_upload.assert_called_once()

@patch("src.fastapi.api.upload_to_minio")
def test_upload_fixtures_file(mock_upload):
    """Test uploading a fixtures file"""
    mock_upload.return_value = None
    
    # Create a temporary test file
    test_content = b"fixture_id,team_h,team_a,date\n1,1,2,2024-08-10"
    files = {"file": ("fixtures.csv", test_content, "text/csv")}
    
    response = client.post("/upload/fixtures", files=files)
    
    assert response.status_code == 200
    assert "Successfully uploaded" in response.json()["message"]
    mock_upload.assert_called_once()

@patch("src.fastapi.api.upload_to_minio")
def test_upload_gameweeks_file(mock_upload):
    """Test uploading a gameweeks file"""
    mock_upload.return_value = None
    
    # Create a temporary test file
    test_content = b"gw,team,points\n1,Arsenal,3"
    files = {"file": ("gameweeks.csv", test_content, "text/csv")}
    
    response = client.post("/upload/gameweeks", files=files)
    
    assert response.status_code == 200
    assert "Successfully uploaded" in response.json()["message"]
    mock_upload.assert_called_once()

def test_upload_invalid_source():
    """Test uploading to an invalid source"""
    files = {"file": ("test.csv", b"test data", "text/csv")}
    response = client.post("/upload/invalid", files=files)
    assert response.status_code == 422  # Validation error

@patch("src.fastapi.api.upload_to_minio")
def test_upload_failure(mock_upload):
    """Test handling of upload failures"""
    mock_upload.side_effect = Exception("Upload failed")
    
    files = {"file": ("test.csv", b"test data", "text/csv")}
    response = client.post("/upload/teams", files=files)
    
    assert response.status_code == 500
    assert "Upload failed" in response.json()["detail"]

@patch("src.components.data_ingestion_fixtures.DataIngestion.ingest_data")
def test_ingest_fixtures(mock_ingest):
    """Test ingesting fixtures data"""
    mock_ingest.return_value = None
    
    response = client.post(
        "/ingest/fixtures",
        json={"source": "fixtures"}
    )
    
    assert response.status_code == 200
    assert "Successfully ingested" in response.json()["message"]
    mock_ingest.assert_called_once()

@patch("src.components.data_ingestion_gameweeks.DataIngestion.ingest_data")
def test_ingest_gameweeks(mock_ingest):
    """Test ingesting gameweeks data"""
    mock_ingest.return_value = None
    
    response = client.post(
        "/ingest/gameweeks",
        json={"source": "gameweeks"}
    )
    
    assert response.status_code == 200
    assert "Successfully ingested" in response.json()["message"]
    mock_ingest.assert_called_once()

def test_ingest_teams():
    """Test ingesting teams data (not supported)"""
    response = client.post(
        "/ingest/teams",
        json={"source": "teams"}
    )
    
    assert response.status_code == 400
    assert "Ingestion not supported" in response.json()["detail"]

def test_ingest_source_mismatch():
    """Test source mismatch in ingestion request"""
    response = client.post(
        "/ingest/fixtures",
        json={"source": "gameweeks"}
    )
    
    assert response.status_code == 400
    assert "Source mismatch" in response.json()["detail"]

@patch("src.components.data_ingestion_fixtures.DataIngestion.ingest_data")
def test_ingest_failure(mock_ingest):
    """Test handling of ingestion failures"""
    mock_ingest.side_effect = Exception("Ingestion failed")
    
    response = client.post(
        "/ingest/fixtures",
        json={"source": "fixtures"}
    )
    
    assert response.status_code == 500
    assert "Ingestion failed" in response.json()["detail"]

def test_ingest_invalid_source():
    """Test ingesting with invalid source"""
    response = client.post(
        "/ingest/invalid",
        json={"source": "invalid"}
    )
    
    assert response.status_code == 422  # Validation error
