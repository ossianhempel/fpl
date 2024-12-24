import pytest
import os
from unittest.mock import patch, MagicMock
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