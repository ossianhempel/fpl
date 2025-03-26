import pytest
import io
from unittest.mock import patch, MagicMock
from requests.exceptions import HTTPError
from minio import Minio

# Import the class to test
from src.etl_pipeline.components.source_extraction import (
    SourceFileIngestor,
    SourceFileIngestorConfig,
)


class TestSourceFileIngestor:
    """Test cases for the SourceFileIngestor class."""

    @pytest.fixture
    def mock_minio_client(self):
        """Fixture that returns a mock Minio client."""
        mock_client = MagicMock(spec=Minio)
        mock_client.bucket_exists.return_value = True
        return mock_client

    @pytest.fixture
    def ingestor(self, mock_minio_client):
        """Fixture that creates an ingestor with a mocked Minio client."""
        with patch(
            "src.etl_pipeline.components.source_extraction.create_minio_client",
            return_value=mock_minio_client,
        ):
            ingestor = SourceFileIngestor()
            return ingestor

    @patch("src.etl_pipeline.components.source_extraction.requests.get")
    def test_successful_download(self, mock_get, ingestor):
        """Test that file is successfully downloaded when request is valid."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.content = b"test,data\n1,2\n3,4"
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Call the function
        result = ingestor.download_source_file("https://example.com/test.csv")

        # Assertions
        mock_get.assert_called_once_with("https://example.com/test.csv")
        mock_response.raise_for_status.assert_called_once()

        # Verify the file content is as expected
        assert isinstance(result, io.BytesIO)
        result.seek(0)
        content = result.read()
        assert content == b"test,data\n1,2\n3,4"

    @patch("src.etl_pipeline.components.source_extraction.requests.get")
    def test_http_error(self, mock_get, ingestor):
        """Test that download_source_file returns None when the request fails."""
        # Setup mock to raise HTTPError
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = HTTPError("404 Client Error")
        mock_get.return_value = mock_response

        # Call function and verify it returns None
        result = ingestor.download_source_file("https://example.com/nonexistent.csv")
        assert result is None

    def test_load_to_minio_success(self, ingestor, mock_minio_client):
        """Test successful upload to MinIO."""
        # Mock data
        test_data = io.BytesIO(b"test,data\n1,2\n3,4")

        # Call function
        result = ingestor.load_to_minio(
            data=test_data,
            destination_bucket="bronze",
            destination_object_path="test_file.csv",
        )

        # Assertions
        assert result is True
        mock_minio_client.put_object.assert_called_once()
        # Verify correct bucket and path were used
        assert mock_minio_client.put_object.call_args[1]["bucket_name"] == "bronze"
        assert (
            mock_minio_client.put_object.call_args[1]["object_name"] == "test_file.csv"
        )

    def test_load_to_minio_create_bucket(self, ingestor, mock_minio_client):
        """Test MinIO upload creates bucket if it doesn't exist."""
        # Setup mock to say bucket doesn't exist
        mock_minio_client.bucket_exists.return_value = False

        # Mock data
        test_data = io.BytesIO(b"test,data\n1,2\n3,4")

        # Call function
        ingestor.load_to_minio(
            data=test_data,
            destination_bucket="bronze",
            destination_object_path="test_file.csv",
        )

        # Verify bucket was created
        mock_minio_client.make_bucket.assert_called_once_with("bronze")

    def test_load_to_minio_exception(self, ingestor, mock_minio_client):
        """Test MinIO upload handles exceptions properly."""
        # Setup mock to raise exception
        mock_minio_client.put_object.side_effect = Exception("Connection error")

        # Mock data
        test_data = io.BytesIO(b"test,data\n1,2\n3,4")

        # Call function
        result = ingestor.load_to_minio(
            data=test_data,
            destination_bucket="bronze",
            destination_object_path="test_file.csv",
        )

        # Verify failure is reported
        assert result is False

    # @patch('src.etl_pipeline.components.source_extraction.pl.DataFrame')
    # def test_add_metadata(self, mock_dataframe, ingestor):
    #     """Test metadata is correctly added to the data."""
    #     # Setup
    #     mock_df = MagicMock()
    #     mock_dataframe.return_value = mock_df
    #     mock_df.__getitem__.return_value = None
    #     test_data = io.BytesIO(b'test,data\n1,2\n3,4')

    #     # Call the method (which is private, so we're accessing it directly for testing)
    #     result = ingestor._add_metadata(test_data)

    #     # Assertions
    #     mock_dataframe.assert_called_once()
    #     assert 'ingestion_timestamp' in mock_df.__setitem__.call_args[0]

    # def test_validate_data(self, ingestor):
    #     """Test data validation."""
    #     # Since the implementation always returns True, this is a simple test
    #     test_data = io.BytesIO(b'test,data\n1,2\n3,4')
    #     result = ingestor._validate_data(test_data)
    #     assert result is True

    def test_config_defaults(self):
        """Test that configuration uses default values."""
        # Create config with default values
        config = SourceFileIngestorConfig()

        # Verify the default values
        assert config.destination_bucket == "bronze"
        # We don't test the exact values of endpoint/keys as they depend on environment
        assert isinstance(config.minio_endpoint, str)
        assert isinstance(config.minio_access_key, str)
        assert isinstance(config.minio_secret_key, str)
