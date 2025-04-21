import pytest
import io
import os
from unittest.mock import patch, MagicMock, Mock
from requests.exceptions import HTTPError
from minio import Minio
import polars as pl
from datetime import datetime

# Import the class to test
from src.etl_pipeline.components.source_extraction import (
    SourceFileIngestor,
)


class TestSourceFileIngestor:
    """Test cases for the SourceFileIngestor class."""

    @pytest.fixture
    def data_dir(self) -> str:
        """Returns the path of the test data"""
        return os.path.join("tests", "test_data")

    # valid csv data
    @pytest.fixture
    def valid_gw_data(self, data_dir: str) -> io.BytesIO:
        """Load a valid test CSV into bytes"""
        path = os.path.join(data_dir, "valid_gw_data.csv")
        with open(path, "rb") as f:
            data = io.BytesIO(f.read())
        return data

    @pytest.fixture
    def mock_minio_client(self) -> MagicMock:
        """Fixture that returns a mock Minio client."""
        mock_client = MagicMock(
            spec=Minio
        )  # creates mock object with correctly named attributes/methods (but not correct behaviors)
        mock_client.bucket_exists.return_value = (
            True  # define the behavior or bucket_exists method
        )

        # Create mock objects
        mock_objects = [
            Mock(
                object_name="data/file1.csv",
                last_modified=datetime(2023, 5, 15, 10, 30, 0),
                etag="a1b2c3d4e5f6",
                size=1024,
                content_type="text/csv",
                is_dir=False,
            ),
            Mock(
                object_name="data/file2.json",
                last_modified=datetime(2023, 5, 16, 14, 45, 0),
                etag="f6e5d4c3b2a1",
                size=2048,
                content_type="application/json",
                is_dir=False,
            ),
        ]
        mock_client.list_objects.return_value = mock_objects

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
        mock_minio_client.put_object.assert_called_once()  # make sure minio upload is called

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
        mock_minio_client.make_bucket.assert_called_once_with(
            "bronze"
        )  # when the bucket doesnt exist, it should call the make_bucket method

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

    def test_add_metadata(self, ingestor, valid_gw_data):
        result = ingestor._add_metadata(data=valid_gw_data)

        assert result, "add_metadata function didn't work"

        df = pl.read_csv(result)
        assert (
            "ingestion_timestamp" in df.columns
        ), f"ingestion_timestamp was not present in the columns: {df.columns}"
        assert (
            df["ingestion_timestamp"].is_not_null().sum() > 0
        ), "There are nulls in timestamp column"
        # assert isinstance(df['ingestion_timestamp'], datetime), f"ingestion_timestamp was of type: {df['ingestion_timestamp'].dtype}"

    def test_add_gameweek(self, ingestor, valid_gw_data) -> None:
        """Test that gameweek is added as a column"""
        data_with_gw = ingestor._add_gameweek(valid_gw_data, 5)
        df = pl.read_csv(data_with_gw)
        assert (
            "gw" in df.columns
        ), f"Couldn't find the gameweek column in data: {df.columns}"
        assert df["gw"].min() == 5, f"Found wrong value for gw: {df['gw'].min()}"
        assert df["gw"].max() == 5, f"Found wrong value for gw: {df['gw'].max()}"

    def test_validate_data(self, mock_minio_client, ingestor, valid_gw_data):
        """Test data validation."""
        # Since the implementation always returns True, this is a simple test
        test_data = io.BytesIO(b"test,data\n1,2\n3,4")
        result = ingestor._validate_data(
            data=test_data, destination_bucket="bronze", destination_object_path=""
        )
        assert result is True

        result2 = ingestor._validate_data(
            data=valid_gw_data, destination_bucket="bronze", destination_object_path=""
        )
        assert result2 is True

        # at this stage it just logs some statistics about the data, but even if data is faulty somehow, it should be added
        # to bronze and then we will catch it when cleaning it into silver
