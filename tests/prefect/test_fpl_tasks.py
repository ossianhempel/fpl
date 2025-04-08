from unittest.mock import Mock, patch
from io import BytesIO
from src.prefect_files.tasks.fpl_tasks import download_gws


@patch("src.prefect_files.tasks.fpl_tasks.SourceFileIngestor")
def test_download_gws(mock_ingestor_class):
    # configure mock class to return our mock instance
    mock_instance = Mock()
    mock_instance.download_source_file.return_value = BytesIO(b"mock,csv,data")
    mock_ingestor_class.return_value = mock_instance

    result = download_gws.fn(
        season="2023-24",
        minio_endpoint="test-endpoint",
        minio_access_key="test-access",
        minio_secret_key="test-secret",
    )

    # verify SourceFileIngestor was initialized with correct creds
    mock_ingestor_class.assert_called_once_with(
        minio_endpoint="test-endpoint",
        minio_access_key="test-access",
        minio_secret_key="test-secret",
    )

    # verify download_source_file was called for each gameweek (sampling first few)
    expected_calls = [
        f"https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/2023-24/gws/gw{i}.csv"
        for i in range(1, 4)  # Just check the first 3 for brevity
    ]

    for url in expected_calls:
        assert any(
            call[0][0] == url
            for call in mock_instance.download_source_file.call_args_list
        ), f"{url} did not match a call in the call list"

    # verify load_to_minio was called correctly
    assert any(
        call[1]["destination_bucket"] == "bronze"
        and "gameweeks/2023-24/gw_2023-24_gw" in call[1]["destination_object_path"]
        for call in mock_instance.load_to_minio.call_args_list
    )

    # verify the number of calls matches our expectation (39 gameweeks)
    assert mock_instance.download_source_file.call_count == 39
    assert mock_instance.load_to_minio.call_count == 39

    assert result is None
