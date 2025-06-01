import os
from minio import Minio
from minio.error import S3Error
import io
import pandas as pd
import csv
from typing import Optional, Dict
import logging

logger = logging.getLogger(__name__)


# TODO: convert print to logging
# TODO: raise error if failing to create client
def create_minio_client(endpoint: str, access_key: str, secret_key: str) -> Minio:
    """Connect to MinIO and create a client, with detailed error logging"""
    logger.info(
        f"Creating MinIO client with endpoint: {endpoint}, access_key: {access_key}"
    )
    logger.info(f"Attempting to connect to MinIO at endpoint: {endpoint}")  # Debug log
    try:
        if not all([endpoint, access_key, secret_key]):
            raise Exception(
                "Missing credentials:",
                {  # Debug log
                    "endpoint": bool(endpoint),
                    "access_key": bool(access_key),
                    "secret_key": bool(secret_key),
                },
            )

        client = Minio(
            endpoint=str(endpoint),
            access_key=access_key,
            secret_key=secret_key,
            secure=False,
            cert_check=False,
        )

        # Test connection by listing buckets
        buckets = client.list_buckets()
        print(
            f"Successfully connected to MinIO. Available buckets: {[b.name for b in buckets]}"
        )  # Debug log
        return client

    except S3Error as e:
        logger.error(f"S3 Error connecting to MinIO: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error connecting to MinIO: {str(e)}")
        raise


# TODO: this one currently only takes a source file path, what if we want to pass data directly?
def upload_to_minio(
    client: Minio,
    source_file_path: str,
    destination_bucket: str,
    destination_folder_path: str = "",
) -> None:
    if client is None:
        logging.error("Failed to upload: No MinIO client provided")
        return

    try:
        # create the destination bucket if it doesnt exist
        if not client.bucket_exists(destination_bucket):
            logging.info(f"Bucket {destination_bucket} does not exist, creating...")
            client.make_bucket(destination_bucket)
            print(f"Created bucket '{destination_bucket}'")

        logging.info(
            f"Attempting to upload file {source_file_path} to {destination_bucket}/{destination_folder_path}"
        )

        # determine the content type based on file stub
        content_type = "application/octet-stream"
        if source_file_path.endswith(".py"):
            content_type = "text/x-python"
        elif source_file_path.endswith(".csv"):
            content_type = "application/csv"
        elif source_file_path.endswith(".json"):
            content_type = "application/json"
        elif source_file_path.endswith(".txt"):
            content_type = "text/plain"
        elif source_file_path.endswith("parquet"):
            content_type = "application/vnd.apache.parquet"

        bucket_name = destination_bucket
        object_name = destination_folder_path

        with open(source_file_path, "rb") as file_data:
            file_size = os.path.getsize(source_file_path)
            client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=file_data,
                length=file_size,
                content_type=content_type,
            )
            logging.info(
                f"Successfully uploaded '{object_name}' to bucket '{bucket_name}'"
            )
    except S3Error as e:
        logging.error(f"S3 Error during upload: {str(e)}")
        raise Exception(f"S3 Error: {str(e)}")
    except Exception as e:
        logging.error(f"Unexpected error during upload: {str(e)}")
        raise Exception(f"Upload error: {str(e)}")


# TODO: refactor to take a client object instead of initializing in method
def fetch_from_minio(
    endpoint: str, access_key: str, secret_key: str, object_name: str
) -> Optional[pd.DataFrame]:
    client = create_minio_client(endpoint, access_key, secret_key)

    if client is None:
        print("Failed to connect to MinIO")
        return None

    bucket_name = "hemnet-listings"

    try:
        response = client.get_object(bucket_name, object_name)
        data = response.read()
        response.release_conn()
        print(f"Fetched '{object_name}' from bucket '{bucket_name}'")

        # Convert bytes data to a pandas DataFrame
        data_stream = io.BytesIO(data)
        df = pd.read_csv(data_stream)
        return df

    except S3Error as e:
        print("S3 Error: ", e)
        return None
    except Exception as e:
        print("Error: ", e)
        return None


# TODO: refactor for clarity, currently it feetches all with the assumption of them being csv compatible and of a certain format
# TODO: should be able to specify a path to a folder (if we want to fetch from just a subfolder within a bucket)
def fetch_all_from_minio(
    client: Minio,
    bucket_name: str,
    folder_path: Optional[str] = None,
) -> Optional[Dict[str, pd.DataFrame]]:
    """
    Fetch all CSV files from a MinIO bucket (optionally within a folder) and return them as a dictionary of DataFrames.

    Args:
        client (Minio): MinIO client object.
        bucket_name (str): Name of the bucket to fetch objects from.
        folder_path (Optional[str]): Path to the folder within the bucket (default: None, fetches from root).

    Returns:
        Optional[Dict[str, pd.DataFrame]]: A dictionary mapping object names to DataFrames,
                                            or None if no dataframes are retrieved or the connection fails.
    """

    dataframes = {}

    try:
        prefix = folder_path.strip("/") + "/" if folder_path else ""
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        for obj in objects:
            try:
                response = client.get_object(bucket_name, obj.object_name)
                try:
                    data = response.read()
                    data_str = data.decode("utf-8", errors="replace").strip()

                    # handle empty or header-only
                    if data_str.count("\n") <= 1:
                        logger.info(f"Empty file or header-only: {obj.object_name}")
                        if data_str:
                            headers = data_str.split("\n")[0].split(",")
                            dataframes[obj.object_name] = pd.DataFrame(columns=headers)
                        continue

                    # parse CSV into a DataFrame
                    df = pd.read_csv(
                        io.StringIO(data_str),
                        engine="python",
                        quoting=csv.QUOTE_MINIMAL,
                        encoding="utf-8",
                        escapechar="\\",
                        na_values=["", "None", "null", "nan", "NaN", "NAN"],
                        keep_default_na=True,
                        on_bad_lines="skip",
                    )
                    dataframes[obj.object_name] = df
                finally:
                    response.release_conn()
            except Exception as e:
                logger.error(f"Error processing file {obj.object_name}: {e}")
    except Exception as e:
        logger.error(f"Error fetching objects from bucket {bucket_name}: {e}")
        return None  # return None if fetching objects fails

    return dataframes if dataframes else None
