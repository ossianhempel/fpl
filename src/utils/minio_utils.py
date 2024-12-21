import os
from minio import Minio
from minio.error import S3Error
import io
import pandas as pd
import csv
import mypy
from typing import Optional, Dict

def create_minio_client(endpoint: str, access_key: str, secret_key: str) -> Optional[Minio]:
    """Connect to MinIO with detailed error logging"""
    try:
        print(f"Attempting to connect to MinIO at endpoint: {endpoint}")  # Debug log
        
        if not all([endpoint, access_key, secret_key]):
            print("Missing credentials:", {  # Debug log
                "endpoint": bool(endpoint),
                "access_key": bool(access_key),
                "secret_key": bool(secret_key)
            })
            return None
            
        client = Minio(endpoint,
                    access_key=access_key,
                    secret_key=secret_key,
                    secure=False,
                    cert_check=False,
                )
        
        # Test connection by listing buckets
        buckets = client.list_buckets()
        print(f"Successfully connected to MinIO. Available buckets: {[b.name for b in buckets]}")  # Debug log
        return client
    
    except S3Error as e:
        print(f"S3 Error connecting to MinIO: {str(e)}")  # Debug log
        return None
    except Exception as e:
        print(f"Unexpected error connecting to MinIO: {str(e)}")  # Debug log
        return None
    
def upload_to_minio(client: Minio, file_path: str, destination_bucket: str, destination_folder_path: str="") -> None:
    """Upload to MinIO with detailed error logging"""
    if client is None:
        print("Failed to upload: No MinIO client provided")
        return

    bucket_name = destination_bucket
    # Fix: Use destination_folder_path directly as the object name
    object_name = destination_folder_path

    try:
        print(f"Checking bucket: {bucket_name}")  # Debug log
        if not client.bucket_exists(bucket_name):
            print(f"Bucket {bucket_name} does not exist, creating...")  # Debug log
            client.make_bucket(bucket_name)
            print(f"Created bucket '{bucket_name}'")

        print(f"Uploading file {file_path} to {bucket_name}/{object_name}")  # Debug log
        
        # Determine the content type
        content_type = 'application/octet-stream'
        if file_path.endswith('.py'):
            content_type = 'text/x-python'
        elif file_path.endswith('.csv'):
            content_type = 'text/csv'
        elif file_path.endswith('.json'):
            content_type = 'application/json'
        elif file_path.endswith('.txt'):
            content_type = 'text/plain'

        with open(file_path, 'rb') as file_data:
            file_size = os.path.getsize(file_path)
            client.put_object(bucket_name, object_name, file_data, file_size, content_type=content_type)
            print(f"Successfully uploaded '{object_name}' to bucket '{bucket_name}'")
        
        # remove the local file after uploading
        if file_path.endswith('.csv'):
            try:
                os.remove(file_path)
                print(f"Removed local version of {file_path}")
            except Exception as e:
                print(f"Failed to remove local file: {str(e)}")

    except S3Error as e:
        print(f"S3 Error during upload: {str(e)}")
        raise Exception(f"S3 Error: {str(e)}")
    except Exception as e:
        print(f"Unexpected error during upload: {str(e)}")
        raise Exception(f"Upload error: {str(e)}")


def fetch_from_minio(endpoint: str, access_key: str, secret_key: str, object_name: str) -> Optional[pd.DataFrame]:
    client = create_minio_client(endpoint, access_key, secret_key)

    if client is None:
        print("Failed to connect to MinIO")
        return None

    bucket_name = 'hemnet-listings'

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
    
def fetch_all_from_minio(endpoint: str, access_key: str, secret_key: str, bucket_name: str) -> Optional[Dict[str, pd.DataFrame]]:
    """
    Fetch all CSV files from a MinIO bucket and return them as a dictionary of DataFrames.

    Args:
        endpoint (str): MinIO server endpoint.
        access_key (str): Access key for MinIO.
        secret_key (str): Secret key for MinIO.
        bucket_name (str): Name of the bucket to fetch objects from.

    Returns:
        Optional[Dict[str, pd.DataFrame]]: A dictionary mapping object names to DataFrames,
                                           or None if no dataframes are retrieved or the connection fails.
    """
    client = create_minio_client(endpoint, access_key, secret_key)
    if client is None:
        print("Failed to connect to MinIO")
        return None  # return None if connection fails

    dataframes = {}

    try:
        objects = client.list_objects(bucket_name, recursive=True)
        for obj in objects:
            try:
                response = client.get_object(bucket_name, obj.object_name)
                try:
                    data = response.read()
                    data_str = data.decode('utf-8', errors='replace').strip()
                    
                    # handle empty or header-only
                    if data_str.count('\n') <= 1:
                        print(f"Empty file or header-only: {obj.object_name}")
                        if data_str:
                            headers = data_str.split('\n')[0].split(',')
                            dataframes[obj.object_name] = pd.DataFrame(columns=headers)
                        continue

                    # parse CSV into a DataFrame
                    df = pd.read_csv(
                        io.StringIO(data_str),
                        engine='python',
                        quoting=csv.QUOTE_MINIMAL,
                        encoding='utf-8',
                        escapechar='\\',
                        na_values=['', 'None', 'null', 'nan', 'NaN', 'NAN'],
                        keep_default_na=True,
                        on_bad_lines='skip'
                    )
                    dataframes[obj.object_name] = df
                finally:
                    response.release_conn()
            except Exception as e:
                print(f"Error processing file {obj.object_name}: {e}")
    except Exception as e:
        print(f"Error fetching objects from bucket {bucket_name}: {e}")
        return None  # return None if fetching objects fails

    return dataframes if dataframes else None