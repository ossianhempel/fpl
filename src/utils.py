import os
import urllib3
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import io
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import csv

# TODO - import most of these from my shared repo insteaD? 

def connect_to_postgres(database, host, user, password, port):
    try:
        connection = psycopg2.connect(
            database=database,
            host=host,
            user=user,
            password=password,
            port=port
        )
        print('Connection to PG established, Connection object returned.')
        return connection  # Return the connection object, not the cursor
    except Exception as e:
        print('Error: ', e)
        return None

def query_postgres(cursor, query):
    cursor.execute(query)
    cursor.connection.commit()
    # cursor.close()
    # cursor.connection.close()


def connect_to_minio(endpoint, access_key, secret_key):
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

def upload_to_minio(client: Minio, file_path: str, destination_bucket: str, destination_folder_path: str=""):
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


def fetch_from_minio(endpoint, access_key, secret_key, object_name):
    client = connect_to_minio(endpoint, access_key, secret_key)

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
    
def fetch_all_from_minio(endpoint, access_key, secret_key, bucket_name=''):
    client = connect_to_minio(endpoint, access_key, secret_key)
    if client is None:
        return None

    CRITICAL_COLUMNS = {
        'gameweeks': ['GW', 'team', 'name'],
        'teams': ['team', 'season']
    }
    critical_cols = CRITICAL_COLUMNS.get(bucket_name, [])
    dataframes = {}

    try:
        objects = client.list_objects(bucket_name, recursive=True)
        for obj in objects:
            response = client.get_object(bucket_name, obj.object_name)
            data = response.read()
            response.release_conn()

            data_str = data.decode('utf-8', errors='replace').strip()
            if not data_str:
                dataframes[obj.object_name] = pd.DataFrame(columns=critical_cols)
                continue

            # read entire file into a df, skip bad lines
            df = pd.read_csv(
                io.StringIO(data_str),
                engine='python',
                quoting=csv.QUOTE_MINIMAL,
                encoding='utf-8',
                escapechar='\\',
                na_values=['', 'None', 'null'],
                keep_default_na=True,
                on_bad_lines='skip'
            )

            # drop rows where any critical col is null
            if critical_cols:
                df = df.dropna(subset=critical_cols)

            dataframes[obj.object_name] = df
    except S3Error as e:
        print(f"S3 Error: {e}")
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None

    return dataframes