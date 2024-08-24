import os
import urllib3
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import io
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# TODO - import most of these from my shared repo insteaD? 

def connect_to_postgres(database, host, user, password, port):
    try:
        connection = psycopg2.connect(
                            database=database,
                            host=host,
                            user=user,
                            password=password,
                            port=port)
        cursor = connection.cursor()
        print('Connection to PG established, Cursor object returned.')
        return cursor
    except Exception as e:
        print('Error: ', e)

def query_postgres(cursor, query):
    cursor.execute(query)
    cursor.connection.commit()
    # cursor.close()
    # cursor.connection.close()


def connect_to_minio(endpoint, access_key, secret_key):
    try:
        client = Minio(endpoint,
                        access_key=access_key, # user id
                        secret_key=secret_key, # service password
                        secure=False, # TODO - make these work with true
                        cert_check=False,
                    )
        
        
        print('Connected to MinIO')
        return client
    
    except S3Error as e:
        print("S3 Error: ", e)
        return None

def upload_to_minio(client: Minio, file_path: str, destination_bucket: str, destination_folder_path: str=""):
    if client is None:
        print("Failed to connect to MinIO")
        return

    bucket_name = destination_bucket
    object_name = os.path.join(destination_folder_path, os.path.basename(file_path)).replace("\\", "/")

    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Created bucket '{bucket_name}'")

        # Determine the content type based on the file extension
        content_type = 'application/octet-stream'
        if file_path.endswith('.py'):
            content_type = 'text/x-python'
        elif file_path.endswith('.csv'):
            content_type = 'text/csv'
        elif file_path.endswith('.json'):
            content_type = 'application/json'
        elif file_path.endswith('.txt'):
            content_type = 'text/plain'
        # Add more content types as needed

        with open(file_path, 'rb') as file_data:
            file_size = os.path.getsize(file_path)
            client.put_object(bucket_name, object_name, file_data, file_size, content_type=content_type)
            print(f"Uploaded '{object_name}' to bucket '{bucket_name}'")
        
        # remove the local file after uploading
        if file_path.endswith('.csv'):
            try:
                os.remove(file_path)
                print(f"Removed local version of {file_path} as the file was successfully uploaded.")
            except:
                print(f"Found no path like: {file_path}")

    except S3Error as e:
        print("S3 Error: ", e)
    except Exception as e:
        print("Error: ", e)


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
    
def fetch_all_from_minio(endpoint, access_key, secret_key):
    # TODO - refactor to take bucket name and client as parameters
    client = connect_to_minio(endpoint, access_key, secret_key)

    if client is None:
        print("Failed to connect to MinIO")
        return None

    bucket_name = 'gameweeks'
    dataframes = {}

    try:
        objects = client.list_objects(bucket_name, recursive=True)
        for obj in objects:
            response = client.get_object(bucket_name, obj.object_name)
            data = response.read()
            response.release_conn()
            
            # Convert bytes data to a pandas DataFrame
            data_stream = io.BytesIO(data)
            df = pd.read_csv(data_stream)
            dataframes[obj.object_name] = df
            print(f"Fetched '{obj.object_name}' from bucket '{bucket_name}'")

    except S3Error as e:
        print("S3 Error: ", e)
        return None
    except Exception as e:
        print("Error: ", e)
        return None
    
    return dataframes