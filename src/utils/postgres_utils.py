import os
import sys
import urllib3
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import io
import pandas as pd
import psycopg2
from psycopg2.extensions import connection, cursor
from sqlalchemy import create_engine
import csv
from typing import Optional, Any

def connect_to_postgres(database: str, host: str, user: str, password: str, port: str) -> Optional[connection]:
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

def query_postgres(cursor: cursor, query: str) -> None:
    """
    Execute a query on the given PostgreSQL cursor and commit the transaction.

    Args:
        cursor (Cursor): The psycopg2 cursor object.
        query (str): The SQL query to execute.

    Returns:
        None
    """
    try:
        cursor.execute(query)
        cursor.connection.commit()
    except Exception as e:
        print(f"Error executing query: {e}")
        cursor.connection.rollback()