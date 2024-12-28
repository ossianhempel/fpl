
import os
import sys
import streamlit as st
import psycopg2
import pandas as pd
from psycopg2.extensions import connection, cursor
from typing import Optional, Tuple, List, Any

# add the project root directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(project_root)

from src.utils.postgres_utils import connect_to_postgres, query_postgres


@st.cache_data
def load_data(
    _connection: connection, 
    schema_name: str, 
    table_name: str
    ) -> Tuple[List[Tuple[Any]], List[str]]:
    connection = _connection  # tell streamlit to not cache connection
    cursor = connection.cursor()
    # Select only the necessary columns based on the dashboard requirements
    columns = """
        player_name, season, gameweek, team, opponent_team, position, player_cost,
        total_points, goals_scored, assists, clean_sheets, ict_index, minutes_played, kickoff_time, selected
    """
    query = f"SELECT {columns} FROM {schema_name}.{table_name}"
    query_postgres(cursor, query)
    data = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    cursor.close()
    connection.close()
    return data, column_names


@st.cache_resource
def postgres_utils(
    database: str, 
    host: str, 
    user: str, 
    password: str, 
    port: int,
    ) -> Optional[connection]:
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