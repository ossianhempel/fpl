
import os
import sys
import streamlit as st
import psycopg2

# add the project root directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(project_root)

from src.utils import connect_to_postgres, query_postgres


@st.cache_data
def load_data(_connection, schema_name, table_name):
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