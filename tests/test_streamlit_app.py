import os
import pytest
from streamlit.testing.v1.app_test import AppTest
import threading
import psycopg2
from typing import Generator, Any
from psycopg2.extensions import connection as PgConnection # rename to avoid variable conflict
from src.utils.postgres_utils import connect_to_postgres

@pytest.fixture(scope="function")
def database_connection() -> Generator[PgConnection, None, None]:
    """Create a test database connection and verify it works."""
    connection = connect_to_postgres(
        database=os.getenv("PG_DATABASE", "fpl"),
        host=os.getenv("PG_HOST", "65.108.88.160"),
        user=os.getenv("PG_USER", "ossian"),
        password=os.getenv("PG_PASSWORD", "password"),
        port=int(os.getenv("PG_PORT", 5436))
    )
    
    if connection is None:
        pytest.fail("Failed to establish database connection")
    
    try:
        # Verify connection works
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            assert cursor.fetchone()[0] == 1
        yield connection
    finally:
        connection.close()

@pytest.fixture(scope="function")
def verify_data_exists(database_connection: PgConnection) -> None:
    """Verify that the required data exists in the database."""
    with database_connection.cursor() as cursor:
        # Check if the table exists and has data
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_schema = 'dbt_ohempel' 
                AND table_name = 'fact_player_performance'
            )
        """)
        assert cursor.fetchone()[0], "Table fact_player_performance does not exist"
        
        # Check if there's data in the table
        cursor.execute("""
            SELECT COUNT(*) 
            FROM dbt_ohempel.fact_player_performance
        """)
        count = cursor.fetchone()[0]
        assert count > 0, f"Table is empty, expected rows but got {count}"
        
        # Check if required columns exist
        required_columns = [
            'player_name', 'season', 'gameweek', 'team', 'opponent_team',
            'position', 'player_cost', 'total_points', 'goals_scored',
            'assists', 'clean_sheets', 'ict_index', 'minutes_played',
            'kickoff_time', 'selected'
        ]
        
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'dbt_ohempel' 
            AND table_name = 'fact_player_performance'
        """)
        actual_columns = {row[0] for row in cursor.fetchall()}
        missing_columns = set(required_columns) - actual_columns
        assert not missing_columns, f"Missing required columns: {missing_columns}"

def test_app(database_connection: PgConnection, verify_data_exists: None) -> None:
    """Test the Streamlit dashboard with real database connection."""
    # Create AppTest instance
    at = AppTest.from_file("src/streamlit/fpl_dashboard.py")
    
    # Set up test secrets
    at.secrets["PG_DATABASE"] = os.getenv("PG_DATABASE", "fpl")
    at.secrets["PG_HOST"] = os.getenv("PG_HOST", "65.108.88.160")
    at.secrets["PG_USER"] = os.getenv("PG_USER", "postgres")
    at.secrets["PG_PASSWORD"] = os.getenv("PG_PASSWORD", "password")
    at.secrets["PG_PORT"] = int(os.getenv("PG_PORT", 5432))
    
    try:
        # Run with increased timeout (30 seconds)
        at.run(timeout=30)
        
        # Verify no exceptions occurred
        assert not at.exception, f"Exception occurred: {at.exception}"
        
        # Basic UI element checks
        assert at.title[0].value == "Fantasy Premier League Dashboard"
        
        # Verify season selector exists
        assert any("Select Season" in str(element) for element in at.sidebar)
        
    except Exception as e:
        pytest.fail(f"Test failed with exception: {str(e)}")
    finally:
        # Clean up
        if hasattr(at, 'clear'):
            at.clear()

if __name__ == "__main__":
    pytest.main([__file__, "-v"])