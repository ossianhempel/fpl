import pytest
from streamlit.testing.v1.app_test import AppTest

def test_app():
    """Test the Streamlit dashboard with real database connection."""
    # Create AppTest instance
    at = AppTest.from_file("src/streamlit/fpl_dashboard.py")

    # Set up test secrets
    at.secrets["PG_DATABASE"] = "fpl"
    at.secrets["PG_HOST"] = "localhost"
    at.secrets["PG_USER"] = "postgres"
    at.secrets["PG_PASSWORD"] = "postgres"
    at.secrets["PG_PORT"] = "5432"

    # Run with increased timeout (30 seconds)
    at.run(timeout=30)

    # Clean up
    at.clear()