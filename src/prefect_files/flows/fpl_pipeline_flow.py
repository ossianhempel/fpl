import os
from dotenv import load_dotenv
from prefect import flow

from src.prefect_files.tasks.fpl_tasks import download_gws

load_dotenv()
WEB_SERVER_URL = os.getenv("PREFECT_WEB_SERVER_URL")

season = "2024-25"


@flow(name="fpl_data_pipeline", log_prints=True, retries=2)
def fpl_pipeline_flow():
    # download new gws
    download_gws(season=season)

    # TODO: download fixtures

    # TODO: bronze -> silver transformation

    # TODO: silver -> gold transformation (is this DBT modeling?)

    # TODO: DBT Core modeling


if __name__ == "__main__":
    fpl_pipeline_flow()


"""
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
2025-03-26T22:32:45.871324742Z pydantic_core._pydantic_core.ValidationError: 1 validation error for Deployment
2025-03-26T22:32:45.871333820Z name
2025-03-26T22:32:45.871342636Z   String should match pattern '^[^/%&><]*$' [type=string_pattern_mismatch, input_value='prefect/flows/fpl_pipeline_flow.py', input_type=str]
2025-03-26T22:32:45.871352284Z     For further information visit https://errors.pydantic.dev/2.10/v/string_pattern_mismatch    
"""
