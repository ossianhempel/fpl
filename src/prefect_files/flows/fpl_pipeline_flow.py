import os
from prefect import flow
from prefect.blocks.system import Secret

from src.prefect_files.tasks.fpl_tasks import download_gws

WEB_SERVER_URL = os.getenv("PREFECT_WEB_SERVER_URL")

season = "2024-25"


secret_block = Secret.load("fpl-minio-secret-key")
secret_block.get()


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
