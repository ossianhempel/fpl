import os
from prefect import flow

from src.prefect_files.tasks.fpl_tasks import get_minio_secret, download_gws

WEB_SERVER_URL = os.getenv("PREFECT_WEB_SERVER_URL")

season = "2024-25"


@flow(name="fpl_data_pipeline", log_prints=True, retries=2)
def fpl_pipeline_flow():
    # load minio secret
    minio_secret = get_minio_secret()

    # download new gws
    download_gws(season=season, minio_secret_key=minio_secret)

    # TODO: download fixtures

    # TODO: bronze -> silver transformation

    # TODO: silver -> gold transformation (is this DBT modeling?)

    # TODO: DBT Core modeling


if __name__ == "__main__":
    fpl_pipeline_flow()
