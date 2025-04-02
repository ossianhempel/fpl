import os
from prefect import flow
from prefect.blocks.system import Secret

from src.prefect_files.tasks.fpl_tasks import download_gws

WEB_SERVER_URL = os.getenv("PREFECT_WEB_SERVER_URL")

season = "2024-25"

# Secret(value="sk-1234567890").save("fpl-minio-secret-key", overwrite=True)
secret_block = Secret.load("fpl-minio-secret-key")
secret_block.get()


@flow(name="fpl_data_pipeline", log_prints=True, retries=2)
def fpl_pipeline_flow():
    # load environment variables fomr Prefect Secret blocks
    os.environ[
        "MINIO_ENDPOINT"
    ] = "minio-yokckg4o44wg40wogk0okgks.65.108.88.160.sslip.io"
    os.environ["MINIO_ACCESS_KEY"] = "minio-fpl"
    os.environ["MINIO_SECRET_KEY"] = Secret.load("fpl-minio-secret-key").get()

    # download new gws
    download_gws(season=season)

    # TODO: download fixtures

    # TODO: bronze -> silver transformation

    # TODO: silver -> gold transformation (is this DBT modeling?)

    # TODO: DBT Core modeling


if __name__ == "__main__":
    fpl_pipeline_flow()
