import os
from prefect import flow
from prefect.blocks.system import Secret

from src.prefect_files.tasks.fpl_tasks import download_gws

WEB_SERVER_URL = os.getenv("PREFECT_WEB_SERVER_URL")

season = "2024-25"


secret_block = Secret.load("fpl-minio-secret-key")
secret_block.get()


# @flow(name="fpl_data_pipeline", log_prints=True, retries=2)
@flow(name="fpl_data_pipeline", log_prints=True)
def fpl_pipeline_flow():
    # load environment variables fomr Prefect Secret blocks
    # print("Setting environment variables...")
    # os.environ[
    #     "MINIO_ENDPOINT"
    # ] = "minio-yokckg4o44wg40wogk0okgks.65.108.88.160.sslip.io"
    # os.environ["MINIO_ACCESS_KEY"] = "minio-fpl"

    # # load the secret key from the Secret block
    # try:
    #     secret_key = Secret.load("fpl-minio-secret-key").get()
    #     os.environ["MINIO_SECRET_KEY"] = secret_key
    #     print("Successfully loaded MINIO_SECRET_KEY from Secret block")
    # except Exception as e:
    #     print(f"Error loading secret: {e}")
    #     raise

    # minio_credentials = {
    #     "minio_endpoint": os.getenv("MINIO_ENDPOINT"),
    #     "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
    #     "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    # }

    # download new gws
    download_gws(season=season)

    # TODO: download fixtures

    # TODO: bronze -> silver transformation

    # TODO: silver -> gold transformation (is this DBT modeling?)

    # TODO: DBT Core modeling


if __name__ == "__main__":
    fpl_pipeline_flow()
