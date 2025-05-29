from prefect import task
from prefect.blocks.system import Secret
from minio import Minio

from src.utils.minio_utils import create_minio_client


@task
def get_minio_secret_task() -> str:
    """Get the minio secret key from the secret block"""
    secret_block = Secret.load("fpl-minio-secret-key")
    return str(secret_block.get())  # casting it as string for type safety


@task
def get_minio_client_task(
    minio_secret_key: str,
    minio_endpoint: str = "minio-yokckg4o44wg40wogk0okgks.65.108.88.160.sslip.io",
    minio_access_key: str = "minio-fpl",
) -> Minio:
    """Get the minio client"""
    return create_minio_client(
        endpoint=minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
    )
