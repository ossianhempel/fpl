import pandas as pd
import logging
import os
from dotenv import load_dotenv


from src.utils.minio_utils import create_minio_client

logger = logging.getLogger(__name__)


def validate_new_or_grain_protecting_column(
    dataframe: pd.DataFrame, new_columns: list[str]
) -> bool:
    df = dataframe.copy()
    for col in new_columns:
        if col not in df.columns:
            logger.error(f"{col} is not present in the dataframe")
            return False
    logger.info("Validation of new columns completed")
    return True


if __name__ == "__main__":
    load_dotenv()
    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    if endpoint is None or access_key is None or secret_key is None:
        raise ValueError("Missing required environment variables")

    client = create_minio_client(endpoint, access_key, secret_key)
