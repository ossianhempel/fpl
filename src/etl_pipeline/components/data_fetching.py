import pandas as pd
from dotenv import load_dotenv
from typing import Optional
from minio import Minio
import logging


from src.etl_pipeline.components.config import DataFetchConfig

from src.utils.minio_utils import fetch_all_from_minio


# create logger for the module
logger = logging.getLogger(__name__)


load_dotenv()


def fetch_gameweeks_from_lake(
    client: Minio, gw_fetcher_config: DataFetchConfig
) -> Optional[pd.DataFrame]:
    """
    Fetches all
    """
    logger.info("Initiating fetching of gameweeks")
    try:
        dfs = fetch_all_from_minio(
            client=client,
            endpoint=gw_fetcher_config.minio.minio_endpoint,
            access_key=gw_fetcher_config.minio.minio_access_key,
            secret_key=gw_fetcher_config.minio.minio_secret_key,
            bucket_name=gw_fetcher_config.bucket_name,
        )
        if dfs is None or len(dfs) == 0:
            logger.error("Fetch operation returned None instead of dataframes")
            raise Exception("No data could fetched from gameweeks bucket")

        logger.info(f"Number of gameweek dataframes fetched: {len(dfs)}")

    except Exception as e:
        logger.error(f"Error occurred: {e}", exc_info=True)

    try:
        if dfs is not None:
            combined_df = pd.concat(dfs.values(), ignore_index=True)
            logger.info(f"Combined gameweeks dataframe shape: {combined_df.shape}")
        return combined_df
    except Exception as e:
        logger.error(f"Couldn't combine the fetched dataframes: {e}")
        return None


def fetch_fixtures_from_lake(
    client: Minio, fixtures_fetcher_config: DataFetchConfig
) -> Optional[pd.DataFrame]:
    pass


if __name__ == "__main__":
    print("hi")
