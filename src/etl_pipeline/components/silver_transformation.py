import pandas as pd
from dotenv import load_dotenv
from typing import Optional, Callable
from minio import Minio
import logging
import os
from dataclasses import dataclass

from src.etl_pipeline.components.config import DataFetchConfig

from src.utils.minio_utils import fetch_all_from_minio
from src.utils.minio_utils import create_minio_client
from src.config.logging_config import setup_logging


# create logger for the module
setup_logging()
logger = logging.getLogger(__name__)

# TODO: fetch from bronze layer and do minimal cleaning (EtLT) into silver layer as Parquet
# TODO: cleaning and standardizing


@dataclass
class SilverTransformationConfig:
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "COULDNT GET ENV VAR")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "COULDNT GET ENV VAR")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "COULDNT GET ENV VAR")
    source_bucket: str = "bronze"
    destination_bucket: str = "silver"


def fetch_bronze_data(
    client: Minio,
    fetch_function: Callable[
        [Minio, str, str, str, str], Optional[dict[str, pd.DataFrame]]
    ] = fetch_all_from_minio,  # injecting util function
    config: Optional[SilverTransformationConfig] = None,
) -> Optional[dict[str, pd.DataFrame]]:
    """
    Fetches all raw files from a given bucket in the bronze layer
    """
    if config is None:
        config = SilverTransformationConfig()
    logger.info(f"Initiating fetching of raw data from bucket: {config.source_bucket}")
    try:
        dfs = fetch_function(
            client,
            config.minio_endpoint,
            config.minio_access_key,
            config.minio_secret_key,
            config.source_bucket,
        )
        if dfs is None or len(dfs) == 0:
            logger.error("Fetch operation returned None instead of dataframes")
            raise Exception("No data could fetched from gameweeks bucket")

        logger.info(f"Number of gameweek dataframes fetched: {len(dfs)}")
        return dfs
    except Exception as e:
        logger.error(f"Error occurred: {e}", exc_info=True)
        raise Exception  # re-raise to handle in caller


def validate_expected_columns(
    dataframe: pd.DataFrame, expected_columns: list[str]
) -> bool:
    # TODO: what to do when actual df misses any cols from expected? fill with null/0 and have separate tests for crucial columns?
    logger.info("Validating expected columns of dataframe")

    # check that all expected cols are present in df
    actual_cols = list(dataframe.columns)
    sorted_actual_cols = sorted(actual_cols)
    sorted_expected_cols = sorted(expected_columns)

    if not sorted_actual_cols == sorted_expected_cols:
        symmetric_difference = list(set(sorted_actual_cols) ^ set(sorted_expected_cols))
        logger.info(
            f"Expected: \n{sorted_expected_cols} \nReceived: \n{sorted_actual_cols}"
        )
        logger.info(f"\nColumns that are unique to each list: {symmetric_difference}")
        # attempt to drop columns that aren't present in expected columns, then check again
        try:
            logger.info(
                f"Dropping columns that aren't expected: {symmetric_difference}"
            )
            df = dataframe.copy()
            df.drop(symmetric_difference, axis=1, inplace=True)
            actual_cols = list(df.columns)
            sorted_actual_cols = sorted(actual_cols)
            symmetric_difference = list(
                set(sorted_actual_cols) ^ set(sorted_expected_cols)
            )

            assert sorted_actual_cols == sorted_expected_cols
        except Exception as e:
            logger.error(f"Retried validation failed: {e}")

            return False

    # check that all cols in df are present in expected cols

    logger.info("Successfully validated dataframe columns")
    return True


def validate_important_columns(dataframe: pd.DataFrame) -> bool:
    df = dataframe.copy()
    columns = df.columns
    # make sure required columns are not null
    for col in columns:
        if df[col].isnull().sum() > 0:
            logger.error(f"{col} contains null values: {df[col].isnull().sum()}")
            return False
    logger.info("Validation of important columns completed")
    return True


def validate_key_columns(
    dataframe: pd.DataFrame, key_columns: list[str], composite_key: bool = False
) -> bool:
    df = dataframe.copy()
    logger.info("Validating key columns")
    if composite_key:
        assert len(key_columns) > 1, "Can't be a composite key with less than 2 columns"
        key = key_columns
        if df[key].duplicated().sum() > 0:
            logger.error(f"Found duplicates in composite key: {key}")
            return False

    else:
        columns = df[key_columns].columns
        for col in columns:
            if df[col].duplicated().sum() > 0:
                logger.error(f"Found duplicates in individual key: {col}")
                return False

    logger.info("Validation of key columns successful")
    return True


def removes_dupes(dataframe: pd.DataFrame) -> pd.DataFrame:
    logger.info("Looking for duplicate rows")
    # look for duplicate rows (after dropping ingestion_time)
    df = dataframe.copy()

    if "ingestion_timestamp" in df.columns:
        cols_to_dedupe_on = df.drop("ingestion_timestamp", axis=1)
    else:
        cols_to_dedupe_on = df.columns

    count_dupes = df.duplicated(subset=cols_to_dedupe_on).sum()
    if count_dupes > 0:
        logger.info(f"Found {count_dupes} duplicate rows, dropping those")
        df.drop_duplicates(subset=cols_to_dedupe_on, inplace=True)
    else:
        logger.info("Found no duplicate rows")

    return df


def test_sequential_values(dataframe: pd.DataFrame) -> bool:
    pass


def test_accepted_ranges(dataframe: pd.DataFrame) -> bool:
    pass


def merge_dataframes(dataframes: list[pd.DataFrame]) -> pd.DataFrame:
    try:
        if dataframes is not None:
            combined_df = pd.concat(dataframes.values(), ignore_index=True)
            logger.info(f"Combined gameweeks dataframe shape: {combined_df.shape}")
            # logger.info("Convert pandas dataframe into polars dataframe")
            # pd_df = pd.from_pandas(combined_df)
        return combined_df
    except Exception as e:
        logger.error(f"Couldn't combine the fetched dataframes: {e}")
        return None


def transform_gameweeks():
    pass


def transform_fixtures():
    pass


def transform_teams():
    pass


def load_dataframe():
    pass


def fetch_gameweeks_from_lake(
    client: Minio,
    gw_fetcher_config: DataFetchConfig,
    fetch_function: Callable = fetch_all_from_minio,  # inject util function
) -> Optional[pd.DataFrame]:
    """
    Fetches all
    """
    logger.info("Initiating fetching of gameweeks")
    try:
        dfs = fetch_function(
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
        raise  # re-raise to handle in caller

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
    """ """
    logger.info("Initiating fetching of fixtures")
    try:
        dfs = fetch_all_from_minio(
            client=client,
            endpoint=fixtures_fetcher_config.minio.minio_endpoint,
            access_key=fixtures_fetcher_config.minio.minio_access_key,
            secret_key=fixtures_fetcher_config.minio.minio_secret_key,
            bucket_name=fixtures_fetcher_config.bucket_name,
        )
        if dfs is None or len(dfs) == 0:
            logger.error("Fetch operation returned None instead of dataframes")
            raise Exception("No data could fetched from fixtures bucket")

        logger.info(f"Number of fixtures dataframes fetched: {len(dfs)}")
    except Exception as e:
        logger.error(f"Error occured: {e}", exc_info=True)

    try:
        if dfs is not None:
            combined_df = pd.concat(dfs.values(), ignore_index=True)
            logger.info(f"Combined fixtures dataframe shape: {combined_df.shape}")
        return combined_df
    except Exception as e:
        logger.error(f"Couldn't combine the fetched dataframes: {e}")
        return None


def fetch_teams_from_lake(
    client: Minio, teams_fetcher_config: DataFetchConfig
) -> Optional[pd.DataFrame]:
    """ """
    logger.info("Initiating fetching of fixtures")
    try:
        dfs = fetch_all_from_minio(
            client=client,
            endpoint=teams_fetcher_config.minio.minio_endpoint,
            access_key=teams_fetcher_config.minio.minio_access_key,
            secret_key=teams_fetcher_config.minio.minio_secret_key,
            bucket_name=teams_fetcher_config.bucket_name,
        )
        if dfs is None or len(dfs) == 0:
            logger.error("Fetch operation returned None instead of dataframes")
            raise Exception("No data could fetched from teams bucket")

        logger.info(f"Number of teams dataframes fetched: {len(dfs)}")
    except Exception as e:
        logger.error(f"Error occured: {e}", exc_info=True)

    try:
        if dfs is not None:
            combined_df = pd.concat(dfs.values(), ignore_index=True)
            logger.info(f"Combined teams dataframe shape: {combined_df.shape}")
        return combined_df
    except Exception as e:
        logger.error(f"Couldn't combine the fetched dataframes: {e}")
        return None


if __name__ == "__main__":
    load_dotenv()
    client = create_minio_client(
        endpoint=os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
    )

    config = SilverTransformationConfig()

    dfs = fetch_bronze_data(
        client=client,
        fetch_function=fetch_all_from_minio,
        config=SilverTransformationConfig,
    )

    expected_cols_gw = [
        "assists",
        "bonus",
        "bps",
        "clean_sheets",
        "creativity",
        "element",
        "expected_assists",
        "expected_goal_involvements",
        "expected_goals",
        "expected_goals_conceded",
        "fixture",
        "goals_conceded",
        "goals_scored",
        "ict_index",
        "influence",
        "ingestion_timestamp",
        "kickoff_time",
        "minutes",
        "modified",
        "name",
        "opponent_team",
        "own_goals",
        "penalties_missed",
        "penalties_saved",
        "position",
        "red_cards",
        "round",
        "saves",
        "selected",
        "starts",
        "team",
        "team_a_score",
        "team_h_score",
        "threat",
        "total_points",
        "transfers_balance",
        "transfers_in",
        "transfers_out",
        "value",
        "was_home",
        "xP",
        "yellow_cards",
    ]
    key_cols = [
        "fixture",
        "kickoff_time",
        "name",
        "team",
        "opponent_team",
        "total_points",
        "position",
    ]

    for df in dfs.values():
        assert validate_expected_columns(
            df, expected_columns=expected_cols_gw
        ), "Expected columns validation failed"
        assert validate_important_columns(
            df, key_columns=key_cols
        ), "Key column validation failed"
