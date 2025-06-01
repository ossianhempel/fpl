import pandas as pd
from typing import Optional, Callable
from minio import Minio, S3Error
import logging
import polars as pl
import io
from src.config.config import SilverTransformationConfig, GoldTransformationConfig
from src.utils.minio_utils import fetch_all_from_minio


# create logger for the module
logger = logging.getLogger(__name__)

# TODO - move to util directory


def fetch_lake_data(
    client: Minio,
    config: SilverTransformationConfig | GoldTransformationConfig,
    fetch_function: Callable[
        [Minio, str, str | None], Optional[dict[str, pd.DataFrame]]
    ] = fetch_all_from_minio,  # injecting util function
    folder: Optional[str] = None,
) -> dict[str, pd.DataFrame]:
    """
    Fetches all files from a given bucket in the bronze/silver/gold layer
    """
    logger.info(
        f"Initiating fetching of data from bucket: {config.source_bucket}/{folder}"
    )
    try:
        dfs = fetch_function(
            client,
            config.source_bucket,
            folder,
        )
        if dfs is None or len(dfs) == 0:
            logger.error("Fetch operation returned None instead of dataframes")
            raise Exception(
                f"No data could fetched from bucket: {config.source_bucket}"
            )

        logger.info(f"Number of dataframes fetched: {len(dfs)}")
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
        # Find columns that are expected but not present
        missing_cols = set(sorted_expected_cols) - set(sorted_actual_cols)
        # Find columns that are present but not expected
        unexpected_cols = set(sorted_actual_cols) - set(sorted_expected_cols)
        logger.info(f"Expected columns: {sorted_expected_cols}")
        logger.info(f"Actual columns: {sorted_actual_cols}")
        logger.info(
            f"\nColumns expected but not present: \n{sorted(list(missing_cols))}"
        )
        logger.info(
            f"Columns present but not expected: \n{sorted(list(unexpected_cols))}"
        )

        # attempt to drop columns that aren't present in expected columns, then check again
        try:
            logger.info(
                f"Dropping columns that aren't expected: {sorted(list(unexpected_cols))}"
            )
            df = dataframe.copy()
            df.drop(list(unexpected_cols), axis=1, inplace=True)
            actual_cols = list(df.columns)
            sorted_actual_cols = sorted(actual_cols)

            # Recalculate missing columns after dropping unexpected ones
            missing_cols = set(sorted_expected_cols) - set(sorted_actual_cols)
            logger.info(
                f"\nColumns expected but not present: {sorted(list(missing_cols))}"
            )

            assert (
                sorted_actual_cols == sorted_expected_cols
            ), "Validation failed after dropping unexpected columns"
        except Exception as e:
            logger.error(f"Retried validation failed: {e}")

            return False

    # check that all cols in df are present in expected cols

    logger.info("Successfully validated dataframe columns")
    return True


def validate_important_columns(
    dataframe: pd.DataFrame, important_columns: list[str]
) -> bool:
    df = dataframe.copy()
    # make sure required columns are not null
    for col in important_columns:
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


def remove_dupes(dataframe: pd.DataFrame) -> pd.DataFrame:
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


def assert_strictly_sequential_values(
    dataframe: pd.DataFrame, column_name: str, step: int = 1
) -> bool:
    """Test if values in a column increase by exactly 'step' each row"""
    df = dataframe.copy()
    is_sequential = bool(
        (df[column_name].diff().dropna() == step).all()
    )  # forced to cast as bool because MyPy doesn't have complete information about the pandas method
    return is_sequential


def assert_continuous_sequential_values(dataframe: pd.DataFrame, column: str) -> bool:
    """Test if column contains a continuous sequence without gaps"""
    df = dataframe.copy()
    unique_values = sorted(df[column].unique())
    expected_range = list(range(min(unique_values), max(unique_values) + 1))
    return unique_values == expected_range


def assert_accepted_ranges(
    dataframe: pd.DataFrame, column: str, max: int, min: int
) -> bool:
    """
    Check if all values in the specified column are within the given range.

    Parameters:
    -----------
    dataframe : pd.DataFrame
        The DataFrame to check
    column : str
        The name of the column to check
    max : int
        The maximum acceptable value (inclusive)
    min : int
        The minimum acceptable value (inclusive)

    Returns:
    --------
    bool
        True if all values in the column are within the specified range,
        False otherwise or if the column doesn't exist
    """
    # Check if column exists in the DataFrame
    if column not in dataframe.columns:
        print(f"Column: {column} is not present in the dataframe")
        return False

    # Check if all values in the column are within the specified range
    is_within_range = (dataframe[column] >= min) & (dataframe[column] <= max)

    # Return True if all values are within range, otherwise False
    return bool(is_within_range.all())


def merge_dataframes(dataframes: dict[str, pd.DataFrame]) -> pd.DataFrame:
    try:
        if dataframes is not None:
            combined_df = pd.concat(dataframes.values(), ignore_index=True)
            logger.info(f"Combined gameweeks dataframe shape: {combined_df.shape}")
        return combined_df
    except Exception as e:
        logger.error(f"Couldn't combine the fetched dataframes: {e}")
        raise


def drop_unnecessary_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    existing_cols = [col for col in columns if col in df.columns]
    return df.drop(columns=existing_cols) if existing_cols else df


def convert_critical_columns(
    df: pd.DataFrame, critical_columns: dict[str, str]
) -> tuple[pd.DataFrame, dict[str, int]]:
    rows_dropped = {}
    for col, dtype in critical_columns.items():
        if col not in df.columns:
            logger.warning(f"Critical column '{col}' missing from data")
            continue
        try:
            if dtype == "int":
                df[col] = df[col].replace(["False", "TRUE", "FALSE"], pd.NA)
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif dtype == "str":
                df[col] = (
                    df[col]
                    .astype(str)
                    .replace({"nan": None, "None": None, "FALSE": None, "TRUE": None})
                )
            elif dtype == "datetime":
                df[col] = pd.to_datetime(df[col], errors="coerce")
            before = len(df)
            df = df.dropna(subset=[col])
            rows_dropped[f"conversion_{col}"] = before - len(df)
        except Exception as e:
            logger.error(f"Error converting column {col} to {dtype}: {str(e)}")
    return df, rows_dropped


def handle_boolean_columns(
    df: pd.DataFrame, boolean_columns: list[str]
) -> pd.DataFrame:
    for column in boolean_columns:
        if column not in df.columns:
            logger.info(f"Adding missing column {column} with default value False")
            df[column] = False
        else:
            try:
                df[column] = (
                    df[column]
                    .replace(
                        {
                            "True": True,
                            "true": True,
                            "TRUE": True,
                            "1": True,
                            1: True,
                            "False": False,
                            "false": False,
                            "FALSE": False,
                            "0": False,
                            0: False,
                            "not_a_bool": False,
                            pd.NA: False,
                            None: False,
                        }
                    )
                    .fillna(False)
                    .astype(bool)
                )
            except Exception as e:
                logger.warning(f"Error converting column {column} to boolean: {str(e)}")
                df[column] = False
    return df


def determine_season(date: pd.Timestamp | pd.DatetimeIndex) -> Optional[str]:
    if pd.isna(date):
        return None
    year = date.year
    if date.month >= 7:
        return f"{year}-{str(year + 1)[-2:]}"
    else:
        return f"{year - 1}-{str(year)[-2:]}"


def add_season_column(
    df: pd.DataFrame,
    season_column_name: str = "season",
    date_column_name: str = "kickoff_time",
) -> pd.DataFrame:
    df[season_column_name] = df[date_column_name].apply(determine_season)
    return df


def remove_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[:, ~df.columns.duplicated()]


def log_rows_dropped(
    rows_dropped: dict[str, int], initial_rows: int, final_rows: int
) -> None:
    total_dropped = sum(rows_dropped.values())
    if total_dropped > 0:
        logger.info("Rows dropped during transformation:")
        for reason, count in rows_dropped.items():
            if count > 0:
                logger.info(f"- {reason}: {count} rows")
        logger.info(f"Final rows: {final_rows} (Started with {initial_rows})")


def load_to_lake(
    dataframe: pd.DataFrame | pl.DataFrame,
    bucket_name: str,
    object_name: str,
    client: Minio,
) -> None:
    # upload to silver bucket as parquet

    buffer = io.BytesIO()
    # Handle dataframe serialization
    if isinstance(dataframe, pd.DataFrame):
        dataframe.to_parquet(buffer, index=False)
        df_type = "pandas"
    elif isinstance(dataframe, pl.DataFrame):
        dataframe.write_parquet(buffer)
        df_type = "polars"
    else:
        raise ValueError(f"Unsupported dataframe type: {type(dataframe)}")

    if not bucket_name or not object_name:
        raise ValueError("bucket_name and object_name cannot be empty")

    if dataframe.empty:  # works for both pandas and polars
        logger.warning("Uploading empty dataframe")

    buffer.seek(0)
    file_size = buffer.getbuffer().nbytes
    logger.debug(f"{df_type} dataframe size: {file_size} bytes")

    try:
        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=buffer,
            content_type="application/parquet",
            length=file_size,
        )
        logger.info(f"Uploaded {df_type} dataframe to {bucket_name}/{object_name}")

    except S3Error as e:
        logger.error(
            f"MinIO client error uploading to {bucket_name}/{object_name}: {e}"
        )
        raise
    except Exception as e:
        logger.error(f"Unexpected error uploading to {bucket_name}/{object_name}: {e}")
        raise


if __name__ == "__main__":
    test_df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
    load_to_lake(
        test_df,
        "test-bucket",
        "test-object",
        Minio(
            endpoint="http://localhost:9000",
            access_key="minio-fpl",
            secret_key="minio-fpl",
        ),
    )
