import pandas as pd
from dotenv import load_dotenv
import logging
import os

from src.utils.minio_utils import fetch_all_from_minio
from src.utils.minio_utils import create_minio_client
from src.utils.etl_utils import (
    fetch_bronze_data,
    SilverTransformationConfig,
    validate_expected_columns,
    validate_important_columns,
    validate_key_columns,
    remove_dupes,
    assert_accepted_ranges,
    merge_dataframes,
    remove_duplicate_columns,
    convert_critical_columns,
    drop_unnecessary_columns,
    log_rows_dropped,
    assert_strictly_sequential_values,
    load_to_silver,
)

logger = logging.getLogger(__name__)


def rename_teams_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={
            "code": "team_code",
            "name": "team_name",
            "position": "league_table_position",
            "id": "seasonal_team_id",  # Season-dependent unique identifier for a team (unique within a season)
        }
    )


def transform_teams(teams_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transforming and deduplicating teams data...")
    try:
        df = teams_df.copy()
        initial_rows = len(df)

        df = drop_unnecessary_columns(df, ["modified"])
        df = rename_teams_columns(df)
        df = remove_duplicate_columns(df)
        df = remove_dupes(df)

        critical_columns = {
            "seasonal_team_id": "int",
            "team_name": "str",
            "season": "str",
        }
        df, rows_dropped = convert_critical_columns(df, critical_columns)
        log_rows_dropped(rows_dropped, initial_rows, len(df))

        return df
    except Exception as e:
        raise Exception(f"Error transforming data: {e}")


if __name__ == "__main__":
    load_dotenv()
    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    if endpoint is None or access_key is None or secret_key is None:
        raise ValueError("Missing required environment variables")

    client = create_minio_client(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
    )

    config = SilverTransformationConfig()

    teams_dfs = fetch_bronze_data(
        client=client,
        fetch_function=fetch_all_from_minio,
        config=config,
        folder="teams",
    )

    EXPECTED_TEAMS_COLS = [
        "team_code",  # The source data's natural key
        "season",
        "draw",
        "form",
        "seasonal_team_id",
        "loss",
        "team_name",
        "played",
        "points",
        "league_table_position",
        "short_name",
        "strength",
        "team_division",
        "unavailable",
        "win",
        "strength_overall_home",
        "strength_overall_away",
        "strength_attack_home",
        "strength_attack_away",
        "strength_defence_home",
        "strength_defence_away",
        "pulse_id",  # Natural key from the source system that only is present in newer data
        "ingestion_timestamp",
    ]

    IMP_TEAMS_COLS = [
        "team_code",
        "season",
        "seasonal_team_id",
        "team_name",
        "played",
    ]

    KEY_TEAMS_COLS = [
        "seasonal_team_id",
        "pulse_id",  # Natural key from the source system that only is present in newer data
        "season",
    ]

    for key, df in teams_dfs.items():
        df = rename_teams_columns(df)

        assert assert_accepted_ranges(
            dataframe=df, column="seasonal_team_id", min=0, max=20
        )
        assert assert_strictly_sequential_values(df, "seasonal_team_id", step=1)

        assert validate_expected_columns(
            df, expected_columns=EXPECTED_TEAMS_COLS
        ), "Expected columns validation failed"
        assert validate_important_columns(
            df, important_columns=IMP_TEAMS_COLS
        ), "Important column validation failed"
        assert validate_key_columns(
            df, key_columns=KEY_TEAMS_COLS, composite_key=True
        ), "Key column validation failed"
        df = remove_dupes(df)

        teams_dfs[key] = df  # Save back to original dictionary

    merged_teams = merge_dataframes(teams_dfs)
    transformed_teams = transform_teams(teams_df=merged_teams)

    print(transformed_teams.head())

    destination_path = "teams/teams_silver.parquet"

    load_to_silver(
        dataframe=transformed_teams,
        bucket_name=config.destination_bucket,
        object_name=destination_path,
        client=client,
    )
