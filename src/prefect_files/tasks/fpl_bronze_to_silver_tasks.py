from prefect import task
from prefect import get_run_logger
import pandas as pd
from minio import Minio


from src.utils.minio_utils import fetch_all_from_minio
from src.etl_pipeline.components.bronze_to_silver_utils import (
    fetch_bronze_data,
    SilverTransformationConfig,
)
from src.etl_pipeline.components.bronze_to_silver_utils import (
    assert_accepted_ranges,
    assert_strictly_sequential_values,
    validate_expected_columns,
    validate_important_columns,
    validate_key_columns,
    remove_dupes,
    merge_dataframes,
    load_to_silver,
)

from src.etl_pipeline.components.bronze_to_silver_teams import (
    transform_teams,
    rename_teams_columns,
)


@task
def get_data_from_bronze_task(
    client: Minio,
    config: SilverTransformationConfig,
    folder: str,
) -> dict[str, pd.DataFrame]:
    logger = get_run_logger()
    logger.info("Fetching files from Bronze layer...")
    return fetch_bronze_data(
        client=client,
        config=config,
        fetch_function=fetch_all_from_minio,
        folder=folder,
    )


@task
def transform_teams_task(teams_dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info("Transforming teams...")

    # TODO: move this to a config file
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

    return transformed_teams


@task
def transform_fixtures_task() -> pd.DataFrame:
    logger = get_run_logger()
    logger.info("Transforming fixtures...")
    pass


@task
def validate_fixtures() -> bool:
    # TODO
    # use gx
    # parse json and return true/false depending on results
    return True


@task
def transform_gameweeks_task() -> pd.DataFrame:
    logger = get_run_logger()
    logger.info("Transforming gameweeks...")
    pass


@task
def validate_gameweeks_task() -> bool:
    # TODO
    # use gx
    # parse json and return true/false depending on results
    return True


@task
def load_to_silver_task(
    transformed_df: pd.DataFrame,
    config: SilverTransformationConfig,
    folder: str,
    client: Minio,
) -> None:
    load_to_silver(
        dataframe=transformed_df,
        bucket_name=config.destination_bucket,
        object_name=folder,
        client=client,
    )


if __name__ == "__main__":
    print("hi")
