from prefect import task
from prefect import get_run_logger
import pandas as pd
from minio import Minio
import polars as pl
from prefect.cache_policies import NONE

from src.utils.minio_utils import fetch_all_from_minio
from src.utils.etl_utils import (
    fetch_bronze_data,
    SilverTransformationConfig,
)
from src.utils.etl_utils import (
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

from src.etl_pipeline.components.bronze_to_silver_fixtures import (
    transform_fixtures,
    rename_fixtures_columns,
)

from src.etl_pipeline.components.bronze_to_silver_gameweeks import (
    transform_gameweeks,
    rename_gameweek_columns,
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
def transform_fixtures_task(
    fixture_dfs: dict[str, pd.DataFrame | pl.DataFrame],
    transformed_teams_df: pd.DataFrame | pl.DataFrame,
) -> pd.DataFrame | pl.DataFrame:
    logger = get_run_logger()
    logger.info("Transforming fixtures...")

    EXPECTED_FIXTURES_COLS = [
        "fixture_code",
        "gameweek",
        "fixture_completed",
        "ingestion_timestamp",
        "seasonal_fixture_id",
        "finished",
        "kickoff_time",
        "minutes",
        "provisional_start_time",
        "started",
        "away_team_id",
        "away_team_score",
        "home_team_id",
        "home_team_score",
        "away_team_difficulty",
        "home_team_difficulty",
        "home_team_name",
        "away_team_name",
    ]

    IMPORTANT_FIXTURES_COLS = [
        "gameweek",
        "away_team_id",
        "home_team_id",
        "kickoff_time",
    ]
    KEY_FIXTURES_COLS = [
        "kickoff_time",
        "gameweek",
        "away_team_id",
        "home_team_id",
    ]

    for key, df in fixture_dfs.items():
        df = rename_fixtures_columns(df)

        assert validate_important_columns(
            df, important_columns=IMPORTANT_FIXTURES_COLS
        ), "Important column validation failed"
        assert validate_key_columns(
            df, key_columns=KEY_FIXTURES_COLS, composite_key=True
        ), "Key column validation failed"
        assert assert_accepted_ranges(dataframe=df, column="gameweek", min=1, max=39)

        fixture_dfs[key] = df  # Save back to original dictionary

    merged_fixtures = merge_dataframes(fixture_dfs)
    transformed_fixtures = transform_fixtures(
        fixtures_df=merged_fixtures, teams_df=transformed_teams_df
    )

    assert validate_expected_columns(
        transformed_fixtures, expected_columns=EXPECTED_FIXTURES_COLS
    ), "Expected columns validation failed"

    return transformed_fixtures


@task
def validate_fixtures() -> bool:
    # TODO
    # use gx
    # parse json and return true/false depending on results
    return True


@task
def transform_gameweeks_task(
    gw_dfs: dict[str, pd.DataFrame | pl.DataFrame],
) -> pd.DataFrame | pl.DataFrame:
    logger = get_run_logger()
    logger.info("Transforming gameweeks...")

    expected_cols_gw = [
        # "season", # added after expected columns are validated
        "gameweek",
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
        "seasonal_fixture_id",
        "goals_conceded",
        "goals_scored",
        "ict_index",
        "influence",
        "ingestion_timestamp",
        "kickoff_time",
        "minutes_played",
        # "modified",
        "player_name",
        "opponent_team",
        "own_goals",
        "penalties_missed",
        "penalties_saved",
        "position",
        "red_cards",
        "round",
        "saves",
        "selected",
        "player_started",
        "team",
        "team_a_score",
        "team_h_score",
        "threat",
        "total_points",
        "transfers_balance",
        "transfers_in",
        "transfers_out",
        "player_cost",
        "was_home",
        "xP",
        "yellow_cards",
    ]
    important_cols = [
        "seasonal_fixture_id",
        "kickoff_time",
        "player_name",
        "team",
        "opponent_team",
        "total_points",
        "position",
    ]
    key_cols = [
        "kickoff_time",
        "player_name",
        "team",
    ]

    for key, df in gw_dfs.items():
        df = rename_gameweek_columns(df)

        assert validate_expected_columns(
            df, expected_columns=expected_cols_gw
        ), "Expected columns validation failed"
        assert validate_important_columns(
            df, important_columns=important_cols
        ), "Important column validation failed"
        assert validate_key_columns(
            df, key_columns=key_cols, composite_key=True
        ), "Key column validation failed"
        df = remove_dupes(df)
        assert assert_accepted_ranges(dataframe=df, column="gameweek", min=1, max=39)

        gw_dfs[key] = df  # Save back to original dictionary

    merged_gw = merge_dataframes(gw_dfs)
    transformed_gw = transform_gameweeks(dataframe=merged_gw)

    return transformed_gw


@task
def validate_gameweeks_task() -> bool:
    # TODO
    # use gx
    # parse json and return true/false depending on results
    return True


@task(cache_policy=NONE)
def load_to_silver_task(
    transformed_df: pd.DataFrame | pl.DataFrame,
    config: SilverTransformationConfig,
    object_path: str,
    client: Minio,
) -> None:
    load_to_silver(
        dataframe=transformed_df,
        bucket_name=config.destination_bucket,
        object_name=object_path,
        client=client,
    )


if __name__ == "__main__":
    print("hi")
