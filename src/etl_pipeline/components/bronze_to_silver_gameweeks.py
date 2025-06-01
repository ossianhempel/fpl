import pandas as pd
from dotenv import load_dotenv
import logging
import os

from src.utils.minio_utils import fetch_all_from_minio
from src.utils.minio_utils import create_minio_client
from src.config.config import SilverTransformationConfig
from src.utils.etl_utils import (
    fetch_lake_data,
    validate_expected_columns,
    validate_important_columns,
    validate_key_columns,
    remove_dupes,
    assert_accepted_ranges,
    merge_dataframes,
    drop_unnecessary_columns,
    remove_duplicate_columns,
    convert_critical_columns,
    handle_boolean_columns,
    add_season_column,
    log_rows_dropped,
    load_to_lake,
)


logger = logging.getLogger(__name__)


def rename_gameweek_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={
            "gw": "gameweek",
            "name": "player_name",
            "minutes": "minutes_played",
            "value": "player_cost",
            "starts": "player_started",
            "fixture": "seasonal_fixture_id",
        }
    )


def identify_opponent_team(group: pd.DataFrame) -> pd.DataFrame:
    if len(group["team"].unique()) == 2:
        teams = group["team"].unique()
        group["opponent_team"] = group["team"].apply(
            lambda x: teams[1] if x == teams[0] else teams[0]
        )
    else:
        group["opponent_team"] = None
    return group


def add_opponent_team_column(df: pd.DataFrame) -> pd.DataFrame:
    is_input_empty = df.empty

    # define grouping columns
    grouping_cols = ["kickoff_time", "seasonal_fixture_id"]

    # check if all necessary columns for grouping are present
    if all(col in df.columns for col in grouping_cols):
        if not is_input_empty:
            # if the dataframe is not empty, apply the grouping and function
            df = df.groupby(grouping_cols, group_keys=False).apply(
                identify_opponent_team
            )
        else:
            # if the dataframe is empty but grouping columns are present,
            # pandas groupby().apply() on an empty df might not add a new column from the applied function to the schema.
            # explicitly add the 'opponent_team' column to the empty dataframe.
            df["opponent_team"] = pd.Series(dtype=object)
    else:
        # if grouping columns are missing, set opponent_team to none for all rows.
        # this will add the 'opponent_team' column if it doesn't exist.
        df["opponent_team"] = None
    return df


def drop_duplicates_gameweeks(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates(
        subset=["player_name", "gameweek", "kickoff_time"], keep="last"
    )


def transform_gameweeks(dataframe: pd.DataFrame) -> pd.DataFrame:
    # TODO: add GW col if not present!! make independent of source extraction
    logger.info("Transforming and deduplicating gameweeks data...")
    try:
        df = dataframe.copy()
        initial_rows = len(df)

        df = drop_unnecessary_columns(df, ["modified", "round"])
        df = rename_gameweek_columns(df)
        df = remove_duplicate_columns(df)

        critical_columns = {
            "gameweek": "int",
            "team": "str",
            "player_name": "str",
            "kickoff_time": "datetime",
            "season": "str",
        }
        df, rows_dropped = convert_critical_columns(df, critical_columns)
        df = add_season_column(
            df
        )  # needs to come after critical columns are converted due to datetime conversion

        df = handle_boolean_columns(df, ["was_home", "player_started"])
        df = add_opponent_team_column(df)

        log_rows_dropped(rows_dropped, initial_rows, len(df))

        df = drop_duplicates_gameweeks(df)

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

    gw_dfs = fetch_lake_data(
        client=client,
        fetch_function=fetch_all_from_minio,
        config=config,
        folder="gameweeks",
    )

    fixture_dfs = fetch_lake_data(
        client=client,
        fetch_function=fetch_all_from_minio,
        config=config,
        folder="fixtures",
    )

    teams_dfs = fetch_lake_data(
        client=client,
        fetch_function=fetch_all_from_minio,
        config=config,
        folder="teams",
    )

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

    print(transformed_gw.head())

    destination_path = "gameweeks/gameweeks_silver.parquet"

    load_to_lake(
        dataframe=transformed_gw,
        bucket_name=config.destination_bucket,
        object_name=destination_path,
        client=client,
    )
