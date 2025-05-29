import pandas as pd
from dotenv import load_dotenv
import logging
import os

from src.utils.minio_utils import fetch_all_from_minio
from src.utils.minio_utils import create_minio_client
from src.etl_pipeline.components.bronze_to_silver_utils import (
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
    handle_boolean_columns,
    add_season_column,
    drop_unnecessary_columns,
    log_rows_dropped,
    load_to_silver,
)

from src.etl_pipeline.components.bronze_to_silver_teams import (
    transform_teams,
    rename_teams_columns,
)

logger = logging.getLogger(__name__)


def rename_fixtures_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={
            "event": "gameweek",
            "id": "seasonal_fixture_id",  # Season-dependent unique identifier for a fixture (unique within a season)
            "code": "fixture_code",  # The source data's natural key (unique globally)
            "finished_provisional": "fixture_completed",  # Boolean indicating if the fixture has been completed
            "team_a": "away_team_id",
            "team_h": "home_team_id",
            "team_a_difficulty": "away_team_difficulty",
            "team_h_difficulty": "home_team_difficulty",
            "team_a_score": "away_team_score",
            "team_h_score": "home_team_score",
        }
    )


def drop_duplicates_fixtures(df: pd.DataFrame) -> pd.DataFrame:
    if (
        df[
            df.duplicated(
                subset=["gameweek", "kickoff_time", "away_team_id", "home_team_id"]
            )
        ].shape[0]
        > 0
    ):
        logger.warning("Found duplicates in fixtures data. Dropping duplicates.")
    return df.drop_duplicates(
        subset=["gameweek", "kickoff_time", "away_team_id", "home_team_id"], keep="last"
    )


def map_team_names(fixtures_df: pd.DataFrame, teams_df: pd.DataFrame) -> pd.DataFrame:
    logger.debug(
        f"Starting map_team_names. Fixtures seasons: {fixtures_df['season'].unique() if 'season' in fixtures_df.columns else 'N/A (season column missing from fixtures_df)'}"
    )
    logger.debug(
        f"Teams seasons: {teams_df['season'].unique() if 'season' in teams_df.columns else 'N/A (season column missing from teams_df)'}"
    )
    logger.debug(
        f"Columns in teams_df provided for mapping: {teams_df.columns.tolist()}"
    )

    # ensure 'season' column exists in teams_df, which is critical for the join.
    # transform_teams is supposed to ensure 'season' is a critical column.
    if "season" not in teams_df.columns:
        logger.error(
            "Critical 'season' column missing from teams_df in map_team_names. Cannot map team names."
        )
        # add empty 'team_h_name' and 'away_team_name' and return fixtures_df
        # to prevent outright crash, but data will be incomplete.
        fixtures_df["home_team_name"] = pd.NA
        fixtures_df["away_team_name"] = pd.NA
        return fixtures_df

    # prepare a teams mapping dataframe that only has the necessary columns for the merge.
    # this prevents unintended column conflicts or carrying over unnecessary data.
    teams_mapping_subset = teams_df[["seasonal_team_id", "team_name", "season"]].copy()
    if teams_mapping_subset.empty:
        logger.warning(
            "Teams data for mapping is empty (teams_mapping_subset). Team names will not be mapped."
        )
        fixtures_df["home_team_name"] = pd.NA
        fixtures_df["away_team_name"] = pd.NA
        return fixtures_df

    # map home team names
    # rename columns on the right side (teams_mapping_subset) before merge to avoid ambiguity and conflicts.
    teams_home_map = teams_mapping_subset.rename(
        columns={
            "seasonal_team_id": "ref_home_seasonal_id",
            "team_name": "home_team_name",
        }
    )

    fixtures_df = pd.merge(
        fixtures_df,
        teams_home_map,
        left_on=["home_team_id", "season"],
        right_on=["ref_home_seasonal_id", "season"],
        how="left",
    )
    # drop the temporary joining key from the right side.
    fixtures_df = fixtures_df.drop(columns=["ref_home_seasonal_id"], errors="ignore")

    # ensure team_h_name column exists, even if no matches were found
    if "home_team_name" not in fixtures_df.columns:
        logger.warning(
            "home_team_name column was not added after home team merge (no matches found or column already existed with different name?). Adding it as pd.NA."
        )
        fixtures_df["home_team_name"] = pd.NA

    # map away team names
    teams_away_map = teams_mapping_subset.rename(
        columns={
            "seasonal_team_id": "ref_away_seasonal_id",
            "team_name": "away_team_name",
        }
    )
    fixtures_df = pd.merge(
        fixtures_df,
        teams_away_map,
        left_on=["away_team_id", "season"],
        right_on=["ref_away_seasonal_id", "season"],
        how="left",
    )
    fixtures_df = fixtures_df.drop(columns=["ref_away_seasonal_id"], errors="ignore")

    # ensure away_team_name column exists
    if "away_team_name" not in fixtures_df.columns:
        logger.warning(
            "away_team_name column was not added after away team merge (no matches found or column already existed with different name?). Adding it as pd.NA."
        )
        fixtures_df["away_team_name"] = pd.NA

    # log how many team names were successfully mapped vs. left as nan
    mapped_home_teams = fixtures_df["home_team_name"].notna().sum()
    mapped_away_teams = fixtures_df["away_team_name"].notna().sum()
    total_fixtures = len(fixtures_df)

    logger.info(
        f"Mapped home team names for {mapped_home_teams}/{total_fixtures} fixtures."
    )
    if mapped_home_teams < total_fixtures:
        logger.warning(
            f"Failed to map home team names for {total_fixtures - mapped_home_teams} fixtures (likely due to missing season/team_id combination in teams data)."
        )

    logger.info(
        f"Mapped away team names for {mapped_away_teams}/{total_fixtures} fixtures."
    )
    if mapped_away_teams < total_fixtures:
        logger.warning(
            f"Failed to map away team names for {total_fixtures - mapped_away_teams} fixtures (likely due to missing season/team_id combination in teams data)."
        )

    return fixtures_df


def transform_fixtures(
    fixtures_df: pd.DataFrame, teams_df: pd.DataFrame
) -> pd.DataFrame:
    logger.info("Transforming and deduplicating fixtures data...")
    try:
        df = fixtures_df.copy()
        initial_rows = len(df)

        df = drop_unnecessary_columns(df, ["modified"])
        df = rename_fixtures_columns(df)
        df = remove_duplicate_columns(df)
        df = drop_duplicates_fixtures(df)
        df = remove_dupes(df)

        # critical columns that must have valid values (after renaming)
        critical_columns = {
            "gameweek": "int",  # renamed from 'event'
            "away_team_id": "int",  # renamed from 'team_a'
            "home_team_id": "int",  # renamed from 'team_h'
            "kickoff_time": "datetime",
        }

        df, rows_dropped = convert_critical_columns(df, critical_columns)

        # Add season column after converting kickoff_time to datetime
        df = add_season_column(df)

        df = handle_boolean_columns(df, ["finished", "fixture_completed", "started"])
        df = drop_unnecessary_columns(df, ["stats"])
        # df = add_opponent_team_column(df) # this is already in fixtures?

        log_rows_dropped(rows_dropped, initial_rows, len(df))

        df = drop_duplicates_fixtures(df)

        df = map_team_names(fixtures_df=df, teams_df=teams_df)

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

    fixture_dfs = fetch_bronze_data(
        client=client,
        fetch_function=fetch_all_from_minio,
        config=config,
        folder="fixtures",
    )

    # add teams so we can map team names
    teams_dfs = fetch_bronze_data(
        client=client,
        fetch_function=fetch_all_from_minio,
        config=config,
        folder="teams",
    )

    for key, df in teams_dfs.items():
        df = rename_teams_columns(df)
        teams_dfs[key] = df

    merged_teams = merge_dataframes(teams_dfs)
    transformed_teams = transform_teams(merged_teams)

    print(transformed_teams.head())
    print(transformed_teams.columns)

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

    IMP_FIXTURES_COLS = [
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
            df, important_columns=IMP_FIXTURES_COLS
        ), "Important column validation failed"
        assert validate_key_columns(
            df, key_columns=KEY_FIXTURES_COLS, composite_key=True
        ), "Key column validation failed"
        assert assert_accepted_ranges(dataframe=df, column="gameweek", min=1, max=39)

        fixture_dfs[key] = df  # Save back to original dictionary

    merged_fixtures = merge_dataframes(fixture_dfs)
    transformed_fixtures = transform_fixtures(
        fixtures_df=merged_fixtures, teams_df=transformed_teams
    )

    assert validate_expected_columns(
        transformed_fixtures, expected_columns=EXPECTED_FIXTURES_COLS
    ), "Expected columns validation failed"

    print(transformed_fixtures.head())
    print(transformed_fixtures.columns)

    destination_path = "fixtures/fixtures_silver.parquet"

    load_to_silver(
        dataframe=transformed_fixtures,
        bucket_name=config.destination_bucket,
        object_name=destination_path,
        client=client,
    )
