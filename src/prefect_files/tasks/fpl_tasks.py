from prefect import task
from prefect import get_run_logger
from prefect.blocks.system import Secret
from typing import Optional, Callable
import pandas as pd
from minio import Minio
from src.etl_pipeline.components.source_extraction import (
    GameweekIngestor,
    DimensionFileIngestor,
)
from src.etl_pipeline.components.silver_transformation import (
    fetch_bronze_data,
    SilverTransformationConfig,
)


@task
def get_minio_secret() -> Optional[str]:
    secret_block = Secret.load("fpl-minio-secret-key")
    return str(secret_block.get())  # casting it as string for type safety


@task
def download_teams(
    season: str = "2024-25",
    minio_endpoint: Optional[str] = None,
    minio_access_key: Optional[str] = None,
    minio_secret_key: Optional[str] = None,
) -> None:
    """Loops through gameweeks and downloads any new ones"""

    creds = {
        "minio_endpoint": minio_endpoint,
        "minio_access_key": minio_access_key,
        "minio_secret_key": minio_secret_key,
    }
    ingestor = DimensionFileIngestor(**creds)

    BASE_URL = (
        "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data"
    )

    teams_url = f"{BASE_URL}/{season}/teams.csv"

    try:
        teams_file = ingestor.download_source_file(teams_url)
        ingestor.load_to_minio(
            data=teams_file,
            destination_bucket="bronze",
            destination_object_path=f"teams/{season}/teams_{season}.csv",
        )
        print(f"{teams_url} successfully ingested")
    except Exception as e:
        print(f"Ran into an error downloading teams: {e}")


@task
def download_fixtures(
    season: str = "2024-25",
    minio_endpoint: Optional[str] = None,
    minio_access_key: Optional[str] = None,
    minio_secret_key: Optional[str] = None,
) -> None:
    """Loops through gameweeks and downloads any new ones"""

    creds = {
        "minio_endpoint": minio_endpoint,
        "minio_access_key": minio_access_key,
        "minio_secret_key": minio_secret_key,
    }
    ingestor = DimensionFileIngestor(**creds)

    BASE_URL = (
        "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data"
    )

    fixtures_url = f"{BASE_URL}/{season}/fixtures.csv"

    try:
        fixtures_file = ingestor.download_source_file(fixtures_url)
        ingestor.load_to_minio(
            data=fixtures_file,
            destination_bucket="bronze",
            destination_object_path=f"fixtures/{season}/fixtures_{season}.csv",
        )
        print(f"{fixtures_url} successfully ingested")
    except Exception as e:
        print(f"Ran into an error downloading fixtures: {e}")


@task
def download_gws(
    season: str = "2024-25",
    minio_endpoint: Optional[str] = None,
    minio_access_key: Optional[str] = None,
    minio_secret_key: Optional[str] = None,
) -> None:
    """Loops through gameweeks and downloads any new ones"""

    MAX_GAMEWEEK = 40
    gameweeks = [num for num in range(1, MAX_GAMEWEEK)]

    creds = {
        "minio_endpoint": minio_endpoint,
        "minio_access_key": minio_access_key,
        "minio_secret_key": minio_secret_key,
    }
    ingestor = GameweekIngestor(**creds)

    base_url = (
        "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data"
    )

    for week in gameweeks:
        # TODO: should be turned into a helper function
        try:
            full_url = f"{base_url}/{season}/gws/gw{week}.csv"
            gw_file = ingestor.download_source_file(full_url)
            gw_file = ingestor.add_gameweek(data=gw_file, gameweek=week)
            ingestor.load_to_minio(
                data=gw_file,
                destination_bucket="bronze",
                destination_object_path=f"gameweeks/{season}/gw_{season}_gw{week}.csv",
            )
            print(f"{full_url} successfully ingested")
        except Exception as e:
            print(f"Ran into an error: {e}")


@task
def get_data_from_bronze(
    client: Minio,
    config: SilverTransformationConfig,
    fetch_function: Callable[
        [Minio, str, str, str, str], Optional[dict[str, pd.DataFrame]]
    ],
) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info("Fetching files from Bronze layer...")
    fetch_bronze_data(client=client, config=config, fetch_function=fetch_function)


@task
def transform_fixtures() -> pd.DataFrame:
    logger = get_run_logger()
    logger.info("Transforming fixtures...")
    pass


@task
def validate_fixtures() -> bool:
    # use gx
    # parse json and return true/false depending on results
    return True


@task
def transform_gameweeks() -> pd.DataFrame:
    logger = get_run_logger()
    logger.info("Transforming gameweeks...")
    pass


@task
def validate_gameweeks() -> bool:
    # use gx
    # parse json and return true/false depending on results
    return True


@task
def load_data_to_silver() -> None:
    pass


if __name__ == "__main__":
    download_gws()
