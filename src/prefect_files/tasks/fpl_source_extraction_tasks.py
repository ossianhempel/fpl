from prefect import task
from typing import Optional
from src.etl_pipeline.components.source_extraction import (
    GameweekIngestor,
    DimensionFileIngestor,
)


@task
def download_teams_task(
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
        teams_file = ingestor.add_season_column(data=teams_file, season=season)
        ingestor.load_to_minio(
            data=teams_file,
            destination_bucket="bronze",
            destination_object_path=f"teams/{season}/teams_{season}.csv",
        )
        print(f"{teams_url} successfully ingested")
    except Exception as e:
        print(f"Ran into an error downloading teams: {e}")


@task
def download_fixtures_task(
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
def download_gws_task(
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


if __name__ == "__main__":
    download_gws_task()
