from prefect import task
from src.etl_pipeline.components.source_extraction import SourceFileIngestor
import logging

logger = logging.getLogger(__name__)


@task
def download_gws(season: str = "2024-25"):
    """Loops through gameweeks and downloads any new ones"""
    gameweeks = [num for num in range(1, 40)]

    ingestor = SourceFileIngestor()

    base_url = (
        "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/"
    )

    for week in gameweeks:
        try:
            full_url = f"{base_url}/{season}/gws/gw{week}.csv"
            gw_file = ingestor.download_source_file(full_url)
            ingestor.load_to_minio(
                data=gw_file,
                destination_bucket="bronze",
                destination_object_path=f"gameweeks/{season}/gw_{season}_gw{week}.csv",
            )
            logger.info(f"{full_url} successfully ingested")
        except Exception as e:
            logger.error(f"Ran into an error: {e}")
