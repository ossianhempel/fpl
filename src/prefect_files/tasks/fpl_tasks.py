from prefect import task
from src.etl_pipeline.components.source_extraction import SourceFileIngestor


@task
def download_gws(
    season: str = "2024-25",
    minio_endpoint=None,
    minio_access_key=None,
    minio_secret_key=None,
):
    """Loops through gameweeks and downloads any new ones"""
    gameweeks = [num for num in range(1, 40)]

    creds = {
        "minio_endpoint": minio_endpoint,
        "minio_access_key": minio_access_key,
        "minio_secret_key": minio_secret_key,
    }
    ingestor = SourceFileIngestor(**creds)

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
            print(f"{full_url} successfully ingested")
        except Exception as e:
            print(f"Ran into an error: {e}")


if __name__ == "__main__":
    download_gws()
