from prefect import flow
import logging
from src.config.logging_config import setup_logging
from src.prefect_files.tasks.fpl_source_extraction_tasks import (
    download_gws_task,
    download_teams_task,
    download_fixtures_task,
)
from src.etl_pipeline.components.bronze_to_silver_utils import (
    SilverTransformationConfig,
)

from src.prefect_files.tasks.fpl_bronze_to_silver_tasks import (
    transform_teams_task,
    get_data_from_bronze_task,
    load_to_silver_task,
)

from src.prefect_files.tasks.fpl_util_tasks import (
    get_minio_secret_task,
    get_minio_client_task,
)

setup_logging()
logger = logging.getLogger(__name__)


season = "2024-25"


@flow(name="fpl_data_pipeline", log_prints=True, retries=2)
def fpl_pipeline_flow() -> None:
    logger.info("Starting FPL pipeline flow")
    # load minio secret
    minio_secret = get_minio_secret_task()
    client = get_minio_client_task(minio_secret_key=minio_secret)

    # download raw data from source
    download_gws_task(season=season, minio_secret_key=minio_secret)
    download_teams_task(season=season, minio_secret_key=minio_secret)
    download_fixtures_task(season=season, minio_secret_key=minio_secret)

    config = SilverTransformationConfig()

    # fetch teams from bronze
    teams = get_data_from_bronze_task(
        client=client,
        config=config,
        folder="teams",
    )

    # transform teams
    transformed_teams = transform_teams_task(teams_dfs=teams)

    # load teams to silver
    load_to_silver_task(transformed_teams, config, "teams", client)

    # TODO: transform fixtures
    # TODO: validate fixtures with gx
    # TODO: upload fixtures

    # TODO: bronze -> silver transformation

    # TODO: silver -> gold transformation (is this DBT modeling?)

    # TODO: DBT Core modeling


if __name__ == "__main__":
    fpl_pipeline_flow()
