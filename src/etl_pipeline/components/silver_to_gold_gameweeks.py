import os
from dotenv import load_dotenv
import json

from src.utils.minio_utils import create_minio_client
from src.config.config import GoldTransformationConfig
from src.utils.etl_utils import (
    fetch_lake_data,
    load_to_lake,
    validate_key_columns,
    validate_important_columns,
)
from src.utils.minio_utils import fetch_all_from_minio


if __name__ == "__main__":
    load_dotenv()
    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    if endpoint is None or access_key is None or secret_key is None:
        raise ValueError("Missing required environment variables")

    client = create_minio_client(endpoint, access_key, secret_key)

    config = GoldTransformationConfig()

    # get the path to the config file relative to this script's directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, "..", "..", "config", "column_config.json")

    with open(config_path, "r") as file:
        column_config = json.load(file)

    gameweeks_dfs = fetch_lake_data(
        client=client,
        fetch_function=fetch_all_from_minio,
        config=config,
        folder="gameweeks",
    )

    for df in gameweeks_dfs.values():
        validate_key_columns(df, key_columns=column_config["key_columns"]["gameweeks"])
        validate_important_columns(
            df, important_columns=column_config["important_columns"]["gameweeks"]
        )

    load_to_lake(
        df, config.destination_bucket, "gameweeks/gameweeks_gold.parquet", client
    )
