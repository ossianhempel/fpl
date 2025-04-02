import requests
from minio import Minio
from src.utils.minio_utils import create_minio_client
import logging
from dotenv import load_dotenv
import os
import io
from dataclasses import dataclass
import polars as pl
import polars.selectors as cs
from datetime import datetime

from src.config.logging_config import setup_logging

setup_logging()

load_dotenv()


@dataclass
class SourceFileIngestorConfig:
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "COULDNT GET ENV VAR")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "COULDNT GET ENV VAR")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "COULDNT GET ENV VAR")
    destination_bucket: str = "bronze"


class SourceFileIngestor:
    """
    An ingestor that downloads a file from an url, adds metadata, and
    loads it to a Minio bucket.
    """

    def __init__(
        self, minio_endpoint=None, minio_access_key=None, minio_secret_key=None
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.config = SourceFileIngestorConfig()
        # allow overwrite ingestorConfig if credentials are passed manually to object (typically for remote deployment) - otherwise it will just use local dataclass config
        self.client = self._create_minio_client()
        if minio_endpoint:
            self.config.minio_endpoint = minio_endpoint
        if minio_access_key:
            self.config.minio_access_key = minio_access_key
        if minio_secret_key:
            self.config.minio_secret_key = minio_secret_key

        self.logger.info(f"Using MinIO endpoint: {self.config.minio_endpoint}")

    def _create_minio_client(self) -> Minio:
        """Creates a Minio client object using a utility function"""
        try:
            self.client = create_minio_client(
                endpoint=self.config.minio_endpoint,
                access_key=self.config.minio_access_key,
                secret_key=self.config.minio_secret_key,
            )
        except Exception as e:
            self.logger.error(f"Minio client failed to be created: {e}")
            raise Exception
        self.logger.info("Minio client was successfully created")
        return self.client

    def download_source_file(self, url: str) -> str | None:
        """Download a file from a URL to a specified destination"""
        try:
            self.logger.info(f"Downloading from {url}")
            response = requests.get(url)
            response.raise_for_status()

            # create in-memory file object
            file_content = io.BytesIO(response.content)
            return file_content
        except Exception as e:
            self.logger.error(f"Error downloading data: {e}")
            return None

    def _validate_data(
        self, data: io.BytesIO, destination_bucket: str, destination_object_path: str
    ) -> bool:
        """Checks source freshness, unique values, and nulls"""

        # check source freshness
        try:
            # TODO: allow option to pass bucket objects so it can be used in loops without having to refetch every iteration
            self.logger.info(f"Fetching all objects from {destination_bucket}..")
            objects = self.client.list_objects(
                bucket_name=destination_bucket, recursive=True
            )

            # loop through objects and check if file already exists
            self.logger.info(
                f"Looping through objects in {destination_bucket} to see if {destination_object_path} has already been ingested"
            )
            for obj in objects:
                if obj.object_name == destination_object_path:
                    self.logger.error(
                        f"The object {obj.object_name} is already present in the destination"
                    )
                    return False
        except Exception as e:
            self.logger.error(
                f"An error occured when fetching or looping through Minio objects: {e}"
            )
            raise Exception

        # check the shape of data
        try:
            self.logger.info("Checking the shape of the dataset")

            # turn bytes object into polars dataframe
            df = pl.read_csv(data)

            self.logger.info(f"Shape of the dataset: {df.shape}")
            self.logger.info(f"Schema of dataset: {df.schema}")

        except Exception as e:
            self.logger.error(f"Ran into an error checking shape and schema: {e}")
            raise Exception

        # check distinct values
        try:
            self.logger.info("Checking distinct values in categorical columns")

            # turn bytes object into polars dataframe
            # df = pl.read_csv(data)

            # select categorical columns
            categorical_cols = df.select(cs.categorical()).columns

            self.logger.info(f"Categorical columns: {categorical_cols}")

            # check unique values
            for col in categorical_cols:
                unique_values = df[col].unique()
                self.logger.info(
                    f"Column {col} has the following unique values: \n{unique_values}"
                )
        except Exception as e:
            self.logger.error(f"Ran into an error checking distinct values: {e}")
            raise Exception

        # check for null values
        try:
            self.logger.info("Checking for null values")
            if df.null_count().sum_horizontal().gt(0).any():
                self.logger.warning(f"Found {df.null_count} null values")

                self.logger.info("Checking which columns contain null values..")
                for col in df.columns:
                    if df[col].null_count() > 0:
                        self.logger.info(
                            f"{col} has {df[col].null_count()} missing values, {(df[col].null_count/len(df))*100}%"
                        )
            else:
                self.logger.info("No null values")
        except Exception as e:
            self.logger.error(f"Ran into an error checking for null values: {e}")
            raise Exception

        # checks are done
        return True

    def _add_metadata(self, data: io.BytesIO) -> io.BytesIO | Exception:
        """Add ingestion metadata to the data"""
        try:
            self.logger.info("Adding ingestion timestamp metadata to source data")
            df = pl.read_csv(data)
            df = df.with_columns(pl.lit(datetime.now()).alias("ingestion_timestamp"))

            # convert back to BytesIO
            buffer = io.BytesIO()
            df.write_csv(buffer)
            buffer.seek(0)
            return buffer
        except Exception as e:
            self.logger.error(f"Failed to add metadata: {e}")
            raise Exception

    def load_to_minio(
        self, data: io.BytesIO, destination_bucket: str, destination_object_path: str
    ) -> bool:
        """Upload data to MinIO"""

        # reset buffer position
        data.seek(0)

        if not self._validate_data(
            data=data,
            destination_bucket=destination_bucket,
            destination_object_path=destination_object_path,
        ):
            self.logger.error("Validation failed, upload canceled")
            raise Exception

        try:
            self.logger.info("Adding metadata..")
            data = self._add_metadata(data)
        except Exception as e:
            self.logger.error(f"Failed to add metadata: {e}")
            raise Exception

        try:
            # reset buffer position
            data.seek(0)
            file_size = data.getbuffer().nbytes

            # ensure bucket exists
            if not self.client.bucket_exists(destination_bucket):
                self.logger.warning(
                    f"Bucket {destination_bucket} does not exist. Creating it."
                )
                self.client.make_bucket(destination_bucket)

            # upload data
            self.logger.info(
                f"Uploading to Minio: {destination_bucket}/{destination_object_path}"
            )
            self.client.put_object(
                bucket_name=destination_bucket,
                object_name=destination_object_path,
                data=data,
                content_type="application/csv",
                length=file_size,
            )

            return True
        except Exception as e:
            self.logger.error(f"Error uploading to MinIO: {e}")
            return False


if __name__ == "__main__":
    load_dotenv()

    # base_url = (
    #     "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/"
    # )
    # full_url = f"{base_url}/2024-25/gws/gw2.csv"

    # ingestor = SourceFileIngestor()

    # file = ingestor.download_source_file(full_url)

    # ingestor.load_to_minio(
    #     data=file, destination_bucket="bronze", destination_object_path="test_file.csv"
    # )

    gameweeks = [num for num in range(40)]

    ingestor = SourceFileIngestor()

    base_url = (
        "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/"
    )

    season = "2024-25"

    for week in gameweeks:
        try:
            full_url = f"{base_url}/{season}/gws/gw{week}.csv"
            gw_file = ingestor.download_source_file(full_url)
            ingestor.load_to_minio(
                data=gw_file,
                destination_bucket="bronze",
                destination_object_path=f"gameweeks/{season}/gw_{season}_gw{week}.csv",
            )
        except Exception as e:
            print(f"Ran into an error: {e}")
