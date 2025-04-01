from dataclasses import dataclass
from abc import abstractmethod
from typing import Optional, List
import os


# base config class
@dataclass
class BaseConfig:
    """Base configuration class"""

    @abstractmethod
    def validate(self) -> bool:
        """Validate configuration settings"""
        pass  # each concrete config will have their specific validations, just enforce that it's implemented


@dataclass
class MinioConfig(BaseConfig):
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "minio-yok44444")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minio-fpl")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "secret-key")

    def validate(self) -> bool:
        return True  # TODO: fix


@dataclass
class PostgresConfig(BaseConfig):
    pg_database: str = os.getenv("PG_DATABASE", "fpl")
    pg_host: str = os.getenv("PG_HOST", "65.108.88.160")
    pg_user: str = os.getenv("PG_USER", "ossian")
    pg_password: str = os.getenv("PG_PASSWORD", "password")
    pg_port: int = int(os.getenv("PG_PORT", 5436))
    pg_table_name: str = os.getenv("PG_TABLE_NAME_GW", "stg_gameweeks")

    def validate(self) -> bool:
        return True  # TODO: fix


@dataclass
class DataFetchConfig(BaseConfig):
    """Configuration for data fetching"""

    minio: MinioConfig
    bucket_name: str
    required_sources: Optional[List[str]] = None  # TODO: do we want this or not?

    def validate(self) -> bool:
        return True


"""
In the context of a data fetcher for your FPL (Fantasy Premier League) data pipeline, required_sources would likely be a list of specific files or data sources that must be present in the MinIO bucket for the fetching operation to be considered valid.
Looking at your ingestion files:

For gameweeks, you're fetching from a single bucket "gameweeks"
For fixtures, you need data from both "fixtures" and "teams" buckets

So required_sources could be used to specify:

Required file patterns/names in the bucket
Specific gameweeks or seasons that must be present
Dependencies that need to exist before fetching
"""


@dataclass
class GameweeksConfig(BaseConfig):
    """Configuration specific to gameweeks ingestion from silver/gold layer into Postgres"""

    table_name: str = "stg_gameweeks"
    required_columns: Optional[List[str]] = None

    def __post_init__(self) -> None:
        if self.required_columns is None:
            self.required_columns = [  # TODO: set these
                "",
                "",
            ]

    def validate(self) -> bool:
        return self.table_name == "stg_gameweeks"


@dataclass
class FixturesConfig(BaseConfig):
    """Configuration specific to gameweeks ingestion from silver/gold layer into Postgres"""

    table_name: str = "stg_fixtures"
    required_columns: Optional[List[str]] = None

    def __post_init__(self) -> None:
        if self.required_columns is None:
            self.required_columns = [  # TODO: set these
                "",
                "",
            ]

    def validate(self) -> bool:
        return self.table_name == "stg_fixtures"


@dataclass
class PipelineConfig(BaseConfig):
    """Main pipeline configuration"""

    data_type: str
    gameweeks_config: Optional[GameweeksConfig]
    fixtures_config: Optional[FixturesConfig]

    def validate(self) -> bool:
        assert self.data_type in [
            "gameweeks",
            "fixtures",
        ], f"Invalid data_type: {self.data_type}"
        if self.data_type == "gameweeks":
            assert (
                self.gameweeks_config is not None
            ), "gameweeks_config is required for gameweeks data_type"
        elif self.data_type == "fixtures":
            assert (
                self.fixtures_config is not None
            ), "fixtures_config is required for fixtures data_type"

        return True
