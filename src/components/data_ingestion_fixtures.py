import os
import sys
import pandas as pd
from dotenv import load_dotenv
from dataclasses import dataclass
from sqlalchemy import create_engine
import great_expectations as ge
from typing import Dict, Tuple, Optional
from minio import Minio
from datetime import date
from psycopg2.extensions import connection
from psycopg2.extensions import cursor as PgCursor # alias to avoid type conflation with cursor variable
from sqlalchemy.engine import Engine

# Add the project's root directory to the PYTHONPATH
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(project_root)

from src.utils.postgres_utils import connect_to_postgres, query_postgres
from src.utils.minio_utils import create_minio_client, fetch_all_from_minio

# Load environment variables from .env file
load_dotenv(os.path.join(project_root, ".env"))

@dataclass
class DataIngestionConfig:
    """Configuration for data ingestion settings."""
    postgres_database: str = os.getenv("PG_DATABASE", "fpl")
    postgres_host: str = os.getenv("PG_HOST", "65.108.88.160")
    postgres_user: str = os.getenv("PG_USER", "ossian")
    postgres_password: str = os.getenv("PG_PASSWORD", "password")
    postgres_port: int = int(os.getenv("PG_PORT", 5436))
    postgres_table_name: str = os.getenv("PG_TABLE_NAME_FIXTURES", "stg_fixtures")
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "minio-yok44444")
    access_key: str = os.getenv("MINIO_ACCESS_KEY", "minio-fpl")
    secret_key: str = os.getenv("MINIO_SECRET_KEY", "secret-key")

    def __post_init__(self) -> None:
        """Validate critical configuration."""
        assert self.postgres_table_name == "stg_fixtures", f"Invalid table name: expected 'stg_fixtures', got '{self.postgres_table_name}'"
        assert self.minio_endpoint, "MinIO endpoint not configured"

class DataIngestion:
    def __init__(self) -> None:
        self.config = DataIngestionConfig()
        self.client = create_minio_client(
            self.config.minio_endpoint,
            self.config.access_key,
            self.config.secret_key
        )
    
    def _initiate_data_ingestion(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Fetch all data from MinIO buckets for fixtures and teams.
        
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: Combined fixtures and teams dataframes.
        
        Raises:
            Exception: If there's an error during data ingestion or if no data is fetched.
        """
        print("Initiating fixtures data ingestion...")
        try:
            # Fetch fixtures data
            dfs = fetch_all_from_minio(
                endpoint=self.config.minio_endpoint,
                access_key=self.config.access_key,
                secret_key=self.config.secret_key,
                bucket_name="fixtures"
            )
            if dfs is None or len(dfs) == 0:
                raise Exception("No data fetched from fixtures bucket. Check if the bucket exists and contains objects.")

            # Fetch teams data for mapping
            teams_dfs = fetch_all_from_minio(
                endpoint=self.config.minio_endpoint,
                access_key=self.config.access_key,
                secret_key=self.config.secret_key,
                bucket_name="teams"
            )
            if teams_dfs is None or len(teams_dfs) == 0:
                raise Exception("No data fetched from teams bucket. Check if the bucket exists and contains objects.")


            
            print(f"Number of dataframes fetched: {len(dfs)}")
            for key, df in dfs.items():
                print(f"Dataframe {key} shape: {df.shape}")

            combined_df = pd.concat(dfs.values(), ignore_index=True)
            combined_teams_df = pd.concat(teams_dfs.values(), ignore_index=True)
            
            print(f"Combined fixtures dataframe shape: {combined_df.shape}")
            print(f"Combined teams dataframe shape: {combined_teams_df.shape}")
            
            return combined_df, combined_teams_df
        except Exception as e:
            raise Exception(f"Error during data ingestion: {e}")
    
    def _transform_and_dedupe_data(self, df: pd.DataFrame, teams_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform and deduplicate the fixtures data.
        
        Args:
            df: The fixtures dataframe
            teams_df: The teams dataframe for mapping
        
        Returns:
            pd.DataFrame: Transformed and deduplicated fixtures dataframe
        
        Raises:
            Exception: If there's an error during data transformation
        """
        print("Transforming and deduplicating fixtures data...")
        try:
            # Create a copy of the DataFrame to avoid SettingWithCopyWarning
            df = df.copy()

            # Critical columns that must have valid values
            critical_columns = {
                "event": "int",
                "team_a": "int",
                "team_h": "int",
                "kickoff_time": "datetime"
            }

            # First handle critical columns
            initial_rows = len(df)
            rows_dropped = {}

            # Convert and validate critical columns
            for col, dtype in critical_columns.items():
                if col not in df.columns:
                    print(f"Warning: Critical column {col} missing from data")
                    continue

                try:
                    if dtype == "int":
                        # Replace 'False', 'TRUE', 'FALSE' with NaN
                        df[col] = df[col].replace(['False', 'TRUE', 'FALSE'], pd.NA)
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    elif dtype == "datetime":
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                    
                    before = len(df)
                    df = df.dropna(subset=[col])
                    rows_dropped[f'conversion_{col}'] = before - len(df)
                except Exception as e:
                    print(f"Error converting column {col} to {dtype}: {str(e)}")

            # Define columns to transform and their target data types for non-critical columns
            columns_to_transform = {
                "id": "int",
                "minutes": "int",
                "team_a_score": "float",
                "team_h_score": "float",
                "team_h_difficulty": "int",
                "team_a_difficulty": "int",
                "pulse_id": "int"
            }

            # Apply transformations to non-critical columns
            for column, dtype in columns_to_transform.items():
                if column in df.columns:
                    try:
                        if dtype == "int":
                            # Replace boolean-like values with NaN
                            df[column] = df[column].replace(['False', 'TRUE', 'FALSE'], pd.NA)
                            df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
                        elif dtype == "float":
                            df[column] = df[column].replace(['False', 'TRUE', 'FALSE'], pd.NA)
                            df[column] = pd.to_numeric(df[column], errors='coerce')
                    except Exception as e:
                        print(f"Warning: Error converting column {column} to {dtype}: {str(e)}")
                        # For non-critical columns, we continue even if conversion fails

            # Handle boolean columns
            boolean_columns = ["finished", "finished_provisional", "started"]
            for column in boolean_columns:
                if column in df.columns:
                    try:
                        # Convert various string representations to boolean
                        df[column] = df[column].map({
                            'True': True, 'true': True, 'TRUE': True, '1': True, 1: True,
                            'False': False, 'false': False, 'FALSE': False, '0': False, 0: False
                        })
                    except Exception as e:
                        print(f"Warning: Error converting column {column} to boolean: {str(e)}")

            # Determine deduplication key
            if "pulse_id" in df.columns and "code" in df.columns:
                dedup_key = ["pulse_id", "code"]
            elif "code" in df.columns:
                dedup_key = ["code"]
            else:
                raise ValueError("Neither 'pulse_id' and 'code' nor 'code' alone found in the dataframe")

            # Remove duplicates
            rows_before = len(df)
            df = df.drop_duplicates(subset=dedup_key, keep="last")
            rows_dropped['duplicates'] = rows_before - len(df)
            
            # Remove 'stats' column if it exists
            if "stats" in df.columns:
                df = df.drop(columns=["stats"])
            
            # Add season column
            def determine_season(date: date) -> Optional[str]:
                if pd.isna(date):
                    return None
                year = date.year
                if date.month >= 8:  # August or later
                    return f"{year}-{str(year + 1)[-2:]}"
                else:  # Before July
                    return f"{year - 1}-{str(year)[-2:]}"
            
            df["season"] = df["kickoff_time"].apply(determine_season)
            
            # Drop rows with null season (should not happen since we filtered kickoff_time)
            df = df.dropna(subset=['season'])

            # Rename columns
            df.rename(columns={
                "event": "gameweek",
                "id": "seasonal_fixture_id"
            }, inplace=True)
            
            # Ensure gameweek is Int64
            df["gameweek"] = pd.to_numeric(df["gameweek"], errors='coerce').astype('Int64')
            
            # Map team names
            teams_df = teams_df[["id", "name", "season"]]
            
            # Map home team names
            df = df.merge(teams_df, left_on=["team_h", "season"], right_on=["id", "season"], how="left")
            df = df.rename(columns={"name": "team_h_name"})
            df = df.drop(columns=["id"])
            
            # Map away team names
            df = df.merge(teams_df, left_on=["team_a", "season"], right_on=["id", "season"], how="left")
            df = df.rename(columns={"name": "team_a_name"})
            df = df.drop(columns=["id"])
            
            # Drop rows where team mapping failed
            rows_before = len(df)
            df = df.dropna(subset=['team_h_name', 'team_a_name'])
            rows_dropped['team_mapping'] = rows_before - len(df)

            # Log transformation results
            total_dropped = sum(rows_dropped.values())
            if total_dropped > 0:
                print("\nRows dropped during transformation:")
                for reason, count in rows_dropped.items():
                    if count > 0:
                        print(f"- {reason}: {count} rows")
                print(f"Final rows: {len(df)} (Started with {initial_rows})\n")
            
            return df
        except Exception as e:
            raise Exception(f"Error transforming data: {e}")
    
    def _create_table_if_not_exists(self, cursor: PgCursor, table_name: str) -> None:
        """
        Create the target table in PostgreSQL if it doesn't already exist.
        
        Args:
            cursor: Database cursor object
            table_name: Name of the table to create
        """
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                fixture_id SERIAL PRIMARY KEY,
                seasonal_fixture_id INTEGER,
                code INTEGER,
                gameweek INTEGER,
                season TEXT,
                finished BOOLEAN,
                finished_provisional BOOLEAN,
                kickoff_time TIMESTAMP,
                minutes INTEGER,
                provisional_start_time BOOLEAN,
                started BOOLEAN,
                team_a INTEGER,
                team_a_name TEXT,
                team_a_score FLOAT,
                team_h INTEGER,
                team_h_name TEXT,
                team_h_score FLOAT,
                team_h_difficulty INTEGER,
                team_a_difficulty INTEGER,
                pulse_id INTEGER
            );
        """
        query_postgres(cursor, create_table_query)
        print(f"Table '{table_name}' created or verified.")
    
    def _validate_data(self, df: pd.DataFrame) -> None:
        """
        Validate the data using Great Expectations.
        
        Args:
            df: The dataframe to validate
        """
        df_ge = ge.from_pandas(df)
        
        # Basic column existence validations
        required_columns = [
            "seasonal_fixture_id", "gameweek", "season", "team_h", "team_a",
            "team_h_name", "team_a_name", "kickoff_time"
        ]
        for column in required_columns:
            df_ge.expect_column_to_exist(column)
        
        # Data type validations
        df_ge.expect_column_values_to_be_of_type("seasonal_fixture_id", "int64")
        df_ge.expect_column_values_to_be_of_type("gameweek", "int64")
        df_ge.expect_column_values_to_be_of_type("season", "str")
        
        # Value validations
        df_ge.expect_column_values_to_not_be_null("seasonal_fixture_id")
        df_ge.expect_column_values_to_not_be_null("gameweek")
        df_ge.expect_column_values_to_not_be_null("season")
        
        # Range validations
        df_ge.expect_column_values_to_be_between("gameweek", 1, 38)
    
    def ingest_data(self) -> None:
        """
        Main method to fetch, transform, deduplicate, and ingest data into PostgreSQL.
        
        Raises:
            Exception: If there's an error during data ingestion
        """
        conn: Optional[connection] = None
        cursor: Optional[PgCursor] = None

        try:
            # Fetch and transform data
            df, teams_df = self._initiate_data_ingestion()
            transformed_df = self._transform_and_dedupe_data(df, teams_df)
            
            # Validate data
            self._validate_data(transformed_df)
            
            # Connect to PostgreSQL
            conn = connect_to_postgres(
                self.config.postgres_database,
                self.config.postgres_host,
                self.config.postgres_user,
                self.config.postgres_password,
                self.config.postgres_port
            )

            if conn is None:
                raise Exception("Failed to establish a database connection.")
            
            cursor = conn.cursor()
            
            # Create table if it doesn't exist
            self._create_table_if_not_exists(cursor, self.config.postgres_table_name)
            
            # Truncate the table for a full refresh
            truncate_query = f"TRUNCATE TABLE {self.config.postgres_table_name};"
            cursor.execute(truncate_query)
            conn.commit()
            print(f"Table '{self.config.postgres_table_name}' truncated for a full refresh.")
            
            # Insert the transformed data
            engine = create_engine(
                f"postgresql://{self.config.postgres_user}:{self.config.postgres_password}"
                f"@{self.config.postgres_host}:{self.config.postgres_port}/{self.config.postgres_database}"
            )
            transformed_df.to_sql(self.config.postgres_table_name, engine, if_exists="append", index=False)
            print(f"Data successfully ingested into '{self.config.postgres_table_name}' table with a full refresh.")
            
        except Exception as e:
            raise Exception(f"Error during data ingestion: {e}")
        
        finally:
            # Ensure proper cleanup
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()

if __name__ == "__main__":
    obj = DataIngestion()
    obj.ingest_data()