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
    postgres_table_name: str = os.getenv("PG_TABLE_NAME_GW", "stg_gameweeks")
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "minio-yok44444")
    access_key: str = os.getenv("MINIO_ACCESS_KEY", "minio-fpl")
    secret_key: str = os.getenv("MINIO_SECRET_KEY", "secret-key")

    def __post_init__(self) -> None:
        """Validate critical configuration."""
        assert self.postgres_table_name == "stg_gameweeks", f"Invalid table name: expected 'stg_gameweeks', got '{self.postgres_table_name}'"
        assert self.minio_endpoint, "MinIO endpoint not configured"

class DataIngestion:
    def __init__(self) -> None:
        self.config = DataIngestionConfig()  # automatically loads from env
        self.client = create_minio_client(
            self.config.minio_endpoint,
            self.config.access_key,
            self.config.secret_key
        )
    
    def _initiate_data_ingestion(self) -> pd.DataFrame:
        """
        Fetch all gameweek data from MinIO.
        
        Returns:
            pd.DataFrame: Combined gameweeks dataframe.
        
        Raises:
            Exception: If there's an error during data ingestion or if no data is fetched.
        """
        print("Initiating gameweeks data ingestion...")
        try:
            # Fetch gameweeks data
            dfs = fetch_all_from_minio(
                endpoint=self.config.minio_endpoint,
                access_key=self.config.access_key,
                secret_key=self.config.secret_key,
                bucket_name="gameweeks"
            )

            if dfs is None or len(dfs) == 0:
                raise Exception("No data fetched from gameweeks bucket. Check if the bucket exists and contains objects.")
            
            print(f"Number of dataframes fetched: {len(dfs)}")
            for key, df in dfs.items():
                print(f"Dataframe {key} shape: {df.shape}")

            combined_df = pd.concat(dfs.values(), ignore_index=True)
            print(f"Combined gameweeks dataframe shape: {combined_df.shape}")
            
            return combined_df
        except Exception as e:
            raise Exception(f"Error during data ingestion: {e}")
    
    def _transform_and_dedupe_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform and deduplicate the gameweeks data.
        
        Args:
            df: The gameweeks dataframe
        
        Returns:
            pd.DataFrame: Transformed and deduplicated gameweeks dataframe
        
        Raises:
            Exception: If there's an error during data transformation
        """
        print("Transforming and deduplicating gameweeks data...")
        try:
            # Create a copy of the DataFrame to avoid SettingWithCopyWarning
            df = df.copy()

            # First rename columns before any operations
            df.rename(columns={
                "GW": "gameweek",
                "name": "player_name",
                "minutes": "minutes_played",
                "value": "player_cost",
                "starts": "player_started",
                "fixture": "seasonal_fixture_id"
            }, inplace=True)

            # Remove any duplicate columns that might have been created
            df = df.loc[:, ~df.columns.duplicated()]

            # Critical columns that must have valid values
            critical_columns = {
                "gameweek": "int",
                "team": "str",
                "player_name": "str",
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
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                    elif dtype == "str":
                        df[col] = df[col].astype(str).replace({'nan': None, 'None': None, 'FALSE': None, 'TRUE': None})
                    elif dtype == "datetime":
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                    
                    before = len(df)
                    df = df.dropna(subset=[col])
                    rows_dropped[f'conversion_{col}'] = before - len(df)
                except Exception as e:
                    print(f"Error converting column {col} to {dtype}: {str(e)}")

            # Handle boolean columns
            boolean_columns = ["was_home", "player_started", "modified"]
            for column in boolean_columns:
                # First ensure the column exists with default False
                if column not in df.columns:
                    print(f"Adding missing column {column} with default value False")
                    df[column] = False
                else:
                    try:
                        # Convert various string representations to boolean
                        df[column] = df[column].replace({
                            'True': True, 'true': True, 'TRUE': True, '1': True, 1: True, True: True,
                            'False': False, 'false': False, 'FALSE': False, '0': False, 0: False, False: False,
                            'not_a_bool': False, pd.NA: False, None: False  # Handle invalid values
                        }).fillna(False).astype(bool)
                    except Exception as e:
                        print(f"Warning: Error converting column {column} to boolean: {str(e)}")
                        df[column] = False

            # Add season column
            def determine_season(date: date) -> Optional[str]:
                if pd.isna(date):
                    return None
                year = date.year
                if date.month >= 7:  # July or later
                    return f"{year}-{str(year + 1)[-2:]}"
                else:  # Before July
                    return f"{year - 1}-{str(year)[-2:]}"

            df["season"] = df["kickoff_time"].apply(determine_season)

            # Drop unnecessary columns
            if "round" in df.columns:
                df = df.drop(columns=["round"])

            # Now handle opponent team
            if "seasonal_fixture_id" in df.columns:
                # Identify opponent team
                def identify_opponent_team(group: pd.DataFrame) -> pd.DataFrame:
                    if len(group["team"].unique()) == 2:
                        group["opponent_team"] = group["team"].apply(
                            lambda x: group["team"].unique()[1] if x == group["team"].unique()[0] else group["team"].unique()[0]
                        )
                    else:
                        group["opponent_team"] = None
                    return group

                # Update the groupby operation to avoid DeprecationWarning
                df = df.groupby(["kickoff_time", "seasonal_fixture_id"], group_keys=False).apply(identify_opponent_team)
            else:
                # If no seasonal_fixture_id, set opponent_team to None
                df["opponent_team"] = None

            # Log transformation results
            total_dropped = sum(rows_dropped.values())
            if total_dropped > 0:
                print("\nRows dropped during transformation:")
                for reason, count in rows_dropped.items():
                    if count > 0:
                        print(f"- {reason}: {count} rows")
                print(f"Final rows: {len(df)} (Started with {initial_rows})\n")

            # Remove duplicates based on player, gameweek, and kickoff time
            df = df.drop_duplicates(subset=["player_name", "gameweek", "kickoff_time"], keep="last")

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
                player_performance_id SERIAL PRIMARY KEY,
                player_name TEXT,
                player_cost NUMERIC,
                total_points INTEGER,
                position TEXT,
                season TEXT,
                gameweek INTEGER,
                seasonal_fixture_id INTEGER,
                team TEXT,
                opponent_team TEXT,
                team_a_score INTEGER,
                team_h_score INTEGER,
                was_home BOOLEAN,
                goals_scored INTEGER,
                assists INTEGER,
                bonus INTEGER,
                bps INTEGER,
                clean_sheets INTEGER,
                creativity NUMERIC,
                element INTEGER,
                "xp" NUMERIC,
                expected_assists NUMERIC,
                expected_goal_involvements NUMERIC,
                expected_goals NUMERIC,
                expected_goals_conceded NUMERIC,
                goals_conceded INTEGER,
                ict_index NUMERIC,
                influence NUMERIC,
                kickoff_time TIMESTAMP,
                minutes_played INTEGER,
                own_goals INTEGER,
                penalties_missed INTEGER,
                penalties_saved INTEGER,
                red_cards INTEGER,
                saves INTEGER,
                player_started BOOLEAN,
                threat NUMERIC,
                transfers_balance INTEGER,
                transfers_in INTEGER,
                transfers_out INTEGER,
                selected INTEGER,
                yellow_cards INTEGER,
                modified BOOLEAN DEFAULT FALSE
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
            "player_name", "player_cost", "total_points", "position", "season",
            "gameweek", "seasonal_fixture_id", "team", "opponent_team", "kickoff_time"
        ]
        for column in required_columns:
            df_ge.expect_column_to_exist(column)
        
        # Data type validations
        df_ge.expect_column_values_to_be_of_type("player_name", "str")
        df_ge.expect_column_values_to_be_of_type("total_points", "int64")
        df_ge.expect_column_values_to_be_of_type("gameweek", "int64")
        df_ge.expect_column_values_to_be_of_type("season", "str")
        
        # Value validations
        df_ge.expect_column_values_to_not_be_null("player_name")
        df_ge.expect_column_values_to_not_be_null("gameweek")
        df_ge.expect_column_values_to_not_be_null("season")
        df_ge.expect_column_values_to_not_be_null("seasonal_fixture_id")
        
        # Range validations
        df_ge.expect_column_values_to_be_between("gameweek", 1, 38)
        df_ge.expect_column_values_to_be_between("total_points", -20, 30)  # Reasonable range for points
    
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
            df = self._initiate_data_ingestion()
            transformed_df = self._transform_and_dedupe_data(df)

            # Validate data
            self._validate_data(transformed_df)

            # Connect to PostgreSQL
            conn = connect_to_postgres(
                self.config.postgres_database,
                self.config.postgres_host,
                self.config.postgres_user,
                self.config.postgres_password,
                self.config.postgres_port,
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
            engine: Engine = create_engine(
                f"postgresql://{self.config.postgres_user}:{self.config.postgres_password}"
                f"@{self.config.postgres_host}:{self.config.postgres_port}/{self.config.postgres_database}"
            )
            transformed_df.to_sql(self.config.postgres_table_name, engine, if_exists="append", index=False)
            print(f"Data successfully ingested into '{self.config.postgres_table_name}' table with a full refresh.")

        except Exception as e:
            raise Exception(f"Error during data ingestion: {e}")

        finally:
            # Ensure proper cleanup
            if cursor:
                cursor.close()
            if conn:
                conn.close()

if __name__ == "__main__":
    obj = DataIngestion()
    obj.ingest_data()