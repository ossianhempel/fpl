import os
import sys
import pandas as pd
from dotenv import load_dotenv
from dataclasses import dataclass
from sqlalchemy import create_engine
import great_expectations as ge
from typing import Dict, Tuple, Optional
from minio import Minio

# Add the project's root directory to the PYTHONPATH
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(project_root)
from src.utils import connect_to_minio, fetch_all_from_minio, connect_to_postgres, query_postgres

# Load environment variables from .env file
load_dotenv(os.path.join(project_root, ".env"))

@dataclass
class DataIngestionConfig:
    """Configuration for data ingestion settings."""
    postgres_database: Optional[str] = None
    postgres_host: Optional[str] = None
    postgres_user: Optional[str] = None
    postgres_password: Optional[str] = None
    postgres_port: Optional[int] = None
    postgres_table_name: Optional[str] = None
    minio_endpoint: Optional[str] = None
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    testing: bool = False

    def load_from_env(self) -> None:
        """Load configuration from environment variables."""
        self.postgres_database = os.getenv("PG_DATABASE")
        self.postgres_host = os.getenv("PG_HOST")
        self.postgres_user = os.getenv("PG_USER")
        self.postgres_password = os.getenv("PG_PASSWORD")
        self.postgres_port = int(os.getenv("PG_PORT", "0")) if os.getenv("PG_PORT") else None
        self.postgres_table_name = os.getenv("PG_TABLE_NAME_GW")
        self.minio_endpoint = os.getenv("MINIO_ENDPOINT")
        self.access_key = os.getenv("MINIO_ACCESS_KEY")
        self.secret_key = os.getenv("MINIO_SECRET_KEY")

        # Validate critical configuration
        assert self.postgres_table_name == "stg_gameweeks", f"Invalid table name: expected 'stg_gameweeks', got '{self.postgres_table_name}'"
        assert self.minio_endpoint is not None, "MinIO endpoint not configured"

class DataIngestion:
    def __init__(self, testing: bool = False):
        self.config = DataIngestionConfig(testing=testing)
        self.config.load_from_env()
        self.client = connect_to_minio(
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
        print("Initiating data ingestion...")
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
        print("Transforming and deduplicating data...")
        try:
            # Create a copy of the DataFrame to avoid SettingWithCopyWarning
            df = df.copy()

            # Handle GW field that might contain commas
            if "GW" in df.columns:
                # If GW is a string and contains commas, extract the first number
                if df["GW"].dtype == "object":
                    df["GW"] = df["GW"].apply(lambda x: str(x).split(",")[0] if pd.notnull(x) else x)

            # Define columns to transform and their target data types
            columns_to_transform = {
                "xp": "float",
                "creativity": "float",
                "expected_assists": "float",
                "expected_goal_involvements": "float",
                "expected_goals": "float",
                "expected_goals_conceded": "float",
                "ict_index": "float",
                "influence": "float",
                "threat": "float",
                "value": "float",
                "kickoff_time": "datetime",
                "was_home": "bool",
                "GW": "int",
                "minutes": "int",
                "total_points": "int",
                "goals_scored": "int",
                "assists": "int",
                "clean_sheets": "int",
                "goals_conceded": "int",
                "own_goals": "int",
                "penalties_saved": "int",
                "penalties_missed": "int",
                "yellow_cards": "int",
                "red_cards": "int",
                "saves": "int",
                "bonus": "int",
                "bps": "int",
                "team_a_score": "int",
                "team_h_score": "int",
                "fixture": "int",
                "selected": "int",
                "transfers_balance": "int",
                "transfers_in": "int",
                "transfers_out": "int"
            }

            # Apply transformations
            for column, dtype in columns_to_transform.items():
                if column in df.columns:
                    if dtype == "int":
                        df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
                    elif dtype == "float":
                        df[column] = pd.to_numeric(df[column], errors="coerce")
                    elif dtype == "datetime":
                        df[column] = pd.to_datetime(df[column], errors="coerce")
                    elif dtype == "bool":
                        df[column] = df[column].astype(bool)

            # Remove duplicates based on player, gameweek, and kickoff time
            df = df.drop_duplicates(subset=["name", "GW", "kickoff_time"], keep="last")

            # Add season column
            def determine_season(date):
                year = date.year
                if date.month >= 7:  # July or later
                    return f"{year}-{str(year + 1)[-2:]}"
                else:  # Before July
                    return f"{year - 1}-{str(year)[-2:]}"

            df["season"] = df["kickoff_time"].apply(determine_season)

            # Rename columns
            df.rename(columns={
                "GW": "gameweek",
                "name": "player_name",
                "minutes": "minutes_played",
                "value": "player_cost",
                "starts": "player_started",
                "fixture": "seasonal_fixture_id"
            }, inplace=True)

            # Drop unnecessary columns
            if "round" in df.columns:
                df = df.drop(columns=["round"])

            # Convert player_started to boolean if it exists
            if "player_started" in df.columns:
                df["player_started"] = df["player_started"].astype(bool)
            else:
                print("Warning: 'player_started' column not found in the DataFrame")

            # Identify opponent team
            def identify_opponent_team(group):
                if len(group["team"].unique()) == 2:
                    group["opponent_team"] = group["team"].apply(
                        lambda x: group["team"].unique()[1] if x == group["team"].unique()[0] else group["team"].unique()[0]
                    )
                else:
                    group["opponent_team"] = None
                return group

            # Update the groupby operation to avoid DeprecationWarning
            df = df.groupby(["kickoff_time", "seasonal_fixture_id"], group_keys=False).apply(identify_opponent_team)

            # Ensure data types match those in the PostgreSQL table
            df = df.astype({
                "player_name": "str",
                "player_cost": "float",
                "total_points": "int",
                "position": "str",
                "season": "str",
                "gameweek": "int",
                "seasonal_fixture_id": "int",
                "team": "str",
                "opponent_team": "str",
                "team_a_score": "int",
                "team_h_score": "int",
                "was_home": "bool",
                "player_started": "bool"
            })

            return df
        except Exception as e:
            raise Exception(f"Error transforming data: {e}")
    
    def _create_table_if_not_exists(self, cursor, table_name: str) -> None:
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
                yellow_cards INTEGER
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
        conn = None
        cursor = None
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
                self.config.postgres_port
            )
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