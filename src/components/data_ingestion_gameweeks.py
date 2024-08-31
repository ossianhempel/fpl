import os
import sys
import pandas as pd
from dotenv import load_dotenv
from dataclasses import dataclass
from sqlalchemy import create_engine

# Add the project's root directory to the PYTHONPATH
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(project_root)
from src.utils import connect_to_minio, fetch_all_from_minio, connect_to_postgres, query_postgres

# TODO - refactor to use Polars

# Load environment variables
load_dotenv()

@dataclass
class DataIngestionConfig:
    postgres_database: str = os.getenv('PG_DATABASE')
    postgres_host: str = os.getenv('PG_HOST')
    postgres_user: str = os.getenv('PG_USER')
    postgres_password: str = os.getenv('PG_PASSWORD')
    postgres_port: int = os.getenv('PG_PORT')
    postgres_table_name: str = os.getenv('PG_TABLE_NAME_GW')
    minio_endpoint: str = os.getenv('MINIO_ENDPOINT')
    access_key: str = os.getenv('MINIO_ACCESS_KEY')
    secret_key: str = os.getenv('MINIO_SECRET_KEY')
    minio_bucket_name: str = os.getenv('MINIO_BUCKET_NAME')

class DataIngestion:
    def __init__(self):
        self.config = DataIngestionConfig()
    
    def _initiate_data_ingestion(self):
        print("Entered the data ingestion component")
        assert self.config.minio_endpoint == "minio-yokckg4o44wg40wogk0okgks.65.108.88.160.sslip.io", "Did not find the Minio endpoint"
        assert self.config.postgres_table_name == "stg_gameweeks", f"Not correct table naming (should be 'stg_gameweeks', received {self.config.postgres_table_name})"
        
        try:
            # Fetch all data from MinIO
            dfs = fetch_all_from_minio(
                self.config.minio_endpoint, 
                self.config.access_key, 
                self.config.secret_key,
                "gameweeks"
            )

            if dfs is None or len(dfs) == 0:
                raise Exception(f"No data fetched from bucket '{self.config.minio_bucket_name}'. Check if the bucket exists and contains objects.")
            
            combined_df = pd.concat(dfs.values(), ignore_index=True)
            print(f"Combined data: {combined_df.head(5)}")
            return combined_df
        except Exception as e:
            raise Exception(f"Error during data ingestion: {e}")
    
    def _transform_and_dedupe_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform the data as needed and remove duplicates.
        """

        print("Transforming and deduplicating data...")
        try:
            # Define the columns and their corresponding transformations
            columns_to_transform = {
                'xP': 'numeric',
                'creativity': 'numeric',
                'expected_assists': 'numeric',
                'expected_goal_involvements': 'numeric',
                'expected_goals': 'numeric',
                'expected_goals_conceded': 'numeric',
                'ict_index': 'numeric',
                'influence': 'numeric',
                'threat': 'numeric',
                'value': 'numeric',
                'kickoff_time': 'datetime',
                'was_home': 'boolean',
            }

            # Apply transformations only if the column exists in the dataframe
            for column, dtype in columns_to_transform.items():
                if column in df.columns:
                    if dtype == 'numeric':
                        df[column] = pd.to_numeric(df[column], errors='coerce')
                    elif dtype == 'boolean':
                        df[column] = df[column].astype(bool)
                    elif dtype == 'datetime':
                        df[column] = pd.to_datetime(df[column], errors='coerce')

            # Remove duplicates based on 'name' and 'GW'
            if 'name' in df.columns and 'GW' in df.columns:
                df = df.drop_duplicates(subset=['name', 'GW'], keep='last')
            else:
                print("Warning: 'name' or 'GW' column missing. Deduplication skipped.")

            # add season column
            def determine_season(date):
                year = date.year
                if date.month >= 7:  # July or later
                    return f"{year}-{str(year + 1)[-2:]}"
                else:  # Before July
                    return f"{year - 1}-{str(year)[-2:]}"
            
            df.loc[:, 'season'] = df['kickoff_time'].apply(determine_season)

            # rename columns
            df.rename(columns={
                'GW': 'gameweek',
                'name': 'player_name',
                'minutes': 'minutes_played',
                'value': 'player_cost',
                'starts': 'player_started',
            }, inplace=True)

            # drop unnecessary columns
            df.drop(['round'], axis=1, inplace=True)

            # Identify the opponent team by using the groupby operation on 'kickoff_time' and 'fixture'
            def identify_opponent_team(group):
                # There should be exactly two teams in each group
                if len(group['team'].unique()) == 2:
                    # The opponent team is the one that is not the current team
                    group['opponent_team'] = group['team'].apply(lambda x: group['team'].unique()[1] if x == group['team'].unique()[0] else group['team'].unique()[0])
                else:
                    group['opponent_team'] = None  # Handle cases where data might be incomplete
                return group

            df = df.groupby(['kickoff_time', 'fixture'], group_keys=False).apply(identify_opponent_team)
            
            # Convert player_started to boolean
            if 'player_started' in df.columns:
                df['player_started'] = df['player_started'].astype(bool)

            return df
        except Exception as e:
            raise Exception(f"Error transforming data: {e}")
    
    def _create_table_if_not_exists(self, cursor, table_name: str):
        """
        Create the target table in PostgreSQL if it doesn't already exist.
        """
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                gameweek_id SERIAL PRIMARY KEY,
                gameweek INTEGER,
                player_name TEXT,
                position TEXT,
                team TEXT,
                season TEXT,
                "xP" NUMERIC,
                assists INTEGER,
                bonus INTEGER,
                bps INTEGER,
                clean_sheets INTEGER,
                creativity NUMERIC,
                element INTEGER,
                expected_assists NUMERIC,
                expected_goal_involvements NUMERIC,
                expected_goals NUMERIC,
                expected_goals_conceded NUMERIC,
                fixture INTEGER,
                goals_conceded INTEGER,
                goals_scored INTEGER,
                ict_index NUMERIC,
                influence NUMERIC,
                kickoff_time TIMESTAMP,
                minutes_played INTEGER,
                opponent_team TEXT,
                own_goals INTEGER,
                penalties_missed INTEGER,
                penalties_saved INTEGER,
                red_cards INTEGER,
                saves INTEGER,
                selected INTEGER,
                player_started BOOLEAN,
                team_a_score INTEGER,
                team_h_score INTEGER,
                threat NUMERIC,
                total_points INTEGER,
                transfers_balance INTEGER,
                transfers_in INTEGER,
                transfers_out INTEGER,
                player_cost NUMERIC,
                was_home BOOLEAN,
                yellow_cards INTEGER
            );
        """
        query_postgres(cursor, create_table_query)
        print(f"Table '{table_name}' created or verified.")
    
    def ingest_data(self):
        """
        Main method to fetch, transform, deduplicate, and ingest data into PostgreSQL.
        """
        conn = None
        cursor = None
        try:
            # Fetch and transform data
            df = self._initiate_data_ingestion()  # Fetch all data
            transformed_df = self._transform_and_dedupe_data(df)  # Transform and deduplicate data
            
            # Connect to PostgreSQL using your utility function
            conn = connect_to_postgres(
                self.config.postgres_database, 
                self.config.postgres_host, 
                self.config.postgres_user, 
                self.config.postgres_password, 
                self.config.postgres_port
            )
            cursor = conn.cursor()  # Create a cursor from the connection object
            
            # Create table if it doesn't exist
            self._create_table_if_not_exists(cursor, self.config.postgres_table_name)
            
            # Truncate the table to perform a full refresh
            truncate_query = f"TRUNCATE TABLE {self.config.postgres_table_name};"
            cursor.execute(truncate_query)
            conn.commit()  # Commit the transaction after truncation
            print(f"Table '{self.config.postgres_table_name}' truncated for a full refresh.")
            
            # Insert the transformed data into the table after truncation
            engine = create_engine(f'postgresql://{self.config.postgres_user}:{self.config.postgres_password}@{self.config.postgres_host}:{self.config.postgres_port}/{self.config.postgres_database}')
            transformed_df.to_sql(self.config.postgres_table_name, engine, if_exists='append', index=False)
            print(f"Data successfully ingested into '{self.config.postgres_table_name}' table with a full refresh.")
            
        except Exception as e:
            raise Exception(f"Error during data ingestion: {e}")
        
        finally:
            # Ensure the cursor and connection are closed properly
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()

if __name__ == "__main__":
    obj = DataIngestion()
    obj.ingest_data()