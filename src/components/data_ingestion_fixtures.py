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


# Load environment variables
load_dotenv()

@dataclass
class DataIngestionConfig:
    postgres_database: str = os.getenv('PG_DATABASE')
    postgres_host: str = os.getenv('PG_HOST')
    postgres_user: str = os.getenv('PG_USER')
    postgres_password: str = os.getenv('PG_PASSWORD')
    postgres_port: int = os.getenv('PG_PORT')
    postgres_table_name: str = os.getenv('PG_TABLE_NAME_FIXTURES')
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
        assert self.config.postgres_table_name == "stg_fixtures", f"Not correct table naming (should be 'stg_fixtures', received {self.config.postgres_table_name})"
        
        try:
            # Fetch all data from MinIO
            dfs = fetch_all_from_minio(
                self.config.minio_endpoint, 
                self.config.access_key, 
                self.config.secret_key,
                "fixtures"
            )

            if dfs is None or len(dfs) == 0:
                raise Exception(f"No data fetched from bucket '{self.config.minio_bucket_name}'. Check if the bucket exists and contains objects.")
            
            print(f"Number of dataframes fetched: {len(dfs)}")
            for key, df in dfs.items():
                print(f"Dataframe {key} shape: {df.shape}")

            combined_df = pd.concat(dfs.values(), ignore_index=True)
            print(f"Combined dataframe shape: {combined_df.shape}")
            return combined_df
        except Exception as e:
            raise Exception(f"Error during data ingestion: {e}")
    
    def _transform_and_dedupe_data(self, df: pd.DataFrame) -> pd.DataFrame:
        print("Transforming and deduplicating data...")
        try:
            # Define the columns and their corresponding transformations
            columns_to_transform = {
                'event': 'int',
                'id': 'int',
                'kickoff_time': 'datetime',
                'minutes': 'int',
                'team_a': 'int',
                'team_a_score': 'float',
                'team_h': 'int',
                'team_h_score': 'float',
                'team_h_difficulty': 'int',
                'team_a_difficulty': 'int',
                'pulse_id': 'int'
            }

            # Apply transformations
            for column, dtype in columns_to_transform.items():
                if column in df.columns:
                    if dtype == 'int':
                        df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
                    elif dtype == 'float':
                        df[column] = pd.to_numeric(df[column], errors='coerce')
                    elif dtype == 'datetime':
                        df[column] = pd.to_datetime(df[column], errors='coerce')

            # Convert boolean columns
            boolean_columns = ['finished', 'finished_provisional', 'started']
            for column in boolean_columns:
                if column in df.columns:
                    df[column] = df[column].astype(bool)

            # Determine the deduplication key
            if 'pulse_id' in df.columns and 'code' in df.columns:
                dedup_key = ['pulse_id', 'code']
            elif 'code' in df.columns:
                dedup_key = ['code']
            else:
                raise ValueError("Neither 'pulse_id' and 'code' nor 'code' alone found in the dataframe")

            # Remove duplicates based on the determined key
            df = df.drop_duplicates(subset=dedup_key, keep='last')
            
            # Remove 'stats' column if it exists
            if 'stats' in df.columns:
                df = df.drop(columns=['stats'])
            
            # add season column
            def determine_season(date):
                year = date.year
                if date.month >= 7:  # July or later
                    return f"{year}-{str(year + 1)[-2:]}"
                else:  # Before July
                    return f"{year - 1}-{str(year)[-2:]}"
            
            df['season'] = df['kickoff_time'].apply(determine_season)

            # rename columns
            df.rename(columns={
                'event': 'gameweek',
            }, inplace=True)
            
            return df
        except Exception as e:
            raise Exception(f"Error transforming data: {e}")
    
    def _create_table_if_not_exists(self, cursor, table_name: str):
        """
        Create the target table in PostgreSQL if it doesn't already exist.
        """
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                fixture_id SERIAL PRIMARY KEY,
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
                team_a_score FLOAT,
                team_h INTEGER,
                team_h_score FLOAT,
                team_h_difficulty INTEGER,
                team_a_difficulty INTEGER,
                pulse_id INTEGER
            );
        """
        query_postgres(cursor, create_table_query)
        print(f"Table '{table_name}' created or verified.")
    
    def ingest_data(self):
        """
        Main method to fetch, transform, deduplicate, and ingest data into PostgreSQL.
        """
        try:
            # Fetch and transform data
            df = self._initiate_data_ingestion()  # Fetch all data
            transformed_df = self._transform_and_dedupe_data(df)  # Transform and deduplicate data
            
            # Connect to PostgreSQL using your utility function
            cursor = connect_to_postgres(
                self.config.postgres_database, 
                self.config.postgres_host, 
                self.config.postgres_user, 
                self.config.postgres_password, 
                self.config.postgres_port
            )
            
            # Create table if it doesn't exist
            self._create_table_if_not_exists(cursor, self.config.postgres_table_name)
            
            # Perform full refresh: replace the entire table with new data
            engine = create_engine(f'postgresql://{self.config.postgres_user}:{self.config.postgres_password}@{self.config.postgres_host}:{self.config.postgres_port}/{self.config.postgres_database}')
            transformed_df.to_sql(self.config.postgres_table_name, engine, if_exists='replace', index=False)
            print(f"Data successfully ingested into '{self.config.postgres_table_name}' table with a full refresh.")
            
            # Close the connection
            cursor.connection.close()
        
        except Exception as e:
            raise Exception(f"Error during data ingestion: {e}")

if __name__ == "__main__":
    obj = DataIngestion()
    obj.ingest_data()