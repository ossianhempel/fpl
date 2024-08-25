from src.utils import upload_to_minio, connect_to_minio
from dotenv import load_dotenv
import os

load_dotenv()

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET = os.getenv('MINIO_SECRET_KEY')

client = connect_to_minio(endpoint=MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET)
upload_to_minio(client=client, file_path="merged_gw_24_25.csv", destination_bucket="gameweeks")