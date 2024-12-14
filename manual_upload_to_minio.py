from src.utils import upload_to_minio, connect_to_minio
from dotenv import load_dotenv
import os

load_dotenv()

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET = os.getenv('MINIO_SECRET_KEY')


FILE_PATH = "src/data/fixtures_24_25.csv"
DESTINATION_BUCKET = "fixtures"

assert FILE_PATH is not None, "FILE_PATH is not set"
assert DESTINATION_BUCKET is not None, "DESTINATION_BUCKET is not set"

client = connect_to_minio(endpoint=MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET)
upload_to_minio(client=client, file_path=FILE_PATH, destination_bucket=DESTINATION_BUCKET)