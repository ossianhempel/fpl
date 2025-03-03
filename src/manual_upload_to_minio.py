from src.utils.minio_utils import create_minio_client, upload_to_minio
from dotenv import load_dotenv
import os

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "ossian")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "password")


FILE_PATH = "src/data/fixtures_24_25.csv"
DESTINATION_BUCKET = "fixtures"

assert FILE_PATH is not None, "FILE_PATH is not set"
assert DESTINATION_BUCKET is not None, "DESTINATION_BUCKET is not set"

client = create_minio_client(
    endpoint=MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET
)

if client is None:
    raise ValueError("Failed to create MinIO client")

upload_to_minio(
    client=client, file_path=FILE_PATH, destination_bucket=DESTINATION_BUCKET
)
