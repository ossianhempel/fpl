import os
from fastapi import FastAPI, HTTPException, UploadFile, File
from pydantic import BaseModel
from enum import Enum
import tempfile
import sys
from pathlib import Path
from typing import Type, Optional

# Add the project's root directory to the PYTHONPATH
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

from src.utils.minio_utils import upload_to_minio, create_minio_client
from src.components.data_ingestion_fixtures import DataIngestion as FixturesIngestion
from src.components.data_ingestion_gameweeks import DataIngestion as GameweeksIngestion

app = FastAPI(title="Fantasy Premier League API")

# Configuration for bucket mapping
UPLOAD_BUCKETS = {
    "fixtures": "fixtures_24_25",
    "gameweeks": "merged_gw_24_25",
    "teams": None,  # Uses original filename
}

class DataSource(str, Enum):
    FIXTURES = "fixtures"
    GAMEWEEKS = "gameweeks"
    TEAMS = "teams"

class IngestionRequest(BaseModel):
    source: DataSource

# Helper functions
async def save_upload_file(file: UploadFile) -> str:
    """Save uploaded file to temporary location and return the path"""
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided")
    with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename)[1]) as temp_file:
        content = await file.read()
        temp_file.write(content)
        return temp_file.name

def get_object_name(source: DataSource, original_filename: str) -> str:
    """Determine the object name for storage"""
    base_name = UPLOAD_BUCKETS.get(source.value)
    return original_filename if not base_name else f"{base_name}{os.path.splitext(original_filename)[1]}"

def get_ingestion_class(source: DataSource) -> Optional[Type[FixturesIngestion | GameweeksIngestion]]:
    """Get the appropriate ingestion class based on the source"""
    ingestion_classes: dict[DataSource, Type[FixturesIngestion | GameweeksIngestion]] = {
        DataSource.FIXTURES: FixturesIngestion,
        DataSource.GAMEWEEKS: GameweeksIngestion
    }
    return ingestion_classes.get(source)

# API Endpoints
@app.post("/upload/{source}")
async def upload_data(source: DataSource, file: UploadFile = File(...)) -> dict[str, str]:
    """Upload file to the appropriate storage bucket"""
    temp_file_path: Optional[str] = None
    try:
        print(f"\nReceived file upload request:")  # Debug log
        print(f"Filename: {file.filename}")
        print(f"Content-Type: {file.content_type}")
        
        # Save uploaded file temporarily
        temp_file_path = await save_upload_file(file)
        print(f"Saved temporary file to: {temp_file_path}")  # Debug log
        print(f"Temporary file size: {os.path.getsize(temp_file_path)} bytes")  # Debug log
        
        # Get MinIO client with required credentials
        endpoint = os.getenv("MINIO_ENDPOINT")
        access_key = os.getenv("MINIO_ACCESS_KEY")
        secret_key = os.getenv("MINIO_SECRET_KEY")
        
        if not all([endpoint, access_key, secret_key]):
            raise HTTPException(
                status_code=500,
                detail="Missing MinIO credentials in environment variables"
            )
        
        client = create_minio_client(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key
        )
        
        if client is None:
            raise HTTPException(
                status_code=500,
                detail="Failed to establish MinIO connection"
            )
        
        # Determine bucket and object name
        bucket_name = source.value
        if not file.filename:
            raise HTTPException(status_code=400, detail="No filename provided")
        object_name = get_object_name(source, file.filename)
        
        print(f"\nFile processing details:")  # Debug log
        print(f"Source: {source.value}")
        print(f"Original filename: {file.filename}")
        print(f"Target bucket: {bucket_name}")
        print(f"Target object name: {object_name}")
        
        # Upload to MinIO with the client
        upload_to_minio(client=client, file_path=temp_file_path, destination_bucket=bucket_name, destination_folder_path=object_name)
        
        # Verify upload
        try:
            stat = client.stat_object(bucket_name, object_name)
            print(f"\nUpload verification:")  # Debug log
            print(f"Object exists in MinIO: {object_name}")
            print(f"Size in MinIO: {stat.size} bytes")
            print(f"Last modified: {stat.last_modified}")
        except Exception as e:
            print(f"Warning: Could not verify upload: {str(e)}")
        
        return {
            "message": f"Successfully uploaded {object_name} to {bucket_name} bucket"
        }
    except Exception as e:
        print(f"Upload error: {str(e)}")  # Debug log
        raise HTTPException(
            status_code=500,
            detail=f"Upload failed: {str(e)}"
        )
    finally:
        # Cleanup temporary file
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
                print(f"\nCleanup: Removed temporary file: {temp_file_path}")  # Debug log
            except Exception as e:
                print(f"Failed to cleanup temp file: {str(e)}")  # Debug log

@app.post("/ingest/{source}")
async def ingest_data(source: DataSource, request: IngestionRequest) -> dict[str, str]:
    """Ingest data using the appropriate ingestion class"""
    if source != request.source:
        raise HTTPException(status_code=400, detail="Source mismatch")
    
    # Get ingestion class first
    ingestion_class = get_ingestion_class(source)
    if not ingestion_class:
        raise HTTPException(status_code=400, detail=f"Ingestion not supported for source: {source.value}")
    
    try:
        # Initialize and run ingestion
        ingestion = ingestion_class()
        ingestion.ingest_data()
        
        return {"message": f"Successfully ingested data from {source.value}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {str(e)}")

@app.get("/health")
async def health_check() -> dict[str, str]:
    """Health check endpoint"""
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)