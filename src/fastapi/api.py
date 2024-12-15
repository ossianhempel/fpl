import os
from fastapi import FastAPI, HTTPException, UploadFile, File
from pydantic import BaseModel
from enum import Enum
import tempfile
import sys
from pathlib import Path

# Add the project's root directory to the PYTHONPATH
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

from src.utils import upload_to_minio, connect_to_minio
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
    with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename)[1]) as temp_file:
        content = await file.read()
        temp_file.write(content)
        return temp_file.name

def get_object_name(source: DataSource, original_filename: str) -> str:
    """Determine the object name for storage"""
    base_name = UPLOAD_BUCKETS.get(source.value)
    return original_filename if not base_name else f"{base_name}{os.path.splitext(original_filename)[1]}"

def get_ingestion_class(source: DataSource):
    """Get the appropriate ingestion class based on the source"""
    ingestion_classes = {
        DataSource.FIXTURES: FixturesIngestion,
        DataSource.GAMEWEEKS: GameweeksIngestion
    }
    return ingestion_classes.get(source)

# API Endpoints
@app.post("/upload/{source}")
async def upload_data(source: DataSource, file: UploadFile = File(...)):
    """Upload file to the appropriate storage bucket"""
    try:
        # Save uploaded file temporarily
        temp_file_path = await save_upload_file(file)
        
        # Get MinIO client with required credentials
        client = connect_to_minio(
            endpoint=os.getenv("MINIO_ENDPOINT"),
            access_key=os.getenv("MINIO_ACCESS_KEY"),
            secret_key=os.getenv("MINIO_SECRET_KEY")
        )
        
        if client is None:
            raise HTTPException(status_code=500, detail="Failed to connect to MinIO")
        
        # Determine bucket and object name
        bucket_name = source.value
        object_name = get_object_name(source, file.filename)
        
        # Upload to MinIO with the client
        upload_to_minio(client, temp_file_path, bucket_name, object_name)
        
        # Cleanup the temporary file
        os.unlink(temp_file_path)
        
        return {
            "message": f"Successfully uploaded {object_name} to {bucket_name} bucket"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@app.post("/ingest/{source}")
async def ingest_data(source: DataSource, request: IngestionRequest):
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
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)