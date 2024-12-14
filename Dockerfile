FROM python:3.11-slim

WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the entire src directory
COPY src/ ./src/

# Set Python path to include src directory
ENV PYTHONPATH=/app

# Run the FastAPI application
CMD ["uvicorn", "src.fastapi.api:app", "--host", "0.0.0.0", "--port", "8000"] 