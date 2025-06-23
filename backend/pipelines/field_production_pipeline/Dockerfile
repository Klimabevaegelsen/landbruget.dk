FROM python:3.11-slim

# Install system dependencies for geospatial libraries
RUN apt-get update && apt-get install -y \
    gdal-bin \
    libgdal-dev \
    libspatialindex-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install uv for faster package installation
RUN pip install uv

# Set environment variables for GDAL
ENV GDAL_CONFIG=/usr/bin/gdal-config
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install Python dependencies using uv
RUN uv pip install --system -r requirements.txt --no-cache

# Copy the pipeline code
COPY . .

# Set Python path to include parent directories for imports
ENV PYTHONPATH="/app:/app/backend:/app/backend/pipelines"

# Command to run the application
CMD ["python", "main.py"] 