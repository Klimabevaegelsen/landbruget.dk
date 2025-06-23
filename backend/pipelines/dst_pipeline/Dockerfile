# Use the base image from our pipeline infrastructure
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install uv
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir uv

# Set the working directory in the container
WORKDIR /app

# Copy only necessary files for dependency installation first to leverage Docker cache
COPY pipelines/dst_pipeline/pyproject.toml pipelines/dst_pipeline/README.md ./

# Install dependencies using uv
# Using --system to install in the global Python environment within the container
RUN uv pip install --system . --no-cache

# Copy the common directory
COPY common ./common

# Copy the dst_pipeline code
COPY pipelines/dst_pipeline/ ./

# Command to run the application
CMD ["python", "main.py"] 