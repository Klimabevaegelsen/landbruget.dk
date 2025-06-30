FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libgdal-dev \
    gdal-bin \
    libspatialindex-dev \
    libproj-dev \
    libgeos-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Create app directory
WORKDIR /app

# Install uv
RUN pip install --no-cache-dir uv

# Copy requirements and install dependencies
COPY pyproject.toml ./
RUN uv pip install --system --no-cache -e .

# Copy application code
COPY . .

# Create data directory
RUN mkdir -p /app/data

# Default command
CMD ["python", "main.py", "--help"] 