# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libspatialindex-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv for faster package installation
RUN pip install uv

# Set the working directory in the container
WORKDIR /app

# Copy all files first
COPY . .

# Install dependencies using uv
RUN uv pip install --system . --no-cache

# Command to run the application
CMD ["python", "main.py"]
