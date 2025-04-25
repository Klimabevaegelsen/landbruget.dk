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

# Copy only necessary files for dependency installation first to leverage Docker cache
COPY pyproject.toml ./

# Install dependencies using uv
RUN uv pip install --system . --no-cache

# Copy the rest of the application code
COPY . .

# Command to run the application
CMD ["python", "main.py"]