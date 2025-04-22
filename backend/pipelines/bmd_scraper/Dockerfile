# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Install common system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    # System build tools
    build-essential \
    gcc \
    # Add other common system dependencies here if identified later
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install uv using pip
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir uv

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the project code
COPY . .

# Create output directories
RUN mkdir -p /data/bronze/bmd /data/silver/bmd

# Set environment variable to indicate container environment
ENV ENVIRONMENT=container

# Command to run the application
CMD ["python", "main.py"] 