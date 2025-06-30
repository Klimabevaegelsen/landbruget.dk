# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Install common system dependencies required by many Python pipelines
# Keep this minimal and add dependencies only if used by multiple pipelines
RUN apt-get update && apt-get install -y --no-install-recommends \
    # System build tools
    build-essential \
    gcc \
    # Add other common system dependencies here if identified later
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install uv using pip
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir uv

# Set a default working directory (can be overridden by downstream images if needed)
WORKDIR /app

# Consider adding common environment variables or users if applicable across pipelines
