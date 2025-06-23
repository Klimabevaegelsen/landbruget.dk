FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv for faster package installation
RUN pip install uv

# Copy project configuration and install Python dependencies
COPY pyproject.toml ./
RUN uv pip install --system . --no-cache

# Copy application code
COPY . .

# Create output directories
RUN mkdir -p /app/silver /app/bronze

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["python", "main.py", "--help"] 