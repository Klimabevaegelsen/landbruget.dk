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

# Copy the project code
COPY . .

# Install the project with dependencies
# Use production extras if ENVIRONMENT is set to production
ARG ENVIRONMENT=container
RUN if [ "$ENVIRONMENT" = "production" ]; then \
    pip install --no-cache-dir -e ".[production]"; \
    else \
    pip install --no-cache-dir -e .; \
    fi

# Create output directories

# Set environment variable to indicate container environment
ENV ENVIRONMENT=container

# Command to run the application
CMD ["python", "main.py"] 