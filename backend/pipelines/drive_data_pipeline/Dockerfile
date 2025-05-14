FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    build-essential \
    libopenjp2-7 \
    libjpeg-dev \
    zlib1g-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Java (required for tabula-py)
RUN apt-get update && apt-get install -y openjdk-11-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy the project files
COPY . /app/

# Install the package in development mode
RUN pip install --no-cache-dir -e .

# Create directories for data
RUN mkdir -p /app/data/bronze /app/data/silver

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the pipeline
ENTRYPOINT ["python", "main.py"] 