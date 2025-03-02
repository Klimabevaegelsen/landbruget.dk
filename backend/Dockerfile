FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libspatialindex-dev \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Create and set working directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY mage/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY mage/ .

# Command to run the application
CMD ["python", "app.py"]
