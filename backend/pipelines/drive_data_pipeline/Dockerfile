FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    build-essential \
    libopenjp2-7 \
    libjpeg-dev \
    zlib1g-dev \
    libproj-dev \
    libgeos-dev \
    gdal-bin \
    libgdal-dev \
    tesseract-ocr \
    poppler-utils \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Java (required for tabula-py)
RUN apt-get update && apt-get install -y openjdk-11-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.cargo/bin:$PATH"

# Set environment variables for GDAL
ENV GDAL_CONFIG=/usr/bin/gdal-config
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Copy the project files
COPY . /app/

# Install the package using uv
RUN uv pip install --system -e .

# Create directories for data
RUN mkdir -p /app/data/bronze /app/data/silver

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the pipeline
ENTRYPOINT ["python", "main.py"] 