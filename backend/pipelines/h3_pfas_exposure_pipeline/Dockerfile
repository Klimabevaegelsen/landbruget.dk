# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Install common system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    # System build tools
    build-essential \
    gcc \
    # Spatial libraries for DuckDB spatial extension
    libgeos-dev \
    libproj-dev \
    libgdal-dev \
    # Git for potential dependencies
    git \
    # Curl for health checks
    curl \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install uv
RUN pip install --no-cache-dir --upgrade pip uv

# Set working directory
WORKDIR /app

# Copy the project files
COPY pyproject.toml ./
COPY src/ ./src/
COPY main.py ./
COPY README.md ./

# Install the project with dependencies
# Use production extras if ENVIRONMENT is set to production
ARG ENVIRONMENT=container
RUN if [ "$ENVIRONMENT" = "production" ]; then \
    uv pip install --system --no-cache -e ".[production]"; \
    else \
    uv pip install --system --no-cache -e .; \
    fi

# Create output directories
RUN mkdir -p /data/gold/h3_pfas_exposure

# Set environment variables
ENV ENVIRONMENT=container
ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.path.insert(0, '/app/src'); from h3_pfas_exposure.config import H3PFASConfig; print('OK')" || exit 1

# Command to run the application
CMD ["python", "main.py"] 