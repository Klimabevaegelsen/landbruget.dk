# Use the common pipeline base image built locally or from a registry
# Make sure to build and tag the base image first, e.g., as landbruget-pipeline-base:latest
FROM landbruget-pipeline-base:latest

# The WORKDIR /app is inherited from the base image, but setting it explicitly is fine too
WORKDIR /app

# Copy the pyproject.toml file specific to this pipeline
# Copy uv.lock as well if you use it for locked dependencies
COPY ./pyproject.toml /app/pyproject.toml
# COPY ./uv.lock /app/uv.lock # Uncomment if using uv.lock

# Install Python packages for this pipeline using uv (installed in base image)
# Installing into the system Python environment provided by the base image
RUN uv pip install --system --no-cache -p /usr/local/bin/python -r /app/pyproject.toml
# RUN uv pip install --system --no-cache --locked -p /usr/local/bin/python -r /app/pyproject.toml # Uncomment if using uv.lock

# Copy the rest of the pipeline's application code
COPY . /app

# Create directory for raw data (specific to svineflytning pipeline)
RUN mkdir -p /data/raw/svineflytning

# Set environment variables (specific to svineflytning pipeline)
ENV DATA_DIR=/data \
    PYTHONUNBUFFERED=1

# Define the command to run this pipeline's main script
CMD ["python", "main.py"]
