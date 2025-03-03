FROM mageai/mageai:latest

# Define build arguments with development defaults (for local builds)
ARG MAGE_ENVIRONMENT=dev
ARG MAGE_DEV_MODE=true

# Install Python packages for geospatial processing
RUN pip install --no-cache-dir \
    geopandas>=1.0.1 \
    shapely>=2.0.7 \
    dask-geopandas>=0.4.2 \
    libpysal>=4.12.1 \
    pyogrio>=0.8.0 \
    && rm -rf /root/.cache/pip/*

# Make sure we start with a clean default repo
RUN rm -rf /home/src/default_repo
RUN mkdir -p /home/src/default_repo

# Copy the Mage project files - preserve directory structure
COPY backend/mage/ /home/src/default_repo/

# Ensure correct permissions for Mage to read files
RUN chmod -R 755 /home/src/default_repo/ && \
    chown -R root:root /home/src/default_repo/ && \
    # Make sure pipelines directory exists with proper permissions
    mkdir -p /home/src/default_repo/pipelines && \
    chmod -R 755 /home/src/default_repo/pipelines

# Set environment variables for pipeline discovery from build arguments
ENV MAGE_ENVIRONMENT=${MAGE_ENVIRONMENT}
ENV MAGE_DEV_MODE=${MAGE_DEV_MODE}

# List files to verify structure (for debugging)
RUN ls -la /home/src/default_repo/ && \
    ls -la /home/src/default_repo/pipelines/

# Keep the default entrypoint from the base image
ENTRYPOINT ["/bin/sh", "-c", "/app/run_app.sh"] 