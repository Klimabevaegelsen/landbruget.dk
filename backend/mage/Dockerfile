FROM mageai/mageai:latest

# Install Python packages for geospatial processing
RUN pip install --no-cache-dir \
    geopandas>=1.0.1 \
    shapely>=2.0.7 \
    dask-geopandas>=0.4.2 \
    libpysal>=4.12.1 \
    pyogrio>=0.8.0 \
    && rm -rf /root/.cache/pip/*

# Keep the default entrypoint from the base image
ENTRYPOINT ["/bin/sh", "-c", "/app/run_app.sh"] 