FROM mageai/mageai:latest

# Install Python packages for geospatial processing
# These are essential for the wetlands_merge_grid implementation
RUN pip install --no-cache-dir \
    shapely>=2.0.7 \
    geopandas>=1.0.1 \
    dask-geopandas>=0.4.2 \
    libpysal>=4.12.1 \
    pyogrio>=0.8.0 \
    # Use a pre-built GDAL wheel instead of system packages
    GDAL>=3.6.2 \
    && rm -rf /root/.cache/pip/*

# You can add more dependencies here as needed
# RUN pip install package1 package2 package3

# Keep the default entrypoint from the base image
ENTRYPOINT ["/bin/sh", "-c", "/app/run_app.sh"] 