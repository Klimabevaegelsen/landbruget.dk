FROM mageai/mageai:latest

# Install GDAL dependencies
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
    gdal-bin \
    libgdal-dev \
    libspatialindex-dev \
    proj-bin \
    && rm -rf /var/lib/apt/lists/*

# Set GDAL environment variables
ENV GDAL_VERSION=3.6.2
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Install Python packages for geospatial processing
# These are essential for the wetlands_merge_grid implementation
RUN pip install --no-cache-dir \
    shapely==2.0.7 \
    geopandas==1.0.1 \
    dask[dataframe]==2024.3.0 \
    dask-geopandas==0.4.2 \
    pyarrow==15.0.0 \
    libpysal==4.12.1 \
    distributed>=2024.6.0 \
    pyogrio>=0.8.0 \
    psutil>=5.9.0 \
    && rm -rf /root/.cache/pip/*

# You can add more dependencies here as needed
# RUN pip install package1 package2 package3

# Keep the default entrypoint from the base image
ENTRYPOINT ["/bin/sh", "-c", "/app/run_app.sh"] 