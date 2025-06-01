# Use an official OSGeo GDAL base image which includes necessary libraries
FROM ghcr.io/osgeo/gdal:ubuntu-small-latest

# Explicitly set WORKDIR
WORKDIR /usr/src/app

# Install build prerequisites and uv
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    curl \
    gcc \
    python3-pip \
    pkg-config \
    && \
    # Install uv using the standalone installer
    curl -LsSf https://astral.sh/uv/install.sh | sh && \
    # Move uv to /usr/local/bin
    mv /root/.local/bin/uv /usr/local/bin/uv && \
    # Clean up
    rm -rf /root/.local && \
    apt-get remove -y curl --autoremove && apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy the entire project context first
# This includes pyproject.toml, README.md, bronze/, silver/, etc.
COPY . /usr/src/app

# Create a virtual environment and install dependencies into it
ENV VENV_PATH=/opt/venv
RUN uv venv ${VENV_PATH} && \
    . ${VENV_PATH}/bin/activate && \
    uv pip install -e .

# Define the command to run this pipeline's main script (can be overridden in compose)
# Ensure the command uses the venv python
CMD ["/opt/venv/bin/python", "main.py"]

# Optional: Add EXPOSE or ENV specific to this pipeline if needed
# EXPOSE 80
# ENV PIPELINE_VAR value
