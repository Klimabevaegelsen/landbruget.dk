#!/bin/bash

# Set the development environment variables
# This script should ONLY be used in local development environments
export MAGE_ENVIRONMENT=dev
export MAGE_DEV_MODE=true

# Log that we're in development mode
echo "Set environment to DEVELOPMENT mode - use only for local development!"

# Execute any provided arguments
exec "$@" 