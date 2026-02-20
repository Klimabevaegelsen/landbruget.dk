#!/bin/bash
# Upload Parquet files to Cloudflare R2 bucket
# Usage: ./scripts/upload-to-r2.sh [data-directory]

set -euo pipefail

# Configuration
BUCKET="${R2_BUCKET:-landbruget-data-explorer}"
DATA_DIR="${1:-./backend/data}"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if wrangler is installed
if ! command -v wrangler &> /dev/null; then
    echo -e "${RED}Error: wrangler CLI not found${NC}"
    echo "Install with: npm install -g wrangler"
    exit 1
fi

# Check if data directory exists
if [ ! -d "$DATA_DIR" ]; then
    echo -e "${RED}Error: Data directory not found: $DATA_DIR${NC}"
    exit 1
fi

echo -e "${BLUE}╔════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  Cloudflare R2 Upload Tool            ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════╝${NC}"
echo
echo -e "Bucket: ${GREEN}$BUCKET${NC}"
echo -e "Source: ${GREEN}$DATA_DIR${NC}"
echo

# Function to upload files for a specific layer
upload_layer() {
    local layer=$1
    local layer_dir="$DATA_DIR/$layer"

    if [ ! -d "$layer_dir" ]; then
        echo -e "${YELLOW}⚠ Skipping $layer (directory not found)${NC}"
        return
    fi

    echo -e "${BLUE}━━━ Processing $layer layer ━━━${NC}"

    # Count files
    local file_count=$(find "$layer_dir" -name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')

    if [ "$file_count" -eq 0 ]; then
        echo -e "${YELLOW}⚠ No Parquet files found${NC}"
        return
    fi

    echo "Found $file_count Parquet file(s)"
    echo

    # Upload each file
    find "$layer_dir" -name "*.parquet" | while read -r file; do
        local filename=$(basename "$file")
        local key="${layer}/${filename}"
        local filesize=$(du -h "$file" | cut -f1)

        echo -e "  Uploading: ${GREEN}$filename${NC} (${filesize})"

        if wrangler r2 object put "${BUCKET}/${key}" \
            --file="$file" \
            --content-type=application/octet-stream \
            2>&1 | grep -q "error"; then
            echo -e "  ${RED}✗ Failed${NC}"
        else
            echo -e "  ${GREEN}✓ Success${NC}"
        fi

        echo
    done
}

# Upload Bronze, Silver, and Gold layers
upload_layer "bronze"
upload_layer "silver"
upload_layer "gold"

# Verify uploads
echo -e "${BLUE}━━━ Verifying uploads ━━━${NC}"
echo

for layer in bronze silver gold; do
    echo -e "Listing ${GREEN}$layer${NC} layer:"
    wrangler r2 object list "$BUCKET" --prefix="${layer}/" | head -20
    echo
done

echo -e "${GREEN}╔════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║  Upload complete!                      ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════╝${NC}"
echo
echo "Files are now available at:"
echo -e "  ${BLUE}https://r2.landbruget.dk/${layer}/${filename}${NC}"
echo
echo "Next steps:"
echo "  1. Verify CORS configuration: wrangler r2 bucket cors get $BUCKET"
echo "  2. Test file access in browser"
echo "  3. Update NEXT_PUBLIC_R2_URL in Vercel if needed"
