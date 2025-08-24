#!/bin/bash

# Upload PMTiles to Cloudflare R2 bucket: pesticidkortet
# Creates a pmtiles/ folder structure for organized storage

set -e  # Exit on error

BUCKET_NAME="pesticidkortet"
PMTILES_DIR="data_cache"
R2_PREFIX="pmtiles"

echo "🚀 Uploading PMTiles to Cloudflare R2..."
echo "   Bucket: ${BUCKET_NAME}"
echo "   Prefix: ${R2_PREFIX}/"
echo ""

# Check if PMTiles files exist
if [ ! -d "$PMTILES_DIR" ]; then
    echo "❌ Error: PMTiles directory not found: $PMTILES_DIR"
    exit 1
fi

# Upload Field Analysis PMTiles (907MB)
if [ -f "$PMTILES_DIR/field_analysis_2024.pmtiles" ]; then
    echo "📊 Uploading field analysis data (907MB)..."
    wrangler r2 object put "$BUCKET_NAME/$R2_PREFIX/field_analysis_2024.pmtiles" \
        --file "$PMTILES_DIR/field_analysis_2024.pmtiles" \
        --content-type "application/octet-stream"
    echo "   ✅ field_analysis_2024.pmtiles uploaded"
else
    echo "   ⚠️  field_analysis_2024.pmtiles not found"
fi

# Upload BNBO PMTiles (7.1MB)
if [ -f "$PMTILES_DIR/bnbo_all_2024.pmtiles" ]; then
    echo "🌱 Uploading BNBO environmental areas (7.1MB)..."
    wrangler r2 object put "$BUCKET_NAME/$R2_PREFIX/bnbo_all_2024.pmtiles" \
        --file "$PMTILES_DIR/bnbo_all_2024.pmtiles" \
        --content-type "application/octet-stream"
    echo "   ✅ bnbo_all_2024.pmtiles uploaded"
else
    echo "   ⚠️  bnbo_all_2024.pmtiles not found"
fi

# Upload Wetlands PMTiles (518MB)
if [ -f "$PMTILES_DIR/wetlands_all_2024.pmtiles" ]; then
    echo "💧 Uploading wetlands areas (518MB)..."
    wrangler r2 object put "$BUCKET_NAME/$R2_PREFIX/wetlands_all_2024.pmtiles" \
        --file "$PMTILES_DIR/wetlands_all_2024.pmtiles" \
        --content-type "application/octet-stream"
    echo "   ✅ wetlands_all_2024.pmtiles uploaded"
else
    echo "   ⚠️  wetlands_all_2024.pmtiles not found"
fi

# Upload Water Projects PMTiles (13.4MB)
if [ -f "$PMTILES_DIR/water_projects_2024.pmtiles" ]; then
    echo "🌊 Uploading water projects (13.4MB)..."
    wrangler r2 object put "$BUCKET_NAME/$R2_PREFIX/water_projects_2024.pmtiles" \
        --file "$PMTILES_DIR/water_projects_2024.pmtiles" \
        --content-type "application/octet-stream"
    echo "   ✅ water_projects_2024.pmtiles uploaded"
else
    echo "   ⚠️  water_projects_2024.pmtiles not found"
fi

# Upload metadata files
echo "📋 Uploading metadata files..."
for metadata_file in "$PMTILES_DIR"/*.json; do
    if [ -f "$metadata_file" ]; then
        filename=$(basename "$metadata_file")
        wrangler r2 object put "$BUCKET_NAME/$R2_PREFIX/metadata/$filename" \
            --file "$metadata_file" \
            --content-type "application/json"
        echo "   ✅ metadata/$filename uploaded"
    fi
done

echo ""
echo "🎉 PMTiles upload complete!"
echo ""
echo "📡 Your PMTiles are now available at:"
echo "   https://a5f130bfd0d34de38f8e77f6a0f40a27.r2.cloudflarestorage.com/pesticidkortet/pmtiles/field_analysis_2024.pmtiles"
echo "   https://a5f130bfd0d34de38f8e77f6a0f40a27.r2.cloudflarestorage.com/pesticidkortet/pmtiles/bnbo_all_2024.pmtiles"
echo "   https://a5f130bfd0d34de38f8e77f6a0f40a27.r2.cloudflarestorage.com/pesticidkortet/pmtiles/wetlands_all_2024.pmtiles"
echo "   https://a5f130bfd0d34de38f8e77f6a0f40a27.r2.cloudflarestorage.com/pesticidkortet/pmtiles/water_projects_2024.pmtiles"
echo ""
echo "🔗 Next steps:"
echo "   1. Update frontend .env.local with these URLs"
echo "   2. Test the /markanalyse page"
echo "   3. Set up custom domain (optional): pmtiles.landbruget.dk"