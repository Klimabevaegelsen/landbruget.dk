# H3 PFAS Visualization Frontend - Implementation Guide

## Overview

This guide provides step-by-step instructions for implementing the H3 PFAS visualization frontend that connects to data stored in GCS and serves it through Supabase.

## Architecture

```
GCS (Data Storage)
    ↓ (Data Sync)
Supabase (Spatial Database)
    ↓ (API)
Next.js Frontend (Visualization)
```

### Data Flow
1. **H3 Pipeline** generates PFAS exposure data → **GCS** (parquet files)
2. **Sync Script** loads data from GCS → **Supabase** (PostGIS database)
3. **Frontend** queries Supabase → **Interactive Map** (Kepler.gl + React)

## Prerequisites

- Node.js 18+ and npm/yarn
- Python 3.9+ with pip
- Google Cloud SDK (for GCS access)
- Supabase account
- Access to the `landbrugsdata-raw-data` GCS bucket

## Step 1: Set Up Supabase Project

### 1.1 Create Supabase Project
1. Go to [supabase.com](https://supabase.com)
2. Create a new project
3. Note down your project URL and anon key

### 1.2 Enable PostGIS Extension
1. Go to SQL Editor in Supabase Dashboard
2. Run: `CREATE EXTENSION IF NOT EXISTS postgis;`

### 1.3 Set Up Database Schema
```bash
# Install dependencies
pip install supabase loguru

# Run database setup
python scripts/analysis/setup_supabase_h3_db.py \
  --supabase-url "YOUR_SUPABASE_URL" \
  --supabase-key "YOUR_SUPABASE_SERVICE_KEY"
```

## Step 2: Sync Data from GCS to Supabase

### 2.1 Install Python Dependencies
```bash
pip install pandas pyarrow supabase loguru numpy
```

### 2.2 Sync H3 Data (Using DuckDB H3 Extension)
```bash
# Install DuckDB (if not already installed)
pip install duckdb

# Sync specific years with proper H3 geometries
python scripts/analysis/sync_h3_data_to_supabase_duckdb.py \
  --supabase-url "YOUR_SUPABASE_URL" \
  --supabase-key "YOUR_SUPABASE_SERVICE_KEY" \
  --years 2022 2023

# Or sync all available years
python scripts/analysis/sync_h3_data_to_supabase_duckdb.py \
  --supabase-url "YOUR_SUPABASE_URL" \
  --supabase-key "YOUR_SUPABASE_SERVICE_KEY" \
  --all-years
```

**Note:** This will process ~1.9M records per year. Each year takes approximately 10-15 minutes to sync.

## Step 3: Configure Frontend

### 3.1 Install Frontend Dependencies
```bash
cd frontend-pesticide
npm install
```

### 3.2 Configure Environment Variables
```bash
# Copy example environment file
cp env.example .env.local

# Edit .env.local with your Supabase credentials
NEXT_PUBLIC_SUPABASE_URL=your_supabase_url_here
NEXT_PUBLIC_SUPABASE_ANON_KEY=your_supabase_anon_key_here
```

### 3.3 Install Required Packages
The frontend uses these key packages:
- **Next.js 15** with React 19
- **Kepler.gl** for map visualization
- **Supabase** for database connection
- **Tailwind CSS** for styling
- **Zustand** for state management

```bash
# Install additional packages if needed
npm install @supabase/supabase-js kepler.gl deck.gl zustand
```

## Step 4: Run the Application

### 4.1 Start Development Server
```bash
cd frontend-pesticide
npm run dev
```

### 4.2 Access the Application
Open [http://localhost:3000](http://localhost:3000) in your browser.

## Step 5: Verify Implementation

### 5.1 Check Database Connection
```bash
# Verify Supabase setup
python scripts/analysis/setup_supabase_h3_db.py \
  --supabase-url "YOUR_SUPABASE_URL" \
  --supabase-key "YOUR_SUPABASE_SERVICE_KEY" \
  --verify-only
```

### 5.2 Test API Endpoints
```bash
# Test H3 data API
curl "http://localhost:3000/api/h3-data?year=2023&bbox=8.0,54.5,15.2,57.8"

# Test BNBO data API
curl "http://localhost:3000/api/bnbo-data"

# Test BBR data API
curl "http://localhost:3000/api/bbr-data"
```

### 5.3 Check Map Visualization
1. Open the frontend in your browser
2. Verify that the map loads with Denmark in view
3. Check that H3 hexagons are visible
4. Test the pesticide/PFAS toggle
5. Verify hover tooltips show correct data

## Data Structure

### H3 PFAS Exposure Data
Each H3 hexagon contains:
- **Spatial**: H3 ID, center coordinates, geometry
- **Agricultural**: Field count, crop diversity, area coverage
- **PFAS**: Total PFAS-containing active ingredients (grams)
- **Pesticide**: Total pesticide load, application count
- **Temporal**: Year, created timestamp

### API Response Format
```json
{
  "data": [
    {
      "h3_id": "621665140420411391",
      "year": 2023,
      "total_pesticide_load": 24.067,
      "total_pfas_grams": 0.0,
      "pesticide_application_count": 8,
      "field_count": 1,
      "agricultural_area_ha": 0.488,
      "avg_field_coverage": 0.414,
      "geometry": { "type": "Polygon", "coordinates": [...] },
      "centroid_lon": 8.158,
      "centroid_lat": 56.457,
      "h3_resolution": 10
    }
  ],
  "metadata": {
    "year": 2023,
    "totalRecords": 1877424,
    "fetchDuration": 1250
  }
}
```

## Performance Optimization

### Database Optimization
- **Spatial indexes** on geometry columns
- **Composite indexes** on year + h3_id
- **Viewport filtering** using PostGIS bounding box queries
- **Data pagination** with limit/offset

### Frontend Optimization
- **Viewport-based loading** - Only load visible hexagons
- **Data caching** with React Query
- **Progressive loading** as users pan/zoom
- **Streaming responses** for large datasets

### Recommended Limits
- **H3 hexagons**: 10,000 per request
- **BNBO polygons**: 1,000 per request
- **BBR buildings**: 5,000 per request

## Troubleshooting

### Common Issues

#### 1. Database Connection Failed
```bash
# Check Supabase credentials
python -c "
from supabase import create_client
client = create_client('YOUR_URL', 'YOUR_KEY')
print('Connection successful!')
"
```

#### 2. No Data Visible on Map
- Check if data sync completed successfully
- Verify API endpoints return data
- Check browser console for JavaScript errors
- Ensure PostGIS extension is enabled

#### 3. Slow Map Performance
- Reduce viewport data limits in environment variables
- Check network tab for slow API responses
- Verify spatial indexes are created
- Consider implementing data virtualization

#### 4. GCS Access Issues
```bash
# Check GCS authentication
gsutil ls gs://landbrugsdata-raw-data/gold/h3_pesticide_2023/

# If authentication fails, run:
gcloud auth application-default login
```

### Debug Mode
Enable debug logging:
```bash
# Backend
export LOG_LEVEL=DEBUG

# Frontend
NEXT_PUBLIC_DEBUG=true npm run dev
```

## Data Updates

### Automated Sync
For production, set up automated data sync:

```bash
# Create cron job for weekly sync
0 2 * * 0 /path/to/sync_h3_data_to_supabase.py --all-years
```

### Manual Sync
```bash
# Sync new data after pipeline runs (with proper H3 geometries)
python scripts/analysis/sync_h3_data_to_supabase_duckdb.py \
  --years 2024 \
  --supabase-url "YOUR_URL" \
  --supabase-key "YOUR_KEY"
```

## Security Considerations

### Environment Variables
- Never commit `.env.local` to version control
- Use Supabase service key only for server-side operations
- Use anon key for client-side operations

### Database Security
- Enable Row Level Security (RLS) in Supabase
- Create appropriate policies for data access
- Use read-only access for frontend

### API Security
- Implement rate limiting
- Add request validation
- Use CORS appropriately

## Deployment

### Vercel Deployment
```bash
# Install Vercel CLI
npm i -g vercel

# Deploy
vercel --prod
```

### Environment Variables for Production
Set these in your deployment platform:
- `NEXT_PUBLIC_SUPABASE_URL`
- `NEXT_PUBLIC_SUPABASE_ANON_KEY`
- `SUPABASE_SERVICE_KEY`

## Monitoring

### Database Monitoring
- Monitor query performance in Supabase dashboard
- Set up alerts for slow queries
- Track API response times

### Frontend Monitoring
- Use browser dev tools for performance profiling
- Monitor bundle size and loading times
- Track user interactions and errors

## Next Steps

### Enhanced Features
1. **Temporal Analysis**: Implement year-over-year comparisons
2. **Advanced Filtering**: Add crop type and pesticide filters
3. **Export Functionality**: Allow data export in various formats
4. **Mobile Optimization**: Improve mobile map experience
5. **Real-time Updates**: Implement live data updates

### Advanced Visualizations
1. **3D Visualization**: Add elevation-based PFAS visualization
2. **Heatmap Overlays**: Implement smooth heatmap rendering
3. **Animation**: Add temporal animation controls
4. **Clustering**: Implement H3 hexagon clustering at different zoom levels

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review the logs for error messages
3. Verify all prerequisites are met
4. Check the data sync status

## File Structure

```
frontend-pesticide/
├── src/
│   ├── app/
│   │   ├── api/
│   │   │   ├── h3-data/route.ts      # H3 data API
│   │   │   ├── bnbo-data/route.ts    # BNBO data API
│   │   │   └── bbr-data/route.ts     # BBR data API
│   │   ├── page.tsx                  # Main page
│   │   └── layout.tsx                # Root layout
│   ├── components/
│   │   ├── map/                      # Map components
│   │   ├── overlays/                 # UI overlays
│   │   └── ui/                       # UI components
│   ├── lib/
│   │   ├── supabase.ts              # Supabase client
│   │   ├── data-processing.ts        # Data processing
│   │   └── data-virtualization.ts   # Data virtualization
│   ├── stores/                       # State management
│   ├── types/                        # TypeScript types
│   └── hooks/                        # Custom hooks
├── database-schema.sql               # Database schema
├── env.example                       # Environment template
└── README-IMPLEMENTATION.md         # This file

scripts/analysis/
├── setup_supabase_h3_db.py              # Database setup
├── sync_h3_data_to_supabase.py          # Data sync (basic version)
└── sync_h3_data_to_supabase_duckdb.py   # Data sync (with DuckDB H3 extension)
```

This implementation provides a solid foundation for the H3 PFAS visualization frontend with proper data architecture, performance optimization, and scalability considerations. 