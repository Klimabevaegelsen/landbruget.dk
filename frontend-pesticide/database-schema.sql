-- H3 PFAS Visualization Database Schema
-- Supabase PostgreSQL with PostGIS extension

-- Enable PostGIS extension (if not already enabled)
CREATE EXTENSION IF NOT EXISTS postgis;

-- H3 PFAS Exposure table
CREATE TABLE IF NOT EXISTS h3_pfas_exposure (
    id BIGSERIAL PRIMARY KEY,
    h3_id BIGINT NOT NULL,
    year INTEGER NOT NULL,
    total_pesticide_load DOUBLE PRECISION,  -- Standardized to kg/ha equivalent
    total_pfas_grams DOUBLE PRECISION,      -- Actual PFAS active ingredient mass (grams)
    pesticide_application_count INTEGER,
    field_count INTEGER,
    agricultural_area_ha DOUBLE PRECISION,
    avg_field_coverage DOUBLE PRECISION,
    geometry GEOMETRY(POLYGON, 4326),       -- H3 hexagon geometry (from WKT)
    h3_centroid GEOMETRY(POINT, 4326),      -- H3 center point (from lat/lon)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- H3 metadata from pipeline
    h3_resolution INTEGER DEFAULT 10,       -- H3 resolution level
    
    -- Constraints
    CONSTRAINT unique_h3_year UNIQUE (h3_id, year),
    CONSTRAINT valid_year CHECK (year >= 2020 AND year <= 2030),
    CONSTRAINT valid_h3_resolution CHECK (h3_resolution >= 0 AND h3_resolution <= 15),
    CONSTRAINT positive_pesticide_load CHECK (total_pesticide_load >= 0),
    CONSTRAINT positive_pfas_grams CHECK (total_pfas_grams >= 0),
    CONSTRAINT positive_area CHECK (agricultural_area_ha >= 0),
    CONSTRAINT valid_coverage CHECK (avg_field_coverage >= 0 AND avg_field_coverage <= 1)
);

-- BNBO Status Areas table
CREATE TABLE IF NOT EXISTS bnbo_status_areas (
    id BIGSERIAL PRIMARY KEY,
    bnbo_id VARCHAR NOT NULL,
    status_code VARCHAR NOT NULL, -- 'protected', 'buffer', 'agricultural', etc.
    status_description TEXT,
    area_ha DOUBLE PRECISION,
    geometry GEOMETRY(MULTIPOLYGON, 4326),
    year INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT valid_bnbo_year CHECK (year >= 2020 AND year <= 2030),
    CONSTRAINT positive_bnbo_area CHECK (area_ha >= 0),
    CONSTRAINT valid_status_code CHECK (status_code IN ('protected', 'buffer', 'agricultural', 'transition', 'unprotected'))
);

-- BBR Buildings table
CREATE TABLE IF NOT EXISTS bbr_buildings (
    id BIGSERIAL PRIMARY KEY,
    bbr_id VARCHAR NOT NULL,
    building_code VARCHAR, -- BBR building type code
    building_type VARCHAR, -- Residential, Agricultural, Industrial, etc.
    construction_year INTEGER,
    floor_area DOUBLE PRECISION,
    geometry GEOMETRY(POINT, 4326), -- Building centroid
    address TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT valid_construction_year CHECK (construction_year >= 1800 AND construction_year <= EXTRACT(YEAR FROM NOW()) + 10),
    CONSTRAINT positive_floor_area CHECK (floor_area >= 0),
    CONSTRAINT valid_building_type CHECK (building_type IN ('Residential', 'Agricultural', 'Industrial', 'Commercial', 'Public', 'Other'))
);

-- ================================
-- INDEXES FOR PERFORMANCE
-- ================================

-- H3 PFAS Exposure indexes
CREATE INDEX IF NOT EXISTS idx_h3_pfas_geometry ON h3_pfas_exposure USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_h3_pfas_centroid ON h3_pfas_exposure USING GIST (h3_centroid);
CREATE INDEX IF NOT EXISTS idx_h3_pfas_year ON h3_pfas_exposure (year);
CREATE INDEX IF NOT EXISTS idx_h3_pfas_h3_id ON h3_pfas_exposure (h3_id);
CREATE INDEX IF NOT EXISTS idx_h3_pfas_resolution ON h3_pfas_exposure (h3_resolution);
CREATE INDEX IF NOT EXISTS idx_h3_pfas_pesticide_load ON h3_pfas_exposure (total_pesticide_load);
CREATE INDEX IF NOT EXISTS idx_h3_pfas_grams ON h3_pfas_exposure (total_pfas_grams);

-- Composite indexes for common queries
CREATE INDEX IF NOT EXISTS idx_h3_pfas_year_h3id ON h3_pfas_exposure (year, h3_id);
CREATE INDEX IF NOT EXISTS idx_h3_pfas_year_pesticide ON h3_pfas_exposure (year, total_pesticide_load);
CREATE INDEX IF NOT EXISTS idx_h3_pfas_year_pfas ON h3_pfas_exposure (year, total_pfas_grams);

-- BNBO Status Areas indexes
CREATE INDEX IF NOT EXISTS idx_bnbo_geometry ON bnbo_status_areas USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_bnbo_status ON bnbo_status_areas (status_code);
CREATE INDEX IF NOT EXISTS idx_bnbo_year ON bnbo_status_areas (year);
CREATE INDEX IF NOT EXISTS idx_bnbo_area ON bnbo_status_areas (area_ha);
CREATE INDEX IF NOT EXISTS idx_bnbo_id ON bnbo_status_areas (bnbo_id);

-- BBR Buildings indexes
CREATE INDEX IF NOT EXISTS idx_bbr_geometry ON bbr_buildings USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_bbr_type ON bbr_buildings (building_type);
CREATE INDEX IF NOT EXISTS idx_bbr_construction_year ON bbr_buildings (construction_year);
CREATE INDEX IF NOT EXISTS idx_bbr_floor_area ON bbr_buildings (floor_area);
CREATE INDEX IF NOT EXISTS idx_bbr_id ON bbr_buildings (bbr_id);

-- ================================
-- VIEWS FOR COMMON QUERIES
-- ================================

-- H3 data with calculated intensity fields
CREATE OR REPLACE VIEW h3_pfas_with_intensity AS
SELECT 
    *,
    CASE 
        WHEN agricultural_area_ha > 0 THEN total_pfas_grams / agricultural_area_ha 
        ELSE 0 
    END AS pfas_intensity,
    CASE 
        WHEN agricultural_area_ha > 0 THEN total_pesticide_load / agricultural_area_ha 
        ELSE 0 
    END AS pesticide_intensity,
    ST_X(h3_centroid) AS centroid_lon,
    ST_Y(h3_centroid) AS centroid_lat
FROM h3_pfas_exposure;

-- BNBO areas with calculated metrics
CREATE OR REPLACE VIEW bnbo_areas_with_metrics AS
SELECT 
    *,
    ST_Area(geometry::geography) / 10000 AS calculated_area_ha, -- Convert to hectares
    ST_Centroid(geometry) AS centroid
FROM bnbo_status_areas;

-- BBR buildings with coordinates
CREATE OR REPLACE VIEW bbr_buildings_with_coords AS
SELECT 
    *,
    ST_X(geometry) AS longitude,
    ST_Y(geometry) AS latitude
FROM bbr_buildings;

-- ================================
-- FUNCTIONS FOR DATA PROCESSING
-- ================================

-- Function to get H3 data for a specific year with optional cumulative mode
CREATE OR REPLACE FUNCTION get_h3_data_for_year(
    target_year INTEGER,
    cumulative_mode BOOLEAN DEFAULT FALSE,
    min_pesticide_load DOUBLE PRECISION DEFAULT NULL,
    max_pesticide_load DOUBLE PRECISION DEFAULT NULL,
    min_pfas_grams DOUBLE PRECISION DEFAULT NULL,
    max_pfas_grams DOUBLE PRECISION DEFAULT NULL
)
RETURNS TABLE (
    h3_id BIGINT,
    year INTEGER,
    total_pesticide_load DOUBLE PRECISION,
    total_pfas_grams DOUBLE PRECISION,
    pesticide_application_count INTEGER,
    field_count INTEGER,
    agricultural_area_ha DOUBLE PRECISION,
    avg_field_coverage DOUBLE PRECISION,
    geometry_json TEXT,
    centroid_lon DOUBLE PRECISION,
    centroid_lat DOUBLE PRECISION,
    h3_resolution INTEGER,
    pfas_intensity DOUBLE PRECISION,
    pesticide_intensity DOUBLE PRECISION
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        h.h3_id,
        h.year,
        h.total_pesticide_load,
        h.total_pfas_grams,
        h.pesticide_application_count,
        h.field_count,
        h.agricultural_area_ha,
        h.avg_field_coverage,
        ST_AsGeoJSON(h.geometry)::TEXT AS geometry_json,
        ST_X(h.h3_centroid) AS centroid_lon,
        ST_Y(h.h3_centroid) AS centroid_lat,
        h.h3_resolution,
        CASE 
            WHEN h.agricultural_area_ha > 0 THEN h.total_pfas_grams / h.agricultural_area_ha 
            ELSE 0 
        END AS pfas_intensity,
        CASE 
            WHEN h.agricultural_area_ha > 0 THEN h.total_pesticide_load / h.agricultural_area_ha 
            ELSE 0 
        END AS pesticide_intensity
    FROM h3_pfas_exposure h
    WHERE 
        (cumulative_mode = FALSE AND h.year = target_year) OR
        (cumulative_mode = TRUE AND h.year >= 2020 AND h.year <= target_year)
        AND (min_pesticide_load IS NULL OR h.total_pesticide_load >= min_pesticide_load)
        AND (max_pesticide_load IS NULL OR h.total_pesticide_load <= max_pesticide_load)
        AND (min_pfas_grams IS NULL OR h.total_pfas_grams >= min_pfas_grams)
        AND (max_pfas_grams IS NULL OR h.total_pfas_grams <= max_pfas_grams)
    ORDER BY h.h3_id, h.year;
END;
$$ LANGUAGE plpgsql;

-- Function to get data within a bounding box
CREATE OR REPLACE FUNCTION get_h3_data_in_bbox(
    min_lon DOUBLE PRECISION,
    min_lat DOUBLE PRECISION,
    max_lon DOUBLE PRECISION,
    max_lat DOUBLE PRECISION,
    target_year INTEGER DEFAULT NULL
)
RETURNS TABLE (
    h3_id BIGINT,
    year INTEGER,
    total_pesticide_load DOUBLE PRECISION,
    total_pfas_grams DOUBLE PRECISION,
    geometry_json TEXT,
    centroid_lon DOUBLE PRECISION,
    centroid_lat DOUBLE PRECISION
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        h.h3_id,
        h.year,
        h.total_pesticide_load,
        h.total_pfas_grams,
        ST_AsGeoJSON(h.geometry)::TEXT AS geometry_json,
        ST_X(h.h3_centroid) AS centroid_lon,
        ST_Y(h.h3_centroid) AS centroid_lat
    FROM h3_pfas_exposure h
    WHERE 
        ST_Intersects(
            h.geometry, 
            ST_MakeEnvelope(min_lon, min_lat, max_lon, max_lat, 4326)
        )
        AND (target_year IS NULL OR h.year = target_year)
    ORDER BY h.h3_id, h.year;
END;
$$ LANGUAGE plpgsql;

-- ================================
-- DATA QUALITY CONSTRAINTS
-- ================================

-- Add comments for documentation
COMMENT ON TABLE h3_pfas_exposure IS 'H3 hexagon-based PFAS and pesticide exposure data from the pipeline';
COMMENT ON COLUMN h3_pfas_exposure.h3_id IS 'H3 hexagon identifier at resolution 10';
COMMENT ON COLUMN h3_pfas_exposure.total_pesticide_load IS 'Standardized pesticide load in kg/ha equivalent';
COMMENT ON COLUMN h3_pfas_exposure.total_pfas_grams IS 'Actual PFAS active ingredient mass in grams';
COMMENT ON COLUMN h3_pfas_exposure.geometry IS 'H3 hexagon polygon geometry in WGS84';
COMMENT ON COLUMN h3_pfas_exposure.h3_centroid IS 'H3 hexagon center point in WGS84';

COMMENT ON TABLE bnbo_status_areas IS 'BNBO (Biodiversity and Nature Protection) status areas';
COMMENT ON COLUMN bnbo_status_areas.status_code IS 'Protection status: protected, buffer, agricultural, transition, unprotected';

COMMENT ON TABLE bbr_buildings IS 'BBR (Building and Dwelling Register) building data';
COMMENT ON COLUMN bbr_buildings.building_type IS 'Building type: Residential, Agricultural, Industrial, Commercial, Public, Other';

-- ================================
-- INITIAL DATA VALIDATION
-- ================================

-- Verify PostGIS is working
SELECT PostGIS_Full_Version();

-- Check if tables were created successfully
SELECT 
    schemaname,
    tablename,
    tableowner
FROM pg_tables 
WHERE tablename IN ('h3_pfas_exposure', 'bnbo_status_areas', 'bbr_buildings');

-- Check indexes were created
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes 
WHERE tablename IN ('h3_pfas_exposure', 'bnbo_status_areas', 'bbr_buildings')
ORDER BY tablename, indexname; 