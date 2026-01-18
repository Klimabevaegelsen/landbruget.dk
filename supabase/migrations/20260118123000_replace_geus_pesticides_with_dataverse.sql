-- Migration: Replace GEUS Borehole Pesticides with GEUS Dataverse Pesticides and PFAS
-- Description: Drop old WFS-based tables and create new schema for GEUS Dataverse data
-- Source: GEUS Dataverse (https://doi.org/10.22008/FK2/IHVDXL)
-- Dataset: "Groundwater chemistry data for the fourth River Basin Management Plan"
-- Files:
--   - AM_pest.rds (Annual Mean pesticide concentrations)
--   - AM_pfas.rds (Annual Mean PFAS concentrations)
-- Frequency: Annual
--
-- This migration replaces the old WFS-based schema with a new schema that supports:
-- Pesticides:
-- - 633 unique substances (vs 6-8 via WFS)
-- - 4.2M+ analyses (vs ~50k via WFS)
-- - 43 years of temporal data (1981-2025) with reliable PROEVEAAR column
-- - Additional hydrogeological context (depth, redox type, etc.)
-- PFAS:
-- - 26 unique substances
-- - 397k+ analyses
-- - 2012-2025 temporal data
--
-- CRS TRANSFORMATION NOTE:
-- Silver layer stores data in EPSG:25832 (UTM 32N) in GCS Parquet files.
-- When loading from Parquet to Supabase, geometries MUST be transformed to EPSG:4326.

-- ============================================================================
-- Drop old tables and views
-- ============================================================================
DROP MATERIALIZED VIEW IF EXISTS public.geus_borehole_pesticides_summary CASCADE;
DROP TABLE IF EXISTS public.geus_pesticide_analyses CASCADE;
DROP TABLE IF EXISTS public.geus_boreholes CASCADE;
DROP FUNCTION IF EXISTS public.refresh_geus_borehole_pesticides_summary CASCADE;

-- ============================================================================
-- Table: geus_dataverse_pesticides
-- Description: Groundwater pesticide analysis data from GEUS Dataverse VP4 dataset
-- ============================================================================
CREATE TABLE IF NOT EXISTS public.geus_dataverse_pesticides (
    id BIGSERIAL PRIMARY KEY,

    -- Temporal
    year INTEGER NOT NULL,

    -- Substance identification
    stofnr INTEGER NOT NULL,
    stof_tekst VARCHAR(200) NOT NULL,
    cas_number VARCHAR(50),

    -- Measurement
    maengde DOUBLE PRECISION NOT NULL,
    enhed VARCHAR(20),

    -- Location
    x DOUBLE PRECISION NOT NULL,
    y DOUBLE PRECISION NOT NULL,

    -- Borehole identifiers
    dgu_nr VARCHAR(50) NOT NULL,
    borid INTEGER,
    indtagsid VARCHAR(50),

    -- Borehole physical properties
    borehole_depth_m DOUBLE PRECISION,
    terrain_elevation_m DOUBLE PRECISION,
    intake_top_m DOUBLE PRECISION,
    intake_bottom_m DOUBLE PRECISION,

    -- Hydrogeological context
    groundwater_body VARCHAR(100),
    redox_water_type VARCHAR(10),
    depth_category VARCHAR(50),

    -- Monitoring metadata
    data_type VARCHAR(50),
    eu_programme_code VARCHAR(50),
    eu_monitoring_site_code VARCHAR(100),

    -- Geometry (transformed to EPSG:4326 during load)
    geom GEOMETRY(Point, 4326),

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- Indexes for geus_dataverse_pesticides
-- ============================================================================
-- Spatial index
CREATE INDEX IF NOT EXISTS idx_geus_dataverse_pesticides_geom
    ON public.geus_dataverse_pesticides USING GIST (geom);

-- Temporal index (key for correlating with pesticide application data)
CREATE INDEX IF NOT EXISTS idx_geus_dataverse_pesticides_year
    ON public.geus_dataverse_pesticides (year);

-- Substance indexes
CREATE INDEX IF NOT EXISTS idx_geus_dataverse_pesticides_stofnr
    ON public.geus_dataverse_pesticides (stofnr);
CREATE INDEX IF NOT EXISTS idx_geus_dataverse_pesticides_cas
    ON public.geus_dataverse_pesticides (cas_number);

-- Borehole indexes
CREATE INDEX IF NOT EXISTS idx_geus_dataverse_pesticides_dgu_nr
    ON public.geus_dataverse_pesticides (dgu_nr);
CREATE INDEX IF NOT EXISTS idx_geus_dataverse_pesticides_borid
    ON public.geus_dataverse_pesticides (borid);

-- Groundwater body index (for joining with water management data)
CREATE INDEX IF NOT EXISTS idx_geus_dataverse_pesticides_gwb
    ON public.geus_dataverse_pesticides (groundwater_body);

-- Data type index
CREATE INDEX IF NOT EXISTS idx_geus_dataverse_pesticides_data_type
    ON public.geus_dataverse_pesticides (data_type);

-- Composite index for common query patterns
CREATE INDEX IF NOT EXISTS idx_geus_dataverse_pesticides_year_stofnr
    ON public.geus_dataverse_pesticides (year, stofnr);

-- ============================================================================
-- Comments
-- ============================================================================
COMMENT ON TABLE public.geus_dataverse_pesticides IS
    'Groundwater pesticide analysis data from GEUS Dataverse VP4 dataset (633 substances, 4.2M+ analyses, 1981-2025)';

COMMENT ON COLUMN public.geus_dataverse_pesticides.year IS 'Sample year (PROEVEAAR)';
COMMENT ON COLUMN public.geus_dataverse_pesticides.stofnr IS 'Substance code from GEUS';
COMMENT ON COLUMN public.geus_dataverse_pesticides.stof_tekst IS 'Substance name in Danish';
COMMENT ON COLUMN public.geus_dataverse_pesticides.cas_number IS 'CAS chemical identifier';
COMMENT ON COLUMN public.geus_dataverse_pesticides.maengde IS 'Annual mean concentration (AM)';
COMMENT ON COLUMN public.geus_dataverse_pesticides.enhed IS 'Unit of measurement (typically µg/L, code 20)';
COMMENT ON COLUMN public.geus_dataverse_pesticides.x IS 'X coordinate in EPSG:25832 (original)';
COMMENT ON COLUMN public.geus_dataverse_pesticides.y IS 'Y coordinate in EPSG:25832 (original)';
COMMENT ON COLUMN public.geus_dataverse_pesticides.dgu_nr IS 'DGU borehole number';
COMMENT ON COLUMN public.geus_dataverse_pesticides.borid IS 'Borehole ID';
COMMENT ON COLUMN public.geus_dataverse_pesticides.indtagsid IS 'Intake ID';
COMMENT ON COLUMN public.geus_dataverse_pesticides.borehole_depth_m IS 'Borehole depth in meters';
COMMENT ON COLUMN public.geus_dataverse_pesticides.terrain_elevation_m IS 'Terrain elevation at borehole (m above sea level)';
COMMENT ON COLUMN public.geus_dataverse_pesticides.intake_top_m IS 'Top of intake screen (m below surface)';
COMMENT ON COLUMN public.geus_dataverse_pesticides.intake_bottom_m IS 'Bottom of intake screen (m below surface)';
COMMENT ON COLUMN public.geus_dataverse_pesticides.groundwater_body IS 'Grundvandsforekomst ID for VP4';
COMMENT ON COLUMN public.geus_dataverse_pesticides.redox_water_type IS 'Redox water type (A/B/C)';
COMMENT ON COLUMN public.geus_dataverse_pesticides.depth_category IS 'Depth category (Shallow/Deep/Unknown)';
COMMENT ON COLUMN public.geus_dataverse_pesticides.data_type IS 'Data source type (VF/GRUMO/DEPOT/ANDET)';
COMMENT ON COLUMN public.geus_dataverse_pesticides.eu_programme_code IS 'EU monitoring programme code';
COMMENT ON COLUMN public.geus_dataverse_pesticides.eu_monitoring_site_code IS 'EU monitoring site identifier';

-- ============================================================================
-- Materialized View: geus_pesticides_by_borehole
-- Description: Aggregated view per borehole with detection summary
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS public.geus_pesticides_by_borehole AS
SELECT
    dgu_nr,
    borid,
    MIN(geom) as geom,  -- Use first geometry (same for all records per borehole)
    groundwater_body,
    data_type,
    COUNT(*) as total_analyses,
    COUNT(DISTINCT stofnr) as substances_tested,
    COUNT(DISTINCT year) as years_with_data,
    MIN(year) as first_year,
    MAX(year) as last_year,
    AVG(borehole_depth_m) as borehole_depth_m,
    -- Detection statistics (maengde > 0.01 µg/L as detection threshold)
    COUNT(*) FILTER (WHERE maengde > 0.01) as positive_detections,
    ARRAY_AGG(DISTINCT stof_tekst) FILTER (WHERE maengde > 0.01) as detected_substances,
    MAX(maengde) as max_concentration,
    -- Key pesticides by common name
    MAX(maengde) FILTER (WHERE stofnr = 438) as max_bam,  -- 2,6-Dichlorbenzamid
    MAX(maengde) FILTER (WHERE stofnr = 846) as max_atrazin,
    MAX(maengde) FILTER (WHERE stofnr = 590) as max_desethyl_atrazin,
    MAX(maengde) FILTER (WHERE stofnr = 1169) as max_bentazon
FROM public.geus_dataverse_pesticides
GROUP BY dgu_nr, borid, groundwater_body, data_type;

-- Indexes on materialized view
CREATE INDEX IF NOT EXISTS idx_geus_pesticides_by_borehole_geom
    ON public.geus_pesticides_by_borehole USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_geus_pesticides_by_borehole_dgu_nr
    ON public.geus_pesticides_by_borehole (dgu_nr);
CREATE INDEX IF NOT EXISTS idx_geus_pesticides_by_borehole_detections
    ON public.geus_pesticides_by_borehole (positive_detections DESC);
CREATE INDEX IF NOT EXISTS idx_geus_pesticides_by_borehole_gwb
    ON public.geus_pesticides_by_borehole (groundwater_body);

COMMENT ON MATERIALIZED VIEW public.geus_pesticides_by_borehole IS
    'Aggregated pesticide detection summary per borehole for visualization and analysis';

-- ============================================================================
-- Materialized View: geus_pesticides_by_substance_year
-- Description: Temporal trends per substance for correlation analysis
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS public.geus_pesticides_by_substance_year AS
SELECT
    year,
    stofnr,
    stof_tekst,
    cas_number,
    COUNT(*) as sample_count,
    COUNT(DISTINCT dgu_nr) as borehole_count,
    AVG(maengde) as avg_concentration,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY maengde) as median_concentration,
    MAX(maengde) as max_concentration,
    COUNT(*) FILTER (WHERE maengde > 0.01) as detection_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE maengde > 0.01) / COUNT(*), 2) as detection_rate_pct
FROM public.geus_dataverse_pesticides
GROUP BY year, stofnr, stof_tekst, cas_number
ORDER BY year, sample_count DESC;

-- Index on temporal trends view
CREATE INDEX IF NOT EXISTS idx_geus_pesticides_by_substance_year_year
    ON public.geus_pesticides_by_substance_year (year);
CREATE INDEX IF NOT EXISTS idx_geus_pesticides_by_substance_year_stofnr
    ON public.geus_pesticides_by_substance_year (stofnr);

COMMENT ON MATERIALIZED VIEW public.geus_pesticides_by_substance_year IS
    'Yearly pesticide detection statistics per substance for temporal trend analysis';

-- ============================================================================
-- Row Level Security (RLS)
-- ============================================================================
ALTER TABLE public.geus_dataverse_pesticides ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow public read on geus_dataverse_pesticides"
    ON public.geus_dataverse_pesticides FOR SELECT
    USING (true);

-- ============================================================================
-- Helper functions
-- ============================================================================
CREATE OR REPLACE FUNCTION public.refresh_geus_pesticides_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY public.geus_pesticides_by_borehole;
    REFRESH MATERIALIZED VIEW CONCURRENTLY public.geus_pesticides_by_substance_year;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION public.refresh_geus_pesticides_views IS
    'Refresh all GEUS pesticides materialized views after data load';

-- ============================================================================
-- Grants
-- ============================================================================
GRANT SELECT ON public.geus_dataverse_pesticides TO anon, authenticated;
GRANT SELECT ON public.geus_pesticides_by_borehole TO anon, authenticated;
GRANT SELECT ON public.geus_pesticides_by_substance_year TO anon, authenticated;

-- ============================================================================
-- PFAS DATA (AM_pfas.rds)
-- ============================================================================
-- PFAS (Per- and polyfluoroalkyl substances) groundwater data from the same
-- GEUS Dataverse VP4 dataset. PFAS has a different schema than pesticides:
-- - Uses substance_name (KORT_NAVN) instead of stofnr/stof_tekst
-- - Has substance_group (GROUP) column
-- - No CAS number available in source data
-- - 26 unique substances, 397k+ analyses, 2012-2025
-- ============================================================================

-- ============================================================================
-- Table: geus_dataverse_pfas
-- Description: Groundwater PFAS analysis data from GEUS Dataverse VP4 dataset
-- ============================================================================
CREATE TABLE IF NOT EXISTS public.geus_dataverse_pfas (
    id BIGSERIAL PRIMARY KEY,

    -- Temporal
    year INTEGER NOT NULL,

    -- Substance identification
    substance_name VARCHAR(100) NOT NULL,
    substance_group VARCHAR(50),

    -- Measurement
    maengde DOUBLE PRECISION NOT NULL,
    enhed VARCHAR(20),

    -- Location
    x DOUBLE PRECISION NOT NULL,
    y DOUBLE PRECISION NOT NULL,

    -- Borehole identifiers
    dgu_nr VARCHAR(50) NOT NULL,
    borid INTEGER,
    indtagsid VARCHAR(50),

    -- Borehole physical properties
    borehole_depth_m DOUBLE PRECISION,
    terrain_elevation_m DOUBLE PRECISION,
    intake_top_m DOUBLE PRECISION,
    intake_bottom_m DOUBLE PRECISION,

    -- Hydrogeological context
    groundwater_body VARCHAR(100),
    redox_water_type VARCHAR(10),
    depth_category VARCHAR(50),

    -- Monitoring metadata
    data_type VARCHAR(50),
    eu_programme_code VARCHAR(50),
    eu_monitoring_site_code VARCHAR(100),

    -- Geometry (transformed to EPSG:4326 during load)
    geom GEOMETRY(Point, 4326),

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- Indexes for geus_dataverse_pfas
-- ============================================================================
-- Spatial index
CREATE INDEX IF NOT EXISTS idx_geus_dataverse_pfas_geom
    ON public.geus_dataverse_pfas USING GIST (geom);

-- Temporal index
CREATE INDEX IF NOT EXISTS idx_geus_dataverse_pfas_year
    ON public.geus_dataverse_pfas (year);

-- Substance indexes
CREATE INDEX IF NOT EXISTS idx_geus_dataverse_pfas_substance_name
    ON public.geus_dataverse_pfas (substance_name);
CREATE INDEX IF NOT EXISTS idx_geus_dataverse_pfas_substance_group
    ON public.geus_dataverse_pfas (substance_group);

-- Borehole indexes
CREATE INDEX IF NOT EXISTS idx_geus_dataverse_pfas_dgu_nr
    ON public.geus_dataverse_pfas (dgu_nr);
CREATE INDEX IF NOT EXISTS idx_geus_dataverse_pfas_borid
    ON public.geus_dataverse_pfas (borid);

-- Groundwater body index
CREATE INDEX IF NOT EXISTS idx_geus_dataverse_pfas_gwb
    ON public.geus_dataverse_pfas (groundwater_body);

-- Data type index
CREATE INDEX IF NOT EXISTS idx_geus_dataverse_pfas_data_type
    ON public.geus_dataverse_pfas (data_type);

-- Composite index for common query patterns
CREATE INDEX IF NOT EXISTS idx_geus_dataverse_pfas_year_substance
    ON public.geus_dataverse_pfas (year, substance_name);

-- ============================================================================
-- Comments
-- ============================================================================
COMMENT ON TABLE public.geus_dataverse_pfas IS
    'Groundwater PFAS analysis data from GEUS Dataverse VP4 dataset (26 substances, 397k+ analyses, 2012-2025)';

COMMENT ON COLUMN public.geus_dataverse_pfas.year IS 'Sample year (PROEVEAAR)';
COMMENT ON COLUMN public.geus_dataverse_pfas.substance_name IS 'PFAS substance short name (KORT_NAVN)';
COMMENT ON COLUMN public.geus_dataverse_pfas.substance_group IS 'PFAS substance group (e.g., Perfluorocarboxylic acids)';
COMMENT ON COLUMN public.geus_dataverse_pfas.maengde IS 'Annual mean concentration (AM)';
COMMENT ON COLUMN public.geus_dataverse_pfas.enhed IS 'Unit of measurement (typically ng/L for PFAS)';
COMMENT ON COLUMN public.geus_dataverse_pfas.x IS 'X coordinate in EPSG:25832 (original)';
COMMENT ON COLUMN public.geus_dataverse_pfas.y IS 'Y coordinate in EPSG:25832 (original)';
COMMENT ON COLUMN public.geus_dataverse_pfas.dgu_nr IS 'DGU borehole number';
COMMENT ON COLUMN public.geus_dataverse_pfas.borid IS 'Borehole ID';
COMMENT ON COLUMN public.geus_dataverse_pfas.indtagsid IS 'Intake ID';
COMMENT ON COLUMN public.geus_dataverse_pfas.borehole_depth_m IS 'Borehole depth in meters';
COMMENT ON COLUMN public.geus_dataverse_pfas.terrain_elevation_m IS 'Terrain elevation at borehole (m above sea level)';
COMMENT ON COLUMN public.geus_dataverse_pfas.intake_top_m IS 'Top of intake screen (m below surface)';
COMMENT ON COLUMN public.geus_dataverse_pfas.intake_bottom_m IS 'Bottom of intake screen (m below surface)';
COMMENT ON COLUMN public.geus_dataverse_pfas.groundwater_body IS 'Grundvandsforekomst ID for VP4';
COMMENT ON COLUMN public.geus_dataverse_pfas.redox_water_type IS 'Redox water type (A/B/C)';
COMMENT ON COLUMN public.geus_dataverse_pfas.depth_category IS 'Depth category (Shallow/Deep/Unknown)';
COMMENT ON COLUMN public.geus_dataverse_pfas.data_type IS 'Data source type (VF/GRUMO/DEPOT/ANDET)';
COMMENT ON COLUMN public.geus_dataverse_pfas.eu_programme_code IS 'EU monitoring programme code';
COMMENT ON COLUMN public.geus_dataverse_pfas.eu_monitoring_site_code IS 'EU monitoring site identifier';

-- ============================================================================
-- Materialized View: geus_pfas_by_borehole
-- Description: Aggregated view per borehole with PFAS detection summary
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS public.geus_pfas_by_borehole AS
SELECT
    dgu_nr,
    borid,
    MIN(geom) as geom,
    groundwater_body,
    data_type,
    COUNT(*) as total_analyses,
    COUNT(DISTINCT substance_name) as substances_tested,
    COUNT(DISTINCT year) as years_with_data,
    MIN(year) as first_year,
    MAX(year) as last_year,
    AVG(borehole_depth_m) as borehole_depth_m,
    -- Detection statistics (PFAS typically measured in ng/L, detection at > 0.1 ng/L)
    COUNT(*) FILTER (WHERE maengde > 0.1) as positive_detections,
    ARRAY_AGG(DISTINCT substance_name) FILTER (WHERE maengde > 0.1) as detected_substances,
    MAX(maengde) as max_concentration,
    -- Key PFAS compounds
    MAX(maengde) FILTER (WHERE substance_name = 'PFOS') as max_pfos,
    MAX(maengde) FILTER (WHERE substance_name = 'PFOA') as max_pfoa,
    MAX(maengde) FILTER (WHERE substance_name = 'PFNA') as max_pfna,
    MAX(maengde) FILTER (WHERE substance_name = 'PFHxS') as max_pfhxs,
    -- Sum of PFAS (useful for regulatory thresholds)
    SUM(CASE WHEN maengde > 0 THEN maengde ELSE 0 END) as sum_pfas
FROM public.geus_dataverse_pfas
GROUP BY dgu_nr, borid, groundwater_body, data_type;

-- Indexes on materialized view
CREATE INDEX IF NOT EXISTS idx_geus_pfas_by_borehole_geom
    ON public.geus_pfas_by_borehole USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_geus_pfas_by_borehole_dgu_nr
    ON public.geus_pfas_by_borehole (dgu_nr);
CREATE INDEX IF NOT EXISTS idx_geus_pfas_by_borehole_detections
    ON public.geus_pfas_by_borehole (positive_detections DESC);
CREATE INDEX IF NOT EXISTS idx_geus_pfas_by_borehole_gwb
    ON public.geus_pfas_by_borehole (groundwater_body);

COMMENT ON MATERIALIZED VIEW public.geus_pfas_by_borehole IS
    'Aggregated PFAS detection summary per borehole for visualization and analysis';

-- ============================================================================
-- Materialized View: geus_pfas_by_substance_year
-- Description: Temporal trends per PFAS substance for analysis
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS public.geus_pfas_by_substance_year AS
SELECT
    year,
    substance_name,
    substance_group,
    COUNT(*) as sample_count,
    COUNT(DISTINCT dgu_nr) as borehole_count,
    AVG(maengde) as avg_concentration,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY maengde) as median_concentration,
    MAX(maengde) as max_concentration,
    COUNT(*) FILTER (WHERE maengde > 0.1) as detection_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE maengde > 0.1) / COUNT(*), 2) as detection_rate_pct
FROM public.geus_dataverse_pfas
GROUP BY year, substance_name, substance_group
ORDER BY year, sample_count DESC;

-- Indexes on temporal trends view
CREATE INDEX IF NOT EXISTS idx_geus_pfas_by_substance_year_year
    ON public.geus_pfas_by_substance_year (year);
CREATE INDEX IF NOT EXISTS idx_geus_pfas_by_substance_year_substance
    ON public.geus_pfas_by_substance_year (substance_name);
CREATE INDEX IF NOT EXISTS idx_geus_pfas_by_substance_year_group
    ON public.geus_pfas_by_substance_year (substance_group);

COMMENT ON MATERIALIZED VIEW public.geus_pfas_by_substance_year IS
    'Yearly PFAS detection statistics per substance for temporal trend analysis';

-- ============================================================================
-- Row Level Security (RLS) for PFAS
-- ============================================================================
ALTER TABLE public.geus_dataverse_pfas ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow public read on geus_dataverse_pfas"
    ON public.geus_dataverse_pfas FOR SELECT
    USING (true);

-- ============================================================================
-- Update helper function to include PFAS views
-- ============================================================================
CREATE OR REPLACE FUNCTION public.refresh_geus_pesticides_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY public.geus_pesticides_by_borehole;
    REFRESH MATERIALIZED VIEW CONCURRENTLY public.geus_pesticides_by_substance_year;
    REFRESH MATERIALIZED VIEW CONCURRENTLY public.geus_pfas_by_borehole;
    REFRESH MATERIALIZED VIEW CONCURRENTLY public.geus_pfas_by_substance_year;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION public.refresh_geus_pesticides_views IS
    'Refresh all GEUS pesticides and PFAS materialized views after data load';

-- ============================================================================
-- Grants for PFAS tables
-- ============================================================================
GRANT SELECT ON public.geus_dataverse_pfas TO anon, authenticated;
GRANT SELECT ON public.geus_pfas_by_borehole TO anon, authenticated;
GRANT SELECT ON public.geus_pfas_by_substance_year TO anon, authenticated;
