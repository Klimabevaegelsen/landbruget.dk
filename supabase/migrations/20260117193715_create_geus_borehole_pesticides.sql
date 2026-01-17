-- Migration: Create GEUS Borehole Pesticides Tables
-- Description: Tables for storing Danish borehole locations and pesticide contamination data
-- Source: GEUS Jupiter Database via WFS (https://data.geus.dk/geusmap/ows/25832.jsp)
-- Frequency: Monthly
--
-- CRS TRANSFORMATION NOTE:
-- Silver layer stores data in EPSG:25832 (UTM 32N) in GCS Parquet files.
-- When loading from Parquet to Supabase, geometries MUST be transformed to EPSG:4326.
-- Example transformation during load:
--   ST_Transform(ST_SetSRID(geom, 25832), 4326)
-- This follows the project's CRS strategy: process in EPSG:25832, store in EPSG:4326.

-- ============================================================================
-- Table: geus_boreholes
-- Description: All Danish boreholes from GEUS Jupiter database
-- ============================================================================
CREATE TABLE IF NOT EXISTS public.geus_boreholes (
    id SERIAL PRIMARY KEY,
    dgunr VARCHAR(50) NOT NULL,
    anlaegid VARCHAR(50),
    kommunenr VARCHAR(10),
    kommunenavn VARCHAR(100),
    region_tekst VARCHAR(100),
    dybde VARCHAR(50),
    formaal VARCHAR(100),
    boringsstatus VARCHAR(50),
    anlaegtype VARCHAR(50),
    geom GEOMETRY(Point, 4326),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT geus_boreholes_dgunr_unique UNIQUE (dgunr)
);

-- Indexes for geus_boreholes
CREATE INDEX IF NOT EXISTS idx_geus_boreholes_geom ON public.geus_boreholes USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_geus_boreholes_dgunr ON public.geus_boreholes (dgunr);
CREATE INDEX IF NOT EXISTS idx_geus_boreholes_anlaegid ON public.geus_boreholes (anlaegid);
CREATE INDEX IF NOT EXISTS idx_geus_boreholes_kommunenr ON public.geus_boreholes (kommunenr);

-- Comments
COMMENT ON TABLE public.geus_boreholes IS 'Danish boreholes from GEUS Jupiter database';
COMMENT ON COLUMN public.geus_boreholes.dgunr IS 'Unique borehole identifier (DGU number)';
COMMENT ON COLUMN public.geus_boreholes.anlaegid IS 'Facility/installation ID for joining with analyses';
COMMENT ON COLUMN public.geus_boreholes.dybde IS 'Borehole depth';
COMMENT ON COLUMN public.geus_boreholes.formaal IS 'Purpose of the borehole';
COMMENT ON COLUMN public.geus_boreholes.boringsstatus IS 'Current status of the borehole';

-- ============================================================================
-- Table: geus_pesticide_analyses
-- Description: Pesticide analysis results for tracked substances
-- ============================================================================
CREATE TABLE IF NOT EXISTS public.geus_pesticide_analyses (
    id SERIAL PRIMARY KEY,
    anlaegid VARCHAR(50) NOT NULL,
    dgunr VARCHAR(50),
    stofnr INTEGER NOT NULL,
    stof VARCHAR(200),
    stof_status VARCHAR(50),
    maengde DOUBLE PRECISION,
    enhed VARCHAR(50),
    proevedato DATE,
    indtastdato DATE,
    is_detection BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for geus_pesticide_analyses
CREATE INDEX IF NOT EXISTS idx_geus_pesticide_analyses_anlaegid ON public.geus_pesticide_analyses (anlaegid);
CREATE INDEX IF NOT EXISTS idx_geus_pesticide_analyses_dgunr ON public.geus_pesticide_analyses (dgunr);
CREATE INDEX IF NOT EXISTS idx_geus_pesticide_analyses_stofnr ON public.geus_pesticide_analyses (stofnr);
CREATE INDEX IF NOT EXISTS idx_geus_pesticide_analyses_proevedato ON public.geus_pesticide_analyses (proevedato);
CREATE INDEX IF NOT EXISTS idx_geus_pesticide_analyses_detection ON public.geus_pesticide_analyses (is_detection) WHERE is_detection = TRUE;

-- Comments
COMMENT ON TABLE public.geus_pesticide_analyses IS 'Pesticide analysis results from GEUS Jupiter for tracked substances';
COMMENT ON COLUMN public.geus_pesticide_analyses.stofnr IS 'Substance number: 438=BAM, 846=Atrazin, 613=Chloridazon, 1448/1534=degradation products';
COMMENT ON COLUMN public.geus_pesticide_analyses.stof IS 'Substance name';
COMMENT ON COLUMN public.geus_pesticide_analyses.maengde IS 'Measured concentration';
COMMENT ON COLUMN public.geus_pesticide_analyses.enhed IS 'Unit of measurement';
COMMENT ON COLUMN public.geus_pesticide_analyses.proevedato IS 'Sample date';
COMMENT ON COLUMN public.geus_pesticide_analyses.is_detection IS 'Whether this is a positive detection (maengde > 0)';

-- ============================================================================
-- Materialized View: geus_borehole_pesticides_summary
-- Description: Aggregated view of boreholes with pesticide detection summary
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS public.geus_borehole_pesticides_summary AS
SELECT
    b.dgunr,
    b.kommunenavn,
    b.region_tekst,
    b.dybde,
    b.geom,
    COUNT(DISTINCT a.id) as total_analyses,
    COUNT(DISTINCT CASE WHEN a.is_detection THEN a.id END) as positive_detections,
    COUNT(DISTINCT a.stofnr) as substances_tested,
    ARRAY_AGG(DISTINCT a.stof) FILTER (WHERE a.is_detection) as detected_substances,
    MAX(a.proevedato) as latest_sample_date,
    MIN(a.proevedato) as earliest_sample_date,
    MAX(a.maengde) FILTER (WHERE a.stofnr = 438) as max_bam_concentration,
    MAX(a.maengde) FILTER (WHERE a.stofnr = 846) as max_atrazin_concentration,
    MAX(a.maengde) FILTER (WHERE a.stofnr = 613) as max_chloridazon_concentration,
    CASE
        WHEN COUNT(DISTINCT CASE WHEN a.is_detection THEN a.id END) > 0 THEN 'contaminated'
        WHEN COUNT(DISTINCT a.id) > 0 THEN 'tested_clean'
        ELSE 'untested'
    END as contamination_status
FROM public.geus_boreholes b
LEFT JOIN public.geus_pesticide_analyses a ON b.anlaegid = a.anlaegid
GROUP BY b.dgunr, b.kommunenavn, b.region_tekst, b.dybde, b.geom;

-- Index on materialized view
CREATE INDEX IF NOT EXISTS idx_geus_borehole_pesticides_summary_geom
    ON public.geus_borehole_pesticides_summary USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_geus_borehole_pesticides_summary_status
    ON public.geus_borehole_pesticides_summary (contamination_status);
CREATE INDEX IF NOT EXISTS idx_geus_borehole_pesticides_summary_kommune
    ON public.geus_borehole_pesticides_summary (kommunenavn);

COMMENT ON MATERIALIZED VIEW public.geus_borehole_pesticides_summary IS
    'Aggregated borehole pesticide detection summary for visualization';

-- ============================================================================
-- Row Level Security (RLS)
-- ============================================================================
ALTER TABLE public.geus_boreholes ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.geus_pesticide_analyses ENABLE ROW LEVEL SECURITY;

-- Public read access policies
CREATE POLICY "Allow public read on geus_boreholes"
    ON public.geus_boreholes FOR SELECT
    USING (true);

CREATE POLICY "Allow public read on geus_pesticide_analyses"
    ON public.geus_pesticide_analyses FOR SELECT
    USING (true);

-- ============================================================================
-- Helper function to refresh materialized view
-- ============================================================================
CREATE OR REPLACE FUNCTION public.refresh_geus_borehole_pesticides_summary()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY public.geus_borehole_pesticides_summary;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION public.refresh_geus_borehole_pesticides_summary IS
    'Refresh the GEUS borehole pesticides summary materialized view';

-- ============================================================================
-- Grants
-- ============================================================================
GRANT SELECT ON public.geus_boreholes TO anon, authenticated;
GRANT SELECT ON public.geus_pesticide_analyses TO anon, authenticated;
GRANT SELECT ON public.geus_borehole_pesticides_summary TO anon, authenticated;
