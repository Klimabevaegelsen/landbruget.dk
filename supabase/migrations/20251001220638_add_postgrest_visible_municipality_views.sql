-- Migration: Add PostgREST-visible views for municipality summaries
-- 
-- Context: PostgreSQL's information_schema does NOT expose materialized views,
-- which means PostgREST (used by Supabase JS Client) cannot discover them.
-- 
-- Solution: Create regular VIEWs that wrap the materialized views.
-- These VIEWs appear in information_schema and are discoverable by PostgREST.

-- Drop existing views if they exist
DROP VIEW IF EXISTS v_municipality_land_use_summary CASCADE;
DROP VIEW IF EXISTS v_municipality_production_summary CASCADE;

-- Create VIEW (not materialized) that wraps municipality_land_use_summary
CREATE VIEW v_municipality_land_use_summary AS
SELECT * FROM municipality_land_use_summary;

-- Create VIEW (not materialized) that wraps municipality_production_summary
CREATE VIEW v_municipality_production_summary AS
SELECT * FROM municipality_production_summary;

-- Grant permissions to all roles
GRANT SELECT ON v_municipality_land_use_summary TO anon, authenticated, service_role;
GRANT SELECT ON v_municipality_production_summary TO anon, authenticated, service_role;

-- Notify PostgREST to reload schema
NOTIFY pgrst, 'reload schema';
