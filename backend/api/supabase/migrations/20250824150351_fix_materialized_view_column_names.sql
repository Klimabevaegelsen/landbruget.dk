-- Fix materialized view column names to match the new capacity-focused schema
-- This addresses the semantic inconsistency where capacity_count is aliased as production_volume_equiv
-- Note: The materialized view is already correctly referencing animal_capacity_log and capacity_count
-- This migration just refreshes the view to ensure it's populated with current data

-- Refresh the materialized view to populate it with current data (if it exists)
-- Note: The view may have been dropped by previous migrations
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname = 'site_species_production_ranked') THEN
        REFRESH MATERIALIZED VIEW public.site_species_production_ranked;
        COMMENT ON MATERIALIZED VIEW public.site_species_production_ranked IS 'Species production rankings by facility - uses animal_capacity_log.capacity_count data (aliased as production_volume_equiv for backward compatibility)';
    END IF;
END $$;
