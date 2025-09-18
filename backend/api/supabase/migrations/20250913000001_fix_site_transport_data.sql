-- Fix site-level transport data aggregation
-- Issue: site_details_summary_ranked materialized view shows 0 for transport_count
-- even though site_yearly_summary has the correct data

-- Drop and recreate the materialized view to fix transport_count aggregation
DROP MATERIALIZED VIEW IF EXISTS site_details_summary_ranked CASCADE;

CREATE MATERIALIZED VIEW site_details_summary_ranked AS
WITH site_data AS (
    SELECT
        sys.id,
        sys.chr,
        sys.year,
        sys.owner_cvr,
        sys.capacity,
        sys.current_disease_status,
        sys.capacity_count,
        sys.antibiotics_ddd,
        sys.transport_count,  -- This should pull the correct value from site_yearly_summary
        ps.site_name,
        ps.address,
        ps.municipality,
        ps.company_id,
        ps.main_species_code
    FROM site_yearly_summary sys
    JOIN production_sites ps ON sys.chr = ps.chr
)
SELECT
    sd.id,
    sd.chr,
    sd.year,
    sd.owner_cvr,
    sd.capacity,
    sd.current_disease_status,
    sd.capacity_count,
    sd.antibiotics_ddd,
    sd.transport_count,
    sd.site_name,
    sd.address,
    sd.municipality,
    sd.company_id,
    sd.main_species_code,
    rank() OVER (PARTITION BY sd.year, sd.municipality ORDER BY sd.capacity_count DESC NULLS LAST) AS rank_municipality_site_production,
    rank() OVER (PARTITION BY sd.year, sd.municipality ORDER BY sd.antibiotics_ddd DESC NULLS LAST) AS rank_municipality_site_antibiotics,
    rank() OVER (PARTITION BY sd.year, sd.municipality ORDER BY sd.transport_count DESC NULLS LAST) AS rank_municipality_site_transport
FROM site_data sd;

-- Set ownership and permissions
ALTER TABLE site_details_summary_ranked OWNER TO postgres;
GRANT SELECT ON site_details_summary_ranked TO anon;
GRANT SELECT ON site_details_summary_ranked TO authenticated;
GRANT SELECT ON site_details_summary_ranked TO service_role;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_site_details_summary_ranked_chr_year
ON site_details_summary_ranked (chr, year);

CREATE INDEX IF NOT EXISTS idx_site_details_summary_ranked_company_year
ON site_details_summary_ranked (company_id, year);

-- Add comment documenting the fix
COMMENT ON MATERIALIZED VIEW site_details_summary_ranked IS
'Site details with correct transport_count aggregation from site_yearly_summary. Fixed transport data showing as 0 in individual site KPIs. Migration 20250913000001.';
