-- Create materialized view for homepage statistics
-- This view calculates the total data points and companies for the homepage
-- Based on actual database analysis showing 29M+ data points across 46K+ companies

CREATE MATERIALIZED VIEW IF NOT EXISTS homepage_statistics AS
WITH all_data_tables AS (
    -- Get row counts from all major data tables that represent agricultural data points
    SELECT
        COUNT(*) as total_rows
    FROM (
        -- Field yearly data (crop management records by field and year) - 4.9M records
        SELECT 1 FROM field_yearly_data
        UNION ALL
        -- Pesticide applications (field-level pesticide applications) - 16.3M records
        SELECT 1 FROM pesticide_applications
        UNION ALL
        -- Company pesticide applications (company-level pesticide data) - 4.8M records
        SELECT 1 FROM company_pesticide_applications
        UNION ALL
        -- Employee monthly counts (employment data) - 1.0M records
        SELECT 1 FROM employee_monthly_counts
        UNION ALL
        -- Field boundaries (field geometry data) - 595K records
        SELECT 1 FROM field_boundaries
        UNION ALL
        -- Animal transports (livestock movement data) - 569K records
        SELECT 1 FROM animal_transports
        UNION ALL
        -- Field wetland areas (environmental data) - 171K records
        SELECT 1 FROM field_wetland_areas
        UNION ALL
        -- Worker yearly summary (labor statistics) - 104K records
        SELECT 1 FROM worker_yearly_summary
        UNION ALL
        -- Vet events (veterinary data) - 86K records
        SELECT 1 FROM vet_events
        UNION ALL
        -- Animal capacity log (livestock capacity data) - 56K records
        SELECT 1 FROM animal_capacity_log
        UNION ALL
        -- Herds (livestock herd data) - 49K records
        SELECT 1 FROM herds
        UNION ALL
        -- Company leadership (management data) - 47K records
        SELECT 1 FROM company_leadership
        UNION ALL
        -- Company owners (ownership data) - 40K records
        SELECT 1 FROM company_owners
        UNION ALL
        -- Production sites (facility data) - 20K records
        SELECT 1 FROM production_sites
        UNION ALL
        -- Site yearly summary (site statistics) - 19K records
        SELECT 1 FROM site_yearly_summary
        UNION ALL
        -- Pesticide products (product catalog) - 10K records
        SELECT 1 FROM pesticide_products
        UNION ALL
        -- Field BNBO areas (environmental protection data) - 10K records
        SELECT 1 FROM field_bnbo_areas
        UNION ALL
        -- Visa yearly counts (immigration data) - 8.4K records
        SELECT 1 FROM visa_yearly_counts
        UNION ALL
        -- Antibiotic usage (veterinary medicine data) - 6K records
        SELECT 1 FROM antibiotic_usage
        UNION ALL
        -- Yearly financials (financial data) - 3.5K records
        SELECT 1 FROM yearly_financials
        UNION ALL
        -- Incidents (safety/compliance data) - 453 records
        SELECT 1 FROM incidents
    ) all_data
),
company_count AS (
    -- Get total unique companies from the companies table
    SELECT COUNT(*) as total_companies
    FROM companies
    WHERE cvr_number IS NOT NULL
)
SELECT
    'homepage_stats' as stat_type,
    all_data_tables.total_rows as total_data_points,
    company_count.total_companies,
    now() as last_updated
FROM all_data_tables, company_count;

-- Set ownership and permissions
ALTER MATERIALIZED VIEW homepage_statistics OWNER TO postgres;
GRANT SELECT ON homepage_statistics TO anon;
GRANT SELECT ON homepage_statistics TO authenticated;
GRANT SELECT ON homepage_statistics TO service_role;

-- Create index for faster lookups
CREATE UNIQUE INDEX IF NOT EXISTS idx_homepage_statistics_stat_type ON homepage_statistics (stat_type);

-- Add comment
COMMENT ON MATERIALIZED VIEW homepage_statistics IS 'Homepage statistics showing total data points and companies across all agricultural datasets. Updated via materialized view refresh.';
