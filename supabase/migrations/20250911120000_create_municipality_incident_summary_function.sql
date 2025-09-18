-- Create function to get municipality incident summary
-- Migration: 20250911120000_create_municipality_incident_summary_function.sql

CREATE OR REPLACE FUNCTION get_municipality_incident_summary(target_year INTEGER DEFAULT 2024)
RETURNS TABLE (
    municipality TEXT,
    total_incidents BIGINT,
    companies_with_incidents BIGINT,
    incident_types BIGINT,
    severe_incidents BIGINT,
    incident_rate_per_company NUMERIC
) 
LANGUAGE SQL
AS $$
    WITH municipality_incident_stats AS (
        SELECT 
            c.municipality,
            COUNT(*) as total_incidents,
            COUNT(DISTINCT i.company_id) as companies_with_incidents,
            COUNT(DISTINCT i.type) as incident_types,
            COUNT(CASE WHEN i.severity = 'high' THEN 1 END) as severe_incidents
        FROM incidents i
        JOIN companies c ON i.company_id = c.id
        WHERE i.year = target_year
          AND c.municipality IS NOT NULL
        GROUP BY c.municipality
    )
    SELECT 
        mis.municipality,
        mis.total_incidents,
        mis.companies_with_incidents,
        mis.incident_types,
        mis.severe_incidents,
        ROUND(mis.total_incidents::NUMERIC / NULLIF(mis.companies_with_incidents, 0), 2) as incident_rate_per_company
    FROM municipality_incident_stats mis
    ORDER BY mis.total_incidents DESC;
$$;

-- Grant execute permission
GRANT EXECUTE ON FUNCTION get_municipality_incident_summary TO anon, authenticated;
