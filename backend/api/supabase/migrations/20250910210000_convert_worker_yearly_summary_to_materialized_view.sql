-- Convert worker_yearly_summary to a proper materialized view
-- This will aggregate employee, visa, and injury data by company and year

-- First, drop the existing table
DROP TABLE IF EXISTS worker_yearly_summary;

-- Create the materialized view with proper aggregation
CREATE MATERIALIZED VIEW worker_yearly_summary AS
WITH employee_yearly AS (
  -- Aggregate employee data by company and year (using latest month per year)
  SELECT 
    company_id,
    EXTRACT(year FROM month_year)::integer as year,
    AVG(employee_count)::integer as average_employee_count
  FROM employee_monthly_counts
  WHERE employee_count > 0
  GROUP BY company_id, EXTRACT(year FROM month_year)
),
visa_yearly AS (
  -- Aggregate visa data by company and year (2024/2025 is the latest year with data)
  SELECT 
    company_id,
    year,
    first_permits_count as active_visa_count
  FROM visa_yearly_counts
  WHERE first_permits_count > 0
  AND year <= 2025  -- Only include years with actual data
),
injury_yearly AS (
  -- Get injury data from incidents table (workplace inspections)
  SELECT 
    company_id,
    EXTRACT(year FROM incident_date)::integer as year,
    COUNT(*)::text as injury_count_reported
  FROM incidents
  WHERE type = 'workplace_inspection'
  GROUP BY company_id, EXTRACT(year FROM incident_date)
),
all_companies_years AS (
  -- Get all company-year combinations from any of the data sources
  SELECT company_id, year FROM employee_yearly
  UNION
  SELECT company_id, year FROM visa_yearly
  UNION 
  SELECT company_id, year FROM injury_yearly
)
SELECT 
  gen_random_uuid() as id,
  acy.company_id,
  acy.year,
  COALESCE(ey.average_employee_count, 0) as average_employee_count,
  COALESCE(vy.active_visa_count, 0) as active_visa_count,
  COALESCE(iy.injury_count_reported, '0') as injury_count_reported,
  CURRENT_TIMESTAMP as created_at,
  CURRENT_TIMESTAMP as updated_at
FROM all_companies_years acy
LEFT JOIN employee_yearly ey ON acy.company_id = ey.company_id AND acy.year = ey.year
LEFT JOIN visa_yearly vy ON acy.company_id = vy.company_id AND acy.year = vy.year
LEFT JOIN injury_yearly iy ON acy.company_id = iy.company_id AND acy.year = iy.year;

-- Create indexes for better performance
CREATE INDEX idx_worker_yearly_summary_company_year ON worker_yearly_summary(company_id, year);
CREATE INDEX idx_worker_yearly_summary_employees ON worker_yearly_summary(average_employee_count DESC) WHERE average_employee_count > 0;
CREATE INDEX idx_worker_yearly_summary_visas ON worker_yearly_summary(active_visa_count DESC) WHERE active_visa_count > 0;
CREATE INDEX idx_worker_yearly_summary_injuries ON worker_yearly_summary(injury_count_reported) WHERE injury_count_reported != '0';

-- Add to materialized view refresh schedule
INSERT INTO materialized_view_refresh_log (view_name, refresh_started_at, refresh_completed_at, refresh_duration_seconds, status, error_message, triggered_by, environment, schema_version)
VALUES (
  'worker_yearly_summary',
  CURRENT_TIMESTAMP,
  CURRENT_TIMESTAMP,
  1,
  'completed',
  NULL,
  'migration',
  'production',
  1
);
