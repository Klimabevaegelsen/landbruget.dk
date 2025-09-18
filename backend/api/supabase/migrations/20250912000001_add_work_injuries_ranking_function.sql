-- Migration: Add work injuries ranking function
-- Description: Creates a function to aggregate incidents by company for rankings

CREATE OR REPLACE FUNCTION get_work_injuries_ranking(
  target_year INTEGER,
  max_results INTEGER DEFAULT 50
)
RETURNS TABLE (
  company_id UUID,
  cvr_number TEXT,
  company_name TEXT,
  municipality TEXT,
  incident_count BIGINT
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
  SELECT
    c.id as company_id,
    c.cvr_number::text,
    c.company_name,
    c.municipality,
    COUNT(i.id) as incident_count
  FROM companies c
  INNER JOIN incidents i ON c.id = i.company_id
  WHERE EXTRACT(YEAR FROM i.incident_date) = target_year
  GROUP BY c.id, c.cvr_number, c.company_name, c.municipality
  ORDER BY COUNT(i.id) DESC
  LIMIT max_results;
END;
$$;

-- Grant execute permissions
GRANT EXECUTE ON FUNCTION get_work_injuries_ranking(INTEGER, INTEGER) TO anon;
GRANT EXECUTE ON FUNCTION get_work_injuries_ranking(INTEGER, INTEGER) TO authenticated;
