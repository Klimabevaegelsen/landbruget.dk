-- Migration: Add Company UUID Lookup Function
-- This function allows other data loading processes to lookup company UUIDs by CVR number
-- ensuring referential integrity and consistent company references across all tables.

-- Create function to lookup company UUID by CVR number
CREATE OR REPLACE FUNCTION get_company_uuid_by_cvr(cvr_text TEXT)
RETURNS UUID AS $$
DECLARE
    company_uuid UUID;
BEGIN
    -- Look up company UUID by CVR number
    SELECT id INTO company_uuid 
    FROM companies 
    WHERE cvr_number = cvr_text;
    
    -- If not found, this indicates the CVR enrichment pipeline hasn't processed this CVR yet
    IF company_uuid IS NULL THEN
        RAISE EXCEPTION 'CVR % not found in companies table. Run CVR enrichment pipeline first.', cvr_text;
    END IF;
    
    RETURN company_uuid;
END;
$$ LANGUAGE plpgsql;

-- Add comment explaining the function's purpose
COMMENT ON FUNCTION get_company_uuid_by_cvr(TEXT) IS 
'Lookup company UUID by CVR number. This function ensures referential integrity by requiring that companies exist in the companies table (populated by CVR enrichment pipeline) before other data can reference them.';

-- Create a helper function for bulk CVR to UUID mapping
CREATE OR REPLACE FUNCTION get_company_uuids_by_cvrs(cvr_array TEXT[])
RETURNS TABLE(cvr_number TEXT, company_uuid UUID) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        unnest(cvr_array) as cvr_number,
        c.id as company_uuid
    FROM unnest(cvr_array) cvr
    LEFT JOIN companies c ON c.cvr_number = cvr;
END;
$$ LANGUAGE plpgsql;

-- Add comment for bulk function
COMMENT ON FUNCTION get_company_uuids_by_cvrs(TEXT[]) IS 
'Bulk lookup of company UUIDs by CVR numbers. Returns a table with cvr_number and corresponding company_uuid. Returns NULL for company_uuid if CVR is not found.';

-- Create an index on cvr_number for faster lookups (if it doesn't exist)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'companies_cvr_number_idx' 
        AND n.nspname = 'public'
    ) THEN
        CREATE INDEX companies_cvr_number_idx ON companies(cvr_number);
    END IF;
END $$;
