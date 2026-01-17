-- Migration: Add Industry Fields to Companies Table
-- This migration adds industry classification fields to the companies table
-- to support filtering by agricultural vs non-agricultural companies.

-- Add industry classification columns if they don't exist
DO $$
BEGIN
    -- Add primary_industry_code column
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'companies'
        AND column_name = 'primary_industry_code'
    ) THEN
        ALTER TABLE companies ADD COLUMN primary_industry_code TEXT;
    END IF;

    -- Add primary_industry_description column
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'companies'
        AND column_name = 'primary_industry_description'
    ) THEN
        ALTER TABLE companies ADD COLUMN primary_industry_description TEXT;
    END IF;

    -- Add is_agricultural_company column
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'companies'
        AND column_name = 'is_agricultural_company'
    ) THEN
        ALTER TABLE companies ADD COLUMN is_agricultural_company BOOLEAN DEFAULT FALSE;
    END IF;
END $$;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_companies_primary_industry_code
ON companies(primary_industry_code);

CREATE INDEX IF NOT EXISTS idx_companies_is_agricultural
ON companies(is_agricultural_company);

-- Add comments to document the columns
COMMENT ON COLUMN companies.primary_industry_code IS
'Primary NACE industry code for the company from CVR register';

COMMENT ON COLUMN companies.primary_industry_description IS
'Primary industry description in Danish from CVR register';

COMMENT ON COLUMN companies.is_agricultural_company IS
'Boolean flag indicating if the company is classified as agricultural (industry code starts with 01)';

-- Update existing companies with industry information if available
-- This will be populated by the CVR enrichment pipeline going forward
UPDATE companies
SET is_agricultural_company = FALSE
WHERE is_agricultural_company IS NULL;

-- Log the migration
DO $$
BEGIN
    RAISE NOTICE 'Added industry classification fields to companies table';
    RAISE NOTICE 'Companies table now supports filtering by agricultural vs non-agricultural companies';
    RAISE NOTICE 'Industry data will be populated by CVR enrichment pipeline';
END $$;
