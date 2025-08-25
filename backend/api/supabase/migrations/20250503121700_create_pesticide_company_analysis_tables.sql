-- Create tables for pesticide company analysis feature
-- Migration: 20250127000000_create_pesticide_company_analysis_tables.sql

-- BMD Pesticide Products Database
CREATE TABLE IF NOT EXISTS pesticide_products (
    registration_number VARCHAR PRIMARY KEY,
    product_name VARCHAR NOT NULL,
    active_ingredients VARCHAR,
    contains_pfas BOOLEAN DEFAULT false,
    contains_diquat BOOLEAN DEFAULT false,
    contains_glyphosate BOOLEAN DEFAULT false,
    total_burden_score NUMERIC,
    health_burden_score NUMERIC,
    environmental_burden_score NUMERIC,
    environmental_behavior_score NUMERIC,
    product_status VARCHAR,
    approval_date DATE,
    expiry_date DATE,
    restriction_date DATE,
    formulation_type VARCHAR,
    concentration_pct VARCHAR,
    unit VARCHAR,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Company-Level Non-Disaggregated Pesticide Applications
CREATE TABLE IF NOT EXISTS company_pesticide_applications (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    company_id UUID, -- REFERENCES companies(id) - will add constraint later
    cvr_number BIGINT NOT NULL,
    agricultural_year VARCHAR NOT NULL, -- e.g., "2022_2023"
    application_year INTEGER NOT NULL, -- e.g., 2023
    pesticide_name VARCHAR,
    registration_number VARCHAR REFERENCES pesticide_products(registration_number),
    dosage_quantity NUMERIC,
    dosage_unit VARCHAR,
    treated_area_ha NUMERIC,
    crop_type VARCHAR,
    crop_code VARCHAR,
    municipality VARCHAR,
    postal_code VARCHAR,
    -- Calculated fields (from BMD join)
    contains_pfas BOOLEAN,
    contains_diquat BOOLEAN,
    contains_glyphosate BOOLEAN,
    total_burden_score NUMERIC,
    health_burden_score NUMERIC,
    environmental_burden_score NUMERIC,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_company_pesticide_cvr_year ON company_pesticide_applications(cvr_number, application_year);
CREATE INDEX IF NOT EXISTS idx_company_pesticide_company_year ON company_pesticide_applications(company_id, application_year);
CREATE INDEX IF NOT EXISTS idx_company_pesticide_municipality ON company_pesticide_applications(municipality, application_year);
CREATE INDEX IF NOT EXISTS idx_company_pesticide_chemicals ON company_pesticide_applications(contains_pfas, contains_diquat, contains_glyphosate);
CREATE INDEX IF NOT EXISTS idx_company_pesticide_registration ON company_pesticide_applications(registration_number);
CREATE INDEX IF NOT EXISTS idx_pesticide_products_chemicals ON pesticide_products(contains_pfas, contains_diquat, contains_glyphosate);

-- Company-level pesticide summary materialized view (country-wide analysis)
CREATE MATERIALIZED VIEW IF NOT EXISTS company_pesticide_summary AS
SELECT
    c.id as company_id,
    c.cvr_number,
    c.company_name,
    c.municipality,
    cpa.application_year,
    -- Aggregated pesticide metrics
    SUM(COALESCE(cpa.total_burden_score, 0) * COALESCE(cpa.dosage_quantity, 0)) as total_belastning,
    SUM(CASE WHEN cpa.contains_pfas THEN COALESCE(cpa.total_burden_score, 0) * COALESCE(cpa.dosage_quantity, 0) ELSE 0 END) as pfas_belastning,
    SUM(CASE WHEN cpa.contains_diquat THEN COALESCE(cpa.total_burden_score, 0) * COALESCE(cpa.dosage_quantity, 0) ELSE 0 END) as diquat_belastning,
    SUM(CASE WHEN cpa.contains_glyphosate THEN COALESCE(cpa.total_burden_score, 0) * COALESCE(cpa.dosage_quantity, 0) ELSE 0 END) as glyphosate_belastning,
    COUNT(cpa.id) as total_applications,
    COUNT(DISTINCT cpa.registration_number) as unique_products,
    SUM(COALESCE(cpa.treated_area_ha, 0)) as total_treated_area_ha,
    -- Additional metrics
    AVG(COALESCE(cpa.total_burden_score, 0)) as avg_burden_per_application,
    COUNT(CASE WHEN cpa.contains_pfas THEN 1 END) as pfas_applications,
    COUNT(CASE WHEN cpa.contains_diquat THEN 1 END) as diquat_applications,
    COUNT(CASE WHEN cpa.contains_glyphosate THEN 1 END) as glyphosate_applications
FROM companies c
JOIN company_pesticide_applications cpa ON c.id = cpa.company_id
GROUP BY c.id, c.cvr_number, c.company_name, c.municipality, cpa.application_year;

-- Municipality-level pesticide summary materialized view (geographic analysis)
CREATE MATERIALIZED VIEW IF NOT EXISTS municipality_pesticide_summary AS
SELECT
    municipality,
    application_year,
    COUNT(DISTINCT cvr_number) as company_count,
    SUM(total_belastning) as total_belastning,
    SUM(pfas_belastning) as pfas_belastning,
    SUM(diquat_belastning) as diquat_belastning,
    SUM(glyphosate_belastning) as glyphosate_belastning,
    SUM(total_applications) as total_applications,
    SUM(total_treated_area_ha) as total_treated_area_ha,
    AVG(avg_burden_per_application) as avg_municipal_burden,
    -- Rankings
    RANK() OVER (PARTITION BY application_year ORDER BY SUM(total_belastning) DESC) as burden_rank,
    RANK() OVER (PARTITION BY application_year ORDER BY SUM(total_treated_area_ha) DESC) as area_rank
FROM company_pesticide_summary
WHERE municipality IS NOT NULL
GROUP BY municipality, application_year;

-- Performance indexes for materialized views
CREATE INDEX IF NOT EXISTS idx_company_pesticide_summary_municipality_year ON company_pesticide_summary(municipality, application_year);
CREATE INDEX IF NOT EXISTS idx_company_pesticide_summary_belastning ON company_pesticide_summary(total_belastning DESC);
CREATE INDEX IF NOT EXISTS idx_municipality_pesticide_summary_year ON municipality_pesticide_summary(application_year);

-- Comments for documentation
COMMENT ON TABLE pesticide_products IS 'BMD pesticide database with chemical classifications for PFAS, diquat, and glyphosate';
COMMENT ON TABLE company_pesticide_applications IS 'Company-level pesticide applications from silver layer (non-disaggregated data)';
COMMENT ON MATERIALIZED VIEW company_pesticide_summary IS 'Pre-calculated company pesticide metrics for fast API queries';
COMMENT ON MATERIALIZED VIEW municipality_pesticide_summary IS 'Pre-calculated municipality pesticide statistics for geographic analysis';
