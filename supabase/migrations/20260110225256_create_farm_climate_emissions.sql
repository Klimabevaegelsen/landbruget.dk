-- farm_climate_emissions: Store calculated emissions per farm
CREATE TABLE IF NOT EXISTS farm_climate_emissions (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    company_id UUID REFERENCES companies(id),
    cvr_number BIGINT NOT NULL,
    year INTEGER NOT NULL,

    -- Total emissions
    total_co2e_kg DECIMAL(14,2),

    -- By category (JSON for flexibility)
    emissions_by_category JSONB,

    -- Intensity metrics
    co2e_per_ha DECIMAL(10,2),
    co2e_per_animal_unit DECIMAL(10,2),
    co2e_per_production_unit DECIMAL(10,4),

    -- Data quality
    data_completeness DECIMAL(3,2),  -- 0.00-1.00
    calculation_timestamp TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(cvr_number, year)
);

-- Enable RLS
ALTER TABLE farm_climate_emissions ENABLE ROW LEVEL SECURITY;

-- Public read policy
CREATE POLICY "Allow public read"
    ON farm_climate_emissions
    FOR SELECT
    USING (true);

-- Index for fast lookups
CREATE INDEX idx_farm_climate_emissions_cvr_year
    ON farm_climate_emissions(cvr_number, year DESC);

CREATE INDEX idx_farm_climate_emissions_company_id
    ON farm_climate_emissions(company_id, year DESC);
