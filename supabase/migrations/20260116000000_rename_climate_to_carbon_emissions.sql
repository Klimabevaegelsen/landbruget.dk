-- Rename farm_climate_emissions to farm_carbon_emissions
-- This aligns with the naming convention change from "climate emissions" to "carbon emissions"

-- Drop existing indexes
DROP INDEX IF EXISTS idx_farm_climate_emissions_cvr_year;
DROP INDEX IF EXISTS idx_farm_climate_emissions_company_id;

-- Drop existing policies
DROP POLICY IF EXISTS "Allow public read" ON farm_climate_emissions;

-- Rename the table
ALTER TABLE farm_climate_emissions RENAME TO farm_carbon_emissions;

-- Recreate indexes with new names
CREATE INDEX idx_farm_carbon_emissions_cvr_year
    ON farm_carbon_emissions(cvr_number, year DESC);

CREATE INDEX idx_farm_carbon_emissions_company_id
    ON farm_carbon_emissions(company_id, year DESC);

-- Recreate policy
CREATE POLICY "Allow public read"
    ON farm_carbon_emissions
    FOR SELECT
    USING (true);
