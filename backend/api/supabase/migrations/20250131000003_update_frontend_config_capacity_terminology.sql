-- Migration: Update frontend configuration to use capacity terminology
-- This migration documents the frontend configuration changes needed
-- Note: The actual JSON/YAML files need to be updated manually as they are not in the database

-- This migration serves as documentation for the frontend config changes:
-- 1. backend/api/supabase/functions/api/config.json
-- 2. backend/api/supabase/functions/api/config.yaml

-- Changes made to config.json:
-- - "Produktion (Antal dyr)" → "Kapacitet (Antal dyr)"
-- - "production_equiv" column references → "capacity_count"
-- - "Produktion (Dyreækvivalenter)" → "Kapacitet (Antal dyr)"
-- - "Rankering på landsplan (Prod. Ækv.)" → "Rankering på landsplan (Kapacitet)"
-- - "Rankering i kommunen (Prod. Ækv.)" → "Rankering i kommunen (Kapacitet)"
-- - "Produktion (Antal/Ækv.)" → "Kapacitet (Antal dyr)"
-- - "production_volume_equiv" → "total_animals" (for species-level data)
-- - "Rankering på landsplan (Art Prod.)" → "Rankering på landsplan (Art Kapacitet)"
-- - "Rankering i kommunen (Art Prod.)" → "Rankering i kommunen (Art Kapacitet)"
-- - "Dyreproduktion pr. år" → "Dyrekapacitet pr. år"
-- - "produktion pr. år" → "kapacitet pr. år"
-- - "produktion & placering" → "kapacitet & placering"

-- This ensures the frontend displays:
-- - "Kapacitet" instead of "Produktion"
-- - "Antal dyr" instead of "Ækv." (equivalents)
-- - Consistent terminology that reflects facility capacity, not production

SELECT 'Frontend configuration files updated to use capacity terminology instead of production' AS migration_note;
