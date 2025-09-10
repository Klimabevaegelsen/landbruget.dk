-- Delete nonsensical municipality views that mix incomparable data
-- These views create meaningless composite scores and mix different animal species

-- Drop the bad views
DROP MATERIALIZED VIEW IF EXISTS municipality_production_summary;
DROP MATERIALIZED VIEW IF EXISTS municipality_animal_health_summary;  
DROP MATERIALIZED VIEW IF EXISTS municipality_worker_safety_summary;
DROP MATERIALIZED VIEW IF EXISTS municipality_environmental_summary;

-- Keep municipality_land_use_summary (this one is actually good)
-- Keep municipality_pesticide_summary (need to check if it's meaningful)
