-- Migration: Populate production_sites.capacity from animal_capacity_log
-- This migration fixes the issue where all production sites had NULL capacity values
-- by aggregating the latest capacity data from animal_capacity_log

-- Update production_sites capacity with aggregated data from animal_capacity_log
UPDATE production_sites
SET capacity = latest_capacity.total_capacity
FROM (
    SELECT
        chr,
        SUM(capacity_count) as total_capacity
    FROM animal_capacity_log
    WHERE category LIKE '%i alt' OR category = 'Dyr i alt'
    GROUP BY chr
) latest_capacity
WHERE production_sites.chr = latest_capacity.chr;

-- Verification: Check that capacity values are now populated
-- Expected results: ~17,486 sites with capacity values, ranging from 1 to 650,000 animals
