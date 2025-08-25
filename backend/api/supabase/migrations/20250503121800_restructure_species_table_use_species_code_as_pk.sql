-- Migration: Restructure species table to use species_code as primary key
-- Date: 2025-08-25
-- Description:
--   - Replace arbitrary species_id with logical species_code as primary key
--   - Update all referencing tables to use species_code instead of species_id
--   - This makes the schema more intuitive (species_code 12 = Kvæg, 15 = Svin, etc.)
--   - Eliminates the arbitrary mapping between species_code and species_id

-- Step 1: Drop existing foreign key constraints
ALTER TABLE animal_capacity_log DROP CONSTRAINT IF EXISTS animal_capacity_log_species_id_fkey;
ALTER TABLE animal_transports DROP CONSTRAINT IF EXISTS animal_transports_species_id_fkey;
ALTER TABLE production_sites DROP CONSTRAINT IF EXISTS production_sites_main_species_id_fkey;
ALTER TABLE vet_events DROP CONSTRAINT IF EXISTS vet_events_species_id_fkey;

-- Step 2: Backup current species data
CREATE TEMP TABLE species_backup AS SELECT * FROM species;

-- Step 3: Drop and recreate species table with species_code as primary key
DROP TABLE species CASCADE;

CREATE TABLE species (
    species_code TEXT PRIMARY KEY,
    species_name TEXT NOT NULL,
    default_animal_equivalent NUMERIC
);

-- Step 4: Restore species data with species_code as primary key
INSERT INTO species (species_code, species_name, default_animal_equivalent)
SELECT species_code, species_name, default_animal_equivalent
FROM species_backup;

-- Step 5: Drop dependent materialized views that reference the columns we're changing
DROP MATERIALIZED VIEW IF EXISTS site_details_summary_ranked CASCADE;
DROP MATERIALIZED VIEW IF EXISTS site_species_production_ranked CASCADE;

-- Step 6: Update referencing tables to use species_code instead of species_id

-- Update animal_capacity_log
ALTER TABLE animal_capacity_log ALTER COLUMN species_id TYPE TEXT;
ALTER TABLE animal_capacity_log RENAME COLUMN species_id TO species_code;

-- Update animal_transports
ALTER TABLE animal_transports ALTER COLUMN species_id TYPE TEXT;
ALTER TABLE animal_transports RENAME COLUMN species_id TO species_code;

-- Update production_sites
ALTER TABLE production_sites ALTER COLUMN main_species_id TYPE TEXT;
ALTER TABLE production_sites RENAME COLUMN main_species_id TO main_species_code;

-- Update vet_events
ALTER TABLE vet_events ALTER COLUMN species_id TYPE TEXT;
ALTER TABLE vet_events RENAME COLUMN species_id TO species_code;

-- Step 7: Recreate foreign key constraints with new column names
ALTER TABLE animal_capacity_log
    ADD CONSTRAINT animal_capacity_log_species_code_fkey
    FOREIGN KEY (species_code) REFERENCES species(species_code);

ALTER TABLE animal_transports
    ADD CONSTRAINT animal_transports_species_code_fkey
    FOREIGN KEY (species_code) REFERENCES species(species_code);

ALTER TABLE production_sites
    ADD CONSTRAINT production_sites_main_species_code_fkey
    FOREIGN KEY (main_species_code) REFERENCES species(species_code);

ALTER TABLE vet_events
    ADD CONSTRAINT vet_events_species_code_fkey
    FOREIGN KEY (species_code) REFERENCES species(species_code);

-- Step 8: Recreate any dropped materialized views if needed
-- (Note: The DROP TABLE CASCADE may have dropped materialized views -
--  they should be recreated by subsequent migrations or application startup)

-- Verification queries (commented out for production)
-- SELECT COUNT(*) as total_species FROM species;
-- SELECT species_code, species_name FROM species ORDER BY species_code::INTEGER LIMIT 5;
