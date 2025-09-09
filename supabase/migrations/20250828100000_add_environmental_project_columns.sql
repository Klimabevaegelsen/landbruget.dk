-- Migration: Add Environmental Project Columns to BNBO and Wetland Tables
-- Date: 2025-08-28 10:00:00
-- Description: Expand field_bnbo_areas and field_wetland_areas tables to include rich environmental project data
--              including water coverage, project status tracking, and environmental restoration details

-- Add environmental project columns to field_bnbo_areas table
ALTER TABLE field_bnbo_areas
ADD COLUMN IF NOT EXISTS water_covered_hectares NUMERIC,
ADD COLUMN IF NOT EXISTS coverage_percentage NUMERIC,
ADD COLUMN IF NOT EXISTS water_coverage_percentage NUMERIC,
ADD COLUMN IF NOT EXISTS action_required_hectares NUMERIC,
ADD COLUMN IF NOT EXISTS completed_hectares NUMERIC,
ADD COLUMN IF NOT EXISTS action_required_water_hectares NUMERIC,
ADD COLUMN IF NOT EXISTS completed_water_hectares NUMERIC,
ADD COLUMN IF NOT EXISTS status_count INTEGER;

-- Add water coverage columns to field_wetland_areas table
ALTER TABLE field_wetland_areas
ADD COLUMN IF NOT EXISTS water_covered_hectares NUMERIC,
ADD COLUMN IF NOT EXISTS coverage_percentage NUMERIC,
ADD COLUMN IF NOT EXISTS water_coverage_percentage NUMERIC;

-- Add comments to document the new columns
COMMENT ON COLUMN field_bnbo_areas.water_covered_hectares IS 'Water-covered area within BNBO zones (hectares)';
COMMENT ON COLUMN field_bnbo_areas.coverage_percentage IS 'Percentage of field covered by BNBO areas';
COMMENT ON COLUMN field_bnbo_areas.water_coverage_percentage IS 'Percentage of BNBO area that is water-covered';
COMMENT ON COLUMN field_bnbo_areas.action_required_hectares IS 'Environmental restoration work required (hectares)';
COMMENT ON COLUMN field_bnbo_areas.completed_hectares IS 'Environmental restoration work completed (hectares)';
COMMENT ON COLUMN field_bnbo_areas.action_required_water_hectares IS 'Water protection work required (hectares)';
COMMENT ON COLUMN field_bnbo_areas.completed_water_hectares IS 'Water protection work completed (hectares)';
COMMENT ON COLUMN field_bnbo_areas.status_count IS 'Number of different environmental project statuses';

COMMENT ON COLUMN field_wetland_areas.water_covered_hectares IS 'Water-covered area within wetland zones (hectares)';
COMMENT ON COLUMN field_wetland_areas.coverage_percentage IS 'Percentage of field covered by wetland areas';
COMMENT ON COLUMN field_wetland_areas.water_coverage_percentage IS 'Percentage of wetland area that is water-covered';

-- Create indexes for performance on the new columns used in queries
CREATE INDEX IF NOT EXISTS idx_field_bnbo_areas_action_required ON field_bnbo_areas(action_required_hectares) WHERE action_required_hectares > 0;
CREATE INDEX IF NOT EXISTS idx_field_bnbo_areas_completed ON field_bnbo_areas(completed_hectares) WHERE completed_hectares > 0;
CREATE INDEX IF NOT EXISTS idx_field_bnbo_areas_water_coverage ON field_bnbo_areas(water_coverage_percentage) WHERE water_coverage_percentage > 0;

CREATE INDEX IF NOT EXISTS idx_field_wetland_areas_water_coverage ON field_wetland_areas(water_coverage_percentage) WHERE water_coverage_percentage > 0;
CREATE INDEX IF NOT EXISTS idx_field_wetland_areas_coverage ON field_wetland_areas(coverage_percentage) WHERE coverage_percentage > 0;
