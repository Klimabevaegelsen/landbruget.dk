-- Create site_species_production_ranked materialized view
-- This view provides species production rankings by facility using actual animal counts

-- Drop if exists to avoid conflicts
DROP MATERIALIZED VIEW IF EXISTS public.site_species_production_ranked CASCADE;

-- Create the materialized view using actual animal counts (not equivalents)
CREATE MATERIALIZED VIEW public.site_species_production_ranked AS
WITH site_species_totals AS (
    SELECT
        apl.chr,
        EXTRACT(YEAR FROM apl.capacity_date) AS year,
        apl.species_code,
        SUM(apl.capacity_count) AS total_animals,
        ps.municipality
    FROM public.animal_capacity_log apl
    JOIN public.production_sites ps ON (apl.chr = ps.chr)
    WHERE apl.species_code IS NOT NULL
    AND (apl.category LIKE '% i alt' OR apl.category = 'Dyr i alt')
    GROUP BY apl.chr, EXTRACT(YEAR FROM apl.capacity_date), apl.species_code, ps.municipality
)
SELECT
    sst.chr,
    sst.year,
    sst.species_code,
    sst.species_code AS species_id,  -- species_code serves as species_id for API compatibility
    s.species_name,
    sst.municipality,
    sst.total_animals,
    -- Keep production_volume_equiv for backward compatibility (same as total_animals)
    sst.total_animals AS production_volume_equiv,
    RANK() OVER (PARTITION BY sst.year, sst.species_code ORDER BY sst.total_animals DESC NULLS LAST) AS rank_dk_species_production,
    RANK() OVER (PARTITION BY sst.year, sst.municipality, sst.species_code ORDER BY sst.total_animals DESC NULLS LAST) AS rank_municipality_species_production
FROM site_species_totals sst
JOIN public.species s ON (sst.species_code = s.species_code);

-- Set ownership
ALTER TABLE public.site_species_production_ranked OWNER TO postgres;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_site_species_prod_ranked_year_species_ranks
ON public.site_species_production_ranked USING btree (year, species_code, rank_dk_species_production);

CREATE INDEX IF NOT EXISTS idx_site_species_prod_ranked_year_mun_species_ranks
ON public.site_species_production_ranked USING btree (year, municipality, species_code, rank_municipality_species_production);

-- Create unique index on the natural key
CREATE UNIQUE INDEX IF NOT EXISTS pk_site_species_production_ranked
ON public.site_species_production_ranked USING btree (chr, year, species_code);

-- Grant permissions
GRANT ALL ON TABLE public.site_species_production_ranked TO anon;
GRANT ALL ON TABLE public.site_species_production_ranked TO authenticated;
GRANT ALL ON TABLE public.site_species_production_ranked TO service_role;

-- Add comment for documentation
COMMENT ON MATERIALIZED VIEW public.site_species_production_ranked IS 'Species production rankings by facility - uses actual animal counts from animal_capacity_log.capacity_count. Uses species_code foreign keys. Created in migration 20250127000002.';

-- Refresh the materialized view to populate it with data
REFRESH MATERIALIZED VIEW public.site_species_production_ranked;
