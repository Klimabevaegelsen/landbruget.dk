-- Migration: Create Landbrugsstøtte (Agricultural Subsidies) tables
--
-- This migration creates tables for Danish agricultural subsidies:
-- - landbrugstoette_eu_betalinger: EU CAP payments (~6.9B DKK)
-- - landbrugstoette_deminimis: National de minimis aid (~157M DKK)
-- - landbrugstoette_slagtepraemie: Slaughter premium (~1.15B DKK)
-- - landbrugstoette_cvr_oversigt: Materialized view for CVR lookups
--
-- CRITICAL: Always filter WHERE is_summary_row = FALSE to avoid double-counting!

-- ============================================================================
-- EU CAP PAYMENTS (Pillar 1 + Pillar 2)
-- ============================================================================

CREATE TABLE IF NOT EXISTS landbrugstoette_eu_betalinger (
    id BIGSERIAL PRIMARY KEY,

    -- Identifiers
    cvr VARCHAR(8) NOT NULL,
    regnskabsaar INTEGER NOT NULL,

    -- Scheme classification
    ordning_kode VARCHAR(50) NOT NULL,  -- GRUNDBETALING, GROEN_STOETTE, OEKOLOGISK, etc.
    ordning_dansk TEXT,                  -- Danish name
    scheme_original TEXT,                -- Original scheme name from source
    pillar INTEGER,                      -- 1 = EAGF, 2 = EAFRD

    -- Amounts in DKK
    eagf_dkk NUMERIC(14, 2) DEFAULT 0,          -- Pillar 1 (EU agricultural fund)
    eafrd_dkk NUMERIC(14, 2) DEFAULT 0,         -- Pillar 2 (EU rural development)
    medfinansiering_dkk NUMERIC(14, 2) DEFAULT 0, -- National co-financing
    total_dkk NUMERIC(14, 2) GENERATED ALWAYS AS (eagf_dkk + eafrd_dkk + medfinansiering_dkk) STORED,

    -- CRITICAL: Flag to prevent double-counting
    -- Summary rows ("Total for beneficiary") MUST be excluded from aggregations
    is_summary_row BOOLEAN DEFAULT FALSE,

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Constraints
    CONSTRAINT valid_cvr CHECK (cvr ~ '^\d{8}$'),
    CONSTRAINT valid_year CHECK (regnskabsaar >= 2014 AND regnskabsaar <= 2030)
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_eu_betalinger_cvr ON landbrugstoette_eu_betalinger(cvr);
CREATE INDEX IF NOT EXISTS idx_eu_betalinger_year ON landbrugstoette_eu_betalinger(regnskabsaar);
CREATE INDEX IF NOT EXISTS idx_eu_betalinger_ordning ON landbrugstoette_eu_betalinger(ordning_kode);
CREATE INDEX IF NOT EXISTS idx_eu_betalinger_cvr_year ON landbrugstoette_eu_betalinger(cvr, regnskabsaar);
CREATE INDEX IF NOT EXISTS idx_eu_betalinger_not_summary ON landbrugstoette_eu_betalinger(cvr, regnskabsaar)
    WHERE is_summary_row = FALSE;

-- Enable RLS
ALTER TABLE landbrugstoette_eu_betalinger ENABLE ROW LEVEL SECURITY;

-- Public read access (transparency project)
CREATE POLICY "Public can read EU payments" ON landbrugstoette_eu_betalinger
    FOR SELECT TO anon, authenticated
    USING (true);

-- Comments for documentation
COMMENT ON TABLE landbrugstoette_eu_betalinger IS
    'EU CAP payments from støtteoplysninger. IMPORTANT: Filter WHERE is_summary_row = FALSE to avoid double-counting!';
COMMENT ON COLUMN landbrugstoette_eu_betalinger.is_summary_row IS
    'TRUE for "Total for beneficiary" rows - MUST exclude from aggregations to avoid 2x overcounting';
COMMENT ON COLUMN landbrugstoette_eu_betalinger.ordning_kode IS
    'Standardized scheme code: GRUNDBETALING, GROEN_STOETTE, OEKOLOGISK, GRAESPLEJE, NATURA2000, etc.';


-- ============================================================================
-- DE MINIMIS (National Aid)
-- ============================================================================

CREATE TABLE IF NOT EXISTS landbrugstoette_deminimis (
    id BIGSERIAL PRIMARY KEY,

    -- Identifiers
    cvr VARCHAR(8) NOT NULL,

    -- Scheme
    ordning_kode VARCHAR(50) NOT NULL,  -- DEMINIMIS_MRDM, DEMINIMIS_MREG, etc.
    ordning_dansk TEXT,

    -- Ledger-aggregated amounts
    netto_dkk NUMERIC(14, 2) NOT NULL,           -- Net amount (credits - debits)
    brutto_kredit_dkk NUMERIC(14, 2) DEFAULT 0,  -- Total credits
    brutto_debit_dkk NUMERIC(14, 2) DEFAULT 0,   -- Total debits (repayments/corrections)
    antal_transaktioner INTEGER DEFAULT 0,

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT valid_cvr_dm CHECK (cvr ~ '^\d{8}$')
);

CREATE INDEX IF NOT EXISTS idx_deminimis_cvr ON landbrugstoette_deminimis(cvr);
CREATE INDEX IF NOT EXISTS idx_deminimis_ordning ON landbrugstoette_deminimis(ordning_kode);

ALTER TABLE landbrugstoette_deminimis ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Public can read de minimis" ON landbrugstoette_deminimis
    FOR SELECT TO anon, authenticated
    USING (true);

COMMENT ON TABLE landbrugstoette_deminimis IS
    'National de minimis aid. Amounts are net (credits minus debits) after ledger aggregation.';


-- ============================================================================
-- SLAUGHTER PREMIUM (Voluntary Coupled Support)
-- ============================================================================

CREATE TABLE IF NOT EXISTS landbrugstoette_slagtepraemie (
    id BIGSERIAL PRIMARY KEY,

    -- Identifiers
    cvr VARCHAR(8) NOT NULL,
    regnskabsaar INTEGER NOT NULL,

    -- Premium data
    antal_dyr INTEGER DEFAULT 0,
    praemie_dkk NUMERIC(14, 2) DEFAULT 0,
    praemie_per_dyr_dkk NUMERIC(10, 2) GENERATED ALWAYS AS (
        CASE WHEN antal_dyr > 0 THEN praemie_dkk / antal_dyr ELSE 0 END
    ) STORED,

    -- Scheme info
    ordning_kode VARCHAR(50) DEFAULT 'SLAGTEPRAEMIE',
    ordning_dansk TEXT DEFAULT 'Slagtepræmie (koblet støtte)',

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT valid_cvr_slp CHECK (cvr ~ '^\d{8}$'),
    CONSTRAINT valid_year_slp CHECK (regnskabsaar >= 2014 AND regnskabsaar <= 2030)
);

CREATE INDEX IF NOT EXISTS idx_slagtepraemie_cvr ON landbrugstoette_slagtepraemie(cvr);
CREATE INDEX IF NOT EXISTS idx_slagtepraemie_year ON landbrugstoette_slagtepraemie(regnskabsaar);

ALTER TABLE landbrugstoette_slagtepraemie ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Public can read slaughter premium" ON landbrugstoette_slagtepraemie
    FOR SELECT TO anon, authenticated
    USING (true);

COMMENT ON TABLE landbrugstoette_slagtepraemie IS
    'Voluntary coupled support for cattle slaughter. Premium per animal for cattle sent to slaughter.';


-- ============================================================================
-- SPATIAL SUBSIDIES (Tilsagn Arealer)
-- ============================================================================

CREATE TABLE IF NOT EXISTS landbrugstoette_tilsagn_arealer (
    id BIGSERIAL PRIMARY KEY,

    -- Identifiers
    cvr VARCHAR(8) NOT NULL,
    field_id VARCHAR(50),
    dataar INTEGER NOT NULL,

    -- Tilsagn classification
    tilsagnstype VARCHAR(20) NOT NULL,  -- OEKOLOGISK, GRAESPLEJE, MILJOE
    tilsagn_kode VARCHAR(10),            -- 36, 37, 66, 67, etc.
    tilsagn_beskrivelse TEXT,            -- Full Danish description
    tilsagn_type_original TEXT,          -- Original code from source

    -- Area and expected payment
    areal_ha NUMERIC(12, 4) NOT NULL,
    forventet_sats_kr_ha NUMERIC(10, 2),
    forventet_betaling_dkk NUMERIC(14, 2) GENERATED ALWAYS AS (areal_ha * forventet_sats_kr_ha) STORED,

    -- Commitment period
    tilsagn_start VARCHAR(20),
    tilsagn_slut VARCHAR(20),

    -- Geometry (PostGIS)
    geom GEOMETRY(MultiPolygon, 4326),

    -- Source tracking
    kilde VARCHAR(50),  -- fvm_organic_subsidies, fvm_grassland_subsidies, etc.

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT valid_cvr_tilsagn CHECK (cvr ~ '^\d{8}$'),
    CONSTRAINT valid_tilsagnstype CHECK (tilsagnstype IN ('OEKOLOGISK', 'GRAESPLEJE', 'MILJOE'))
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_tilsagn_cvr ON landbrugstoette_tilsagn_arealer(cvr);
CREATE INDEX IF NOT EXISTS idx_tilsagn_year ON landbrugstoette_tilsagn_arealer(dataar);
CREATE INDEX IF NOT EXISTS idx_tilsagn_type ON landbrugstoette_tilsagn_arealer(tilsagnstype);
CREATE INDEX IF NOT EXISTS idx_tilsagn_cvr_year ON landbrugstoette_tilsagn_arealer(cvr, dataar);
CREATE INDEX IF NOT EXISTS idx_tilsagn_geom ON landbrugstoette_tilsagn_arealer USING GIST(geom);

-- RLS
ALTER TABLE landbrugstoette_tilsagn_arealer ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Public can read tilsagn arealer" ON landbrugstoette_tilsagn_arealer
    FOR SELECT TO anon, authenticated
    USING (true);

COMMENT ON TABLE landbrugstoette_tilsagn_arealer IS
    'Spatial subsidies: organic farming, grassland management, environmental commitments. Includes geometry for field-level mapping.';
COMMENT ON COLUMN landbrugstoette_tilsagn_arealer.tilsagnstype IS
    'Type of commitment: OEKOLOGISK (organic), GRAESPLEJE (grassland), MILJOE (environmental)';
COMMENT ON COLUMN landbrugstoette_tilsagn_arealer.forventet_sats_kr_ha IS
    'Expected payment rate derived from actual data (more accurate than official rates)';


-- ============================================================================
-- CVR OVERVIEW MATERIALIZED VIEW (One-Stop Lookup)
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS landbrugstoette_cvr_oversigt AS
WITH eu_agg AS (
    SELECT
        cvr,
        regnskabsaar,
        -- Scheme breakdown (EXCLUDING summary rows!)
        SUM(CASE WHEN ordning_kode = 'GRUNDBETALING' AND NOT is_summary_row THEN total_dkk ELSE 0 END) AS grundbetaling_dkk,
        SUM(CASE WHEN ordning_kode = 'GROEN_STOETTE' AND NOT is_summary_row THEN total_dkk ELSE 0 END) AS groen_stoette_dkk,
        SUM(CASE WHEN ordning_kode LIKE 'OEKOLOGISK%' AND NOT is_summary_row THEN total_dkk ELSE 0 END) AS oekologisk_dkk,
        SUM(CASE WHEN ordning_kode LIKE 'GRAESPLEJE%' AND NOT is_summary_row THEN total_dkk ELSE 0 END) AS graespleje_dkk,
        SUM(CASE WHEN ordning_kode = 'NATURA2000' AND NOT is_summary_row THEN total_dkk ELSE 0 END) AS natura2000_dkk,
        -- Pillar totals
        SUM(CASE WHEN pillar = 1 AND NOT is_summary_row THEN total_dkk ELSE 0 END) AS pillar1_total_dkk,
        SUM(CASE WHEN pillar = 2 AND NOT is_summary_row THEN total_dkk ELSE 0 END) AS pillar2_total_dkk,
        -- EU total
        SUM(CASE WHEN NOT is_summary_row THEN total_dkk ELSE 0 END) AS eu_total_dkk
    FROM landbrugstoette_eu_betalinger
    GROUP BY cvr, regnskabsaar
),
deminimis_agg AS (
    SELECT
        cvr,
        SUM(netto_dkk) AS deminimis_dkk
    FROM landbrugstoette_deminimis
    GROUP BY cvr
),
slagtepraemie_agg AS (
    SELECT
        cvr,
        SUM(praemie_dkk) AS slagtepraemie_dkk,
        SUM(antal_dyr) AS slagtepraemie_dyr
    FROM landbrugstoette_slagtepraemie
    GROUP BY cvr
)
SELECT
    COALESCE(e.cvr, d.cvr, s.cvr) AS cvr,
    e.regnskabsaar,

    -- EU breakdown
    COALESCE(e.grundbetaling_dkk, 0) AS grundbetaling_dkk,
    COALESCE(e.groen_stoette_dkk, 0) AS groen_stoette_dkk,
    COALESCE(e.oekologisk_dkk, 0) AS oekologisk_dkk,
    COALESCE(e.graespleje_dkk, 0) AS graespleje_dkk,
    COALESCE(e.natura2000_dkk, 0) AS natura2000_dkk,
    COALESCE(e.pillar1_total_dkk, 0) AS pillar1_total_dkk,
    COALESCE(e.pillar2_total_dkk, 0) AS pillar2_total_dkk,
    COALESCE(e.eu_total_dkk, 0) AS eu_total_dkk,

    -- National
    COALESCE(d.deminimis_dkk, 0) AS deminimis_dkk,
    COALESCE(s.slagtepraemie_dkk, 0) AS slagtepraemie_dkk,
    COALESCE(s.slagtepraemie_dyr, 0) AS slagtepraemie_dyr,

    -- Grand total
    COALESCE(e.eu_total_dkk, 0) + COALESCE(d.deminimis_dkk, 0) + COALESCE(s.slagtepraemie_dkk, 0) AS total_stoette_dkk,

    -- Derived payment rate (reveals historical payment rights!)
    CASE
        WHEN COALESCE(e.grundbetaling_dkk, 0) > 0
        THEN e.grundbetaling_dkk  -- Will be divided by area when area data is available
        ELSE NULL
    END AS grundbetaling_for_rate_calc,

    -- Data quality indicator
    CASE
        WHEN e.eu_total_dkk IS NOT NULL AND e.eu_total_dkk > 0 THEN 'KOMPLET'
        WHEN d.deminimis_dkk IS NOT NULL OR s.slagtepraemie_dkk IS NOT NULL THEN 'DELVIS'
        ELSE 'MANGLER'
    END AS datakvalitet,

    NOW() AS refreshed_at

FROM eu_agg e
FULL OUTER JOIN deminimis_agg d ON e.cvr = d.cvr
FULL OUTER JOIN slagtepraemie_agg s ON COALESCE(e.cvr, d.cvr) = s.cvr;

-- Indexes on materialized view
CREATE UNIQUE INDEX IF NOT EXISTS idx_cvr_oversigt_pk ON landbrugstoette_cvr_oversigt(cvr, regnskabsaar);
CREATE INDEX IF NOT EXISTS idx_cvr_oversigt_cvr ON landbrugstoette_cvr_oversigt(cvr);
CREATE INDEX IF NOT EXISTS idx_cvr_oversigt_year ON landbrugstoette_cvr_oversigt(regnskabsaar);
CREATE INDEX IF NOT EXISTS idx_cvr_oversigt_total ON landbrugstoette_cvr_oversigt(total_stoette_dkk DESC);

COMMENT ON MATERIALIZED VIEW landbrugstoette_cvr_oversigt IS
    'One-stop CVR lookup for all agricultural subsidies. Refresh with: REFRESH MATERIALIZED VIEW CONCURRENTLY landbrugstoette_cvr_oversigt';


-- ============================================================================
-- VALIDATION VIEW (Data Quality Metrics)
-- ============================================================================

CREATE OR REPLACE VIEW landbrugstoette_validering AS
SELECT
    regnskabsaar,

    -- EU payments
    COUNT(DISTINCT CASE WHEN eu_total_dkk > 0 THEN cvr END) AS eu_cvr_count,
    SUM(eu_total_dkk) AS eu_total_dkk,
    SUM(pillar1_total_dkk) AS pillar1_total_dkk,
    SUM(pillar2_total_dkk) AS pillar2_total_dkk,

    -- National
    SUM(deminimis_dkk) AS deminimis_total_dkk,
    SUM(slagtepraemie_dkk) AS slagtepraemie_total_dkk,

    -- Grand total
    SUM(total_stoette_dkk) AS grand_total_dkk,

    -- Ratios (for validation)
    CASE
        WHEN SUM(pillar2_total_dkk) > 0
        THEN ROUND(SUM(pillar1_total_dkk) / SUM(pillar2_total_dkk), 2)
        ELSE NULL
    END AS pillar1_pillar2_ratio,

    NOW() AS calculated_at

FROM landbrugstoette_cvr_oversigt
GROUP BY regnskabsaar
ORDER BY regnskabsaar DESC;

COMMENT ON VIEW landbrugstoette_validering IS
    'Data quality validation metrics by year. Expected Pillar 1/Pillar 2 ratio: ~7:1';


-- ============================================================================
-- REFRESH FUNCTION
-- ============================================================================

CREATE OR REPLACE FUNCTION refresh_landbrugstoette_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY landbrugstoette_cvr_oversigt;
    RAISE NOTICE 'Refreshed landbrugstoette_cvr_oversigt at %', NOW();
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION refresh_landbrugstoette_views IS
    'Refresh all landbrugsstøtte materialized views. Call after data updates.';


-- ============================================================================
-- UPDATED_AT TRIGGER
-- ============================================================================

CREATE OR REPLACE FUNCTION update_landbrugstoette_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_eu_betalinger_updated_at
    BEFORE UPDATE ON landbrugstoette_eu_betalinger
    FOR EACH ROW EXECUTE FUNCTION update_landbrugstoette_updated_at();

CREATE TRIGGER trigger_deminimis_updated_at
    BEFORE UPDATE ON landbrugstoette_deminimis
    FOR EACH ROW EXECUTE FUNCTION update_landbrugstoette_updated_at();

CREATE TRIGGER trigger_slagtepraemie_updated_at
    BEFORE UPDATE ON landbrugstoette_slagtepraemie
    FOR EACH ROW EXECUTE FUNCTION update_landbrugstoette_updated_at();

CREATE TRIGGER trigger_tilsagn_updated_at
    BEFORE UPDATE ON landbrugstoette_tilsagn_arealer
    FOR EACH ROW EXECUTE FUNCTION update_landbrugstoette_updated_at();
