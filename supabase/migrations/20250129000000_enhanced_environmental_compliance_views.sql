-- Migration: Enhanced Environmental Compliance Views
-- Date: 2025-01-29 00:00:00
-- Description: Create comprehensive views for environmental compliance visualization
--              including BNBO, wetlands, and water project coverage analysis

-- Create comprehensive environmental compliance summary view
CREATE OR REPLACE VIEW environmental_compliance_summary AS
WITH company_environmental_data AS (
    SELECT
        c.id AS company_id,
        c.municipality,
        EXTRACT(YEAR FROM CURRENT_DATE) AS year,

        -- BNBO problematic areas (action required)
        COALESCE(SUM(CASE WHEN fba.bnbo_status IN ('Action Required', 'Gennemgået, indsats nødvendig', 'Ikke gennemgået (default værdi)')
                         THEN fba.area_ha ELSE 0 END), 0) AS bnbo_problematic_hectares,

        -- BNBO dealt with areas (completed)
        COALESCE(SUM(CASE WHEN fba.bnbo_status IN ('Completed', 'Gennemgået, indsats ikke nødvendig', 'Indsats gennemført', 'Ingen erhvervsmæssig anvendelse af pesticider')
                         THEN fba.area_ha ELSE 0 END), 0) AS bnbo_dealt_with_hectares,

        -- BNBO miljø- og klimaprojekt covered areas
        COALESCE(SUM(fba.water_covered_hectares), 0) AS bnbo_water_covered_hectares,

        -- Lavbundsjorde (Low-lying soils) problematic areas
        COALESCE(SUM(CASE WHEN fwa.status IN ('action_required', 'not_dealt_with')
                         THEN fwa.area_ha ELSE 0 END), 0) AS wetlands_problematic_hectares,

        -- Lavbundsjorde dealt with areas
        COALESCE(SUM(CASE WHEN fwa.status IN ('completed', 'protected')
                         THEN fwa.area_ha ELSE 0 END), 0) AS wetlands_dealt_with_hectares,

        -- Lavbundsjorde miljø- og klimaprojekt covered areas
        COALESCE(SUM(fwa.water_covered_hectares), 0) AS wetlands_water_covered_hectares

    FROM companies c
    LEFT JOIN field_boundaries fb ON c.id = fb.company_id
    LEFT JOIN field_bnbo_areas fba ON fb.field_uuid = fba.field_uuid
    LEFT JOIN field_wetland_areas fwa ON fb.field_uuid = fwa.field_uuid
    WHERE fba.year = EXTRACT(YEAR FROM CURRENT_DATE) OR fwa.year = EXTRACT(YEAR FROM CURRENT_DATE) OR (fba.year IS NULL AND fwa.year IS NULL)
    GROUP BY c.id, c.municipality
)
SELECT
    company_id,
    municipality,
    year,

    -- Total problematic areas
    (bnbo_problematic_hectares + wetlands_problematic_hectares) AS total_problematic_hectares,

    -- Total dealt with areas
    (bnbo_dealt_with_hectares + wetlands_dealt_with_hectares) AS total_dealt_with_hectares,

    -- Miljø- og klimaprojekt coverage
    (bnbo_water_covered_hectares + wetlands_water_covered_hectares) AS total_water_covered_hectares,

    -- Compliance percentage
    CASE
        WHEN (bnbo_problematic_hectares + wetlands_problematic_hectares + bnbo_dealt_with_hectares + wetlands_dealt_with_hectares) > 0
        THEN ROUND(
            (bnbo_dealt_with_hectares + wetlands_dealt_with_hectares) * 100.0 /
            (bnbo_problematic_hectares + wetlands_problematic_hectares + bnbo_dealt_with_hectares + wetlands_dealt_with_hectares),
            2
        )
        ELSE 100.0
    END AS compliance_percentage,

    -- Miljø- og klimaprojekt coverage percentage
    CASE
        WHEN (bnbo_problematic_hectares + wetlands_problematic_hectares) > 0
        THEN ROUND(
            (bnbo_water_covered_hectares + wetlands_water_covered_hectares) * 100.0 /
            (bnbo_problematic_hectares + wetlands_problematic_hectares),
            2
        )
        ELSE 0.0
    END AS water_coverage_percentage

FROM company_environmental_data;

-- Create BNBO environmental status view for time series
CREATE OR REPLACE VIEW bnbo_environmental_status AS
WITH yearly_bnbo_data AS (
    SELECT
        c.id AS company_id,
        c.municipality,
        fba.year,

        -- Action required areas
        COALESCE(SUM(CASE WHEN fba.bnbo_status IN ('Action Required', 'Gennemgået, indsats nødvendig', 'Ikke gennemgået (default værdi)')
                         THEN fba.area_ha ELSE 0 END), 0) AS action_required_hectares,

        -- Completed areas
        COALESCE(SUM(CASE WHEN fba.bnbo_status IN ('Completed', 'Gennemgået, indsats ikke nødvendig', 'Indsats gennemført', 'Ingen erhvervsmæssig anvendelse af pesticider')
                         THEN fba.area_ha ELSE 0 END), 0) AS completed_hectares,

        -- Miljø- og klimaprojekt covered areas
        COALESCE(SUM(fba.water_covered_hectares), 0) AS water_covered_hectares

    FROM companies c
    LEFT JOIN field_boundaries fb ON c.id = fb.company_id
    LEFT JOIN field_bnbo_areas fba ON fb.field_uuid = fba.field_uuid
    WHERE fba.year IS NOT NULL
    GROUP BY c.id, c.municipality, fba.year
)
SELECT
    company_id,
    municipality,
    year,
    action_required_hectares,
    completed_hectares,
    water_covered_hectares,

    -- Compliance rate
    CASE
        WHEN (action_required_hectares + completed_hectares) > 0
        THEN ROUND(completed_hectares * 100.0 / (action_required_hectares + completed_hectares), 2)
        ELSE 100.0
    END AS compliance_rate

FROM yearly_bnbo_data
WHERE year >= 2020;

-- Create lavbundsjorde (low-lying soils) environmental status view for time series
CREATE OR REPLACE VIEW wetlands_environmental_status AS
WITH yearly_wetlands_data AS (
    SELECT
        c.id AS company_id,
        c.municipality,
        fwa.year,

        -- Action required areas
        COALESCE(SUM(CASE WHEN fwa.status IN ('action_required', 'not_dealt_with')
                         THEN fwa.area_ha ELSE 0 END), 0) AS action_required_hectares,

        -- Completed areas
        COALESCE(SUM(CASE WHEN fwa.status IN ('completed', 'protected')
                         THEN fwa.area_ha ELSE 0 END), 0) AS completed_hectares,

        -- Miljø- og klimaprojekt covered areas
        COALESCE(SUM(fwa.water_covered_hectares), 0) AS water_covered_hectares

    FROM companies c
    LEFT JOIN field_boundaries fb ON c.id = fb.company_id
    LEFT JOIN field_wetland_areas fwa ON fb.field_uuid = fwa.field_uuid
    WHERE fwa.year IS NOT NULL
    GROUP BY c.id, c.municipality, fwa.year
)
SELECT
    company_id,
    municipality,
    year,
    action_required_hectares,
    completed_hectares,
    water_covered_hectares,

    -- Protection rate
    CASE
        WHEN (action_required_hectares + completed_hectares) > 0
        THEN ROUND(completed_hectares * 100.0 / (action_required_hectares + completed_hectares), 2)
        ELSE 100.0
    END AS protection_rate

FROM yearly_wetlands_data
WHERE year >= 2020;

-- Create environmental action status view for horizontal stacked bar chart
CREATE OR REPLACE VIEW environmental_action_status AS
WITH current_year_data AS (
    SELECT
        c.id AS company_id,
        c.municipality,
        EXTRACT(YEAR FROM CURRENT_DATE) AS year,

        -- BNBO areas by status
        'BNBO' AS area_type,
        CASE
            WHEN fba.bnbo_status IN ('Action Required', 'Gennemgået, indsats nødvendig', 'Ikke gennemgået (default værdi)') THEN 'Kræver Handling'
            WHEN fba.bnbo_status IN ('Completed', 'Gennemgået, indsats ikke nødvendig', 'Indsats gennemført', 'Ingen erhvervsmæssig anvendelse af pesticider') THEN 'Gennemført'
            WHEN fba.water_covered_hectares > 0 THEN 'Areal til Klima- eller Miljøprojekter'
            ELSE 'Ukendt'
        END AS action_status,
        COALESCE(SUM(fba.area_ha), 0) AS area_hectares

    FROM companies c
    LEFT JOIN field_boundaries fb ON c.id = fb.company_id
    LEFT JOIN field_bnbo_areas fba ON fb.field_uuid = fba.field_uuid
    WHERE fba.year = EXTRACT(YEAR FROM CURRENT_DATE) OR fba.year IS NULL
    GROUP BY c.id, c.municipality, area_type, action_status

    UNION ALL

    SELECT
        c.id AS company_id,
        c.municipality,
        EXTRACT(YEAR FROM CURRENT_DATE) AS year,

        -- Lavbundsjorde areas by status
        'Lavbundsjorde' AS area_type,
        CASE
            WHEN fwa.status IN ('action_required', 'not_dealt_with') THEN 'Kræver Handling'
            WHEN fwa.status IN ('completed', 'protected') THEN 'Gennemført'
            WHEN fwa.water_covered_hectares > 0 THEN 'Areal til Klima- eller Miljøprojekter'
            ELSE 'Ukendt'
        END AS action_status,
        COALESCE(SUM(fwa.area_ha), 0) AS area_hectares

    FROM companies c
    LEFT JOIN field_boundaries fb ON c.id = fb.company_id
    LEFT JOIN field_wetland_areas fwa ON fb.field_uuid = fwa.field_uuid
    WHERE fwa.year = EXTRACT(YEAR FROM CURRENT_DATE) OR fwa.year IS NULL
    GROUP BY c.id, c.municipality, area_type, action_status
)
SELECT
    company_id,
    municipality,
    year,
    area_type,
    action_status,
    area_hectares
FROM current_year_data
WHERE area_hectares > 0;

-- Create water coverage effectiveness view for stacked bar chart
CREATE OR REPLACE VIEW water_coverage_effectiveness AS
WITH yearly_coverage_data AS (
    SELECT
        c.id AS company_id,
        c.municipality,
        fba.year,
        'BNBO Klima- eller Miljøprojekter' AS coverage_type,
        COALESCE(SUM(fba.water_covered_hectares), 0) AS coverage_hectares

    FROM companies c
    LEFT JOIN field_boundaries fb ON c.id = fb.company_id
    LEFT JOIN field_bnbo_areas fba ON fb.field_uuid = fba.field_uuid
    WHERE fba.year IS NOT NULL AND fba.water_covered_hectares > 0
    GROUP BY c.id, c.municipality, fba.year, coverage_type

    UNION ALL

    SELECT
        c.id AS company_id,
        c.municipality,
        fwa.year,
        'Lavbundsjorde Klima- eller Miljøprojekter' AS coverage_type,
        COALESCE(SUM(fwa.water_covered_hectares), 0) AS coverage_hectares

    FROM companies c
    LEFT JOIN field_boundaries fb ON c.id = fb.company_id
    LEFT JOIN field_wetland_areas fwa ON fb.field_uuid = fwa.field_uuid
    WHERE fwa.year IS NOT NULL AND fwa.water_covered_hectares > 0
    GROUP BY c.id, c.municipality, fwa.year, coverage_type
)
SELECT
    company_id,
    municipality,
    year,
    coverage_type,
    coverage_hectares
FROM yearly_coverage_data
WHERE year >= 2020 AND coverage_hectares > 0;

-- Create environmental compliance ranking view
CREATE OR REPLACE VIEW environmental_compliance_ranking AS
WITH company_compliance_metrics AS (
    SELECT
        ecs.*,

        -- Calculate environmental risk score (higher = more problematic)
        CASE
            WHEN ecs.compliance_percentage < 50 THEN 100
            WHEN ecs.compliance_percentage < 70 THEN 75
            WHEN ecs.compliance_percentage < 85 THEN 50
            WHEN ecs.compliance_percentage < 95 THEN 25
            ELSE 10
        END +
        CASE
            WHEN ecs.water_coverage_percentage < 25 THEN 50
            WHEN ecs.water_coverage_percentage < 50 THEN 25
            WHEN ecs.water_coverage_percentage < 75 THEN 10
            ELSE 0
        END AS environmental_risk_score,

        -- Non-compliant areas
        ecs.total_problematic_hectares AS total_non_compliant_hectares

    FROM environmental_compliance_summary ecs
)
SELECT
    company_id,
    municipality,
    year,
    total_non_compliant_hectares,
    environmental_risk_score,
    compliance_percentage,
    water_coverage_percentage,

    -- Rankings
    RANK() OVER (ORDER BY compliance_percentage DESC) AS rank_dk_compliance,
    RANK() OVER (PARTITION BY municipality ORDER BY compliance_percentage DESC) AS rank_municipality_compliance,
    RANK() OVER (ORDER BY water_coverage_percentage DESC) AS rank_dk_water_coverage,
    RANK() OVER (PARTITION BY municipality ORDER BY water_coverage_percentage DESC) AS rank_municipality_water_coverage

FROM company_compliance_metrics;

-- Add comments to document the views
COMMENT ON VIEW environmental_compliance_summary IS 'Comprehensive environmental compliance overview combining BNBO and lavbundsjorde data with miljø- og klimaprojekt coverage';
COMMENT ON VIEW bnbo_environmental_status IS 'Time series view of BNBO environmental status showing action required, completed, and miljø- og klimaprojekt covered areas';
COMMENT ON VIEW wetlands_environmental_status IS 'Time series view of lavbundsjorde (low-lying soils) environmental status showing action required, completed, and miljø- og klimaprojekt covered areas';
COMMENT ON VIEW environmental_action_status IS 'Current year environmental action status breakdown by area type and action status';
COMMENT ON VIEW water_coverage_effectiveness IS 'Time series view of miljø- og klimaprojekt coverage effectiveness for BNBO and lavbundsjorde';
COMMENT ON VIEW environmental_compliance_ranking IS 'Environmental compliance rankings and risk scoring for companies';
