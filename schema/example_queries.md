# Danish Agricultural Database - Example DuckDB Queries

> **Purpose**: This document provides common SQL query examples for analyzing Danish agricultural data using DuckDB. All examples use realistic Danish agricultural domain scenarios.

---

## Query Categories

1. [Basic Data Exploration](#basic-data-exploration)
2. [Company Analysis](#company-analysis)
3. [Livestock and Animal Health](#livestock-and-animal-health)
4. [Pesticide Usage](#pesticide-usage)
5. [Geographic Analysis](#geographic-analysis)
6. [Environmental Compliance](#environmental-compliance)
7. [Temporal Analysis](#temporal-analysis)
8. [Advanced Aggregations](#advanced-aggregations)

---

## Basic Data Exploration

### Count Total Companies
**Danish**: "Hvor mange landbrugsvirksomheder er der i databasen?"
**English**: "How many agricultural companies are in the database?"

```sql
SELECT COUNT(*) AS total_companies
FROM companies;
```

### List Companies by Municipality
**Danish**: "Vis alle virksomheder i Varde Kommune"
**English**: "Show all companies in Varde Municipality"

```sql
SELECT
    cvr_number,
    company_name,
    address,
    city,
    postal_code
FROM companies
WHERE municipality = 'Varde'
ORDER BY company_name;
```

### Count Fields per Municipality
**Danish**: "Hvor mange landbrugsarealer er der i hver kommune?"
**English**: "How many agricultural fields are in each municipality?"

```sql
SELECT
    municipality,
    COUNT(*) AS field_count,
    SUM(area_ha) AS total_area_ha,
    ROUND(AVG(area_ha), 2) AS avg_field_size_ha
FROM field_boundaries
WHERE year = 2024
GROUP BY municipality
ORDER BY total_area_ha DESC;
```

---

## Company Analysis

### Company with Most Agricultural Land
**Danish**: "Hvilken virksomhed har mest landbrugsareal?"
**English**: "Which company has the most agricultural land?"

```sql
SELECT
    c.cvr_number,
    c.company_name,
    c.municipality,
    COUNT(fb.field_uuid) AS num_fields,
    ROUND(SUM(fb.area_ha), 2) AS total_area_ha
FROM companies c
JOIN field_boundaries fb ON c.id = fb.company_id
WHERE fb.year = 2024
GROUP BY c.cvr_number, c.company_name, c.municipality
ORDER BY total_area_ha DESC
LIMIT 10;
```

### Companies with Both Crops and Livestock
**Danish**: "Find virksomheder der både dyrker afgrøder og har husdyr"
**English**: "Find companies that have both crops and livestock"

```sql
SELECT
    c.cvr_number,
    c.company_name,
    COUNT(DISTINCT fb.field_uuid) AS num_fields,
    SUM(fb.area_ha) AS total_crop_area_ha,
    COUNT(DISTINCT ps.chr) AS num_production_sites,
    SUM(hs.count) AS total_animals
FROM companies c
JOIN field_boundaries fb ON c.id = fb.company_id
JOIN production_sites ps ON c.id = ps.company_id
JOIN herd_sizes hs ON ps.chr = hs.chr_number
WHERE fb.year = 2024
GROUP BY c.cvr_number, c.company_name
HAVING COUNT(DISTINCT fb.field_uuid) > 0
   AND COUNT(DISTINCT ps.chr) > 0
ORDER BY total_animals DESC;
```

### Largest Employers in Agriculture
**Danish**: "Hvilke landbrugsvirksomheder har flest ansatte?"
**English**: "Which agricultural companies have the most employees?"

```sql
SELECT
    c.cvr_number,
    c.company_name,
    c.municipality,
    ws.year,
    ws.avg_employee_count AS employees
FROM companies c
JOIN worker_yearly_summary ws ON c.id = ws.company_id
WHERE ws.year = 2024
ORDER BY employees DESC
LIMIT 20;
```

---

## Livestock and Animal Health

### Count Animals by Species
**Danish**: "Hvor mange dyr af hver art er der registreret?"
**English**: "How many animals of each species are registered?"

```sql
SELECT
    species_name,
    SUM(count) AS total_animals,
    COUNT(DISTINCT chr_number) AS num_herds
FROM herd_sizes
GROUP BY species_name
ORDER BY total_animals DESC;
```

### Pig Farms with Most Animals
**Danish**: "Hvilke svineproduktioner har flest grise?"
**English**: "Which pig farms have the most pigs?"

```sql
SELECT
    c.company_name,
    c.municipality,
    ps.chr,
    ps.site_name,
    hs.species_name,
    SUM(hs.count) AS total_pigs
FROM companies c
JOIN production_sites ps ON c.id = ps.company_id
JOIN herd_sizes hs ON ps.chr = hs.chr_number
WHERE hs.species_name LIKE '%Svin%'  -- Pigs
   OR hs.species_name LIKE '%svin%'
GROUP BY c.company_name, c.municipality, ps.chr, ps.site_name, hs.species_name
ORDER BY total_pigs DESC
LIMIT 20;
```

### Antibiotic Usage by Species
**Danish**: "Sammenlign antibiotikaforbrug mellem dyrearter"
**English**: "Compare antibiotic usage between animal species"

```sql
SELECT
    species_code,
    year,
    COUNT(DISTINCT chr_number) AS num_herds,
    SUM(animal_days) AS total_animal_days,
    SUM(animal_doses) AS total_antibiotic_doses,
    ROUND(AVG(add_per_100_dyr_per_dag), 2) AS avg_add_per_100_animals
FROM antibiotic_usage
WHERE year >= 2020
GROUP BY species_code, year
ORDER BY year DESC, total_antibiotic_doses DESC;
```

### Cattle Farms in Specific Region
**Danish**: "Find alle kvægbedrifter i Midtjylland"
**English**: "Find all cattle farms in Central Jutland"

```sql
SELECT
    c.company_name,
    c.municipality,
    ps.chr,
    ps.site_name,
    hs.species_name,
    hs.count AS cattle_count
FROM companies c
JOIN production_sites ps ON c.id = ps.company_id
JOIN herd_sizes hs ON ps.chr = hs.chr_number
WHERE (hs.species_name LIKE '%Kvæg%' OR hs.species_name LIKE '%kvæg%')
  AND c.municipality IN ('Viborg', 'Silkeborg', 'Herning', 'Holstebro', 'Struer')
ORDER BY cattle_count DESC;
```

---

## Pesticide Usage

### Most Used Pesticides
**Danish**: "Hvilke pesticider bruges mest i Danmark?"
**English**: "Which pesticides are used most in Denmark?"

```sql
SELECT
    pesticide_name,
    COUNT(*) AS application_count,
    SUM(treated_area_ha) AS total_treated_area_ha,
    SUM(total_burden_score * treated_area_ha) AS total_burden
FROM pesticide_applications
WHERE year = 2024
GROUP BY pesticide_name
ORDER BY application_count DESC
LIMIT 20;
```

### PFAS Pesticide Usage
**Danish**: "Hvor meget PFAS-holdigt pesticid bruges i landbruget?"
**English**: "How much PFAS-containing pesticide is used in agriculture?"

```sql
SELECT
    pa.pesticide_name,
    pa.year,
    COUNT(*) AS applications,
    SUM(pa.treated_area_ha) AS total_area_ha,
    COUNT(DISTINCT fb.company_id) AS num_companies
FROM pesticide_applications pa
JOIN field_boundaries fb ON pa.field_uuid = fb.field_uuid
WHERE pa.contains_pfas = TRUE
  AND pa.year >= 2020
GROUP BY pa.pesticide_name, pa.year
ORDER BY pa.year DESC, total_area_ha DESC;
```

### Companies with Highest Pesticide Burden
**Danish**: "Hvilke virksomheder har højest pesticide belastning?"
**English**: "Which companies have the highest pesticide burden?"

```sql
SELECT
    cps.company_name,
    cps.municipality,
    cps.application_year,
    cps.total_belastning AS pesticide_burden_score,
    cps.total_applications,
    cps.total_treated_area_ha,
    cps.pfas_belastning,
    cps.glyphosate_belastning
FROM company_pesticide_summary cps
WHERE cps.application_year = 2024
ORDER BY cps.total_belastning DESC
LIMIT 20;
```

### Glyphosate Usage by Municipality
**Danish**: "Sammenlign glyphosatforbrug på tværs af kommuner"
**English**: "Compare glyphosate usage across municipalities"

```sql
SELECT
    fb.municipality,
    COUNT(DISTINCT pa.id) AS glyphosate_applications,
    SUM(pa.treated_area_ha) AS treated_area_ha,
    COUNT(DISTINCT fb.company_id) AS num_companies
FROM pesticide_applications pa
JOIN field_boundaries fb ON pa.field_uuid = fb.field_uuid
WHERE pa.contains_glyphosate = TRUE
  AND pa.year = 2024
GROUP BY fb.municipality
ORDER BY treated_area_ha DESC;
```

### Pesticide Use on Organic vs Conventional Fields
**Danish**: "Sammenlign pesticidanvendelse mellem økologiske og konventionelle marker"
**English**: "Compare pesticide use between organic and conventional fields"

```sql
SELECT
    fyd.is_organic,
    COUNT(DISTINCT pa.field_uuid) AS fields_with_pesticides,
    COUNT(pa.id) AS total_applications,
    SUM(pa.treated_area_ha) AS total_treated_area_ha,
    AVG(pa.total_burden_score) AS avg_burden_score
FROM field_yearly_data fyd
LEFT JOIN pesticide_applications pa ON fyd.field_uuid = pa.field_uuid
    AND fyd.year = pa.year
WHERE fyd.year = 2024
GROUP BY fyd.is_organic;
```

---

## Geographic Analysis

### Fields Near Water Bodies
**Danish**: "Find marker tæt på vandområder"
**English**: "Find fields close to water bodies"

```sql
SELECT
    fb.field_identifier,
    fb.municipality,
    fb.area_ha,
    pa.pesticide_name,
    pa.proximity_water_m
FROM field_boundaries fb
JOIN pesticide_applications pa ON fb.field_uuid = pa.field_uuid
WHERE pa.proximity_water_m < 100  -- Within 100 meters
  AND pa.year = 2024
ORDER BY pa.proximity_water_m ASC;
```

### Agricultural Area by Region
**Danish**: "Hvor meget landbrugsareal er der i hver region?"
**English**: "How much agricultural area is in each region?"

```sql
SELECT
    CASE
        WHEN municipality IN ('København', 'Frederiksberg', 'Dragør', 'Tårnby') THEN 'Hovedstaden'
        WHEN municipality IN ('Helsingør', 'Hillerød', 'Hørsholm') THEN 'Nordsjælland'
        -- Add more municipalities per region as needed
        ELSE 'Andet'
    END AS region,
    COUNT(field_uuid) AS num_fields,
    SUM(area_ha) AS total_area_ha
FROM field_boundaries
WHERE year = 2024
GROUP BY region
ORDER BY total_area_ha DESC;
```

### Pesticide Applications Near Schools
**Danish**: "Find pesticidanvendelse nær skoler"
**English**: "Find pesticide applications near schools"

```sql
SELECT
    pa.field_uuid,
    pa.pesticide_name,
    pa.application_date,
    pa.proximity_school_m,
    pa.total_burden_score,
    fb.municipality
FROM pesticide_applications pa
JOIN field_boundaries fb ON pa.field_uuid = fb.field_uuid
WHERE pa.proximity_school_m < 500  -- Within 500 meters
  AND pa.year = 2024
ORDER BY pa.proximity_school_m ASC, pa.total_burden_score DESC;
```

---

## Environmental Compliance

### BNBO Environmental Status
**Danish**: "Hvor mange marker har gennemført nødvendige miljøforanstaltninger?"
**English**: "How many fields have completed necessary environmental measures?"

```sql
SELECT
    bnbo_status,
    COUNT(DISTINCT field_uuid) AS num_fields,
    SUM(area_ha) AS total_area_ha,
    SUM(action_required_hectares) AS action_needed_ha,
    SUM(completed_hectares) AS completed_ha
FROM field_bnbo_areas
WHERE year = 2024
GROUP BY bnbo_status
ORDER BY total_area_ha DESC;
```

### Companies with Most Environmental Action Needed
**Danish**: "Hvilke virksomheder skal gøre mest for miljøet?"
**English**: "Which companies need to do the most for the environment?"

```sql
SELECT
    c.company_name,
    c.municipality,
    SUM(fba.action_required_hectares) AS action_needed_ha,
    SUM(fba.completed_hectares) AS completed_ha,
    ROUND(
        100.0 * SUM(fba.completed_hectares) /
        NULLIF(SUM(fba.action_required_hectares + fba.completed_hectares), 0),
        1
    ) AS completion_percentage
FROM companies c
JOIN field_boundaries fb ON c.id = fb.company_id
JOIN field_bnbo_areas fba ON fb.field_uuid = fba.field_uuid
WHERE fba.year = 2024
GROUP BY c.company_name, c.municipality
HAVING SUM(fba.action_required_hectares) > 0
ORDER BY action_needed_ha DESC
LIMIT 20;
```

### Wetland Coverage on Agricultural Land
**Danish**: "Hvor meget af landbrugsarealet er vådt?"
**English**: "How much of the agricultural area is wetland?"

```sql
SELECT
    fb.municipality,
    COUNT(DISTINCT fwa.field_uuid) AS fields_with_wetlands,
    SUM(fb.area_ha) AS total_field_area_ha,
    SUM(fwa.water_covered_hectares) AS wetland_area_ha,
    ROUND(
        100.0 * SUM(fwa.water_covered_hectares) / SUM(fb.area_ha),
        2
    ) AS wetland_percentage
FROM field_boundaries fb
JOIN field_wetland_areas fwa ON fb.field_uuid = fwa.field_uuid
WHERE fwa.year = 2024
  AND fb.year = 2024
GROUP BY fb.municipality
ORDER BY wetland_area_ha DESC;
```

---

## Temporal Analysis

### Crop Rotation Over Time
**Danish**: "Hvordan har afgrødevalg ændret sig over tid?"
**English**: "How has crop choice changed over time?"

```sql
SELECT
    crop_name,
    year,
    COUNT(DISTINCT field_uuid) AS num_fields,
    SUM(area_ha) AS total_area_ha
FROM field_yearly_data
WHERE year >= 2020
  AND crop_name IS NOT NULL
GROUP BY crop_name, year
ORDER BY year DESC, total_area_ha DESC;
```

### Pesticide Usage Trends
**Danish**: "Er pesticidforbruget steget eller faldet?"
**English**: "Has pesticide usage increased or decreased?"

```sql
SELECT
    year,
    COUNT(*) AS total_applications,
    SUM(treated_area_ha) AS total_treated_area_ha,
    SUM(total_burden_score * treated_area_ha) AS total_burden,
    COUNT(DISTINCT field_uuid) AS unique_fields_treated
FROM pesticide_applications
WHERE year >= 2015
GROUP BY year
ORDER BY year DESC;
```

### Antibiotic Usage Trend
**Danish**: "Udvikling i antibiotikaforbrug i husdyrproduktion"
**English**: "Trend in antibiotic usage in livestock production"

```sql
SELECT
    year,
    species_code,
    COUNT(DISTINCT chr_number) AS num_herds,
    SUM(animal_doses) AS total_doses,
    SUM(animal_days) AS total_animal_days,
    ROUND(
        SUM(animal_doses) / NULLIF(SUM(animal_days), 0) * 100,
        3
    ) AS doses_per_100_animal_days
FROM antibiotic_usage
WHERE year >= 2015
GROUP BY year, species_code
ORDER BY year DESC, species_code;
```

### Field Size Changes
**Danish**: "Bliver marker større eller mindre?"
**English**: "Are fields getting larger or smaller?"

```sql
SELECT
    year,
    COUNT(*) AS num_fields,
    AVG(area_ha) AS avg_field_size_ha,
    MIN(area_ha) AS smallest_field_ha,
    MAX(area_ha) AS largest_field_ha,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY area_ha) AS median_field_size_ha
FROM field_boundaries
WHERE year >= 2015
GROUP BY year
ORDER BY year DESC;
```

---

## Advanced Aggregations

### Top Organic Producers
**Danish**: "Hvilke virksomheder har mest økologisk areal?"
**English**: "Which companies have the most organic area?"

```sql
SELECT
    c.company_name,
    c.municipality,
    COUNT(DISTINCT fyd.field_uuid) AS organic_fields,
    SUM(fyd.area_ha) AS total_organic_area_ha,
    -- Calculate percentage of company's land that is organic
    ROUND(
        100.0 * SUM(fyd.area_ha) /
        (SELECT SUM(fb.area_ha)
         FROM field_boundaries fb
         WHERE fb.company_id = c.id AND fb.year = 2024),
        1
    ) AS organic_percentage
FROM companies c
JOIN field_yearly_data fyd ON c.cvr_number = fyd.cvr_number
WHERE fyd.is_organic = TRUE
  AND fyd.year = 2024
GROUP BY c.company_name, c.municipality, c.id
ORDER BY total_organic_area_ha DESC
LIMIT 20;
```

### Carbon Emissions by Company
**Danish**: "Hvilke virksomheder har højest CO2-udledning?"
**English**: "Which companies have the highest CO2 emissions?"

```sql
SELECT
    c.company_name,
    c.municipality,
    cs.year,
    cs.total_co2e_tonnes,
    cs.rank_dk_total_co2e_tonnes AS national_rank,
    cs.rank_municipality_total_co2e_tonnes AS municipality_rank
FROM companies c
JOIN carbon_summary cs ON c.id = cs.company_id
WHERE cs.year = 2024
ORDER BY cs.total_co2e_tonnes DESC
LIMIT 20;
```

### Combined Environmental Score
**Danish**: "Samlede miljøpåvirkning per virksomhed"
**English**: "Combined environmental impact per company"

```sql
SELECT
    c.cvr_number,
    c.company_name,
    c.municipality,

    -- Pesticide burden
    COALESCE(cps.total_belastning, 0) AS pesticide_burden,

    -- Environmental compliance
    COALESCE(bes.action_required_hectares, 0) AS env_action_needed_ha,
    COALESCE(bes.compliance_rate, 0) AS env_compliance_rate,

    -- Carbon emissions
    COALESCE(cs.total_co2e_tonnes, 0) AS co2_emissions,

    -- Create combined score (example weighting)
    (
        COALESCE(cps.total_belastning, 0) * 0.3 +
        COALESCE(bes.action_required_hectares, 0) * 0.3 +
        COALESCE(cs.total_co2e_tonnes, 0) * 0.4
    ) AS combined_env_score

FROM companies c
LEFT JOIN company_pesticide_summary cps ON c.id = cps.company_id
    AND cps.application_year = 2024
LEFT JOIN bnbo_environmental_status bes ON c.id = bes.company_id
    AND bes.year = 2024
LEFT JOIN carbon_summary cs ON c.id = cs.company_id
    AND cs.year = 2024

WHERE c.id IN (
    SELECT DISTINCT company_id FROM field_boundaries WHERE year = 2024
)
ORDER BY combined_env_score DESC
LIMIT 50;
```

### Multi-Year Field Analysis
**Danish**: "Analyser samme mark over flere år"
**English**: "Analyze same field over multiple years"

```sql
SELECT
    fb.field_identifier,
    fb.municipality,
    fyd.year,
    fyd.crop_name,
    fyd.area_ha,
    fyd.is_organic,
    COUNT(pa.id) AS pesticide_applications,
    SUM(pa.total_burden_score * pa.treated_area_ha) AS total_pesticide_burden
FROM field_boundaries fb
JOIN field_yearly_data fyd ON fb.field_uuid = fyd.field_uuid
LEFT JOIN pesticide_applications pa ON fb.field_uuid = pa.field_uuid
    AND fyd.year = pa.year
WHERE fb.field_identifier = 'SPECIFIC_FIELD_ID'  -- Replace with actual field ID
  AND fyd.year >= 2020
GROUP BY fb.field_identifier, fb.municipality, fyd.year, fyd.crop_name, fyd.area_ha, fyd.is_organic
ORDER BY fyd.year DESC;
```

---

## Production and Economic Analysis

### Most Productive Crop Types
**Danish**: "Hvilke afgrøder giver højest økonomisk udbytte?"
**English**: "Which crops provide the highest economic yield?"

```sql
SELECT
    fyd.crop_name,
    fyd.year,
    COUNT(DISTINCT fyd.field_uuid) AS num_fields,
    SUM(fyd.area_ha) AS total_area_ha,
    AVG(fyd.area_ha) AS avg_field_size_ha
FROM field_yearly_data fyd
WHERE fyd.year = 2024
  AND fyd.crop_name IS NOT NULL
GROUP BY fyd.crop_name, fyd.year
ORDER BY total_area_ha DESC
LIMIT 20;
```

### Company Financial Performance
**Danish**: "Økonomiske nøgletal for landbrugsvirksomheder"
**English**: "Financial key figures for agricultural companies"

```sql
SELECT
    c.company_name,
    c.municipality,
    yf.reporting_year,
    yf.revenue,
    yf.profit,
    yf.assets,
    yf.equity,
    ROUND(100.0 * yf.equity / NULLIF(yf.assets, 0), 1) AS equity_ratio_pct,
    ROUND(100.0 * yf.profit / NULLIF(yf.revenue, 0), 1) AS profit_margin_pct
FROM companies c
JOIN yearly_financials yf ON c.id = yf.company_id
WHERE yf.reporting_year = 2024
  AND yf.revenue > 0
ORDER BY yf.revenue DESC
LIMIT 20;
```

### Worker Statistics by Municipality
**Danish**: "Medarbejderstatistik fordelt på kommuner"
**English**: "Employee statistics by municipality"

```sql
SELECT
    c.municipality,
    COUNT(DISTINCT c.id) AS num_companies,
    SUM(ws.avg_employee_count) AS total_employees,
    AVG(ws.avg_employee_count) AS avg_employees_per_company
FROM companies c
JOIN worker_yearly_summary ws ON c.id = ws.company_id
WHERE ws.year = 2024
GROUP BY c.municipality
ORDER BY total_employees DESC;
```

---

## Data Quality and Completeness Checks

### Check Data Completeness
**Danish**: "Hvor komplet er data for hver tabel?"
**English**: "How complete is data for each table?"

```sql
-- Companies with missing location data
SELECT
    COUNT(*) AS companies_without_location,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM companies), 1) AS percentage
FROM companies
WHERE address_geom IS NULL;

-- Fields without pesticide data
SELECT
    COUNT(DISTINCT fb.field_uuid) AS fields_without_pesticides,
    ROUND(
        100.0 * COUNT(DISTINCT fb.field_uuid) /
        (SELECT COUNT(DISTINCT field_uuid) FROM field_boundaries WHERE year = 2024),
        1
    ) AS percentage
FROM field_boundaries fb
WHERE fb.year = 2024
  AND NOT EXISTS (
      SELECT 1 FROM pesticide_applications pa
      WHERE pa.field_uuid = fb.field_uuid AND pa.year = 2024
  );
```

### Validate CVR Numbers
**Danish**: "Tjek gyldigheden af CVR-numre"
**English**: "Check validity of CVR numbers"

```sql
SELECT
    cvr_number,
    company_name,
    CASE
        WHEN LENGTH(CAST(cvr_number AS VARCHAR)) = 8 THEN 'Valid'
        ELSE 'Invalid'
    END AS cvr_status
FROM companies
WHERE LENGTH(CAST(cvr_number AS VARCHAR)) != 8
ORDER BY company_name;
```

---

## Joins with Multiple Data Sources

### Complete Company Profile
**Danish**: "Komplet virksomhedsprofil med alle data"
**English**: "Complete company profile with all data"

```sql
SELECT
    c.cvr_number,
    c.company_name,
    c.municipality,

    -- Field data
    COUNT(DISTINCT fb.field_uuid) AS num_fields,
    SUM(fb.area_ha) AS total_crop_area_ha,

    -- Livestock data
    COUNT(DISTINCT ps.chr) AS num_production_sites,
    SUM(hs.count) AS total_animals,

    -- Pesticide data
    COUNT(DISTINCT cpa.id) AS pesticide_applications,
    SUM(cpa.dosage_quantity) AS total_pesticide_kg,

    -- Financial data
    MAX(yf.revenue) AS latest_revenue,
    MAX(yf.profit) AS latest_profit,

    -- Worker data
    MAX(ws.avg_employee_count) AS employees

FROM companies c
LEFT JOIN field_boundaries fb ON c.id = fb.company_id AND fb.year = 2024
LEFT JOIN production_sites ps ON c.id = ps.company_id
LEFT JOIN herd_sizes hs ON ps.chr = hs.chr_number
LEFT JOIN company_pesticide_applications cpa ON c.id = cpa.company_id
    AND cpa.application_year = 2024
LEFT JOIN yearly_financials yf ON c.id = yf.company_id
    AND yf.reporting_year = 2024
LEFT JOIN worker_yearly_summary ws ON c.id = ws.company_id
    AND ws.year = 2024

WHERE c.cvr_number = '12345678'  -- Replace with actual CVR

GROUP BY c.cvr_number, c.company_name, c.municipality;
```

---

## Notes for Query Generation

### DuckDB-Specific Considerations

1. **String Operations**: Use `LIKE` or `~` for pattern matching
2. **Aggregations**: `PERCENTILE_CONT` for median calculations
3. **Window Functions**: Support for `ROW_NUMBER()`, `RANK()`, etc.
4. **NULL Handling**: Use `COALESCE()` and `NULLIF()` for safe calculations

### Common Patterns

1. **Temporal Joins**: Always join on year when combining temporal data
2. **Left Joins**: Use for optional relationships (not all companies have all data types)
3. **Distinct**: Use `COUNT(DISTINCT ...)` when counting across joins
4. **Rounding**: Use `ROUND(..., 2)` for readability

### Performance Tips

1. **Filter Early**: Apply WHERE clauses before joins when possible
2. **Use Indexes**: Join on indexed columns (CVR, CHR, field_uuid, company_id)
3. **Limit Results**: Use LIMIT for exploratory queries
4. **Materialized Views**: Use pre-aggregated views when available

---

## References

- **Table Relationships**: See `schema/relationships.md`
- **Data Lineage**: See `docs/DATA_LINEAGE_COMPREHENSIVE.md`
- **Schema Definition**: See `supabase/migrations/20250830083344_remote_schema.sql`

---

**Document Purpose**: This guide provides Gemini File Search with realistic query examples to generate accurate SQL from natural language requests in both Danish and English.

**Last Updated**: January 2025
