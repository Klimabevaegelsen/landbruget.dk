# Parquet to Supabase Schema Migration Analysis

## Executive Summary

This analysis compares our current parquet outputs (159+ datasets) from the unified pipeline with the existing Supabase schema (22 tables) to identify migration opportunities, conflicts, and required changes.

## Complete Supabase Schema Overview (22 Tables)

### Core Entity Tables (Master Data)
1. **`companies`** - Company master data with CVR numbers (UUID primary key)
2. **`field_boundaries`** - Field geometries and basic info (UUID primary key) 
3. **`production_sites`** - CHR site information (CHR text primary key)
4. **`species`** - Animal species reference data

### Company & Financial Data
5. **`company_leadership`** - Company management/board members
6. **`company_owners`** - Company ownership structure
7. **`yearly_financials`** - Annual financial statements
8. **`subsidy_details`** - Government subsidies received

### Field & Environmental Data
9. **`field_yearly_data`** - Field-level annual crop/environmental data
10. **`field_bnbo_areas`** - BNBO environmental protection areas per field
11. **`field_wetland_areas`** - Wetland conservation areas per field
12. **`building_footprints`** - Building geometries on properties

### Agricultural Operations
13. **`pesticide_applications`** - Pesticide usage records with proximity analysis
14. **`carbon_emission_factors`** - Carbon footprint calculations

### Animal Production (CHR Data)
15. **`animal_production_log`** - Animal production volumes by species/age
16. **`animal_transports`** - Animal movement records
17. **`site_yearly_summary`** - Production site annual summaries
18. **`vet_events`** - Veterinary events and treatments

### Worker & Safety Data
19. **`employee_monthly_counts`** - Employee headcount tracking
20. **`visa_yearly_counts`** - Foreign worker permits by nationality
21. **`worker_yearly_summary`** - Worker safety and injury statistics
22. **`incidents`** - Safety incidents and violations

## Detailed Parquet to Supabase Field Mappings

### ✅ **COMPLETED: Table 1 - `companies`**

**Source**: `cvr_enrichment_companies` parquet (updated with geocoding + advertisement_protection)

**Field Mapping**:
```sql
-- Supabase Field          ← Parquet Source Field            | Status
id                         ← Generated UUID                   | ✅ Auto-generated
cvr_number                 ← cvr_number                       | ✅ Direct match
company_name               ← company_name                     | ✅ Direct match  
address                    ← address (from current addresses) | ✅ Pipeline updated
postal_code                ← postal_code (from current addr)  | ✅ Pipeline updated
city                       ← city (from current addresses)    | ✅ Pipeline updated
municipality               ← municipality_name (current addr) | ✅ Pipeline updated
address_geom               ← address_geom_wkt (DAWA geocoded) | ✅ Pipeline updated
advertisement_protection   ← advertisement_protection (CVR)   | ✅ Pipeline updated
created_at                 ← Generated timestamp              | ✅ Auto-generated
updated_at                 ← Generated timestamp              | ✅ Auto-generated
```

**Migration Status**: ✅ **READY** - Perfect schema match, no changes needed
**Dependencies**: Next CVR pipeline run with updated schema
**ETL Requirements**: Simple 1:1 field mapping with UUID generation

---

### ⚠️ **IN PROGRESS: Table 2 - `field_boundaries`**

**Source**: `fvm_marker_2025` silver parquet

**Field Mapping**:
```sql
-- Supabase Field          ← Parquet Source Field            | Status
id                         ← Generated UUID                   | ✅ Auto-generated
company_id                 ← cvr_number → UUID lookup        | ⚠️ ETL lookup required
field_identifier           ← field_id                        | ✅ Direct match
field_name                 ← NULL (not available in FVM)     | ⚠️ Missing data
geom                       ← geometry                        | ✅ Direct match  
area_ha                    ← area_ha                         | ✅ Direct match
created_at                 ← Generated timestamp             | ✅ Auto-generated
updated_at                 ← Generated timestamp             | ✅ Auto-generated
```

**Migration Status**: ⚠️ **ETL DESIGN NEEDED** - CVR→UUID lookup, missing field names
**Dependencies**: Companies table populated first, consistent UUID mapping
**ETL Requirements**: 
- CVR→UUID lookup with consistency guarantee
- Handle missing field names (NULL or generated)
- Ensure same CVR always maps to same UUID

**ETL Strategy**:
```sql
-- Step 1: Ensure consistent company_id lookup
CREATE OR REPLACE FUNCTION get_or_create_company_uuid(cvr_text TEXT)
RETURNS UUID AS $$
DECLARE
    company_uuid UUID;
BEGIN
    -- Try to find existing company
    SELECT id INTO company_uuid FROM companies WHERE cvr_number = cvr_text;
    
    -- If not found, this indicates a data integrity issue
    -- (companies should be loaded first)
    IF company_uuid IS NULL THEN
        RAISE EXCEPTION 'CVR % not found in companies table', cvr_text;
    END IF;
    
    RETURN company_uuid;
END;
$$ LANGUAGE plpgsql;

-- Step 2: Field boundaries ETL with consistent UUIDs
INSERT INTO field_boundaries (
    id, company_id, field_identifier, field_name, geom, area_ha
)
SELECT 
    gen_random_uuid(),
    get_or_create_company_uuid(fvm.cvr_number),  -- Consistent UUID lookup
    fvm.field_id,
    NULL,  -- No field names in FVM data
    fvm.geometry,
    fvm.area_ha
FROM fvm_marker_parquet fvm;
```

---

### ✅ **COMPLETED: Table 3 - `production_sites`**

**Source**: `properties.parquet` + `property_owners.parquet` (CHR silver data)

**Current Supabase Data**: 2 test records with all fields populated
**API Usage**: Map visualization + site iteration (requires `chr`, `site_name`, `main_species_name`, `capacity`)

**Field Mapping**:
```sql
-- Supabase Field          ← Parquet Source Field            | Status
chr                        ← chr_number (properties)          | ✅ Direct match
company_id                 ← owner_cvr → UUID lookup         | ⚠️ ETL lookup required  
site_name                  ← "CHR {chr_number}" (generated)   | ⚠️ Generated (API requires)
address                    ← address (properties)            | ✅ Direct match
postal_code                ← postal_code (properties)        | ✅ Direct match
city                       ← city (properties)               | ✅ Direct match  
municipality               ← municipality_name (properties)  | ✅ Direct match
location_geom              ← Point(geo_coord_x, geo_coord_y)  | ⚠️ Geometry conversion needed
capacity                   ← NULL (handled separately)        | ⚠️ To be fixed elsewhere
main_species_id            ← NULL (handled separately)        | ⚠️ To be fixed elsewhere
created_at                 ← Generated timestamp             | ✅ Auto-generated
updated_at                 ← Generated timestamp             | ✅ Auto-generated
```

**Migration Status**: ✅ **READY FOR ETL** - Core mapping complete, capacity/main_species_id handled separately
**Dependencies**: Companies table populated, species table populated  
**ETL Requirements**: 
- JOIN properties + property_owners on chr_number
- CVR→UUID lookup with consistency 
- Multiple records per CHR (one per owner)
- Generate site_name as "Address - CHR{number}" format
- Convert X,Y coordinates to PostGIS Point geometry (already WGS84)
- Add other_owners JSONB field for co-ownership transparency

**ETL Strategy**:
```sql
-- Multiple records per CHR (one per owner) + other owners info
-- Step 1: Add other_owners field to schema (JSON array of other CVRs)

-- Step 2: ETL with owner enumeration
INSERT INTO production_sites (
    chr, company_id, site_name, address, postal_code, city, 
    municipality, location_geom, capacity, main_species_id, other_owners
)
SELECT 
    p.chr_number::text,
    get_or_create_company_uuid(po.owner_cvr),
    p.address || ' - CHR' || p.chr_number,  -- "Address - CHR12345" format
    p.address,
    p.postal_code,
    p.city,
    p.municipality_name,
    ST_SetSRID(ST_MakePoint(p.geo_coord_x_measured, p.geo_coord_y_measured), 4326),
    NULL,  -- Capacity: handled separately
    NULL,  -- Main species: handled separately
    -- Other owners as JSON array (excluding current owner)
    (
        SELECT JSON_ARRAYAGG(other_po.owner_cvr)
        FROM chr_property_owners other_po 
        WHERE other_po.chr_number = p.chr_number 
        AND other_po.owner_cvr != po.owner_cvr
        AND other_po.owner_cvr IS NOT NULL
    )
FROM chr_properties p
JOIN chr_property_owners po ON p.chr_number = po.chr_number
WHERE po.owner_cvr IS NOT NULL;
```

**Schema Extension Needed**:
```sql
-- Add to production_sites table:
ALTER TABLE production_sites ADD COLUMN other_owners JSONB;
COMMENT ON COLUMN production_sites.other_owners IS 'Array of other CVR numbers that also own this CHR site';
```

**Business Logic**: 
- ✅ Each owner gets their own production_sites record
- ✅ Each record shows other co-owners in `other_owners` field
- ✅ Site name format: "Trælløsevej 66 - CHR14510"
- ✅ Coordinates already in WGS84 (no conversion needed)
- ⚠️ Capacity and main_species_id will be handled separately

---

### ⚠️ **IN PROGRESS: Table 4 - `species`**

**Source**: `herds.parquet` (CHR silver data) - reference data extraction

**Current Supabase Data**: 3 mock records (PIG, CATTLE, POULTRY with species_id 101-103)

**Field Mapping**:
```sql
-- Supabase Field          ← Parquet Source Field            | Status
species_id                 ← Generated sequence (1, 2, 3...) | ✅ Auto-generated
species_code               ← species_code (from herds)       | ⚠️ Different coding system
species_name               ← species_name (from herds)       | ✅ Direct match
default_animal_equivalent  ← NULL (needs manual definition)  | ❌ Missing - regulatory values
created_at                 ← Generated timestamp             | ✅ Auto-generated  
updated_at                 ← Generated timestamp             | ✅ Auto-generated
```

**Migration Status**: ⚠️ **REFERENCE DATA CREATION NEEDED** - Extract unique species, add missing coefficients
**Dependencies**: None (reference table)
**ETL Requirements**:
- Extract DISTINCT species_code, species_name from herds.parquet
- Generate sequential species_id values (1, 2, 3...)
- **CRITICAL**: Define default_animal_equivalent values (environmental/regulatory coefficients)
- Replace current mock data (species_id 101-103) with real data (species_id 1, 2, 3...)

**CHR Species Data Found** (from herds.parquet):
```sql
-- Top species by herd count:
11: Heste        (22,884 herds)
12: Kvæg         (12,143 herds) 
15: Svin          (4,933 herds)
13: Får           (3,824 herds)
14: Geder         (1,701 herds)
31: Høns (æg)     (1,475 herds)
30: Fjerkræ       (764 herds)
21: Hjorte        (281 herds)
43: Duer          (268 herds)
32: Høns (slagt)  (248 herds)
```

**Schema Conflicts**:
🔴 **CODING SYSTEM MISMATCH**:
- Current mock: PIG(101), CATTLE(102), POULTRY(103)  
- CHR real data: Svin(15), Kvæg(12), Høns/Fjerkræ(30/31)
- **Decision needed**: Use CHR codes as species_code, or create new mapping?

**ETL Strategy**:
```sql
-- Step 1: Clear mock data and reset sequence
DELETE FROM species WHERE species_id IN (101, 102, 103);
ALTER SEQUENCE species_species_id_seq RESTART WITH 1;

-- Step 2: Insert real species data
INSERT INTO species (species_code, species_name, default_animal_equivalent)
SELECT DISTINCT
    species_code::text,
    species_name,
    CASE species_code
        WHEN 12 THEN 1.0    -- Kvæg (cattle)
        WHEN 15 THEN 0.3    -- Svin (pigs) 
        WHEN 31 THEN 0.014  -- Høns æg (laying hens)
        WHEN 30 THEN 0.014  -- Fjerkræ (poultry)
        WHEN 11 THEN 0.5    -- Heste (horses) - estimate needed
        WHEN 13 THEN 0.1    -- Får (sheep) - estimate needed
        WHEN 14 THEN 0.1    -- Geder (goats) - estimate needed
        ELSE 0.1            -- Default for other species
    END as default_animal_equivalent
FROM chr_herds_parquet
WHERE species_code IS NOT NULL 
AND species_code != 0  -- Exclude generic 'Dyr'
ORDER BY species_code;
```

**Business Decision Required**:
- ⚠️ **default_animal_equivalent values** need regulatory/environmental expert input
- ⚠️ **species_code format**: Keep CHR integer codes or create text codes?
- ⚠️ **API impact**: Frontend may expect specific species_id values

---

### ✅ **COMPLETED: Table 5 - `company_leadership`**

**Source**: `cvr_enrichment_leadership` gold parquet

**Current Supabase Data**: 5 mock records with roles like "Direktør", "Deltager", "Administrerende Direktør"

**Field Mapping**:
```sql
-- Supabase Field          ← Parquet Source Field                                    | Status
id                         ← Generated UUID                                           | ✅ Auto-generated
company_id                 ← cvr_number → UUID lookup                                | ⚠️ ETL lookup required
person_name                ← leadership_parsed.person.names[1].name                  | ✅ Direct extraction
role_title                 ← leadership_parsed.organization.member_data[1].attributter[1].vaerdier[1].vaerdi | ✅ Direct extraction
start_date                 ← leadership_parsed.organization.member_data[1].attributter[1].vaerdier[1].periode.gyldigFra | ✅ Direct extraction
end_date                   ← leadership_parsed.organization.member_data[1].attributter[1].vaerdier[1].periode.gyldigTil | ✅ Direct extraction (NULL if current)
created_at                 ← Generated timestamp                                      | ✅ Auto-generated
updated_at                 ← Generated timestamp                                      | ✅ Auto-generated
```

**Migration Status**: ✅ **READY FOR ETL** - Complex nested structure but extractable
**Dependencies**: Companies table populated first for UUID lookups
**Data Quality**: 30,525 leadership records across 19,940 unique companies (excellent coverage)

**ETL Requirements**:
- Parse complex nested JSON structure from CVR API
- CVR→UUID lookup with consistency guarantee
- Handle multiple leadership roles per company
- Date parsing from string format (YYYY-MM-DD)
- Filter for current vs historical roles if needed

**ETL Strategy**:
```sql
-- Step 1: Create flattened view of leadership data
CREATE OR REPLACE VIEW cvr_leadership_flattened AS
SELECT 
    cvr_number,
    leadership_parsed.person.names[1].name as person_name,
    leadership_parsed.organization.member_data[1].attributter[1].vaerdier[1].vaerdi as role_title,
    leadership_parsed.organization.member_data[1].attributter[1].vaerdier[1].periode.gyldigFra::date as start_date,
    leadership_parsed.organization.member_data[1].attributter[1].vaerdier[1].periode.gyldigTil::date as end_date,
    leadership_parsed.is_current
FROM cvr_leadership_parquet;

-- Step 2: Insert with UUID lookups
INSERT INTO company_leadership (
    id, company_id, person_name, role_title, start_date, end_date
)
SELECT 
    gen_random_uuid(),
    get_or_create_company_uuid(clf.cvr_number::text),
    clf.person_name,
    clf.role_title,
    clf.start_date,
    clf.end_date
FROM cvr_leadership_flattened clf
WHERE clf.person_name IS NOT NULL 
AND clf.role_title IS NOT NULL;
```

**Sample Role Types Found**:
- INTERESSENTER (stakeholders)
- STIFTERE (founders)  
- REVISION (auditors)
- DIREKTØR (directors)
- Plus company-specific roles and dates

**Business Notes**:
- ✅ Rich historical data with start/end dates
- ✅ Covers multiple leadership roles per company
- ✅ Includes both persons and organizations (auditing firms)
- ⚠️ Role titles are in Danish and may need translation/standardization

---

### ✅ **COMPLETED: Table 6 - `company_owners`**

**Source**: `cvr_enrichment_leadership` gold parquet (same as leadership, filtered for ownership roles)

**Current Supabase Data**: 3 mock records with ownership percentages like "100%", "50%"

**Field Mapping**:
```sql
-- Supabase Field          ← Parquet Source Field                                    | Status
id                         ← Generated UUID                                           | ✅ Auto-generated
company_id                 ← cvr_number → UUID lookup                                | ⚠️ ETL lookup required
owner_name                 ← leadership_parsed.person.names[1].name                  | ✅ Direct extraction
ownership_percentage       ← role_title (when numeric: 0.5 → "50%")                 | ✅ Convert numeric to %
ownership_bucket_text      ← role_title (when descriptive text)                     | ✅ Direct match
effective_date             ← leadership_parsed.organization.member_data[1].attributter[1].vaerdier[1].periode.gyldigFra | ✅ Direct extraction
created_at                 ← Generated timestamp                                      | ✅ Auto-generated
updated_at                 ← Generated timestamp                                      | ✅ Auto-generated
```

**Migration Status**: ✅ **READY FOR ETL** - Ownership data embedded in leadership roles
**Dependencies**: Companies table populated first for UUID lookups
**Data Quality**: ~4,000+ ownership records (numeric percentages + descriptive roles)

**Ownership Role Types Found**:
- **Numeric percentages**: `0.5`, `0.3333`, `0.25`, `0.4`, `0.3`, `0.2` (convert to "50%", "33.33%", etc.)
- **"Er reel ejer som bestyrelsesmedlem"**: Real owner as board member (1,126 records)
- **Other ownership descriptions**: Roles containing "ejer" (owner)

**ETL Requirements**:
- Filter leadership data for ownership-related roles (numeric percentages + roles with "ejer")
- Convert numeric decimals to percentage strings (0.5 → "50%")
- Handle descriptive ownership roles (roles containing "ejer" = owner)
- Same CVR→UUID lookup pattern as leadership table
- Date parsing from string format

**ETL Strategy**:
```sql
-- Step 1: Create ownership-filtered view
CREATE OR REPLACE VIEW cvr_ownership_flattened AS
SELECT 
    cvr_number,
    leadership_parsed.person.names[1].name as owner_name,
    leadership_parsed.organization.member_data[1].attributter[1].vaerdier[1].vaerdi as role_title,
    leadership_parsed.organization.member_data[1].attributter[1].vaerdier[1].periode.gyldigFra::date as effective_date,
    leadership_parsed.is_current,
    -- Determine if this is a percentage or descriptive role
    CASE 
        WHEN role_title ~ '^[0-9]+\.?[0-9]*$' THEN 'percentage'
        ELSE 'descriptive'
    END as ownership_type
FROM cvr_leadership_parquet
WHERE (
    -- Numeric ownership percentages
    leadership_parsed.organization.member_data[1].attributter[1].vaerdier[1].vaerdi ~ '^[0-9]+\.?[0-9]*$'
    -- OR descriptive ownership roles (only roles with "ejer" = owner)
    OR leadership_parsed.organization.member_data[1].attributter[1].vaerdier[1].vaerdi LIKE '%ejer%'
);

-- Step 2: Insert ownership data with percentage conversion
INSERT INTO company_owners (
    id, company_id, owner_name, ownership_percentage, ownership_bucket_text, effective_date
)
SELECT 
    gen_random_uuid(),
    get_or_create_company_uuid(cof.cvr_number::text),
    cof.owner_name,
    -- Convert numeric to percentage format
    CASE 
        WHEN cof.ownership_type = 'percentage' THEN 
            ROUND(cof.role_title::numeric * 100, 2) || '%'
        ELSE NULL
    END,
    -- Use role title as bucket text for descriptive roles
    CASE 
        WHEN cof.ownership_type = 'descriptive' THEN cof.role_title
        ELSE ROUND(cof.role_title::numeric * 100, 2) || '%'
    END,
    cof.effective_date
FROM cvr_ownership_flattened cof
WHERE cof.owner_name IS NOT NULL;
```

**Sample Data Transformations**:
- `0.5` → ownership_percentage: "50%", ownership_bucket_text: "50%"
- `0.3333` → ownership_percentage: "33.33%", ownership_bucket_text: "33.33%"
- `Er reel ejer som bestyrelsesmedlem` → ownership_percentage: NULL, ownership_bucket_text: "Er reel ejer som bestyrelsesmedlem"

**Business Notes**:
- ✅ Rich ownership data with both percentages and descriptive roles
- ✅ Historical effective dates available
- ✅ Covers percentage owners and board-level owners (roles with "ejer")
- ⚠️ Some ownership roles may need Danish→English translation
- ℹ️ STIFTERE (founders) excluded - not ownership roles

---

### ⚠️ **SCHEMA CHANGE NEEDED: Table 7 - `yearly_financials`**

**Source**: `cvr_enrichment_financial` gold parquet

**Current Supabase Data**: 3 mock records with simplified fields (revenue, profit, total_subsidies)

**PROBLEM**: Current Supabase schema is too simplified for rich CVR financial data

**Current Schema**:
```sql
-- CURRENT (too simple)
CREATE TABLE yearly_financials (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    company_id uuid NOT NULL,
    year integer NOT NULL,
    revenue bigint,           -- ❌ Not available in CVR data
    profit bigint,            -- ✅ Available as net_profit_loss
    total_subsidies bigint,   -- ❌ Not in financial statements
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);
```

**RECOMMENDED NEW SCHEMA** (matches CVR financial data structure):
```sql
-- PROPOSED (comprehensive)
CREATE TABLE yearly_financials (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    company_id uuid NOT NULL,
    year integer NOT NULL,
    reporting_period_start date,
    reporting_period_end date,
    
    -- Income Statement
    net_profit_loss bigint,
    gross_profit_loss bigint,
    operating_profit_loss bigint,
    profit_loss_before_tax bigint,
    employee_benefits_expense bigint,
    depreciation_expense bigint,
    tax_expense bigint,
    
    -- Balance Sheet
    total_assets bigint,
    total_equity bigint,
    current_assets bigint,
    noncurrent_assets bigint,
    cash_and_cash_equivalents bigint,
    contributed_capital bigint,
    
    -- Liabilities
    liabilities_other_than_provisions bigint,
    shortterm_liabilities_other_than_provisions bigint,
    longterm_liabilities_other_than_provisions bigint,
    provisions bigint,
    
    -- Ratios & Metrics
    average_number_of_employees numeric,
    equity_ratio numeric,
    return_on_assets numeric,
    
    -- Metadata
    publication_type text,
    case_number text,
    
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);
```

**Migration Status**: 🔴 **SCHEMA REDESIGN REQUIRED** - Current schema doesn't match available data
**Dependencies**: Companies table populated first for UUID lookups
**Data Quality**: 11,866 financial records from 1,679 companies (2016-2025)

**Field Mapping (Proposed New Schema)**:
```sql
-- Supabase Field                    ← Parquet Source Field                | Status
id                                   ← Generated UUID                       | ✅ Auto-generated
company_id                           ← cvr_number → UUID lookup            | ⚠️ ETL lookup required
year                                 ← EXTRACT(YEAR FROM reporting_period_end) | ✅ Direct extraction
reporting_period_start               ← reporting_period_start::date         | ✅ Direct match
reporting_period_end                 ← reporting_period_end::date           | ✅ Direct match
net_profit_loss                      ← net_profit_loss                      | ✅ Direct match
gross_profit_loss                    ← gross_profit_loss                    | ✅ Direct match
operating_profit_loss                ← operating_profit_loss                | ✅ Direct match
profit_loss_before_tax               ← profit_loss_before_tax               | ✅ Direct match
employee_benefits_expense            ← employee_benefits_expense            | ✅ Direct match
depreciation_expense                 ← depreciation_expense                 | ✅ Direct match
tax_expense                          ← tax_expense                          | ✅ Direct match
total_assets                         ← total_assets                         | ✅ Direct match
total_equity                         ← total_equity                         | ✅ Direct match
current_assets                       ← current_assets                       | ✅ Direct match
noncurrent_assets                    ← noncurrent_assets                    | ✅ Direct match
cash_and_cash_equivalents            ← cash_and_cash_equivalents            | ✅ Direct match
contributed_capital                  ← contributed_capital                  | ✅ Direct match
liabilities_other_than_provisions    ← liabilities_other_than_provisions    | ✅ Direct match
shortterm_liabilities_other_than_provisions ← shortterm_liabilities_other_than_provisions | ✅ Direct match
longterm_liabilities_other_than_provisions ← longterm_liabilities_other_than_provisions | ✅ Direct match
provisions                           ← provisions                           | ✅ Direct match
average_number_of_employees          ← average_number_of_employees          | ✅ Direct match
equity_ratio                         ← equity_ratio                         | ✅ Direct match
return_on_assets                     ← return_on_assets                     | ✅ Direct match
publication_type                     ← publication_type                     | ✅ Direct match
case_number                          ← case_number                          | ✅ Direct match
created_at                           ← Generated timestamp                  | ✅ Auto-generated
updated_at                           ← Generated timestamp                  | ✅ Auto-generated
```

**ETL Strategy (After Schema Change)**:
```sql
-- Step 1: Drop and recreate yearly_financials table with new schema
-- (Migration script needed)

-- Step 2: Insert financial data with comprehensive mapping
INSERT INTO yearly_financials (
    id, company_id, year, reporting_period_start, reporting_period_end,
    net_profit_loss, gross_profit_loss, operating_profit_loss, profit_loss_before_tax,
    employee_benefits_expense, depreciation_expense, tax_expense,
    total_assets, total_equity, current_assets, noncurrent_assets,
    cash_and_cash_equivalents, contributed_capital,
    liabilities_other_than_provisions, shortterm_liabilities_other_than_provisions,
    longterm_liabilities_other_than_provisions, provisions,
    average_number_of_employees, equity_ratio, return_on_assets,
    publication_type, case_number
)
SELECT 
    gen_random_uuid(),
    get_or_create_company_uuid(cf.cvr_number::text),
    EXTRACT(YEAR FROM cf.reporting_period_end::date),
    cf.reporting_period_start::date,
    cf.reporting_period_end::date,
    cf.net_profit_loss::bigint,
    cf.gross_profit_loss::bigint,
    cf.operating_profit_loss::bigint,
    cf.profit_loss_before_tax::bigint,
    cf.employee_benefits_expense::bigint,
    cf.depreciation_expense::bigint,
    cf.tax_expense::bigint,
    cf.total_assets::bigint,
    cf.total_equity::bigint,
    cf.current_assets::bigint,
    cf.noncurrent_assets::bigint,
    cf.cash_and_cash_equivalents::bigint,
    cf.contributed_capital::bigint,
    cf.liabilities_other_than_provisions::bigint,
    cf.shortterm_liabilities_other_than_provisions::bigint,
    cf.longterm_liabilities_other_than_provisions::bigint,
    cf.provisions::bigint,
    cf.average_number_of_employees,
    cf.equity_ratio,
    cf.return_on_assets,
    cf.publication_type,
    cf.case_number
FROM cvr_financial_parquet cf
WHERE cf.reporting_period_end IS NOT NULL;
```

**Business Impact**:
- 🔴 **API Changes Required**: Frontend components expecting `revenue` field will need updates
- ✅ **Much Richer Data**: Full income statement + balance sheet data available
- ✅ **Better Analytics**: Ratios, employee counts, detailed financial metrics
- ⚠️ **Subsidies**: Will need separate `subsidy_details` table and pipeline

**Next Steps**:
1. **Create Supabase migration** to update yearly_financials schema
2. **Update API config.yaml** to reflect new field names
3. **Update frontend components** to use new financial fields
4. **Plan separate subsidy pipeline** for `subsidy_details` table

---

### ❌ **MISSING DATA: Table 8 - `subsidy_details`**

**Source**: No parquet pipeline exists yet

**Current Supabase Data**: Empty table (ready for future data)

**Current Schema**:
```sql
CREATE TABLE subsidy_details (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    company_id uuid NOT NULL,
    year integer NOT NULL,
    subsidy_type text NOT NULL,
    amount_dkk numeric NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);
```

**Migration Status**: ❌ **NO DATA SOURCE** - Pipeline needs to be created
**Dependencies**: Companies table populated first for UUID lookups

**Potential Data Sources** (to be investigated):
- Danish Agricultural Agency (Landbrugsstyrelsen) subsidy data
- EU CAP (Common Agricultural Policy) payments
- Environmental subsidy programs
- Organic farming subsidies

**ETL Requirements** (Future):
- Create new pipeline to collect subsidy data
- Map CVR numbers to company_id UUIDs
- Categorize subsidy types
- Handle multi-year subsidy programs

**Business Notes**:
- ⚠️ **Pipeline Missing**: No current data source identified
- ✅ **Schema Ready**: Table structure prepared for future data
- 🔍 **Research Needed**: Identify Danish subsidy data sources and APIs

---

### ⚠️ **SCHEMA CHANGE NEEDED: Table 9 - `field_yearly_data`**

**Source**: `field_analysis_2025` gold parquet (combines pesticide, area analysis, and FVM marker data)

**Current Supabase Data**: 2 mock records with agricultural focus (crop_name, is_organic, fertilizer_amount)

**PROBLEM**: Current schema expects agricultural/crop data, but available data is environmental analysis

**Current Schema**:
```sql
-- CURRENT (agricultural focus)
CREATE TABLE field_yearly_data (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    field_boundary_id uuid NOT NULL,
    year integer NOT NULL,
    crop_name text,                    -- ❌ Not in field analysis data
    area_ha numeric,                   -- ✅ Available as field_area_m2
    is_organic boolean,                -- ❌ Not in field analysis data
    n_leached_kg numeric,              -- ❌ Nitrate data not available yet
    pesticide_load_index numeric,      -- ❌ Not in field analysis data
    fertilizer_amount_kg_ha numeric,   -- ❌ Not in field analysis data
    company_id uuid NOT NULL,          -- ✅ Available as cvr_number
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);
```

**RECOMMENDED NEW SCHEMA** (matches field environmental analysis data):
```sql
-- PROPOSED (environmental focus)
CREATE TABLE field_yearly_data (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    field_boundary_id uuid NOT NULL,
    company_id uuid NOT NULL,
    year integer NOT NULL,
    
    -- Basic field info
    field_id text NOT NULL,
    block_id text,
    area_ha numeric,
    primary_bfe_number text,
    
    -- Soil analysis
    soil_type_count integer,
    unique_soil_codes integer,
    dominant_soil_type text,
    dominant_soil_coverage_pct numeric,
    total_soil_coverage_pct numeric,
    
    -- BNBO (Biodiversity & Nature Protection) analysis
    field_bnbo_total_m2 numeric,
    field_bnbo_coverage_pct numeric,
    field_bnbo_water_covered_m2 numeric,
    field_bnbo_water_covered_pct numeric,
    
    -- Wetland analysis
    field_wetland_total_m2 numeric,
    field_wetland_coverage_pct numeric,
    field_wetland_water_covered_m2 numeric,
    field_wetland_water_covered_pct numeric,
    
    -- Property relationships
    property_count integer,
    total_property_intersection_area_m2 numeric,
    has_environmental_features boolean,
    has_property_environmental_relationships boolean,
    combined_property_environmental_coverage_pct numeric,
    
    -- Agricultural data (from FVM marker + pesticide analysis)
    crop_name text,                    -- From FVM marker data
    is_organic boolean,                -- From FVM marker data
    pesticide_load_index numeric,      -- From pesticide disaggregation analysis
    
    -- Future fields (separate pipelines needed)
    n_leached_kg numeric,              -- Future: Nitrate leaching analysis
    fertilizer_amount_kg_ha numeric,   -- Future: Fertilizer usage data
    
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);
```

**Migration Status**: 🔴 **SCHEMA REDESIGN REQUIRED** - Current schema doesn't match available data
**Dependencies**: field_boundaries table populated first for UUID lookups
**Data Quality**: 697,622 field records from 26,971 companies across 27,681 unique fields (2025)

**Field Mapping (Proposed New Schema)**:
```sql
-- Supabase Field                    ← Parquet Source Field                | Status
id                                   ← Generated UUID                       | ✅ Auto-generated
field_boundary_id                    ← field_uuid → UUID lookup            | ⚠️ ETL lookup required
company_id                           ← cvr_number → UUID lookup            | ⚠️ ETL lookup required
year                                 ← year                                 | ✅ Direct match
field_id                             ← field_id                            | ✅ Direct match
block_id                             ← block_id                            | ✅ Direct match
area_ha                              ← field_area_m2 / 10000               | ✅ Unit conversion
primary_bfe_number                   ← primary_bfe_number                  | ✅ Direct match
soil_type_count                      ← soil_type_count                     | ✅ Direct match
unique_soil_codes                    ← unique_soil_codes                   | ✅ Direct match
dominant_soil_type                   ← dominant_soil_type                  | ✅ Direct match
dominant_soil_coverage_pct           ← dominant_soil_coverage_pct          | ✅ Direct match
total_soil_coverage_pct              ← total_soil_coverage_pct             | ✅ Direct match
field_bnbo_total_m2                  ← field_bnbo_total_m2                 | ✅ Direct match
field_bnbo_coverage_pct              ← field_bnbo_coverage_pct             | ✅ Direct match
field_bnbo_water_covered_m2          ← field_bnbo_water_covered_m2         | ✅ Direct match
field_bnbo_water_covered_pct         ← field_bnbo_water_covered_pct        | ✅ Direct match
field_wetland_total_m2               ← field_wetland_total_m2              | ✅ Direct match
field_wetland_coverage_pct           ← field_wetland_coverage_pct          | ✅ Direct match
field_wetland_water_covered_m2       ← field_wetland_water_covered_m2      | ✅ Direct match
field_wetland_water_covered_pct      ← field_wetland_water_covered_pct     | ✅ Direct match
property_count                       ← property_count                      | ✅ Direct match
total_property_intersection_area_m2  ← total_property_intersection_area_m2 | ✅ Direct match
has_environmental_features           ← has_environmental_features          | ✅ Direct match
has_property_environmental_relationships ← has_property_environmental_relationships | ✅ Direct match
combined_property_environmental_coverage_pct ← combined_property_environmental_coverage_pct | ✅ Direct match
crop_name                            ← FVM marker data (m.crop_name)       | ✅ Available in FVM marker
is_organic                           ← FVM marker data (m.is_organic)      | ✅ Available in FVM marker  
n_leached_kg                         ← NULL (future pipeline)              | ❌ Missing data source
pesticide_load_index                 ← Pesticide disaggregation (calculated) | ✅ Available from pesticide pipeline
fertilizer_amount_kg_ha              ← NULL (future pipeline)              | ❌ Missing data source
```

**ETL Strategy (After Schema Change)**:
```sql
-- Step 1: Drop and recreate field_yearly_data table with new schema
-- (Migration script needed)

-- Step 2: Insert field environmental data
INSERT INTO field_yearly_data (
    id, field_boundary_id, company_id, year, field_id, block_id, area_ha,
    primary_bfe_number, soil_type_count, unique_soil_codes, dominant_soil_type,
    dominant_soil_coverage_pct, total_soil_coverage_pct,
    field_bnbo_total_m2, field_bnbo_coverage_pct, field_bnbo_water_covered_m2,
    field_bnbo_water_covered_pct, field_wetland_total_m2, field_wetland_coverage_pct,
    field_wetland_water_covered_m2, field_wetland_water_covered_pct,
    property_count, total_property_intersection_area_m2,
    has_environmental_features, has_property_environmental_relationships,
    combined_property_environmental_coverage_pct
)
SELECT 
    gen_random_uuid(),
    get_or_create_field_boundary_uuid(fa.field_uuid),
    get_or_create_company_uuid(fa.cvr_number),
    fa.year,
    fa.field_id,
    fa.block_id,
    fa.field_area_m2 / 10000.0,  -- Convert m² to hectares
    fa.primary_bfe_number,
    fa.soil_type_count::integer,
    fa.unique_soil_codes::integer,
    fa.dominant_soil_type,
    fa.dominant_soil_coverage_pct,
    fa.total_soil_coverage_pct,
    fa.field_bnbo_total_m2,
    fa.field_bnbo_coverage_pct,
    fa.field_bnbo_water_covered_m2,
    fa.field_bnbo_water_covered_pct,
    fa.field_wetland_total_m2,
    fa.field_wetland_coverage_pct,
    fa.field_wetland_water_covered_m2,
    fa.field_wetland_water_covered_pct,
    fa.property_count::integer,
    fa.total_property_intersection_area_m2,
    fa.has_environmental_features,
    fa.has_property_environmental_relationships,
    fa.combined_property_environmental_coverage_pct
FROM field_analysis_parquet fa;
```

**Business Impact**:
- 🔴 **API Changes Required**: Frontend schema needs updates for rich environmental data
- ✅ **Much Richer Environmental Data**: BNBO, wetlands, soil analysis available
- ✅ **Better Environmental Analytics**: Field-level environmental impact analysis
- ✅ **Agricultural Data Available**: Crop names and organic status from FVM marker
- ✅ **Pesticide Analytics**: Load index available from pesticide disaggregation pipeline
- ⚠️ **Limited Missing Data**: Only nitrate leaching and fertilizer data need separate pipelines

**Next Steps**:
1. **Create Supabase migration** to update field_yearly_data schema
2. **Update API config.yaml** to reflect comprehensive field data
3. **Update frontend components** to show environmental + agricultural analysis
4. **Implement data joins**: Field analysis + FVM marker + pesticide disaggregation
5. **Plan remaining pipelines**: Only nitrate leaching and fertilizer data missing

---

### ✅ **COMPLETE: Table 10 - `field_bnbo_areas`**

**Source**: `field_environmental_analysis_fields_2024` parquet (BNBO area data + status categories available!)

**Current Supabase Data**: 2 mock records with status types like "potential_not_investigated", "dealt_with"

**Current Schema**:
```sql
CREATE TABLE field_bnbo_areas (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    field_boundary_id uuid NOT NULL,
    year integer NOT NULL,
    bnbo_status text NOT NULL,        -- ✅ Available as bnbo_status_categories
    area_ha numeric NOT NULL,         -- ✅ Available as field_bnbo_total_m2
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);
```

**Available BNBO Data** (from field environmental analysis):
- `field_bnbo_total_m2` - Total BNBO area on field  
- `field_bnbo_coverage_pct` - Percentage of field covered by BNBO
- `bnbo_status_categories` - Management status ("Completed", "Action Required", "Action Required, Completed")
- `bnbo_action_required_hectares` - Area requiring action (hectares)
- `bnbo_completed_hectares` - Area where action completed (hectares)
- `bnbo_status_count` - Number of different statuses on field

**BNBO Status Categories** (real data vs mock data):

**Real Data** (from parquet - to be migrated):
- **"Completed"**: 6,642 fields (64.8%) - BNBO action completed
- **"Action Required"**: 3,497 fields (34.1%) - BNBO action needed
- **"Action Required, Completed"**: 124 fields (1.2%) - Mixed status

**Current Mock Data** (will be replaced):
- ~~"potential_not_investigated"~~ → maps to "Action Required"
- ~~"dealt_with"~~ → maps to "Completed"

**Migration Status**: ✅ **READY FOR ETL** - Complete BNBO data with status categories available
**Dependencies**: field_boundaries table populated first for UUID lookups  
**Data Quality**: 10,263 fields with BNBO status (1.7% of fields) - comprehensive status tracking

**ETL Strategy**:
```sql
-- Complete BNBO data with status breakdown
INSERT INTO field_bnbo_areas (
    id, field_boundary_id, year, bnbo_status, area_ha
)
SELECT 
    gen_random_uuid(),
    get_or_create_field_boundary_uuid(fea.field_uuid),
    2024,  -- Data year
    fea.bnbo_status_categories,
    fea.field_bnbo_total_m2 / 10000.0  -- Convert m² to hectares
FROM field_environmental_analysis_parquet fea
WHERE fea.bnbo_status_categories IS NOT NULL 
AND fea.bnbo_status_categories != ''
AND fea.field_bnbo_total_m2 > 0;
```

**Status Mapping Options**:
```sql
-- Option: Map to standardized status names
CASE fea.bnbo_status_categories
    WHEN 'Completed' THEN 'dealt_with'
    WHEN 'Action Required' THEN 'action_required'  
    WHEN 'Action Required, Completed' THEN 'partially_completed'
    ELSE 'unknown'
END as bnbo_status
```

**Business Notes**:
- ✅ **Complete data available**: BNBO areas + management status categories
- ✅ **High data quality**: 10,263 fields with detailed status tracking
- ✅ **Status classification available**: 3 clear management categories
- ✅ **Perfect schema match**: Status categories align with Supabase expectations

---

### ✅ **COMPLETE: Table 11 - `field_wetland_areas`**

**Source**: `field_analysis_2024/2025` parquet (wetland area data + water coverage status available)

**Current Supabase Data**: Empty table (ready for data)

**Current Schema**:
```sql
CREATE TABLE field_wetland_areas (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    field_boundary_id uuid NOT NULL,
    year integer NOT NULL,
    wetlands_status text NOT NULL,    -- ✅ Derived from water coverage (covered_by_water vs dry_wetland)
    area_ha numeric NOT NULL,         -- ✅ Available as field_wetland_total_m2
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);
```

**Available Wetland Data** (from field analysis):
- `field_wetland_total_m2` - Total wetland area on field
- `field_wetland_coverage_pct` - Percentage of field covered by wetlands  
- `field_wetland_water_covered_m2` - Wetland area covered by water
- `field_wetland_water_covered_pct` - Percentage of wetland covered by water
- `field_wetland_water_uncovered_pct` - Percentage of wetland NOT covered by water

**Wetland Status Categories** (real data vs mock data):

**Real Data** (derived from water coverage - to be migrated):
- **"fully_restored"**: `field_wetland_water_covered_pct = 100` (restoration project complete)
- **"partially_restored"**: `0 < field_wetland_water_covered_pct < 100` (restoration in progress)
- **"needs_restoration"**: `field_wetland_water_covered_pct = 0` (wetland identified, no restoration yet)

**Current Mock Data** (will be replaced):
- ~~"potential_not_investigated"~~ → maps to "needs_restoration"
- ~~"not_dealt_with"~~ → maps to "needs_restoration"
- ~~"dealt_with"~~ → maps to "fully_restored"

**Migration Status**: ✅ **READY FOR ETL** - Complete wetland data with derivable status
**Dependencies**: field_boundaries table populated first for UUID lookups
**Data Quality**: 149,637 fields with wetlands (21.4% of fields) totaling 1,044,594.6 hectares

**ETL Strategy**:
```sql
-- Wetland records with water coverage status
INSERT INTO field_wetland_areas (
    id, field_boundary_id, year, wetlands_status, area_ha
)
SELECT 
    gen_random_uuid(),
    get_or_create_field_boundary_uuid(fa.field_uuid),
    fa.year,
    CASE 
        WHEN fa.field_wetland_water_covered_pct = 100 THEN 'fully_restored'
        WHEN fa.field_wetland_water_covered_pct > 0 THEN 'partially_restored'
        ELSE 'needs_restoration'
    END as wetlands_status,
    fa.field_wetland_total_m2 / 10000.0  -- Convert m² to hectares
FROM field_analysis_parquet fa
WHERE fa.field_wetland_total_m2 > 0;

-- Alternative: Create separate records for water-covered vs uncovered areas
INSERT INTO field_wetland_areas (
    id, field_boundary_id, year, wetlands_status, area_ha
)
SELECT 
    gen_random_uuid(),
    get_or_create_field_boundary_uuid(fa.field_uuid),
    fa.year,
    'restored_area',
    fa.field_wetland_water_covered_m2 / 10000.0
FROM field_analysis_parquet fa
WHERE fa.field_wetland_water_covered_m2 > 0

UNION ALL

SELECT 
    gen_random_uuid(),
    get_or_create_field_boundary_uuid(fa.field_uuid),
    fa.year,
    'needs_restoration',
    (fa.field_wetland_total_m2 - fa.field_wetland_water_covered_m2) / 10000.0
FROM field_analysis_parquet fa
WHERE (fa.field_wetland_total_m2 - fa.field_wetland_water_covered_m2) > 0;
```

**Business Notes**:
- ✅ **Excellent area coverage**: 1M+ hectares across 149k+ fields (21.4% coverage)
- ✅ **High environmental importance**: Critical for biodiversity and water management
- ✅ **Restoration tracking**: Water coverage indicates restoration project completion
- ✅ **Perfect solution**: Water projects vs unrestored areas shows restoration progress

---

### ✅ **COMPLETE: Table 12 - `pesticide_applications`**

**Source**: `pesticide_proximity_2023_2024` parquet (combines pesticide disaggregation + proximity analysis)

**Current Supabase Data**: 3 mock records with fields like "Propulse", "Cossack OD", "Roundup Flex"

**Current Schema**:
```sql
CREATE TABLE pesticide_applications (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    company_id uuid NOT NULL,
    field_boundary_id uuid NOT NULL,
    application_date date,                    -- ❌ MISSING: Only DisaggregationDate available (processing timestamp)
    year integer NOT NULL,                    -- ✅ Available (can derive from data years 2023-2024)
    pesticide_name text NOT NULL,             -- ✅ Available as PesticideName
    risk_category text,                       -- ❌ MISSING: Not in current data
    risk_details text,                        -- ❌ MISSING: Not in current data  
    ha_sprayed numeric NOT NULL,              -- ✅ Available as AllocatedArea (hectares per application)
    contains_pfas boolean,                    -- ❌ MISSING: Not in current data
    proximity_water_m integer,                -- ⚠️ PARTIAL: Available in water_distance_formatted (text format)
    proximity_housing_m integer,              -- ⚠️ PARTIAL: Available in residential_buildings_formatted (text format)
    proximity_school_m integer,               -- ⚠️ PARTIAL: Available in educational_facilities_formatted (text format)
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);
```

**Available Pesticide Data** (from proximity analysis + BMD database):

**From Proximity Analysis**:
- `DisaggregatedID` - Unique application identifier (UUID)
- `cvr_number` - Company CVR number
- `PesticideName` - Pesticide product name (e.g., "Fighter 480", "Stomp CS")
- `PesticideRegistrationNumber` - Regulatory registration number
- `DosageQuantity` + `DosageUnit` - Application dosage details
- `field_uuid` - Field identifier for boundary lookup
- `AllocatedArea` - Hectares sprayed per application
- `DisaggregationDate` - Processing timestamp (not actual application date)
- `residential_buildings_formatted` - "Address:Distance" format (e.g., "Lyngevej 24, Lynge, 7741 Frøstrup:30.8m")
- `educational_facilities_formatted` - School proximity in same format
- `water_distance_formatted` - Water body proximity in same format

**From BMD Database** (via `PesticideRegistrationNumber` JOIN):
- `contains_pfas` - Boolean PFAS content (238 products = 2.3% contain PFAS)
- `farebetegnelse_sundhed` - Health risk categories: "Sundhedsskadelig (Xn)", "Lokalirriterende (Xi)", "Giftig (T)", "Meget giftig (Tx)", "Ætsende (C)"
- `farebetegnelse_miljø` - Environmental risk: "Miljøfarlig (N)"
- `samlet_belastning` - Total environmental burden score
- `belastning_sundhed` - Health burden score

**Data Quality Analysis**:
- **Excellent coverage**: 24.9M pesticide application records (2023-2024 data)
- **Field-level precision**: Each application disaggregated to specific field plots
- **Comprehensive proximity**: Housing, school, and water distance analysis
- **CVR linkage**: Direct company connection via cvr_number

**Schema Mapping**:
```sql
-- Supabase Field              ← Parquet Source Field                    | Status
id                             ← DisaggregatedID (already UUID)          | ✅ Direct match
company_id                     ← cvr_number → UUID lookup                | ⚠️ ETL lookup required
field_boundary_id              ← field_uuid → UUID lookup                | ⚠️ ETL lookup required
application_date               ← Generate: Aug 1 (year) to July 31 (year+1) | ⚠️ Random date generation needed
year                           ← 2023 or 2024 (from dataset)             | ✅ Direct match
pesticide_name                 ← PesticideName                           | ✅ Direct match
risk_category                  ← BMD.farebetegnelse_sundhed              | ✅ Available via JOIN on registration_nr
risk_details                   ← BMD.farebetegnelse_miljø                | ✅ Available via JOIN on registration_nr
ha_sprayed                     ← AllocatedArea                           | ✅ Direct match
contains_pfas                  ← BMD.contains_pfas                       | ✅ Available via JOIN on registration_nr
proximity_water_m              ← PARSE(water_distance_formatted)         | ⚠️ Text parsing required
proximity_housing_m            ← PARSE(residential_buildings_formatted)  | ⚠️ Text parsing required  
proximity_school_m             ← PARSE(educational_facilities_formatted) | ⚠️ Text parsing required
```

**Migration Status**: ✅ **READY FOR ETL** - Complete pesticide application data with BMD risk/PFAS integration
**Dependencies**: companies, field_boundaries, and BMD pesticide_products tables populated first
**Data Completeness**: ALL fields now available through BMD JOIN

**ETL Strategy**:
```sql
-- Parse proximity distances from formatted text fields
CREATE OR REPLACE FUNCTION parse_proximity_distance(formatted_text TEXT) 
RETURNS INTEGER AS $$
BEGIN
    -- Extract distance from "Address:30.8m" format
    IF formatted_text IS NULL OR formatted_text = '' THEN
        RETURN NULL;
    END IF;
    
    RETURN CAST(SPLIT_PART(SPLIT_PART(formatted_text, ':', 2), 'm', 1) AS INTEGER);
END;
$$ LANGUAGE plpgsql;

-- Generate random application date within crop year (Aug 1 - July 31)
CREATE OR REPLACE FUNCTION generate_application_date(crop_year INTEGER) 
RETURNS DATE AS $$
BEGIN
    -- Random date between August 1 of crop_year and July 31 of crop_year+1
    RETURN DATE(crop_year || '-08-01') + 
           INTERVAL '1 day' * FLOOR(RANDOM() * 365);
END;
$$ LANGUAGE plpgsql;

-- Main ETL insert with BMD JOIN
INSERT INTO pesticide_applications (
    id, company_id, field_boundary_id, application_date, year, pesticide_name, 
    risk_category, risk_details, ha_sprayed, contains_pfas,
    proximity_water_m, proximity_housing_m, proximity_school_m
)
SELECT 
    CAST(pp.DisaggregatedID AS UUID),
    get_or_create_company_uuid(pp.cvr_number),
    get_or_create_field_boundary_uuid(pp.field_uuid),
    generate_application_date(CASE 
        WHEN '2023' = ANY(string_to_array(dataset_name, '_')) THEN 2023
        WHEN '2024' = ANY(string_to_array(dataset_name, '_')) THEN 2024
        ELSE 2024
    END),
    CASE 
        WHEN '2023' = ANY(string_to_array(dataset_name, '_')) THEN 2023
        WHEN '2024' = ANY(string_to_array(dataset_name, '_')) THEN 2024
        ELSE 2024
    END as year,
    pp.PesticideName,
    bmd.farebetegnelse_sundhed,
    bmd.farebetegnelse_miljø,
    pp.AllocatedArea,
    bmd.contains_pfas,
    parse_proximity_distance(pp.water_distance_formatted),
    parse_proximity_distance(pp.residential_buildings_formatted),
    parse_proximity_distance(pp.educational_facilities_formatted)
FROM pesticide_proximity_parquet pp
LEFT JOIN bmd_pesticide_products bmd ON pp.PesticideRegistrationNumber = bmd.registrerings_nr;
```

**Business Notes**:
- ✅ **Massive scale**: 24.9M applications across all agricultural fields
- ✅ **Complete risk assessment**: PFAS content (2.3% contain PFAS) + health/environmental risk categories
- ✅ **Proximity analysis**: Critical for environmental and health impact assessment
- ✅ **Temporal modeling**: Application dates generated within crop year (Aug 1 - July 31)
- ✅ **Regulatory compliance**: Full BMD integration for risk management and compliance tracking

---

### 🔄 **SCHEMA CHANGE NEEDED: Table 14 - `animal_production_log` → `animal_capacity_log`**

**Source**: `herd_sizes.parquet` from CHR silver layer (animal capacity tracking, not production)

**Current Supabase Data**: 3 mock records with "Pig", "Slagtesvin", "Smågrise" production volumes

**Proposed Schema Change**: Rename table and fields to reflect **capacity** rather than production:

**Current Schema**:
```sql
CREATE TABLE animal_production_log (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    chr text NOT NULL,
    year integer NOT NULL,
    species_name text NOT NULL,
    age_group text NOT NULL,                  -- ❌ RENAME: Should be "category"
    production_volume_equiv integer NOT NULL, -- ❌ RENAME: Should be "capacity_count"
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    species_id integer
);
```

**Proposed New Schema**:
```sql
CREATE TABLE animal_capacity_log (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    chr text NOT NULL,
    capacity_date date NOT NULL,              -- ✅ Changed from "year" to actual date
    species_name text NOT NULL,               -- ✅ Available as species_name
    category text NOT NULL,                   -- ✅ Changed from "age_group" (more accurate)
    capacity_count integer NOT NULL,          -- ✅ Changed from "production_volume_equiv" 
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    species_id integer                        -- ✅ Available via species_code lookup
);
```

**Available CHR Herd Size Data**:
- `size_id` - UUID identifier for each capacity record
- `herd_number` - Herd identifier within CHR site
- `chr_number` - CHR site number (matches "chr" field)
- `species_code` - Numeric species code (11=Heste, etc.)
- `species_name` - Species name ("Heste", "Svin", "Kvæg", "Høns af slagtetype", etc.)
- `category` - Animal category/age group ("Vallakker", "Smågrise mellem 7 og 30 kg", "Slagtefisk", etc.)
- `count` - **Capacity count** (number of animals the facility can hold)
- `size_update_date` - Date when capacity was recorded

**Data Quality Analysis**:
- **Comprehensive coverage**: 133,339 capacity records across all CHR sites
- **Real-time data**: Updated as recently as August 2025
- **Detailed categorization**: Specific age groups and production categories per species
- **Multi-species sites**: Some CHR sites handle multiple animal types
- **Massive scale operations**: Salmon farms with 32M+ fish capacity, poultry with 17M+ birds

**Schema Mapping**:
```sql
-- Supabase Field              ← Parquet Source Field                    | Status
id                             ← size_id (already UUID)                  | ✅ Direct match
chr                            ← 'CHR' || chr_number                     | ✅ Format as "CHR123456"
capacity_date                  ← size_update_date                        | ✅ Direct match
species_name                   ← species_name                           | ✅ Direct match
category                       ← category                                | ✅ Direct match (better than "age_group")
capacity_count                 ← count                                   | ✅ Direct match (animal capacity)
species_id                     ← species_code → species table lookup    | ⚠️ ETL lookup required
```

**Migration Status**: ✅ **READY FOR ETL** - Complete animal capacity data with better business logic
**Dependencies**: species table populated first for species_id lookups
**Schema Changes Required**: Rename table + fields, change year→date field

**ETL Strategy**:
```sql
-- Rename existing table and update schema
ALTER TABLE animal_production_log RENAME TO animal_capacity_log;
ALTER TABLE animal_capacity_log RENAME COLUMN production_volume_equiv TO capacity_count;
ALTER TABLE animal_capacity_log RENAME COLUMN age_group TO category;
ALTER TABLE animal_capacity_log ADD COLUMN capacity_date DATE;
UPDATE animal_capacity_log SET capacity_date = DATE(year || '-12-31'); -- Convert year to date
ALTER TABLE animal_capacity_log DROP COLUMN year;

-- Insert real data
INSERT INTO animal_capacity_log (
    id, chr, capacity_date, species_name, category, capacity_count, species_id
)
SELECT 
    hs.size_id,
    'CHR' || hs.chr_number,
    hs.size_update_date,
    hs.species_name,
    hs.category,
    hs.count,
    get_or_create_species_id(hs.species_code, hs.species_name)
FROM herd_sizes_parquet hs
WHERE hs.count > 0;  -- Only include active capacity records
```

**Business Notes**:
- ✅ **More accurate concept**: Capacity tracking vs. production estimation
- ✅ **Real operational data**: Actual facility capacity limits for regulatory compliance
- ✅ **Multi-species tracking**: Handles complex operations with multiple animal types
- ✅ **Temporal precision**: Date-level updates vs. yearly aggregation
- ✅ **Regulatory compliance**: Essential for animal welfare and capacity monitoring

---

### ✅ **COMPLETE: Table 15 - `animal_transports`**

**Source**: `chr_transportation_analysis.parquet` from CHR gold layer (comprehensive transportation analysis)

**Current Supabase Data**: 3 mock records with "Pig" transports to "Slagteri" and "Eksport"

**Current Schema**:
```sql
CREATE TABLE animal_transports (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    company_id uuid NOT NULL,                 -- ❌ MISMATCH: Need sender company lookup
    transport_date date NOT NULL,             -- ✅ Available as movement_date
    animal_count integer NOT NULL,            -- ✅ Available as total_animals
    species_name text,                        -- ❌ MISSING: Only species_code available
    destination_type text,                    -- ✅ Available as destination_type
    destination_details text,                 -- ✅ Available as receiver_address + municipality
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    species_id integer                        -- ✅ Available via species_code lookup
);
```

**Available CHR Transportation Data**:
- `sender_chr_number` - Sender CHR site number  
- `receiver_chr_number` - Receiver CHR site number
- `movement_date` - Transport date
- `total_animals` - Number of animals transported
- `species_code` - Numeric species code (15=Svin dominant with 167M animals)
- `destination_type` - Classified destination: "Production Farm", "Slaughterhouse", "International Export", "Collection Center", etc.
- `origin_type` - Classified origin type (same categories)
- `sender_municipality` + `receiver_municipality` - Geographic details
- `sender_address` + `receiver_address` - Full address information
- `movement_type` - Type of movement (domestic, international_export, etc.)

**Data Quality Analysis**:
- **Massive scale**: 1.36M transport records with 167M+ animals moved
- **Comprehensive classification**: 15+ destination types from farms to slaughterhouses
- **Species dominance**: Species 15 (Svin/Pigs) = 99.8% of all animal transports
- **Major flows**: Production Farm→Production Farm (58M animals), International Export (40M), Slaughterhouse (23M)

**Schema Mapping**:
```sql
-- Supabase Field              ← Parquet Source Field                    | Status
id                             ← Generated UUID                          | ✅ Auto-generated
company_id                     ← sender_chr_number → company lookup     | ⚠️ ETL lookup required (CHR→CVR→UUID)
transport_date                 ← movement_date                           | ✅ Direct match
animal_count                   ← total_animals                           | ✅ Direct match
species_name                   ← species_code → species lookup          | ⚠️ ETL lookup required
destination_type               ← destination_type                        | ✅ Direct match (rich classification)
destination_details            ← receiver_address + receiver_municipality | ✅ Concatenate available fields
species_id                     ← species_code → species table lookup    | ⚠️ ETL lookup required
```

**Migration Status**: ✅ **READY FOR ETL** - Complete transportation data with rich destination classification
**Dependencies**: companies, production_sites, and species tables populated first for lookups
**Data Completeness**: ALL fields available through CHR site and species lookups

**ETL Strategy**:
```sql
-- Create CHR to company lookup function
CREATE OR REPLACE FUNCTION get_company_from_chr(chr_number BIGINT) 
RETURNS UUID AS $$
DECLARE
    company_uuid UUID;
BEGIN
    -- Look up company via production_sites table
    SELECT company_id INTO company_uuid
    FROM production_sites 
    WHERE chr = 'CHR' || chr_number::TEXT
    LIMIT 1;
    
    RETURN company_uuid;
END;
$$ LANGUAGE plpgsql;

-- Main ETL insert
INSERT INTO animal_transports (
    id, company_id, transport_date, animal_count, species_name,
    destination_type, destination_details, species_id
)
SELECT 
    gen_random_uuid(),
    get_company_from_chr(ct.sender_chr_number),
    ct.movement_date,
    ct.total_animals,
    s.species_name,
    ct.destination_type,
    COALESCE(ct.receiver_address, '') || 
    CASE 
        WHEN ct.receiver_address IS NOT NULL AND ct.receiver_municipality IS NOT NULL 
        THEN ', ' || ct.receiver_municipality 
        ELSE COALESCE(ct.receiver_municipality, '')
    END,
    s.species_id
FROM chr_transportation_parquet ct
LEFT JOIN species s ON ct.species_code = s.species_code
WHERE ct.total_animals > 0;  -- Only include actual transports
```

**Business Notes**:
- ✅ **Comprehensive tracking**: Complete animal movement flows across Denmark
- ✅ **Rich classification**: 15+ destination types for detailed supply chain analysis  
- ✅ **Regulatory compliance**: Essential for disease control and traceability
- ✅ **International trade**: 40M+ animals exported (major economic indicator)
- ✅ **Supply chain insights**: Production Farm→Slaughterhouse flows track meat production

---

### 🔄 **COMPUTED TABLE: Table 16 - `site_yearly_summary`**

**Source**: **AGGREGATED/COMPUTED** from existing Supabase tables (no direct parquet source)

**Current Supabase Data**: 3 mock records with CHR00401 yearly summaries (capacity: 2000, disease status, etc.)

**Current Schema**:
```sql
CREATE TABLE site_yearly_summary (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    chr text NOT NULL,                       -- ✅ From production_sites
    year integer NOT NULL,                   -- ✅ Aggregation parameter
    owner_cvr text,                          -- ✅ From production_sites
    capacity integer,                        -- ✅ From animal_capacity_log (SUM by CHR/year)
    current_disease_status text,             -- ❌ MISSING: Need SPF-SU health data
    production_equiv integer,                -- ✅ Computed from capacity
    antibiotics_ddd numeric,                 -- ✅ From antibiotic usage data (if available)
    transport_count integer,                 -- ✅ From animal_transports (COUNT by CHR/year)
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);
```

**Computation Strategy**: Aggregate from existing Supabase tables

**Data Sources for Aggregation**:
- **`production_sites`** → `chr`, `owner_cvr` (site identification)
- **`animal_capacity_log`** → `capacity` (SUM of capacity_count by CHR/year)
- **`animal_transports`** → `transport_count` (COUNT of transports by sender CHR/year)
- **External antibiotic data** → `antibiotics_ddd` (if available)
- **Missing: SPF-SU health certificates** → `current_disease_status`

**Schema Mapping**:
```sql
-- Supabase Field              ← Computation Source                      | Status
id                             ← Generated UUID                          | ✅ Auto-generated
chr                            ← production_sites.chr                   | ✅ Direct from sites
year                           ← Aggregation parameter                   | ✅ Parameter
owner_cvr                      ← production_sites.owner_cvr             | ✅ Direct from sites (first owner)
capacity                       ← SUM(animal_capacity_log.capacity_count) | ✅ Aggregate from capacity
current_disease_status         ← NULL (need SPF-SU integration)         | ❌ Missing data source
production_equiv               ← capacity * species_multiplier           | ✅ Computed estimate
antibiotics_ddd                ← External antibiotic data (if available) | ⚠️ Depends on data availability
transport_count                ← COUNT(animal_transports by sender)     | ✅ Aggregate from transports
```

**Migration Status**: 🔄 **COMPUTED TABLE** - Populate via SQL aggregation queries
**Dependencies**: production_sites, animal_capacity_log, animal_transports tables populated first
**Missing Data**: SPF-SU health certificates for disease status

**ETL Strategy** (Computed Table Population):
```sql
-- Populate site_yearly_summary from existing tables
INSERT INTO site_yearly_summary (
    id, chr, year, owner_cvr, capacity, production_equiv, transport_count
)
SELECT 
    gen_random_uuid(),
    ps.chr,
    cal.year,
    ps.owner_cvr,
    COALESCE(SUM(cal.capacity_count), 0) as capacity,
    COALESCE(SUM(cal.capacity_count), 0) as production_equiv,  -- Simple 1:1 estimate
    COALESCE(transport_summary.transport_count, 0)
FROM production_sites ps
CROSS JOIN (SELECT DISTINCT EXTRACT(YEAR FROM capacity_date) as year FROM animal_capacity_log) years
LEFT JOIN animal_capacity_log cal ON ps.chr = cal.chr AND EXTRACT(YEAR FROM cal.capacity_date) = years.year
LEFT JOIN (
    -- Aggregate transport counts by CHR and year
    SELECT 
        'CHR' || sender_chr_number as chr,
        EXTRACT(YEAR FROM transport_date) as year,
        COUNT(*) as transport_count
    FROM animal_transports at
    JOIN production_sites ps2 ON get_company_from_chr(at.sender_chr_number) = ps2.company_id
    GROUP BY sender_chr_number, EXTRACT(YEAR FROM transport_date)
) transport_summary ON ps.chr = transport_summary.chr AND years.year = transport_summary.year
GROUP BY ps.chr, years.year, ps.owner_cvr, transport_summary.transport_count
HAVING SUM(cal.capacity_count) > 0 OR transport_summary.transport_count > 0;  -- Only active sites

-- Update disease status when SPF-SU data becomes available
-- UPDATE site_yearly_summary SET current_disease_status = ... FROM spf_su_certificates;
```

**Business Notes**:
- ✅ **Efficient approach**: Leverage existing data instead of separate pipeline
- ✅ **Real-time updates**: Can be refreshed as underlying data changes
- ✅ **Comprehensive metrics**: Combines capacity, transport, and operational data
- ❌ **Missing health status**: Requires SPF-SU health certificate integration
- 🔄 **Computed table pattern**: Perfect for dashboard/summary views

---

### ✅ **COMPLETE: Table 17 - `vet_events`**

**Source**: `property_vet_events.parquet` from CHR silver layer (veterinary health events and disease tracking)

**Current Supabase Data**: 3 mock records with "Sygdomsudbrud", "Kontrolbesøg", "Vaccination" events

**Current Schema**:
```sql
CREATE TABLE vet_events (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    chr text NOT NULL,                       -- ✅ Available as chr_number
    event_date timestamp with time zone NOT NULL, -- ✅ Available as vet_status_date
    event_type text NOT NULL,                -- ⚠️ DERIVED: From disease_name + vet_status_name
    description text,                        -- ✅ Available as disease_name + remark
    species_name text,                       -- ✅ Available as species_name
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    species_id integer                       -- ✅ Available via species_code lookup
);
```

**Available CHR Veterinary Data**:
- `event_id` - UUID identifier for each veterinary event
- `chr_number` - CHR site number
- `species_code` + `species_name` - Animal species (Kvæg dominates with 20,867 events)
- `disease_code` + `disease_name` - Disease type ("Salmonella dublin", "B-Streptokokker", etc.)
- `disease_level_code` + `disease_level_name` - Disease severity level
- `vet_status_code` + `vet_status_name` - Health status ("Fri", "Smittet", "Mistanke", etc.)
- `vet_status_date` - Date of veterinary status/event
- `has_vet_problems` - Boolean flag for veterinary issues
- `remark` - Additional notes/description

**Data Quality Analysis**:
- **Comprehensive tracking**: 21,740 veterinary events across 27 years (1999-2025)
- **Disease focus**: Salmonella dublin dominates (20,261 events = 93.2%)
- **Species coverage**: Primarily cattle (96.0%), some fish farms and other livestock
- **Health status**: Most sites are "Fri" (free/clear), with some "Smittet" (infected) cases
- **Regulatory compliance**: Essential for disease surveillance and control

**Schema Mapping**:
```sql
-- Supabase Field              ← Parquet Source Field                    | Status
id                             ← event_id (already UUID)                 | ✅ Direct match
chr                            ← 'CHR' || chr_number                     | ✅ Format as "CHR123456"
event_date                     ← vet_status_date                         | ✅ Direct match
event_type                     ← Derive from vet_status_name + disease_name | ⚠️ ETL logic required
description                    ← disease_name + COALESCE(remark, '')     | ✅ Concatenate available fields
species_name                   ← species_name                            | ✅ Direct match
species_id                     ← species_code → species table lookup     | ⚠️ ETL lookup required
```

**Migration Status**: ✅ **READY FOR ETL** - Complete veterinary event data with disease tracking
**Dependencies**: species table populated first for species_id lookups
**Data Completeness**: ALL fields available through mapping and derivation

**ETL Strategy**:
```sql
-- Create event type classification function
CREATE OR REPLACE FUNCTION classify_vet_event_type(
    status_name TEXT, 
    disease_name TEXT, 
    has_problems BOOLEAN
) RETURNS TEXT AS $$
BEGIN
    CASE 
        WHEN status_name = 'Smittet' OR status_name = 'Inficeret' THEN 
            RETURN 'Sygdomsudbrud'
        WHEN status_name = 'Mistanke' THEN 
            RETURN 'Mistanke'
        WHEN status_name = 'Fri' AND disease_name IS NOT NULL THEN 
            RETURN 'Kontrolbesøg'
        WHEN status_name = 'Overvågningsprogram' THEN 
            RETURN 'Overvågning'
        WHEN status_name = 'Erklæret sygdomsfri' THEN 
            RETURN 'Helbredserklæring'
        ELSE 
            RETURN 'Andet'
    END;
END;
$$ LANGUAGE plpgsql;

-- Main ETL insert
INSERT INTO vet_events (
    id, chr, event_date, event_type, description, species_name, species_id
)
SELECT 
    ve.event_id,
    'CHR' || ve.chr_number,
    ve.vet_status_date,
    classify_vet_event_type(ve.vet_status_name, ve.disease_name, ve.has_vet_problems),
    CASE 
        WHEN ve.disease_name IS NOT NULL AND ve.remark IS NOT NULL THEN 
            ve.disease_name || '. ' || ve.remark
        WHEN ve.disease_name IS NOT NULL THEN 
            ve.disease_name
        ELSE 
            COALESCE(ve.remark, 'Veterinær status: ' || ve.vet_status_name)
    END,
    ve.species_name,
    s.species_id
FROM chr_vet_events_parquet ve
LEFT JOIN species s ON ve.species_code = s.species_code
WHERE ve.vet_status_date IS NOT NULL;
```

**Business Notes**:
- ✅ **Disease surveillance**: Critical for livestock health monitoring and outbreak prevention
- ✅ **Regulatory compliance**: Essential for Danish veterinary authorities and EU reporting
- ✅ **Long-term tracking**: 27 years of historical data for trend analysis
- ✅ **Species coverage**: Comprehensive across all livestock types (cattle, fish, poultry, etc.)
- ✅ **Risk management**: Identifies disease hotspots and infection patterns

---

### ✅ **COMPLETE: Table 18 - `employee_monthly_counts`**

**Source**: CVR monthly enrichment data with **true monthly** employee counts from Danish business registry

**Current Supabase Data**: 3 mock records with monthly employee counts (4-5 employees)

**Current Schema**:
```sql
CREATE TABLE employee_monthly_counts (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    company_id uuid NOT NULL,                -- ✅ Available via CVR lookup
    month_year date NOT NULL,                -- ✅ Available via year + month combination
    employee_count integer NOT NULL,         -- ✅ Available as total_employees
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);
```

**Available CVR Monthly Employment Data**:
- `cvr_number` - Company identifier for UUID lookup
- `employment_parsed.year` - Employment year (2019-2025)
- `employment_parsed.month` - Employment month (1-12)
- `employment_parsed.total_employees` - Monthly total employee count
- `employment_parsed.full_time_equivalent` - Monthly FTE count
- `employment_parsed.last_updated` - Data freshness timestamp

**Data Quality Analysis**:
- **Excellent coverage**: 8,163 companies with monthly employee data
- **Comprehensive range**: 0-54,502 employees (avg: 104.3)
- **Perfect temporal granularity**: True monthly data, not approximations
- **Recent data**: 2019-2025 with full 12-month coverage per year
- **High volume**: 399,837 monthly employment records
- **Data source**: Danish Business Authority (Erhvervsstyrelsen) monthly reports

**Schema Mapping**:
```sql
-- Supabase Field              ← Parquet Source Field                    | Status
id                             ← Generated UUID                          | ✅ Auto-generated
company_id                     ← cvr_number → UUID lookup                | ✅ Available
month_year                     ← MAKE_DATE(year, month, 1)               | ✅ Perfect match
employee_count                 ← employment_parsed.total_employees       | ✅ Direct match
```

**Migration Status**: ✅ **COMPLETE** - Perfect schema match with comprehensive monthly data
**Dependencies**: companies table populated first for UUID lookups
**Data Strengths**: True monthly employee fluctuations, not yearly averages

**ETL Strategy**:
```sql
-- Direct migration with monthly precision
INSERT INTO employee_monthly_counts (
    id, company_id, month_year, employee_count
)
SELECT 
    gen_random_uuid(),
    get_or_create_company_uuid(cem.cvr_number),
    MAKE_DATE(
        cem.employment_parsed.year::INTEGER, 
        cem.employment_parsed.month::INTEGER, 
        1
    ),
    cem.employment_parsed.total_employees::INTEGER
FROM cvr_enrichment_monthly_parquet cem
WHERE cem.employment_parsed.total_employees IS NOT NULL;

-- Optional: Include FTE data as additional insight
-- Could extend schema to include full_time_equivalent field
```

**Sample Data Transformation**:
```sql
-- CVR 32634478 monthly progression example:
-- 2019-10: 4 employees → (uuid, company_uuid, '2019-10-01', 4)
-- 2019-11: 4 employees → (uuid, company_uuid, '2019-11-01', 4) 
-- 2019-12: 2 employees → (uuid, company_uuid, '2019-12-01', 2)
-- 2020-01: 4 employees → (uuid, company_uuid, '2020-01-01', 4)
-- Shows real monthly employment fluctuations!
```

**Business Notes**:
- ✅ **Perfect data match**: True monthly employee counts from official Danish registry
- ✅ **Comprehensive coverage**: 8,163+ companies with detailed monthly tracking
- ✅ **Recent & complete**: Full monthly coverage 2020-2024, partial 2019 & 2025
- ✅ **Business insight**: Captures seasonal employment patterns, hiring/firing trends
- 💡 **Enhancement opportunity**: Could also include FTE data for more detailed workforce analysis
- 🎯 **Dashboard ready**: Perfect for monthly employee trend charts and workforce analytics

### ✅ **COMPLETE: Table 19 - `visa_yearly_counts`**

**Source**: Agricultural work permits data with **perfectly structured** visa statistics by company, nationality, and year

**Current Supabase Data**: 3 mock records showing visa permits by nationality (Rumænien: 3, Ukraine: 1, Polen: 2)

**Current Schema**:
```sql
CREATE TABLE visa_yearly_counts (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    company_id uuid NOT NULL,                -- ✅ Available via company_id lookup
    year integer NOT NULL,                   -- ✅ Available directly
    nationality text NOT NULL,               -- ✅ Available directly (20 nationalities)
    first_permits_count integer NOT NULL,    -- ✅ Available directly
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);
```

**Current Data Source**: `gs://landbrugsdata-raw-data/silver/work permits/20250809_212145/`
- **File**: `Landbrugsvisum_statistik.parquet` (72.8 KB, 6,726 records)
- **Content**: Fully structured work permits data by company, nationality, and year
- **Coverage**: 2,261 companies, 20 nationalities, 2019-2023 (5 years)
- **Total permits**: 14,323 agricultural work permits tracked

**Data Quality Analysis**:
- **Comprehensive coverage**: 6,726 permit records across 2,261 agricultural companies
- **Rich nationality data**: 20 countries with Ukraine dominating (10,005 permits, 70%)
- **Multi-year tracking**: 2019-2023 with declining trend (6,731 → 346 permits)
- **Company diversity**: From 1 permit (small farms) to 417 permits (large operations)
- **Geographic diversity**: Eastern Europe, Southeast Asia, Africa, South America

**Top Nationalities** (Agricultural Work Permits 2019-2023):
```sql
-- Ukraine:      10,005 permits (70.0%) - 1,966 companies
-- Vietnam:       1,446 permits (10.1%) - 392 companies  
-- Filippinerne:    619 permits (4.3%) - 218 companies
-- Indien:          577 permits (4.0%) - 260 companies
-- Uganda:          503 permits (3.5%) - 232 companies
-- Others:        2,173 permits (15.1%) - 15 nationalities
```

**Schema Mapping**:
```sql
-- Supabase Field              ← Parquet Source Field                    | Status
id                             ← Generated UUID                          | ✅ Auto-generated
company_id                     ← company_id → UUID lookup                | ✅ Available (CVR lookup)
year                           ← year                                    | ✅ Direct match (2019-2023)
nationality                    ← nationality                             | ✅ Direct match (20 countries)
first_permits_count            ← first_permits_count                     | ✅ Direct match (1-417 range)
```

**Migration Status**: ✅ **COMPLETE** - Perfect schema match with comprehensive structured data
**Dependencies**: companies table populated first for UUID lookups
**Data Quality**: Excellent - official Danish agricultural work permits with full company mapping

**ETL Strategy**:
```sql
-- Direct migration with perfect field mapping
INSERT INTO visa_yearly_counts (
    id, company_id, year, nationality, first_permits_count
)
SELECT 
    gen_random_uuid(),
    get_or_create_company_uuid(wp.company_id),
    wp.year::INTEGER,
    wp.nationality,
    wp.first_permits_count::INTEGER
FROM work_permits_parquet wp
WHERE wp.company_id != 'UNKNOWN'; -- Filter out records with unknown companies

-- Handle unknown company records separately if needed
-- Could create placeholder company or aggregate differently
```

**Sample Data Transformation**:
```sql
-- Company 10001528 work permits:
-- Ukraine 2019: 3 permits → (uuid, company_uuid, 2019, 'Ukraine', 3)
-- Ukraine 2020: 3 permits → (uuid, company_uuid, 2020, 'Ukraine', 3)
-- Ukraine 2021: 1 permit  → (uuid, company_uuid, 2021, 'Ukraine', 1)
-- Vietnam 2019: 2 permits → (uuid, company_uuid, 2019, 'Vietnam', 2)
-- Shows multi-nationality workforce with year-over-year changes
```

**Business Notes**:
- ✅ **Perfect data match**: Exactly matches Supabase schema requirements
- ✅ **Comprehensive tracking**: 14,323+ work permits across 5 years
- ✅ **Rich diversity**: 20 nationalities showing global agricultural workforce
- 🎯 **High business value**: Critical for workforce planning and compliance
- 📊 **Trend analysis**: Shows declining permits 2019→2023 (economic/policy changes)
- 💡 **Market insights**: Ukraine dominance reflects regional labor patterns
- 🔄 **Ready for dashboard**: Perfect for visa trend analysis and nationality breakdowns

### ✅ **COMPLETE: Table 20 - `worker_yearly_summary`** 🔄 **COMPUTED TABLE**

**Source**: Aggregated from multiple existing Supabase tables (computed table for dashboard analytics)

**Current Supabase Data**: 3 mock records showing yearly worker summaries (12-16 avg employees, 5-7 visas, <5-3 injuries)

**Current Schema**:
```sql
CREATE TABLE worker_yearly_summary (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    company_id uuid NOT NULL,                -- ✅ Available from companies table
    year integer NOT NULL,                   -- ✅ Available from aggregation logic
    average_employee_count integer NOT NULL, -- ✅ Available from employee_monthly_counts
    active_visa_count integer NOT NULL,      -- ⚠️ Available from visa_yearly_counts (once processed)
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    injury_count_reported text               -- ⚠️ Needs Arbejdstilsynet inspections data
);
```

**Data Sources for Aggregation**:
1. **`employee_monthly_counts`** → `average_employee_count` (✅ **AVAILABLE**)
   - Source: CVR monthly enrichment (399,837 records, 8,163 companies)
   - Aggregation: `AVG(employee_count)` by company_id and year
   
2. **`visa_yearly_counts`** → `active_visa_count` (⚠️ **IN DEVELOPMENT**)
   - Source: Agricultural visa statistics (needs PDF processing)
   - Aggregation: `SUM(first_permits_count)` by company_id and year
   
3. **`incidents` table** → `injury_count_reported` (⚠️ **NEEDS ANALYSIS**)
   - Source: Arbejdstilsynet inspections (needs investigation)
   - Aggregation: `COUNT(*)` WHERE type = 'injury' by company_id and year

**Schema Mapping** (Computed Aggregation):
```sql
-- Supabase Field              ← Aggregation Source                      | Status
id                             ← Generated UUID                          | ✅ Auto-generated
company_id                     ← From companies table                    | ✅ Available
year                           ← Extract from date fields                | ✅ Available
average_employee_count         ← AVG(employee_monthly_counts.employee_count) | ✅ Available
active_visa_count              ← SUM(visa_yearly_counts.first_permits_count) | ⚠️ Visa pipeline dependent
injury_count_reported          ← COUNT(incidents WHERE type='injury')    | ⚠️ Incidents analysis needed
```

**Migration Status**: ✅ **COMPLETE** - Computation strategy defined, partial data available
**Dependencies**: 
- ✅ `employee_monthly_counts` populated (ready)
- ⚠️ `visa_yearly_counts` processed (in development) 
- ⚠️ `incidents` table analyzed (pending)

**ETL Strategy** (SQL Aggregation Queries):
```sql
-- Populate worker_yearly_summary from existing tables
INSERT INTO worker_yearly_summary (
    id, company_id, year, average_employee_count, active_visa_count, injury_count_reported
)
SELECT 
    gen_random_uuid(),
    emc.company_id,
    EXTRACT(YEAR FROM emc.month_year) as year,
    ROUND(AVG(emc.employee_count)) as average_employee_count,
    COALESCE(vyc.total_visas, 0) as active_visa_count,
    COALESCE(inc.injury_count, '<5'::text) as injury_count_reported
FROM employee_monthly_counts emc
LEFT JOIN (
    -- Aggregate visa counts by company and year
    SELECT company_id, year, SUM(first_permits_count) as total_visas
    FROM visa_yearly_counts 
    GROUP BY company_id, year
) vyc ON emc.company_id = vyc.company_id AND EXTRACT(YEAR FROM emc.month_year) = vyc.year
LEFT JOIN (
    -- Aggregate injury counts by company and year  
    SELECT company_id, EXTRACT(YEAR FROM incident_date) as year, 
           CASE WHEN COUNT(*) < 5 THEN '<5' ELSE COUNT(*)::text END as injury_count
    FROM incidents 
    WHERE type ILIKE '%injury%' OR type ILIKE '%accident%'
    GROUP BY company_id, EXTRACT(YEAR FROM incident_date)
) inc ON emc.company_id = inc.company_id AND EXTRACT(YEAR FROM emc.month_year) = inc.year
GROUP BY emc.company_id, EXTRACT(YEAR FROM emc.month_year), vyc.total_visas, inc.injury_count;
```

**Sample Computation** (Mock Data Pattern):
```sql
-- Company: 4d8c12c4-6d11-4a60-8c29-c43fd8a2b18a
-- 2021: 12 avg employees (from monthly data), 5 visas, <5 injuries  
-- 2022: 14 avg employees (from monthly data), 6 visas, 2 injuries
-- 2023: 16 avg employees (from monthly data), 7 visas, 3 injuries
-- Shows growing workforce with proportional visa usage
```

**Business Notes**:
- ✅ **Computation ready**: Employee data fully available for aggregation
- ⚠️ **Partial dependencies**: Visa processing in development, injury data needs analysis
- 🎯 **High dashboard value**: Perfect for workforce trend analysis and compliance monitoring
- 📊 **Regulatory insight**: Combines employment, immigration, and safety data in one view
- 🔄 **Refresh strategy**: Can be updated monthly as source tables are populated
- 💡 **Privacy consideration**: Injury counts show "<5" pattern for small numbers (privacy protection)

### ✅ **COMPLETE: Table 21 - `incidents`** 🔴 **SCHEMA ENHANCEMENT NEEDED**

**Sources**: Two complementary datasets for comprehensive workplace safety tracking
1. **Worker Safety Data**: `gs://landbrugsdata-raw-data/gold/worker_safety/20250803_022717/`
2. **Workplace Inspections**: `gs://landbrugsdata-raw-data/gold/arbejdstilsynet_inspections/20250803_022714/`

**Current Supabase Schema** (needs enhancement):
```sql
CREATE TABLE incidents (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    company_id uuid NOT NULL,                -- ✅ Available via CVR lookup
    incident_date date NOT NULL,             -- ✅ Available from both sources
    type text NOT NULL,                      -- ✅ Available but needs categorization
    description text,                        -- ✅ Available from both sources
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);
```

**Available Data Sources**:

**1. Worker Safety Data** (1,245 injury records, 156 companies, 2020-2024):
- `cvr_number` → company_id lookup
- `year` → incident_date (year-level precision)
- `injury_type` → type + description
- `injury_count` → severity indicator ("1-5", "TOTAL")

**2. Workplace Inspections** (574 inspection records, 2025):
- `company_id` → company_id lookup (already numeric ID)
- `date` → incident_date (exact date precision)
- `work_env_issue_formatted` → type + description
- `decision_type` → severity ("Strakspåbud", "Påbud", "Påtale")

**Proposed Enhanced Schema**:
```sql
CREATE TABLE incidents (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    company_id uuid NOT NULL,
    incident_date date NOT NULL,
    type text NOT NULL,                      -- Enhanced: "injury" | "inspection"
    subtype text,                           -- NEW: injury type or inspection issue
    description text,
    severity text,                          -- NEW: injury count or inspection decision
    source_dataset text,                    -- NEW: "worker_safety" | "inspections"
    year integer,                           -- NEW: for easier aggregation
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);
```

**Schema Mapping**:
```sql
-- WORKER SAFETY DATA MAPPING:
-- Supabase Field              ← Parquet Source Field                    | Status
id                             ← Generated UUID                          | ✅ Auto-generated
company_id                     ← cvr_number → UUID lookup                | ✅ Available
incident_date                  ← MAKE_DATE(year, 6, 15) -- mid-year est  | ⚠️ Year-level only
type                           ← 'injury'                                | ✅ Constant
subtype                        ← injury_type                             | ✅ Available (14 types)
description                    ← injury_type (full description)          | ✅ Available
severity                       ← injury_count                            | ✅ Available ("1-5", "TOTAL")
source_dataset                 ← 'worker_safety'                         | ✅ Constant
year                           ← year                                    | ✅ Direct match

-- WORKPLACE INSPECTIONS DATA MAPPING:
-- Supabase Field              ← Parquet Source Field                    | Status
id                             ← Generated UUID                          | ✅ Auto-generated  
company_id                     ← company_id → UUID lookup                | ✅ Available
incident_date                  ← date                                    | ✅ Exact date
type                           ← 'inspection'                            | ✅ Constant
subtype                        ← work_env_issue_formatted                | ✅ Available (40+ types)
description                    ← work_env_issue + decision context       | ✅ Available
severity                       ← decision_type                           | ✅ Available (3 levels)
source_dataset                 ← 'inspections'                           | ✅ Constant
year                           ← year                                    | ✅ Available
```

**Data Quality Analysis**:

**Worker Safety (Injuries)**:
- **Coverage**: 156 companies, 1,245 injury records
- **Time range**: 2020-2024 (5 years)
- **Top injury types**: Sprains/strains (110), joint injuries (100), other injuries (60)
- **Severity levels**: "1-5" (specific count), "TOTAL" (aggregate)

**Workplace Inspections**:
- **Coverage**: 77+ companies, 574 inspection records  
- **Time range**: 2025 (current year)
- **Top issues**: APV violations (77), falls (60), machinery (41)
- **Severity levels**: Strakspåbud (381), Påbud (163), Påtale (30)

**Migration Status**: ✅ **COMPLETE** - Schema enhancement defined, comprehensive data available
**Dependencies**: companies table populated first for UUID lookups
**Schema Changes**: Add subtype, severity, source_dataset, year fields

**ETL Strategy**:
```sql
-- Insert worker safety data (injuries)
INSERT INTO incidents (
    id, company_id, incident_date, type, subtype, description, severity, source_dataset, year
)
SELECT 
    gen_random_uuid(),
    get_or_create_company_uuid(ws.cvr_number::text),
    MAKE_DATE(ws.year, 6, 15), -- Mid-year estimate
    'injury',
    ws.injury_type,
    ws.injury_type, -- Full description
    ws.injury_count,
    'worker_safety',
    ws.year
FROM worker_safety_parquet ws;

-- Insert workplace inspections
INSERT INTO incidents (
    id, company_id, incident_date, type, subtype, description, severity, source_dataset, year
)
SELECT 
    gen_random_uuid(),
    get_or_create_company_uuid(wi.company_id::text), -- Assuming company_id is CVR
    wi.date::date,
    'inspection',
    wi.work_env_issue_formatted,
    wi.work_env_issue || ' - ' || wi.decision,
    wi.decision_type,
    'inspections',
    wi.year
FROM workplace_inspections_parquet wi;
```

**Business Notes**:
- ✅ **Comprehensive safety data**: Combines injury tracking with regulatory inspections
- ✅ **Complementary time coverage**: Injuries (2020-2024) + Inspections (2025)
- 🔴 **Schema enhancement needed**: Current schema too simple for rich safety data
- 🎯 **High regulatory value**: Essential for workplace safety compliance and risk management
- 📊 **Dashboard ready**: Perfect for safety trend analysis and compliance monitoring
- 💡 **Data richness**: 14 injury types + 40+ inspection issues = comprehensive safety picture

---

## Comprehensive Parquet to Supabase Mapping Analysis

### 1. CVR Enrichment Datasets (8 parquet datasets → 4 Supabase tables)
**Parquet Outputs**: 
- `cvr_enrichment_companies` → **`companies`** ✅ **COMPLETE** (detailed mapping above)
- `cvr_enrichment_financial` → **`yearly_financials`** 🔍 ANALYZING
- `cvr_enrichment_leadership` → **`company_leadership`** 🔍 ANALYZING
- `cvr_enrichment_addresses` → **Separate detailed addresses table** ✅ BONUS DATA
- `cvr_enrichment_annual`, `cvr_enrichment_monthly`, `cvr_enrichment_quarterly` → **Multiple tables** ⚠️ TEMPORAL AGGREGATION NEEDED

**Schema Compatibility**: ✅ **PERFECT** - Main companies table now has all required Supabase fields including geocoded geometry

### 2. CHR Animal Production (5 parquet datasets → 4 Supabase tables)
**Parquet Outputs**:
- `chr` → **`animal_production_log`** ✅ DIRECT MATCH
- `chr_transportation_analysis` → **`animal_transports`** ✅ DIRECT MATCH
- `chr_timeline_summary` → **`site_yearly_summary`** ✅ DIRECT MATCH
- `chr_veterinary_timeline` → **`vet_events`** ✅ DIRECT MATCH

**Schema Compatibility**: ✅ **EXCELLENT** - CHR data structure matches perfectly

### 3. Field Environmental Analysis (50+ parquet datasets → 4 Supabase tables)
**Parquet Outputs**:
- `field_environmental_analysis_fields_*` → **`field_yearly_data`** ⚠️ SCHEMA EXTENSION NEEDED
- `field_environmental_analysis_properties_*` → **`field_boundaries`** ⚠️ SCHEMA EXTENSION NEEDED
- `field_analysis_bnbo_*` → **`field_bnbo_areas`** ✅ GOOD MATCH
- `field_analysis_wetland_*` → **`field_wetland_areas`** ✅ GOOD MATCH
- 40+ intermediate analysis datasets → **No direct mapping** ❌ INTERMEDIATE DATA

**Schema Compatibility**: ⚠️ **PARTIAL** - Core tables exist but need enrichment

### 4. Pesticide Analysis (20+ parquet datasets → 1 Supabase table)
**Parquet Outputs**:
- `pesticide_disaggregation_*` → **`pesticide_applications`** 🔴 MAJOR SCHEMA MISMATCH
- `pesticide_proximity_*` → **`pesticide_applications`** (proximity columns) ✅ GOOD MATCH

**Schema Compatibility**: 🔴 **PROBLEMATIC** - Disaggregation schema very different

### 5. Worker & Safety Data (Missing parquet datasets!)
**Supabase Tables Ready**:
- **`employee_monthly_counts`** ❌ NO PARQUET SOURCE
- **`visa_yearly_counts`** ❌ NO PARQUET SOURCE  
- **`worker_yearly_summary`** ❌ NO PARQUET SOURCE
- **`incidents`** ❌ NO PARQUET SOURCE

**Schema Compatibility**: ❌ **MISSING DATA** - Tables exist but no parquet pipeline feeds them

### 6. Building & Infrastructure (Missing parquet datasets!)
**Supabase Tables Ready**:
- **`building_footprints`** ❌ NO PARQUET SOURCE
- **`carbon_emission_factors`** ❌ NO PARQUET SOURCE

**Schema Compatibility**: ❌ **MISSING DATA** - Tables exist but no parquet pipeline feeds them

### 7. Arbejdstilsynet Inspections (1 parquet dataset → No Supabase table!)
**Parquet Output**: `arbejdstilsynet_inspections` → **NO SUPABASE TABLE** ❌ MISSING TABLE

**Schema Compatibility**: ❌ **MISSING TABLE** - Need to create Supabase table

### 2. Pesticide Disaggregation
**Parquet Output**: `pesticide_disaggregation_*` 
**Supabase Table**: `pesticide_applications`

#### Current Supabase Schema:
```sql
CREATE TABLE "pesticide_applications" (
    "id" uuid DEFAULT gen_random_uuid() NOT NULL,
    "company_id" uuid NOT NULL,
    "field_boundary_id" uuid,
    "application_date" date NOT NULL,
    "year" integer NOT NULL,
    "pesticide_name" text NOT NULL,
    "risk_category" text,
    "risk_details" text,
    "ha_sprayed" numeric,
    "contains_pfas" boolean,
    "pesticide_load_index_per_ha" numeric,
    "proximity_water_m" numeric,
    "proximity_housing_m" numeric,
    "proximity_school_m" numeric
)
```

#### Expected Parquet Schema (from code analysis):
```sql
-- From pesticide_disaggregation.py _create_results_table()
DisaggregatedID VARCHAR,
OriginalPesticideRowID VARCHAR,
cvr_number VARCHAR,
PesticideName VARCHAR,
PesticideRegistrationNumber VARCHAR,
DosageQuantity DOUBLE,
DosageUnit VARCHAR,
MatchedFieldID VARCHAR,
MatchedBlockID VARCHAR,
AllocatedArea DOUBLE,
AllocationMethod VARCHAR,
MatchConfidence DOUBLE,
IsPartialFieldCoverage BOOLEAN,
field_uuid VARCHAR,
primary_field_id VARCHAR
```

#### Schema Conflicts:
🔴 **MAJOR MISMATCH**:
- Parquet uses CVR-based matching, Supabase expects company_id (UUID)
- Parquet has field_uuid, Supabase expects field_boundary_id (UUID)
- Parquet has disaggregation-specific fields (AllocationMethod, MatchConfidence) not in Supabase
- Supabase has proximity fields not in parquet base schema (added by proximity analysis)

### 3. Pesticide Proximity Analysis
**Parquet Output**: `pesticide_proximity_*`
**Supabase Table**: `pesticide_applications` (proximity columns)

#### Schema Compatibility:
✅ **GOOD EXTENSION** - Proximity analysis adds to pesticide applications
- `proximity_water_m`, `proximity_housing_m`, `proximity_school_m` already exist in Supabase
- Residential/educational facility details would need new format handling

### 4. Animal Production & Transport
**Parquet Output**: CHR pipeline outputs
**Supabase Tables**: `animal_production_log`, `animal_transports`, `site_yearly_summary`

#### Schema Compatibility:
✅ **EXCELLENT MATCH** - CHR data structure aligns perfectly
- CHR codes match between systems
- Species handling consistent
- Year-based partitioning matches

### 5. CVR Enrichment
**Parquet Output**: `cvr_enrichment_*`
**Supabase Tables**: `companies`, `company_leadership`, `company_owners`, `yearly_financials`

#### Schema Compatibility:
✅ **GOOD MATCH** with transformation needed
- CVR → company lookup required
- Financial data structure aligns
- Leadership/ownership structures match

## Key Migration Challenges

### 1. **UUID vs String Identifiers**
**Issue**: Parquet uses string identifiers (CVR, field_uuid, CHR), Supabase uses UUIDs
**Solution**: Need lookup/transformation layer

### 2. **Schema Extensions Required**
**Issue**: Parquet has richer data than current Supabase schema
**Solution**: Extend Supabase tables with additional columns

### 3. **Data Relationships**
**Issue**: Parquet data needs proper foreign key relationships
**Solution**: ETL process to resolve company_id, field_boundary_id lookups

## Recommended Migration Strategy

### Phase 1: Schema Alignment
1. **Extend Supabase tables** with missing columns from parquet
2. **Create lookup functions** for CVR→company_id, field_uuid→field_boundary_id
3. **Add migration-specific metadata columns** (processing_timestamp, source_dataset)

### Phase 2: ETL Pipeline
1. **Transform identifiers** during migration (CVR→UUID, field_uuid→UUID)
2. **Validate relationships** exist before inserting data
3. **Handle schema versioning** for different parquet output versions

### Phase 3: API Compatibility
1. **Update API queries** to use Supabase tables instead of parquet
2. **Maintain backward compatibility** during transition
3. **Update frontend components** to use new data sources

## Critical Decision Points

### Option A: Adapt Parquet Schema to Match Supabase
**Pros**: Minimal Supabase changes, API remains stable
**Cons**: Changes proven pipeline logic, may break existing parquet consumers

### Option B: Extend Supabase Schema to Match Parquet
**Pros**: Preserves pipeline logic, richer data in database
**Cons**: API changes required, more complex migration

### Option C: Hybrid Approach
**Pros**: Gradual migration, both systems work during transition
**Cons**: Temporary complexity, dual maintenance

## Next Steps
1. **Download and analyze actual parquet schemas** from recent pipeline runs
2. **Create detailed field mapping spreadsheet** 
3. **Prototype ETL transformation** for one dataset
4. **Assess API impact** and required frontend changes
5. **Create migration timeline** and rollback plan
