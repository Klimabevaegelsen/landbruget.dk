# Pesticide Compliance Workflow

> **Manual Processing**: Advanced regulatory compliance analysis for Danish pesticide applications

---

## What This Workflow Does

The Pesticide Compliance workflow performs comprehensive regulatory compliance analysis of Danish agricultural pesticide applications by cross-referencing field-level pesticide usage data with official BMD (Danish Pesticide Database) restrictions, withdrawal dates, and approved dosage limits. This sophisticated analysis identifies timing violations, dosage exceedances, and usage of withdrawn products.

### Why This Data Matters
- **Regulatory Enforcement**: Enables identification of clear pesticide regulation violations for enforcement action
- **Agricultural Compliance**: Helps agricultural companies ensure compliance with Danish pesticide regulations
- **Environmental Protection**: Identifies misuse that could impact environmental safety and water quality
- **EU Reporting**: Supports Denmark's compliance reporting under EU pesticide regulation frameworks
- **Risk Assessment**: Provides data for assessing agricultural practices' regulatory compliance risks

### Key Statistics
- **Analysis Scope**: Field-level pesticide applications cross-referenced with BMD regulatory database
- **Coverage**: ~92% of agricultural pesticide applications through disaggregation matching
- **Detection Types**: Timing violations, dosage exceedances, withdrawn product usage, registration errors
- **Processing**: Gold layer only (advanced analysis combining multiple processed datasets)
- **Execution**: Matrix-based processing by agricultural year for comprehensive temporal analysis

---

## Data Sources and Integration

### Primary Data Sources
This workflow integrates multiple processed datasets for comprehensive compliance analysis:

| Data Source | Purpose | Layer | Temporal Scope |
|-------------|---------|-------|----------------|
| **BMD Pesticide Database** | Regulatory restrictions and withdrawal dates | Silver | Current regulatory status |
| **Pesticide Disaggregation** | Field-level pesticide applications | Gold | Agricultural years 2015-2024 |
| **FVM Marker Data** | Crop classification for dosage limits | Silver | Field boundaries (Y+1 pattern) |
| **Plante IT API** | Official dosage limits by crop-product combination | External API | Real-time regulatory limits |

### Advanced Data Integration

#### Temporal Alignment System
- **Agricultural Years**: August 1 - July 31 (e.g., 2023 = Aug 2023 - Jul 2024)
- **Y+1 Pattern**: Pesticide year X uses field boundaries from year X+1 (matching disaggregation logic)
- **Restriction Date Comparison**: Applications compared against BMD `frist_for_anvendelse_og_besiddelse` dates
- **Multi-Year Analysis**: Matrix processing across available agricultural years (2015-2024)

#### Registration Mismatch Detection
- **Product Matching**: Identifies cases where expired registration numbers were used but valid alternatives exist
- **Ingredient Analysis**: Matches products by name, active ingredients, and concentration
- **Status Classification**: Distinguishes between true violations and potential input errors
- **Alternative Suggestions**: Provides valid registration numbers for expired products

#### Unit Sanitization Integration
- **Statistical Correction**: Applies unit sanitization to fix common dosage unit mismatches
- **BMD Cross-Reference**: Uses BMD unit standards (`enhed_er`) for validation
- **Automatic Correction**: Fixes detectable unit errors before compliance analysis
- **Manual Review Flagging**: Identifies cases requiring human verification

### Data Privacy and Compliance
- **Company Identification**: Uses CVR numbers for regulatory compliance tracking
- **Field-Level Analysis**: Maintains field UUID linkage for precise violation location
- **Regulatory Purpose**: Data processing justified for official regulatory compliance monitoring
- **Access Controls**: Results accessible only for regulatory enforcement and compliance support

---

## Data Processing Steps

### 🥇 Gold Layer: Advanced Regulatory Compliance Analysis
**What happens**: We perform sophisticated multi-dimensional compliance analysis combining regulatory restrictions, field applications, and approved limits
**Why**: Regulatory compliance requires precise cross-referencing of multiple authoritative sources with complex temporal and dosage validation rules

**Core Analysis Components**:

#### 1. Timing Violation Detection
- **Restriction Date Analysis**: Compare application dates with BMD `frist_for_anvendelse_og_besiddelse` (deadline for use and possession)
- **Agricultural Year Mapping**: Align pesticide applications with proper seasonal boundaries
- **Clear Violation Logic**: Applications after restriction date = definitive violation
- **Temporal Precision**: Uses exact date comparisons, not approximations

#### 2. Dosage Compliance Assessment
- **API Integration**: Fetches official dosage limits from Plante IT Pesticide Service API
- **Crop-Product Mapping**: Links internal crop codes to API crop classifications (44 mapped crop types)
- **Per-Hectare Calculation**: Converts application quantities to per-hectare dosages for comparison
- **Violation Categories**: 
  - `DOSAGE_COMPLIANT`: Within approved limits
  - `MODERATE_OVERDOSE`: 1-2x approved dosage
  - `MAJOR_OVERDOSE`: >2x approved dosage
  - `UNIT_MISMATCH`: Incompatible dosage units requiring review

#### 3. Withdrawn Product Detection
- **Status Analysis**: Identifies usage of products with BMD status `Tilbagekaldt`, `Udløbet`, `Produkt udløbet`, `Produkt afmeldt`
- **Registration Error Detection**: Distinguishes between true violations and potential input errors using product matching
- **Alternative Identification**: Suggests valid registration numbers for products used with expired registrations
- **Violation Classification**: Separates confirmed violations from likely data entry errors

#### 4. Statistical Unit Sanitization
- **Automatic Correction**: Uses `PesticideUnitSanitizer` to fix common unit mismatches before analysis
- **BMD Cross-Reference**: Validates units against official BMD standards
- **Success Rate Tracking**: Monitors correction rates and manual review requirements
- **Quality Metrics**: Reports on unit standardization effectiveness

**Quality Controls**:
- **Field-Level Precision**: Maintains field UUID linkage for exact violation locations
- **Multi-Source Validation**: Cross-references BMD, disaggregation, and API data
- **Temporal Accuracy**: Uses agricultural year boundaries for proper seasonal analysis
- **Dosage Precision**: Calculates exact per-hectare dosages with unit standardization

**Output**: Comprehensive compliance analysis with field-level violation details, company summaries, and regulatory enforcement data

---

## Workflow Schedule and Execution

### Matrix Processing Architecture
- **Execution Type**: Manual trigger with matrix-based year processing
- **Matrix Strategy**: Parallel processing of individual agricultural years (2015-2024)
- **Concurrency Control**: Configurable parallel jobs (default: 3 concurrent years)
- **Processing Options**: 
  - Single year: `"2023"`
  - Multiple years: `"2020,2021,2022"`
  - All available: `"all"`

### Processing Performance
- **Data Volume**: ~500K+ field-level pesticide applications across analyzed years
- **Memory Usage**: High (~8-16GB) due to complex multi-table joins and API integration
- **API Calls**: Extensive Plante IT API integration for dosage limits (cached for efficiency)
- **Processing Duration**: 2-4 hours per agricultural year depending on data volume
- **Storage**: ~50-200MB per year for compliance analysis results

### Advanced Features
- **Unit Sanitization**: Statistical correction of dosage unit mismatches
- **Registration Mismatch Detection**: Identifies potential data entry errors vs. true violations
- **Crop-Product API Integration**: Real-time dosage limit validation
- **Multi-Dimensional Analysis**: Timing, dosage, and product status violations in single workflow

---

## Data Quality and Limitations

### Data Quality Assessment
| Quality Metric | Status | Details |
|----------------|--------|---------|
| **Completeness** | Excellent | 92% coverage through pesticide disaggregation, BMD regulatory database |
| **Accuracy** | Excellent | Multi-source validation (BMD, API, field data) with unit sanitization |
| **Timeliness** | Good | BMD restrictions current, field applications by agricultural year |
| **Precision** | Excellent | Field-level analysis with exact dosage calculations and date comparisons |

### Known Issues and Limitations

#### Data Integration Challenges
- **Unit Standardization**: Dosage units may require sanitization (~5-10% of records)
- **Registration Mismatches**: Some violations may be data entry errors rather than true violations
- **API Availability**: Dosage compliance depends on Plante IT API accessibility
- **Temporal Alignment**: Complex Y+1 pattern matching between pesticide and field data

#### Analysis Scope Limitations
- **Coverage Gaps**: 8% of pesticide applications not covered by disaggregation matching
- **Crop Mapping**: Limited to 44 mapped crop types for API dosage validation
- **Historical Restrictions**: BMD restriction dates may not cover all historical applications
- **Product Variations**: Similar products with different registrations may complicate analysis

#### Methodological Considerations
- **Violation Certainty**: Focus on "clear violations" only - no speculation about edge cases
- **Registration Errors**: Potential data entry mistakes flagged separately from confirmed violations
- **Dosage Context**: Per-hectare calculations may not reflect application method variations
- **Enforcement Scope**: Analysis identifies violations but doesn't determine enforcement action

### Recommended Uses
✅ **This data is excellent for**:
- Regulatory enforcement identification of clear pesticide violations
- Agricultural compliance monitoring and company self-assessment
- Environmental risk assessment of pesticide misuse patterns
- Policy analysis of regulatory effectiveness and compliance rates
- Research on agricultural compliance behavior and violation patterns

⚠️ **Use with caution for**:
- Registration mismatch cases - May indicate data entry errors rather than violations
- Unit mismatch violations - Require manual review for accuracy
- Historical comparisons - Regulatory standards may have changed over time

❌ **Not recommended for**:
- Automated penalty assessment - Requires regulatory review and due process
- Individual farmer targeting - Use for pattern analysis, not individual enforcement
- Real-time monitoring - Analysis based on processed annual datasets, not live data

---

## Usage Examples and Access

### Common Questions This Data Answers
1. **Which companies have clear pesticide regulation violations?** - Field-level analysis with CVR identification and violation details
2. **What are the most commonly violated pesticide restrictions?** - Product analysis showing violation frequency and affected area
3. **How effective are current regulatory compliance rates?** - Statistical analysis of compliance vs. violation rates across agricultural sector
4. **Where are dosage exceedances most common?** - Spatial and crop-based analysis of dosage violations

### Example Analyses
#### Regulatory Enforcement Support
**Question**: Which agricultural operations used restricted pesticides after official withdrawal dates?
**Data Used**: BMD restriction dates, field-level pesticide applications, company CVR numbers
**Method**: Temporal analysis comparing application dates with `frist_for_anvendelse_og_besiddelse` dates
**Output**: Field-level violation records with company identification and affected areas
**Limitations**: Requires verification of restriction date accuracy and consideration of grace periods

#### Agricultural Compliance Assessment
**Question**: How do dosage compliance rates vary by crop type and company size?
**Data Used**: Plante IT dosage limits, field applications, crop classifications, company data
**Method**: Statistical analysis of dosage ratios by crop and company characteristics
**Output**: Compliance rate analysis with risk factor identification
**Limitations**: Limited to mapped crop types and requires unit standardization verification

#### Environmental Risk Evaluation
**Question**: What is the spatial distribution of pesticide violations near sensitive water bodies?
**Data Used**: Field-level violations, water body proximity data, environmental sensitivity mapping
**Method**: Spatial analysis of violation locations relative to environmental features
**Output**: Risk maps showing violation concentration near sensitive areas
**Limitations**: Requires integration with environmental datasets not included in this workflow

### Data Access
- **Regulatory Access**: Field-level violation data for enforcement agencies
- **Company Access**: Company-specific compliance reports for self-assessment
- **Research Access**: Anonymized compliance statistics for academic research
- **Public Access**: Aggregate compliance statistics and violation trend analysis

---

## Technical Details

<details>
<summary>Click to expand technical specifications</summary>

### Data Schema
#### Compliance Analysis Dataset
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
field_uuid | VARCHAR | Unique field identifier | "123e4567-e89b-12d3-a456-426614174000"
agricultural_year | VARCHAR | Analysis year (Aug-Jul) | "2023_2024"
application_year | INTEGER | Application year | 2023
cvr_number | VARCHAR | Company registration number | "12345678"
pesticide_name | VARCHAR | Commercial pesticide name | "Roundup Energy"
pesticide_registration_number | VARCHAR | BMD registration number | "2019-00123"
crop_code | VARCHAR | Internal crop classification | "11"
crop_name | VARCHAR | Crop name in Danish | "Vinterhvede"
api_crop_name_from_plante_it | VARCHAR | Official API crop name | "Vinterhvede"
restriction_date_parsed | DATE | BMD restriction deadline | "2023-06-30"
produktstatus | VARCHAR | BMD product status | "Produkt godkendt"
compliance_status | VARCHAR | Overall compliance result | "TIMING_VIOLATION"
dosage_compliance_status | VARCHAR | Dosage-specific compliance | "DOSAGE_COMPLIANT"
actual_dosage_per_ha | DOUBLE | Applied dosage per hectare | 2.5
api_max_dosage_per_ha | DOUBLE | Maximum allowed dosage | 2.0
dosage_ratio | DOUBLE | Actual/allowed dosage ratio | 1.25
is_potential_registration_error | BOOLEAN | Likely data entry error flag | false
suggested_valid_registration | VARCHAR | Alternative valid registration | "2023-00456"
analysis_timestamp | TIMESTAMP | Analysis execution time | "2025-01-15T10:30:00"
```

#### Compliance Status Values
- **COMPLIANT**: No violations detected
- **TIMING_VIOLATION**: Application after BMD restriction date
- **WITHDRAWN_PRODUCT_USE**: Use of withdrawn/expired product
- **DOSAGE_VIOLATION**: Exceeds approved dosage limits
- **POTENTIAL_REGISTRATION_ERROR**: Likely data entry mistake

#### Dosage Compliance Status Values
- **DOSAGE_COMPLIANT**: Within approved limits
- **MODERATE_OVERDOSE**: 1-2x approved dosage
- **MAJOR_OVERDOSE**: >2x approved dosage
- **UNIT_MISMATCH**: Incompatible units
- **NO_API_LIMIT**: No dosage limit available

### Storage Locations
- **Gold Output**: `gs://landbruget-data/gold/pesticide_compliance/{timestamp}/`
- **Compliance Data**: `compliance_analysis_{agricultural_year}.parquet`
- **Summary Statistics**: `compliance_summary.json`
- **Human Report**: `compliance_report.md`

### Processing Infrastructure
- **Platform**: Manual matrix execution via GitHub Actions
- **Resources**: 16GB RAM, high-CPU for complex joins and API integration
- **Dependencies**: 
  - BMD pesticide database (silver)
  - Pesticide disaggregation (gold)
  - FVM marker data (silver)
  - Plante IT Pesticide Service API
- **Performance**: 2-4 hours per agricultural year

### API Integration Details
- **Service**: Plante IT Pesticide Service API
- **Endpoint**: `https://pesticideservice.dlbr.dk/api`
- **Authentication**: HTTP Basic Auth via GitHub Secrets
- **Crop Mapping**: 44 internal crop codes to API crop IDs
- **Caching**: API responses cached to minimize repeated requests
- **Rate Limiting**: Controlled request frequency to respect API limits

### Matrix Processing Configuration
```yaml
# Example matrix configuration
years: "2020,2021,2022"  # Specific years
years: "all"             # All available years
max_parallel_jobs: "3"   # Concurrent processing
stage: "gold"            # Processing stage
dry_run: false          # Execute analysis
```

### Unit Sanitization Features
- **Statistical Correction**: Automatic unit mismatch detection and correction
- **BMD Cross-Reference**: Uses official BMD unit standards for validation
- **Success Tracking**: Reports correction rates and manual review requirements
- **Quality Metrics**: Monitors sanitization effectiveness across products

</details>

---

## Contact and Support

### Workflow Maintainer
- **Primary Contact**: Regulatory Compliance Data Team
- **Response Time**: 1-2 business days for regulatory matters

### Reporting Issues
- **Data Quality Issues**: Report via GitHub issues with "Pesticide Compliance" label
- **API Problems**: Contact system administrators for Plante IT API issues
- **Compliance Questions**: Direct regulatory questions to appropriate Danish authorities

### Documentation Updates
- **Last Updated**: January 2025
- **Update Schedule**: Reviewed when regulatory requirements change
- **Version**: 1.0

---

*This documentation is part of the Landbruget.dk transparency initiative to make agricultural regulatory compliance data accessible and trustworthy.*
