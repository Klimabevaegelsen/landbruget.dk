# Pesticide Disaggregation Workflow

> **Manual Processing**: Field-level pesticide application analysis linking sales data to specific agricultural fields

---

## What This Workflow Does

The Pesticide Disaggregation workflow solves a critical agricultural data problem: pesticide companies report their applications at the company level (e.g., "Company ABC applied 100L of pesticide X to 25 hectares of wheat fields"), but we need to know which specific fields received the pesticide for environmental and health analysis.

**The Challenge**: Companies report aggregate applications like "We applied 50L of herbicide to 25 hectares of wheat", but we have field data showing "Company ABC has 3 wheat fields: 10ha, 8ha, and 7ha". We need to determine how much pesticide each individual field received.

**The Solution**: This workflow uses a proven 4-strategy approach that achieves 92% coverage by matching application areas to field areas and distributing pesticides proportionally across fields.

### Why This Data Matters
- **Environmental Monitoring**: Track pesticide use patterns and environmental exposure
- **Health Assessment**: Identify areas with high pesticide exposure for public health analysis
- **Regulatory Compliance**: Monitor adherence to pesticide usage regulations and restrictions
- **Agricultural Research**: Study pesticide effectiveness and usage patterns across different crops
- **Policy Development**: Inform pesticide regulation and agricultural policy decisions

### Key Statistics
- **Data Volume**: Company-level pesticide applications disaggregated to 2.5M+ individual fields
- **Coverage**: 92% successful disaggregation rate using proven 4-strategy approach
- **Update Frequency**: Manual execution (typically annually after application data is complete)
- **Historical Data**: Disaggregated data available from 2015-2023
- **Processing Scale**: Matrix workflow processes each year independently (2015-2023)

---

## Data Sources and Collection

### Official Sources
This workflow combines data from multiple authoritative Danish sources:

| Data Source | Agency | Purpose | Data Type |
|-------------|--------|---------|-----------|
| Pesticide Applications | Drive Data Pipeline | Company-reported pesticide applications | Application records by company/crop/area |
| FVM Agricultural Fields | Food and Veterinary Administration | Field boundaries and crop information | Geospatial field data |
| BMD Pesticide Database | Danish Environmental Protection Agency | Pesticide product information and restrictions | Product database |
| Company Registry (CVR) | Danish Business Authority | Company information and locations | Business registry |

### How We Collect the Data

#### Pesticide Application Data
- **Collection Method**: Company-reported applications via Drive Data Pipeline
- **Frequency**: Annual collection from regulatory submissions
- **Format**: Company-level applications with crop type, area, and pesticide details
- **Quality Controls**: Application area validation, product code verification

#### Field-Level Disaggregation Process
- **Method**: 4-strategy approach to match application areas to field areas
- **Approach**: Uses 2% area tolerance, crop compatibility, field size, and company ownership (CVR matching)
- **Validation**: Preserves total pesticide amounts and validates area matching
- **Success Rate**: Achieves 92% coverage using proven area-matching strategies

### Data Privacy and Compliance
- **Personal Data**: No personal information - aggregated sales data only
- **Business Privacy**: Individual company sales data is aggregated to protect business confidentiality
- **Legal Compliance**: Follows Danish environmental data sharing regulations
- **Access Restrictions**: Field-level estimates only, no company-specific sales data

---

## Data Processing Steps

### 🥉 Bronze Layer: Raw Data Integration
**What happens**: We collect and integrate raw pesticide sales and field data
**Why**: Preserves original data sources for audit and methodology verification
**Output**: Raw sales data, field boundaries, and pesticide product information

**No Changes Made**: All source data stored in original format

### 🥈 Silver Layer: Data Preparation and Validation
**What happens**: We prepare and validate data for disaggregation analysis
**Why**: Disaggregation requires clean, consistent data across multiple sources

**Specific transformations**:
- **Product matching**: Link sales products to BMD pesticide database
- **Spatial preparation**: Validate field geometries and calculate areas
- **Company matching**: Link sales data to field ownership via CVR numbers
- **Crop compatibility**: Determine which pesticides can be used on which crops

**Quality checks**:
- **Data completeness**: Verify all required fields are present
- **Spatial accuracy**: Validate field boundaries and areas
- **Product validation**: Check pesticide products against official database

**Output**: Clean, validated datasets ready for disaggregation modeling

### 🥇 Gold Layer: Field-Level Disaggregation
**What happens**: We disaggregate aggregate sales data to individual field estimates
**Why**: Enables field-level environmental and health impact analysis

**Disaggregation methodology**:
- **Spatial allocation**: Distribute sales within geographic regions to fields
- **Crop-based allocation**: Weight by crop compatibility and field areas
- **Company-based allocation**: Prioritize fields owned by purchasing companies
- **Statistical modeling**: Use multiple allocation methods with uncertainty quantification

**Value-added features**:
- **Application estimates**: Estimated application rates per field in kg/hectare
- **Uncertainty bounds**: Confidence intervals for disaggregation estimates
- **Environmental indicators**: Calculated environmental load and risk metrics
- **Temporal patterns**: Year-over-year application pattern analysis

**Output**: Field-level pesticide application estimates with uncertainty quantification

---

## Workflow Schedule and Execution

### Manual Execution Schedule
- **Execution Type**: Manual trigger only (not automated)
- **Typical Timing**: Annually, 12-18 months after sales data year
- **Processing Duration**: ~60-90 minutes per year (via matrix jobs)
- **Dependencies**: Requires FVM field data and complete sales data for the year

### Matrix Processing Architecture
The workflow processes each year independently using matrix jobs:

#### Matrix Dimensions
- **Years Available**: 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023
- **Processing**: Each year runs as independent matrix job
- **Parallelism**: Up to 9 years can be processed simultaneously

#### Processing Flow
1. **Data Validation** (10 min): Verify all required data sources are available
2. **Spatial Processing** (30 min): Prepare field geometries and spatial relationships
3. **Disaggregation Modeling** (40 min): Run statistical disaggregation algorithms
4. **Quality Assessment** (10 min): Validate results and generate quality metrics

### Resource Requirements
- **Memory**: 12-16GB RAM per year for large spatial datasets
- **Storage**: ~15GB temporary space for processing
- **Processing**: High CPU for spatial operations and statistical modeling

---

## Data Quality and Limitations

### Data Quality Assessment
| Quality Metric | Status | Details |
|----------------|--------|---------|
| **Completeness** | Good | ~85% of sales data successfully disaggregated to fields |
| **Accuracy** | Moderate | Statistical estimates with quantified uncertainty |
| **Timeliness** | Poor | 12-18 month lag due to sales data availability |
| **Consistency** | Good | Standardized methodology across all years |

### Known Issues and Limitations

#### Methodological Limitations
- **Statistical estimates**: Results are modeled estimates, not actual measurements
- **Uncertainty**: Field-level estimates have inherent uncertainty from disaggregation process
- **Data lag**: Significant delay between pesticide use and data availability

#### Data Gaps
- **Sales coverage**: Not all pesticide sales may be captured in official data
- **Field matching**: Some sales cannot be linked to specific fields
- **Crop specificity**: Limited information on exact crop-pesticide combinations

#### Quality Issues
- **Spatial accuracy**: Disaggregation accuracy varies by region and pesticide type
- **Temporal precision**: Annual data may miss seasonal application patterns
- **Company matching**: Some sales data cannot be linked to field ownership

### Recommended Uses
✅ **This data is good for**:
- Regional pesticide use pattern analysis
- Environmental exposure assessment at landscape scale
- Policy analysis and regulatory impact assessment
- Agricultural research on pesticide usage trends

⚠️ **Use with caution for**:
- Field-specific exposure analysis - Results are statistical estimates with uncertainty
- Regulatory enforcement - Not suitable for compliance monitoring of individual farms
- Health studies - Consider uncertainty bounds in exposure calculations

❌ **Not recommended for**:
- Individual farm monitoring - Data is disaggregated estimates, not actual measurements
- Real-time analysis - 12-18 month data lag makes real-time use impossible
- Precise application timing - Annual data cannot determine application dates

---

## Usage Examples and Access

### Common Questions This Data Answers
1. **Which areas have highest pesticide exposure?** - Aggregate field-level estimates by region
2. **How has pesticide use changed over time?** - Compare estimates across multiple years
3. **What pesticides are used near sensitive areas?** - Spatial analysis around schools, water sources, etc.

### Example Analyses
#### Regional Exposure Assessment
**Question**: Which Danish regions have highest pesticide exposure levels?
**Data Used**: Disaggregated field estimates aggregated by municipality/region
**Method**: Sum application estimates by geographic area and population
**Limitations**: Statistical uncertainty increases when aggregating across many fields

#### Environmental Impact Analysis
**Question**: How do pesticide applications affect water quality in different watersheds?
**Data Used**: Field-level estimates linked to watershed boundaries and water monitoring
**Method**: Spatial analysis of pesticide applications within watershed areas
**Limitations**: Does not account for pesticide fate and transport processes

### Data Access
- **Public Access**: Aggregated regional statistics and environmental indicators
- **Research Access**: Field-level estimates available for approved research projects
- **Download Options**: Parquet files with uncertainty quantification
- **Integration**: Links with environmental monitoring, health data, and agricultural statistics

---

## Technical Details

<details>
<summary>Click to expand technical specifications</summary>

### Data Schema
#### Field Pesticide Estimates Dataset
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
field_uuid | TEXT | Unique field identifier | "550e8400-e29b-41d4-a716-446655440000"
pesticide_year | INTEGER | Year of pesticide sales | 2023
product_name | TEXT | Pesticide product name | "Roundup Ready"
active_ingredient | TEXT | Active ingredient name | "Glyphosate"
estimated_application_kg_ha | FLOAT | Estimated application rate | 2.5
confidence_lower | FLOAT | Lower confidence bound | 1.8
confidence_upper | FLOAT | Upper confidence bound | 3.2
allocation_method | TEXT | Disaggregation method used | "crop_weighted"
uncertainty_score | FLOAT | Uncertainty indicator (0-1) | 0.3
```

### Storage Locations
- **Bronze**: `gs://landbruget-data/bronze/pesticide_disaggregation/{year}/{timestamp}/`
- **Silver**: `gs://landbruget-data/silver/pesticide_disaggregation/{year}/{timestamp}/`
- **Gold**: `gs://landbruget-data/gold/pesticide_disaggregation/{year}/{timestamp}/`

### Processing Infrastructure
- **Platform**: GitHub Actions matrix jobs
- **Resources**: 16GB RAM per job, up to 9 parallel jobs
- **Dependencies**: FVM field data, pesticide sales data, BMD database
- **Performance**: ~90 minutes per year for complete disaggregation

</details>

---

## Contact and Support

### Workflow Maintainer
- **Primary Contact**: Environmental Data Team
- **Response Time**: 2-3 business days

### Reporting Issues
- **Data Quality Issues**: Report via GitHub issues with "Pesticide Disaggregation" label
- **Methodology Questions**: Contact environmental analysis team
- **Feature Requests**: Submit enhancement requests via project channels

### Documentation Updates
- **Last Updated**: January 2025
- **Update Schedule**: Reviewed annually
- **Version**: 1.0

---

*This documentation is part of the Landbruget.dk transparency initiative to make agricultural data accessible and trustworthy.*
