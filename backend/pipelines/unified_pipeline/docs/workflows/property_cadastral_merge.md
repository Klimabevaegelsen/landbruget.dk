# Property Cadastral Merge Workflow

> **Monthly Dependent Processing**: Merging Danish property ownership with cadastral boundaries for comprehensive property analytics

---

## What This Workflow Does

The Property Cadastral Merge workflow combines Danish property ownership data with official cadastral boundary data to create comprehensive property analytics datasets. This gold-layer-only workflow merges privacy-protected property ownership information from the Property Owners SFTP pipeline with spatial boundary data from the Cadastral workflow, creating business-ready datasets for property analysis and agricultural land ownership studies.

### Why This Data Matters
- **Property Ownership Analytics**: Comprehensive property ownership analysis with spatial context
- **Agricultural Land Ownership**: Understanding of agricultural property ownership patterns and structures
- **Business Intelligence**: Company and individual property portfolios for business analysis
- **Spatial Property Analysis**: Property ownership combined with precise boundary information
- **CVR Integration**: Automated extraction of company registration numbers for business analytics
- **Privacy-Protected Analytics**: Secure analysis of property ownership with comprehensive privacy protections

### Key Statistics
- **Data Integration**: Merges ~8.5M property ownership records with ~2.8M cadastral parcels
- **Processing Scale**: High-memory processing (12GB memory allocation) for large spatial datasets
- **Match Rate Target**: Minimum 80% match rate between property ownership and cadastral data
- **Privacy Protection**: Comprehensive privacy transformations applied to ownership data
- **CVR Extraction**: Automated extraction of company CVR numbers for business analytics
- **Update Frequency**: Monthly dependent processing after cadastral foundation data

---

## Data Sources and Dependencies

### Primary Dependencies
This workflow depends on silver data from two sources:

| Source Pipeline | Data Type | Dependency | Content |
|----------------|-----------|------------|---------|
| **Property Owners SFTP** | Property ownership records | External pipeline | ~8.5M ownership records with privacy protection |
| **Cadastral Workflow** | Property boundaries | Monthly foundation | ~2.8M cadastral parcels with spatial boundaries |

### Data Integration Architecture

#### Property Owners SFTP Pipeline
- **Data Source**: Datafordeleren SFTP server with IP whitelisting requirements
- **Processing**: Secure Google Cloud VM processing with comprehensive privacy transformations
- **Privacy Protection**: CPR numbers → UUIDs, address anonymization, demographic data removal
- **Output**: Privacy-transformed Parquet files with preserved business relationships
- **Scale**: ~8.5M property ownership records (~12GB original GeoJSON)

#### Cadastral Workflow Integration
- **Data Source**: Official Danish cadastral boundaries from Datafordeler WFS service
- **Processing**: High-precision property boundary data with administrative metadata
- **Spatial Data**: Complete property parcel geometries with BFE number identifiers
- **Scale**: ~2.8M cadastral parcels with comprehensive spatial validation

### Data Privacy and Compliance
- **Privacy-First Architecture**: Comprehensive privacy transformations applied to ownership data
- **CPR Protection**: Personal identification numbers converted to consistent UUIDs
- **Address Anonymization**: Residential addresses removed while preserving business context
- **Demographic Protection**: Birth dates and gender information removed
- **Business Context Preservation**: Company information and business relationships maintained
- **Legal Compliance**: Full compliance with Danish data protection regulations

---

## Data Processing Steps

### 🥇 Gold Layer: Advanced Property-Cadastral Integration (Unified Pipeline Only)
**What happens**: We merge privacy-protected property ownership data with official cadastral boundaries using BFE number matching
**Why**: Property analytics require both ownership information and spatial boundaries for comprehensive analysis

**Specific processing**:

#### High-Performance Streaming Architecture
- **Memory Optimization**: 12GB memory allocation (75% of 16GB) for large dataset processing
- **Streaming Processing**: Direct GCS-to-DuckDB streaming without in-memory loading
- **CPU Optimization**: 4-thread processing for optimal performance on large datasets
- **Temporary Storage**: Disk-based temporary storage for memory-intensive operations

#### Advanced BFE-Based Matching
- **Primary Key Matching**: BFE (Bestemt Fast Ejendom) number-based inner join
- **Nested JSON Processing**: Direct access to BFE numbers from nested property data structures
- **Data Structure Validation**: Comprehensive validation of nested JSON property structures
- **Match Rate Monitoring**: Real-time calculation and validation of merge success rates

#### Comprehensive Data Integration
- **Property Ownership Integration**: Complete ownership structure including person and company data
- **Spatial Boundary Integration**: Full cadastral geometry and administrative metadata
- **Ownership Structure Preservation**: Numerator/denominator ownership fractions maintained
- **Administrative Context**: Business events, processes, and authority information included

#### Advanced CVR Extraction and Collection
- **Automated CVR Detection**: Extraction of company registration numbers from ownership data
- **CVR Validation**: Comprehensive validation using regex patterns and length checks
- **CVR Collection Integration**: Automated saving using unified pipeline CVR collection utility
- **Business Analytics Enhancement**: CVR numbers enable downstream business analysis

#### Quality Assurance and Validation
- **Match Rate Validation**: Minimum 80% match rate requirement with quality warnings
- **Data Completeness Assessment**: Comprehensive validation of merged data structure
- **BFE Number Validation**: Verification of BFE number consistency and completeness
- **Merge Statistics**: Detailed logging of merge performance and success rates

#### Optimized Output Processing
- **Direct GCS Upload**: Streaming upload to GCS without intermediate storage
- **Parquet Optimization**: Efficient columnar storage format for analytical queries
- **Metadata Preservation**: Complete processing metadata and data lineage information
- **Memory Management**: Automatic cleanup of temporary tables and resources

**Output**: Business-ready property-cadastral merged dataset with privacy protection and comprehensive analytics capabilities

---

## Workflow Schedule and Execution

### Monthly Dependent Processing
- **Schedule**: 1st of every month at 1 AM UTC (dependent batch, priority 10)
- **Execution Type**: Automated monthly processing after foundation workflows complete
- **Processing Duration**: ~1.5 hours for complete merge processing (estimated 90 minutes)
- **Dependencies**: Requires cadastral silver data from monthly foundation processing
- **External Dependency**: Requires property owners silver data from SFTP pipeline

### Processing Performance
- **Data Volume**: ~8.5M property records merged with ~2.8M cadastral parcels
- **Memory Usage**: High (~12GB allocation) for large spatial dataset merging
- **Storage**: Multi-GB storage requirements for merged property-cadastral dataset
- **Network**: GCS streaming access for large dataset processing
- **Match Rate**: Target minimum 80% match rate between datasets

### Advanced Features
- **High-Memory Processing**: Optimized for large-scale property data processing
- **Streaming Architecture**: Memory-efficient processing of large datasets via GCS streaming
- **CVR Integration**: Automated extraction and collection of company registration numbers
- **Privacy Compliance**: Comprehensive privacy protection throughout processing pipeline
- **Quality Monitoring**: Real-time validation of merge quality and success rates

### Resource Management
- **Memory Optimization**: 12GB memory allocation with disk-based temporary storage
- **CPU Utilization**: 4-thread processing for optimal performance
- **Storage Efficiency**: Direct GCS streaming without intermediate file storage
- **Cleanup Automation**: Automatic cleanup of temporary tables and resources

---

## Data Quality and Limitations

### Data Quality Assessment
| Quality Metric | Status | Details |
|----------------|--------|---------|
| **Completeness** | Good | High match rate (~80%+) between property ownership and cadastral data |
| **Accuracy** | Excellent | Official Danish property and cadastral data with comprehensive validation |
| **Privacy Protection** | Excellent | Comprehensive privacy transformations with CPR protection and address anonymization |
| **Business Context** | Excellent | Complete business relationship preservation with CVR extraction |

### Known Issues and Limitations

#### Data Integration and Matching Constraints
- **BFE Number Dependency**: Merge success depends on BFE number consistency between datasets
- **Temporal Alignment**: Property ownership and cadastral data may have different update frequencies
- **Match Rate Variability**: Some property records may lack corresponding cadastral entries
- **Data Structure Complexity**: Nested JSON property structures require careful parsing

#### Privacy and Compliance Considerations
- **Privacy Transformation Dependency**: Relies on comprehensive privacy processing in source pipeline
- **CPR Protection Requirements**: Strict requirements for personal identification number protection
- **Address Anonymization**: Residential address information removed for privacy compliance
- **Data Access Restrictions**: Property ownership data subject to strict access controls

#### Processing and Technical Limitations
- **Memory Requirements**: High memory requirements (12GB) for large dataset processing
- **Processing Duration**: Extended processing time (~90 minutes) for complete merge
- **External Pipeline Dependency**: Depends on successful execution of external Property Owners SFTP pipeline
- **GCS Streaming Dependency**: Requires reliable GCS access for streaming processing

### Recommended Uses
✅ **This data is excellent for**:
- Property ownership analysis with spatial context for Danish properties
- Agricultural land ownership pattern analysis and business intelligence
- Company property portfolio analysis using CVR-based business identification
- Regional property ownership distribution and concentration analysis
- Property-based business analytics with comprehensive privacy protection

⚠️ **Use with caution for**:
- Individual property analysis - Privacy transformations limit personal information
- Real-time property ownership - Monthly processing with potential data lag
- Historical ownership analysis - Privacy transformations may affect historical consistency

❌ **Not recommended for**:
- Personal identification or contact analysis - CPR numbers and addresses anonymized
- Daily property transaction analysis - Monthly updates, not real-time transaction data
- International property comparisons without context - Danish-specific property and cadastral systems

---

## Usage Examples and Access

### Common Questions This Data Answers
1. **Which companies own the most agricultural land in Denmark?** - Company property portfolio analysis using CVR-based identification
2. **How is agricultural land ownership distributed across Danish regions?** - Regional property ownership analysis with spatial aggregation
3. **What are the ownership patterns of large agricultural properties?** - Property size and ownership structure analysis
4. **Which areas have the highest concentration of company-owned agricultural land?** - Spatial analysis of corporate agricultural land ownership

### Example Analyses
#### Agricultural Land Ownership Analysis
**Question**: Which companies own the largest agricultural properties and where are they located?
**Data Used**: Property-cadastral merged data with company ownership information and spatial boundaries
**Method**: Spatial aggregation of property areas by CVR numbers with geographic distribution analysis
**Output**: Company agricultural property portfolios with spatial distribution and ownership concentration maps
**Limitations**: Privacy transformations limit individual ownership analysis, CVR extraction success affects company identification

#### Regional Property Ownership Distribution
**Question**: How is property ownership distributed across Danish administrative regions?
**Data Used**: Merged property-cadastral data with administrative boundary integration
**Method**: Spatial aggregation by administrative regions with ownership type and size distribution analysis
**Output**: Regional property ownership statistics with ownership pattern visualization and concentration analysis
**Limitations**: Administrative boundaries may not align with property boundaries, privacy protections limit demographic analysis

#### Property Portfolio Business Intelligence
**Question**: What are the characteristics of company property portfolios in Danish agriculture?
**Data Used**: Company ownership data (via CVR numbers) merged with property characteristics and spatial information
**Method**: Business intelligence analysis of company property holdings with size, location, and ownership structure analysis
**Output**: Company property portfolio profiles with business analytics and investment pattern identification
**Limitations**: CVR extraction success affects analysis completeness, privacy protections limit detailed company analysis

### Data Access
- **Research Access**: Property-cadastral merged datasets for academic and scientific property research
- **Business Access**: Property ownership analytics for business intelligence and market analysis
- **Policy Access**: Property ownership data for agricultural policy development and land use planning
- **Regulatory Access**: Property information for regulatory compliance and monitoring (with privacy protections)

---

## Technical Details

<details>
<summary>Click to expand technical specifications</summary>

### Data Schema
#### Property-Cadastral Merged Data (Gold Layer)

**Comprehensive Property-Ownership Dataset**
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
bfe_number | BIGINT | BFE property identifier | 1234567890
person_data | JSON | Privacy-protected person ownership data | {"uuid": "abc-123-def", ...}
company_data | JSON | Company ownership information | {"cvrNummer": "12345678", ...}
ownership_code | VARCHAR | Ownership type code | "10"
ownership_denominator | INTEGER | Ownership fraction denominator | 100
ownership_numerator | INTEGER | Ownership fraction numerator | 50
business_event | VARCHAR | Cadastral business event | "Opdeling"
business_process | VARCHAR | Cadastral process type | "Matrikulær sag"
authority | VARCHAR | Responsible authority | "Geodatastyrelsen"
is_worker_housing | BOOLEAN | Worker housing flag | false
is_common_lot | BOOLEAN | Common lot flag | false
has_owner_apartments | BOOLEAN | Owner apartments flag | true
cadastral_registration_from | TIMESTAMP | Cadastral registration date | "2024-01-15T10:30:00"
cadastral_effect_from | TIMESTAMP | Cadastral effective date | "2024-01-15T10:30:00"
geometry | GEOMETRY | Property boundary polygon | POLYGON((...)
```

### Storage Locations
- **Gold Output**: `gs://landbrugsdata-raw-data/gold/property_cadastral_merged/{timestamp}/property_cadastral_merged.parquet`
- **CVR Collection**: `gs://landbrugsdata-raw-data/cvr_collection/property_cadastral_merge/{timestamp}/cvr_numbers.json`

### Processing Infrastructure
- **Platform**: Automated monthly execution as dependent data source
- **Resources**: 12GB RAM allocation, 4-thread processing, disk-based temporary storage
- **Dependencies**: Cadastral silver data, Property Owners SFTP silver data
- **Performance**: ~1.5 hours for complete merge processing (90 minutes estimated)

### Data Integration Details
#### BFE-Based Matching
```sql
-- Primary merge logic
SELECT
    p.properties.bestemtFastEjendomBFENr as bfe_number,
    p.properties.ejendePerson as person_data,
    p.properties.ejendeVirksomhed as company_data,
    c.geometry
FROM property_owners p
INNER JOIN cadastral c ON p.properties.bestemtFastEjendomBFENr = c.bfe_number
WHERE p.properties.bestemtFastEjendomBFENr IS NOT NULL
```

#### CVR Extraction Logic
```sql
-- CVR number extraction from company data
SELECT DISTINCT
    TRIM(CAST(JSON_EXTRACT_STRING(company_data, '$.cvrNummer') AS VARCHAR)) as cvr_number
FROM merged_properties
WHERE company_data IS NOT NULL
  AND JSON_EXTRACT_STRING(company_data, '$.cvrNummer') IS NOT NULL
  AND REGEXP_MATCHES(TRIM(CAST(JSON_EXTRACT_STRING(company_data, '$.cvrNummer') AS VARCHAR)), '^[1-9][0-9]{7}$')
```

### Privacy Protection Features
- **CPR Anonymization**: Personal identification numbers converted to consistent UUIDs
- **Address Protection**: Residential addresses removed from person data
- **Demographic Protection**: Birth dates and gender information removed
- **Business Context Preservation**: Company information and CVR numbers maintained
- **Relationship Preservation**: Property ownership relationships maintained through UUID consistency

### Performance Optimization Features
- **Memory Management**: 12GB allocation with 75% of available system memory
- **Streaming Processing**: Direct GCS-to-DuckDB streaming without memory loading
- **CPU Optimization**: 4-thread processing for parallel operations
- **Temporary Storage**: Disk-based temporary storage for large dataset operations
- **Resource Cleanup**: Automatic cleanup of temporary tables and resources

### Quality Assurance Features
- **Match Rate Validation**: Minimum 80% match rate requirement with warnings
- **BFE Number Validation**: Comprehensive validation of property identifier consistency
- **Data Structure Validation**: Nested JSON structure validation and error handling
- **CVR Validation**: Regex-based validation of company registration numbers
- **Processing Statistics**: Detailed logging of merge performance and quality metrics

</details>

---

## Contact and Support

### Workflow Maintainer
- **Primary Contact**: Property Analytics Data Team
- **Response Time**: 1-2 business days

### Reporting Issues
- **Data Quality Issues**: Report via GitHub issues with "Property Cadastral Merge" label
- **Privacy Concerns**: Contact data privacy team for privacy-related issues
- **CVR Integration Issues**: Contact CVR collection team for business registration problems

### Documentation Updates
- **Last Updated**: January 2025
- **Update Schedule**: Reviewed when property ownership or cadastral data structures change
- **Version**: 1.0

---

*This documentation is part of the Landbruget.dk transparency initiative to make property analytics data accessible and trustworthy while maintaining comprehensive privacy protection.*
