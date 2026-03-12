# CHR Pipeline: Danish Central Livestock Register Data Processing

> **Weekly Processing**: Comprehensive collection and processing of Danish livestock registration, movement, and health data through complex SOAP service integration

---

## What This Pipeline Does

The CHR (Central Husdyr Register - Central Livestock Register) Pipeline is a sophisticated data collection and processing system that integrates with multiple Danish government SOAP web services to gather comprehensive livestock data. The pipeline performs complex authentication with PKI certificates, handles large-scale data collection with advanced pagination and concurrency controls, and processes data through bronze-silver-gold medallion architecture. It creates detailed analytical products including veterinary timelines, transportation analysis, and regulatory compliance monitoring.

### Why This Data Matters
- **Animal Welfare Monitoring**: Comprehensive tracking of animal health interventions and welfare compliance
- **Disease Prevention**: Early detection and tracking of animal diseases to prevent outbreaks
- **Food Safety**: Complete traceability from farm to table for food safety assurance
- **Regulatory Compliance**: Monitoring of veterinary medicine usage and regulatory compliance
- **Agricultural Intelligence**: Business insights for farmers, veterinarians, and agricultural policy makers
- **Public Health Protection**: Surveillance of antibiotic usage and resistance patterns

### Key Technical Statistics
- **SOAP Service Integration**: 5 distinct FVM SOAP web services with complex authentication protocols
- **Processing Scale**: Species-specific herd discovery with pagination handling for 50,000+ herds
- **Concurrency Management**: Advanced concurrency controls (3-6 workers) with rate limiting and timeout handling  
- **Authentication Complexity**: PKI certificate authentication for VetStat with XML digital signatures
- **Data Volume**: Large XML processing (VetStat antibiotic data) with streaming and memory management
- **Matrix Processing**: Monthly matrix jobs for historical animal movements (2021-2025 year range)
- **Update Frequency**: Weekly automated processing with sophisticated error handling and retry logic

---

## Data Sources and Dependencies

### SOAP Web Service Integration Architecture
This pipeline integrates with 5 distinct Danish government SOAP web services with complex authentication protocols:

| SOAP Service | WSDL Endpoint | Authentication | Primary Operations | Technical Details |
|--------------|---------------|----------------|-------------------|-------------------|
| **CHR_stamdataWS** | `https://ws.fvst.dk/service/CHR_stamdataWS?wsdl` | FVM Username/Password | `ListDyrearterMedBrugsarter` | Species and usage combinations discovery |
| **CHR_besaetningWS** | `https://ws.fvst.dk/service/CHR_besaetningWS?wsdl` | FVM Username/Password | `listBesaetningerMedBrugsart`, `getBesaetning` | Herd discovery with pagination, detailed herd information |
| **DIKOWS** | `https://ws.fvst.dk/service/DIKOWS?wsdl` | FVM Username/Password | `besaetningListFlytninger` | Animal movements (cattle, sheep, goats, pigs only) |
| **CHR_ejendomWS** | `https://ws.fvst.dk/service/CHR_ejendomWS?wsdl` | FVM Username/Password | `getEjendomsOplysninger`, `listVeterinærBegivenheder` | Property details, veterinary events |
| **VetStat External** | `https://vetstat.fvst.dk/vetstat/services/external/CHRWS` | PKI Certificate + XML Signatures | `hentAntibiotikaforbrug` | Antibiotic usage data with complex XML processing |

### Complex Authentication and Data Collection Architecture

#### Advanced Authentication Systems
- **FVM Services**: Username/password authentication with session management and retry logic
- **VetStat PKI Authentication**: PKCS#12 certificate loading with private key extraction for XML digital signatures
- **Certificate Management**: Support for both base64-encoded certificates and file-based certificates
- **Session Handling**: Robust session management with timeout handling and connection pooling

#### Sophisticated Data Collection Patterns
- **Species-Usage Discovery**: Dynamic discovery of valid species/usage combinations before herd processing
- **Pagination Handling**: Complex pagination logic with `FraBesNr` and `TilBesNr` parameters for large herd datasets
- **Concurrency Controls**: ThreadPoolExecutor with configurable worker limits (3-6 workers) and rate limiting
- **Error Recovery**: Comprehensive error handling with retry logic and partial failure recovery
- **Memory Management**: Streaming XML processing for large VetStat datasets to prevent memory overflow

#### Silver Layer: Data Standardization and Processing
- **Data Standardization**: Harmonization of data formats and structures across different source systems
- **Quality Validation**: Comprehensive data quality checks and validation of animal movements and treatments
- **Relationship Building**: Establishment of relationships between animals, herds, properties, and treatments
- **Temporal Alignment**: Synchronization of data across different time periods and reporting cycles
- **CVR Integration**: Integration with company registration data for business intelligence

#### Gold Layer: High-Value Analytical Products
- **Veterinary Timeline**: Comprehensive timeline combining animal welfare, health certificates, veterinary status changes, and stable fires
- **Transportation Analysis**: Analysis of animal movement patterns and transportation safety
- **Disease Surveillance Analytics**: Advanced analytics for disease tracking and outbreak prevention
- **Compliance Monitoring**: Regulatory compliance analysis and reporting

---

## Data Processing Steps

### 🥉 Bronze Layer: Complex SOAP Service Integration and Raw Data Collection
**What happens**: We perform sophisticated SOAP web service calls with complex authentication, pagination, and concurrency management to collect raw data from Danish government systems
**Why**: Danish livestock data is distributed across multiple government SOAP services requiring complex integration protocols

**Specific processing**:

#### Advanced SOAP Client Creation and Authentication
- **Zeep SOAP Framework**: Python Zeep library for SOAP client creation with WSDL parsing and type factories
- **Complex Request Structure**: Proper WSDL-compliant request structures using `GLRCHRWSInfoInboundType` and operation-specific request types
- **VetStat PKI Integration**: PKCS#12 certificate loading with cryptographic signature generation for XML security headers
- **Session Management**: Connection pooling and session persistence across multiple SOAP operations
- **Certificate Flexibility**: Support for base64-encoded certificates (Secret Manager) and file-based certificates

#### Species-Driven Herd Discovery Architecture
- **Dynamic Species Discovery**: `ListDyrearterMedBrugsarter` operation to discover all valid species/usage combinations
- **Pagination-Based Herd Collection**: `listBesaetningerMedBrugsart` with complex pagination using `FraBesNr` and `TilBesNr` parameters
- **Detailed Herd Information**: `getBesaetning` operation for comprehensive herd details including ownership and operational data
- **Concurrent Processing**: ThreadPoolExecutor with configurable worker limits (default 3-6) for parallel herd processing
- **Error Recovery**: Partial failure handling allowing successful herds to be saved even if some operations fail

#### Species-Specific Animal Movement Collection (DIKO)
- **Species Validation**: DIKO service limited to specific species codes (12: Cattle, 13: Sheep, 14: Goats, 15: Pigs)
- **Movement Operation**: `besaetningListFlytninger` operation for herd-specific movement data collection
- **Matrix Processing**: Monthly matrix jobs for historical animal movements with year-range processing (2021-2025)
- **Concurrency Control**: GitHub Actions matrix job limits (max 6 concurrent) to prevent server overload
- **Request Structure**: Complex SOAP request with `BesaetningsNummer` and `DyreArtKode` parameters

#### Complex VetStat XML Processing with PKI Authentication
- **PKI Certificate Authentication**: PKCS#12 certificate loading with private key extraction for XML digital signatures
- **XML Security Headers**: WS-Security headers with digital signatures using RSA-SHA256 and XML canonicalization
- **Large XML Processing**: Streaming XML processing for large antibiotic usage datasets to prevent memory overflow
- **Date Range Processing**: Configurable date range processing with monthly matrix jobs for historical data
- **Error Handling**: Robust error handling for certificate issues, XML parsing errors, and network timeouts

#### Health Surveillance (SPF-SU)
- **Disease Certificates**: Health certificates and disease status monitoring for pigs and cattle
- **Surveillance Programs**: Integration with national disease surveillance programs
- **Health Status Tracking**: Temporal tracking of herd health status changes
- **Outbreak Prevention**: Early detection data for disease outbreak prevention

**Output**: Comprehensive raw dataset covering all aspects of Danish livestock management and health

### 🥈 Silver Layer: Advanced Data Standardization and Integration
**What happens**: We standardize, validate, and integrate raw data from multiple sources into coherent analytical datasets
**Why**: Different source systems use different formats and structures that must be harmonized for analysis

**Specific processing**:

#### Advanced Data Standardization
- **Schema Harmonization**: Standardization of data schemas across different source systems
- **Data Type Validation**: Comprehensive validation of data types, formats, and value ranges
- **Temporal Synchronization**: Alignment of data across different temporal reporting cycles
- **Identifier Mapping**: Mapping and validation of identifiers across different systems

#### Comprehensive Quality Assurance
- **Data Completeness Checks**: Validation of data completeness across all critical fields
- **Referential Integrity**: Validation of relationships between animals, herds, properties, and treatments
- **Temporal Consistency**: Validation of temporal sequences in movements and treatments
- **Business Rule Validation**: Application of agricultural and veterinary business rules

#### Advanced Relationship Building
- **Animal-Herd Associations**: Establishment of relationships between individual animals and their herds
- **Property-Herd Connections**: Mapping of herds to their registered properties and locations
- **Movement Chain Construction**: Construction of complete movement chains for traceability
- **Treatment-Animal Linkage**: Linking veterinary treatments to specific animals and herds

#### CVR Integration and Business Intelligence
- **Company Registration Integration**: Integration with Danish company registration (CVR) data
- **Business Entity Mapping**: Mapping of agricultural properties to business entities
- **Ownership Analysis**: Analysis of property ownership patterns and business structures
- **Corporate Agriculture Tracking**: Tracking of corporate agricultural operations and compliance

**Output**: High-quality, standardized datasets ready for analytical processing and business intelligence

### 🥇 Gold Layer: High-Value Analytical Products and Intelligence
**What happens**: We create comprehensive analytical products combining animal health, movement, and regulatory data
**Why**: Policy makers, researchers, and industry need comprehensive insights for decision making

**Specific processing**:

#### Comprehensive Veterinary Timeline
- **Multi-Source Integration**: Integration of animal welfare interventions, health certificates, veterinary status changes, and stable fires
- **Temporal Analysis**: Complete timeline analysis of veterinary events per CHR number (farm)
- **Spatial Matching**: Spatial matching of stable fire events to CHR properties for comprehensive risk assessment
- **Regulatory Integration**: Integration with pig tail cutting control inspections and other regulatory data
- **Event Correlation**: Analysis of correlations between different types of veterinary and welfare events

#### Advanced Transportation Analysis
- **Movement Pattern Analysis**: Analysis of animal movement patterns and transportation efficiency
- **Safety Assessment**: Transportation safety analysis and risk assessment
- **Route Optimization**: Analysis of transportation routes and optimization opportunities
- **Compliance Monitoring**: Monitoring of transportation compliance with animal welfare regulations

#### Disease Surveillance and Analytics
- **Disease Tracking**: Advanced analytics for disease surveillance and outbreak detection
- **Health Status Analysis**: Analysis of herd health status changes and trends
- **Risk Assessment**: Risk assessment for disease transmission and outbreak potential
- **Prevention Analytics**: Analytics for disease prevention and intervention strategies

#### Regulatory Compliance and Reporting
- **Compliance Monitoring**: Comprehensive monitoring of regulatory compliance across all aspects
- **Violation Detection**: Automated detection of potential regulatory violations
- **Reporting Analytics**: Advanced analytics for regulatory reporting and compliance assessment
- **Policy Impact Analysis**: Analysis of policy impacts on animal health and welfare

**Output**: Comprehensive analytical intelligence for animal health, welfare, and regulatory compliance

---

## Workflow Schedule and Execution

### Weekly Processing Schedule
- **Schedule**: Every Monday at 2 AM UTC with comprehensive data refresh
- **Execution Type**: Automated weekly processing with manual override capability
- **Processing Duration**: ~4-6 hours for complete data collection and processing
- **Matrix Processing**: Monthly matrix jobs for historical data processing (animal movements, VetStat)
- **Concurrency Control**: Workflow-level concurrency to prevent interference between runs

### Advanced Processing Architecture
- **Bronze Foundation**: Initial data collection from all 7 major sources with parallel processing
- **Bronze Data Collection**: Detailed data collection with monthly matrix processing for large datasets
- **Silver Processing**: Data standardization and integration with comprehensive quality assurance
- **Gold Processing**: High-value analytical product creation with veterinary timeline and transportation analysis

### Processing Performance and Resource Management
- **Data Volume**: ~50,000+ herds processed with complete movement and health records
- **Parallel Processing**: Concurrent data collection using optimized worker pools (up to 10 workers)
- **Memory Management**: Advanced memory management for processing large XML and JSON datasets
- **Network Optimization**: Intelligent retry logic and connection management for reliable data collection
- **Storage Optimization**: Efficient storage and compression of large historical datasets

### Matrix Job Architecture for Historical Processing
- **Monthly Processing**: Matrix jobs for processing historical data by month and year
- **Year Range Processing**: Configurable year range processing (default 2021-2025)
- **Species-Specific Processing**: Ability to process specific species codes for targeted analysis
- **Concurrent Job Management**: Configurable concurrent job limits (default 6) for resource management
- **Herd Limiting**: Configurable herd processing limits for development and testing

---

## Data Quality and Limitations

### Data Quality Assessment
| Quality Metric | Status | Details |
|----------------|--------|---------|
| **Completeness** | Excellent | >95% coverage of Danish livestock with comprehensive movement and health records |
| **Accuracy** | Excellent | Official Danish government data with comprehensive validation and quality checks |
| **Timeliness** | Good | Weekly updates with some data sources having longer reporting cycles |
| **Traceability** | Excellent | Complete traceability from individual animals to farms and treatments |

### Known Issues and Limitations

#### Data Collection and Integration Challenges
- **Source System Dependencies**: Reliance on multiple government systems with varying availability and performance
- **Authentication Complexity**: Complex authentication requirements including PKI certificates for VetStat
- **Data Volume Challenges**: Large historical datasets requiring sophisticated processing and storage management
- **Temporal Alignment**: Different source systems have different reporting cycles and data availability

#### Processing and Technical Limitations
- **Memory Requirements**: High memory requirements for processing large XML datasets (VetStat)
- **Processing Duration**: Extended processing time (4-6 hours) for complete data collection and processing
- **Network Dependencies**: Dependency on reliable network connections to multiple government services
- **Certificate Management**: Ongoing management of PKI certificates for secure data access

#### Data Coverage and Scope Limitations
- **Historical Data Availability**: Some data sources have limited historical coverage
- **Species Coverage**: Some specialized species may have limited data availability
- **Treatment Detail Granularity**: Level of treatment detail varies by data source and time period
- **Cross-Border Movements**: Limited coverage of international animal movements

#### Privacy and Compliance Considerations
- **Data Sensitivity**: Animal health and movement data includes commercially sensitive information
- **Regulatory Compliance**: Strict compliance requirements for handling veterinary and agricultural data
- **Access Controls**: Sophisticated access controls required for different types of sensitive data
- **Data Retention**: Complex data retention requirements across different data types

### Quality Assurance Features
- **Multi-Level Validation**: Comprehensive validation at bronze, silver, and gold layers
- **Referential Integrity Checks**: Validation of relationships between animals, herds, properties, and treatments
- **Temporal Consistency Validation**: Validation of temporal sequences and business rule compliance
- **Cross-Source Validation**: Validation of data consistency across multiple authoritative sources

### Recommended Uses
✅ **This data is excellent for**:
- Animal health and welfare research with comprehensive movement and treatment tracking
- Disease surveillance and outbreak prevention with complete traceability and health monitoring
- Food safety analysis with farm-to-table traceability and veterinary treatment history
- Agricultural policy development with comprehensive livestock management and compliance data
- Veterinary research with detailed treatment patterns and antibiotic usage analysis

⚠️ **Use with caution for**:
- Real-time disease monitoring - Weekly processing with potential delays in data reporting
- Individual animal tracking without proper authorization - Data includes commercially sensitive information
- International comparisons - Danish-specific regulatory and data collection systems

❌ **Not recommended for**:
- Daily operational decisions - Weekly updates not suitable for daily farm management
- Individual farm business decisions without context - Aggregated data may not reflect specific farm conditions
- Personal animal identification - Privacy protections limit individual animal identification

---

## Usage Examples and Access

### Common Questions This Data Answers
1. **How are animals moving between farms in Denmark and what are the patterns?** - Animal movement analysis with transportation patterns and farm-to-farm tracking
2. **What are the antibiotic usage patterns across different livestock sectors?** - Veterinary treatment analysis with antibiotic usage patterns and compliance monitoring
3. **Which farms have experienced disease outbreaks or health issues?** - Health surveillance analysis with disease tracking and outbreak detection
4. **How effective are animal welfare interventions and regulations?** - Welfare compliance analysis with intervention tracking and outcome assessment

### Example Analyses
#### Animal Movement and Transportation Analysis
**Question**: What are the patterns of cattle movements between farms in Denmark and how do they relate to disease transmission risk?
**Data Used**: DIKO movement data combined with herd information and health certificates
**Method**: Spatial and temporal analysis of movement patterns with disease risk assessment
**Output**: Movement network analysis with disease transmission risk mapping and transportation safety assessment
**Limitations**: Movement data dependent on proper registration; international movements may have limited coverage

#### Antibiotic Usage and Resistance Surveillance
**Question**: How has antibiotic usage changed across Danish livestock farms and what are the implications for resistance?
**Data Used**: VetStat prescription data combined with herd information and production data
**Method**: Temporal analysis of antibiotic usage patterns with resistance surveillance integration
**Output**: Antibiotic usage trends with resistance pattern analysis and regulatory compliance assessment
**Limitations**: Prescription data may not capture all usage; resistance data requires integration with laboratory surveillance

#### Disease Surveillance and Outbreak Prevention
**Question**: How effective is the current disease surveillance system in detecting and preventing livestock disease outbreaks?
**Data Used**: SPF-SU health certificates combined with veterinary timeline and movement data
**Method**: Disease surveillance analysis with outbreak detection and prevention assessment
**Output**: Disease surveillance effectiveness analysis with outbreak prevention recommendations and policy insights
**Limitations**: Disease detection dependent on reporting compliance; some diseases may have limited surveillance coverage

### Data Access
- **Research Access**: Comprehensive livestock data for academic research on animal health, welfare, and food safety
- **Policy Access**: Agricultural policy data for livestock regulation development and compliance monitoring
- **Industry Access**: Agricultural industry data for business intelligence and market analysis (with privacy protections)
- **Regulatory Access**: Regulatory compliance data for monitoring and enforcement activities

---

## Technical Details

<details>
<summary>Click to expand technical specifications</summary>

### Data Schema
#### Veterinary Timeline (Gold Layer Primary Output)

**Comprehensive Veterinary Event Timeline Dataset**
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
chr_number | BIGINT | CHR farm identifier | 1234567
event_date | DATE | Date of veterinary event | 2024-01-15
event_type | VARCHAR | Type of veterinary event | "antibiotic_treatment"
event_source | VARCHAR | Data source for event | "vetstat"
species_code | INTEGER | Livestock species code | 12
treatment_details | JSON | Detailed treatment information | {"medicine": "penicillin", "dosage": "10ml"}
herd_size | INTEGER | Herd size at time of event | 150
property_info | JSON | Property information | {"location": "Jutland", "type": "dairy"}
compliance_status | VARCHAR | Regulatory compliance status | "compliant"
health_certificate | VARCHAR | Associated health certificate | "SPF-SU-2024-001"
```

#### Animal Movements (Silver Layer)

**Comprehensive Animal Movement Tracking Dataset**
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
movement_id | VARCHAR | Unique movement identifier | "MOV_2024_001234"
from_chr | BIGINT | Source CHR number | 1234567
to_chr | BIGINT | Destination CHR number | 7654321
movement_date | DATE | Date of movement | 2024-01-15
animal_count | INTEGER | Number of animals moved | 25
species_code | INTEGER | Species of animals moved | 12
transportation_method | VARCHAR | Method of transportation | "truck"
compliance_check | BOOLEAN | Compliance verification status | true
movement_purpose | VARCHAR | Purpose of movement | "sale"
```

### Storage Locations
- **Bronze Output**: `gs://landbruget-data/bronze/chr/{timestamp}/`
- **Silver Output**: `gs://landbruget-data/silver/chr/{timestamp}/`
- **Gold Output**: `gs://landbruget-data/gold/chr/{timestamp}/`

### Processing Infrastructure
- **Platform**: GitHub Actions with Docker containerization
- **Resources**: High-memory processing for large XML datasets with parallel worker pools
- **Dependencies**: 7 major Danish government data sources with secure authentication
- **Performance**: ~4-6 hours for complete weekly processing cycle

### Data Collection Details
#### Authentication Systems
```python
# VetStat PKI Certificate Authentication
vetstat_cert = load_certificate("vetstat.p12", password)
vetstat_client = create_vetstat_client(certificate=vetstat_cert)

# FVM Service Authentication
fvm_credentials = get_fvm_credentials(username, password)
stamdata_client = create_stamdata_client(credentials=fvm_credentials)
```

#### Parallel Processing Architecture
```python
# Concurrent data collection with worker pools
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    futures = []
    for data_source in data_sources:
        future = executor.submit(collect_data, data_source)
        futures.append(future)
    
    results = [future.result() for future in futures]
```

### Quality Assurance Features
- **Multi-Source Validation**: Cross-validation of data across multiple authoritative sources
- **Temporal Consistency Checks**: Validation of temporal sequences in movements and treatments
- **Business Rule Validation**: Application of agricultural and veterinary business rules
- **Referential Integrity**: Comprehensive validation of relationships between entities
- **Data Completeness Assessment**: Validation of data completeness across all critical fields

### Security and Privacy Features
- **PKI Certificate Management**: Secure certificate-based authentication for sensitive veterinary data
- **Access Control**: Sophisticated access controls for different types of sensitive agricultural data
- **Data Encryption**: Encryption of sensitive data in transit and at rest
- **Audit Logging**: Comprehensive audit logging of data access and processing activities
- **Privacy Protection**: Privacy protections for commercially sensitive farm and veterinary data

</details>

---

## Contact and Support

### Pipeline Maintainer
- **Primary Contact**: Animal Health Data Team
- **Response Time**: 1-2 business days

### Reporting Issues
- **Data Quality Issues**: Report via GitHub issues with "CHR Pipeline" label
- **Access Issues**: Contact data access team for authentication and authorization issues
- **Performance Issues**: Contact infrastructure team for processing performance optimization

### Documentation Updates
- **Last Updated**: January 2025
- **Update Schedule**: Reviewed when source system changes or data structures are modified
- **Version**: 1.0

---

*This documentation is part of the Landbruget.dk transparency initiative to make animal health and welfare data accessible and trustworthy for research, policy development, and public health protection.*
