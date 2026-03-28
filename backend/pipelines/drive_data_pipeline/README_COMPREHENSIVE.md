# Drive Data Pipeline: Danish Agricultural Regulatory Compliance Data Collection

> **Manual Upload Processing**: Collection and processing of regulatory compliance documents from 12+ Danish agricultural authorities through Google Drive integration

---

## What This Pipeline Does

The Drive Data Pipeline is a sophisticated document processing system that collects, processes, and standardizes regulatory compliance documents from multiple Danish agricultural authorities. The pipeline connects to a centralized Google Drive folder containing 12 distinct data sources, each representing different aspects of agricultural regulatory compliance. Using advanced PDF parsing and Excel processing capabilities, it transforms unstructured regulatory documents into standardized, analyzable datasets for agricultural compliance monitoring and policy analysis.

### Why This Data Matters
- **Regulatory Compliance Monitoring**: Comprehensive tracking of agricultural regulatory compliance across multiple domains
- **Policy Impact Assessment**: Data for evaluating the effectiveness of agricultural policies and regulations
- **Agricultural Safety**: Worker safety incident tracking and prevention analysis
- **Environmental Protection**: Monitoring of environmental incidents and mitigation measures
- **Agricultural Workforce Analytics**: Analysis of international agricultural work permits and labor patterns
- **Transparency and Accountability**: Public access to agricultural regulatory data for oversight and research

### Key Technical Statistics
- **Data Source Integration**: 12 distinct Google Drive subfolders with specialized processing pipelines
- **Document Processing**: Advanced PDF parsing with table extraction and Excel harmonization
- **Authentication Systems**: Google Drive API integration with service account authentication
- **Processing Architecture**: Medallion architecture (Bronze-Silver) with specialized transformers
- **Data Standardization**: Automated schema harmonization across diverse document formats
- **Update Frequency**: Manual upload processing with automated data transformation and quality validation

---

## Data Sources and Dependencies

### 12 Primary Regulatory Data Sources
This pipeline processes documents from 12 distinct Danish agricultural regulatory authorities and compliance areas:

| Data Source | Authority/Domain | Document Types | Content Focus |
|-------------|------------------|----------------|---------------|
| **Animal Welfare** | Danish Veterinary and Food Administration | PDF Reports, Excel Files | Animal welfare inspections, compliance violations, intervention measures |
| **Fertiliser** | Agricultural Agency | Excel Files (GKEA, Gødningsregnskaber) | Fertilizer usage accounts, cover crops (efterafgrøder), nutrient management |
| **Pesticides** | Environmental Protection Agency | PDF/Excel Reports | Pesticide application records, compliance monitoring, usage statistics |
| **Work Permits** | Immigration/Labor Authorities | PDF Documents | Agricultural work permits (Landbrugsvisum), nationality statistics, labor analytics |
| **Worker Safety** | Danish Working Environment Authority | Excel Files | Workplace safety incidents, injury reports, compliance violations |
| **Stable Fires** | Emergency Services/Insurance | PDF Reports | Stable fire incidents, damage reports, prevention measures |
| **Slurry Leaks** | Environmental Authorities | PDF/Excel Reports | Slurry spill incidents, environmental impact assessments |
| **Transportation Accidents** | Transport Authorities | PDF Reports | Agricultural transport accidents, safety compliance |
| **Subsidies** | Agricultural Support Agency | Excel Files | Agricultural subsidies, support scheme compliance |
| **Pig Tail Cutting** | Animal Welfare Authorities | PDF/Excel Reports | Pig tail cutting procedures, welfare compliance |
| **International Animal Movements** | Veterinary Authorities | Excel Files | Cross-border animal movement tracking |
| **Animal Mortality** | Veterinary Authorities | PDF/Excel Reports | Animal mortality reporting and analysis |

### Technical Integration Architecture

#### Google Drive API Integration
- **Authentication**: Service account-based authentication with JSON key management
- **Folder Structure**: Hierarchical processing of 12 main subfolders with nested document organization
- **File Discovery**: Recursive folder scanning with metadata extraction and deduplication
- **Download Management**: Parallel file downloading with retry logic and progress tracking
- **Public Access Support**: Optional public folder access without authentication for publicly shared datasets

#### Advanced Document Processing Pipeline
- **Medallion Architecture**: Bronze layer for raw document storage, Silver layer for processed data
- **Specialized Transformers**: Custom processing logic for different document types and regulatory domains
- **PDF Processing**: Advanced PDF parsing using `pdfplumber` for table extraction and text analysis
- **Excel Harmonization**: Automated schema standardization across diverse Excel formats
- **Data Quality Validation**: Comprehensive validation and cleaning of extracted data

---

## Data Processing Steps

### 🥉 Bronze Layer: Document Collection and Metadata Management
**What happens**: We systematically collect regulatory documents from Google Drive, preserving original formats and extracting comprehensive metadata for lineage tracking
**Why**: Regulatory compliance requires maintaining complete audit trails and preserving original document integrity

**Specific processing**:

#### Sophisticated Google Drive Integration
- **Hierarchical Folder Processing**: Recursive processing of 12 main subfolders with preservation of internal folder structure
- **Metadata Extraction**: Comprehensive metadata capture including file size, modification dates, checksums, and Google Drive properties
- **Deduplication Logic**: Advanced deduplication based on file checksums to prevent duplicate processing
- **Progress Tracking**: Real-time progress monitoring with file-level and folder-level statistics
- **Error Recovery**: Robust error handling with partial failure recovery and detailed logging

#### Document Organization and Storage
- **Dataset Separation**: Each Google Drive subfolder becomes its own dataset directory (e.g., `bronze/fertiliser/`, `bronze/work_permits/`)
- **Timestamp Management**: Timestamped run directories for historical processing tracking
- **Metadata Preservation**: JSON metadata files alongside each document for complete provenance tracking
- **File Integrity**: Checksum validation and file integrity verification
- **Storage Flexibility**: Support for both local filesystem and Google Cloud Storage backends

### 🥈 Silver Layer: Advanced Document Processing and Data Extraction
**What happens**: We apply specialized transformers to extract structured data from regulatory documents, standardize formats, and create analyzable datasets
**Why**: Regulatory documents come in diverse formats requiring specialized processing to create consistent, analyzable data

**Specific processing**:

#### Specialized Document Transformers
- **Work Permits Transformer**: Advanced PDF parsing for Danish "Landbrugsvisum" documents with pivot table extraction, CVR number identification, and nationality statistics processing
- **Fertiliser Transformer**: Excel harmonization for multiple fertilizer data sources (GKEA markplan files, Gødningsregnskaber, Efterafgrøder) with schema standardization
- **Advanced PDF Transformer**: General-purpose PDF processing with table detection, text extraction, and structured data conversion
- **Excel Transformer**: Comprehensive Excel processing with multi-sheet handling, data type detection, and schema normalization

#### Data Standardization and Quality Control
- **Schema Harmonization**: Automatic standardization of column names, data types, and value formats across diverse document sources
- **Data Type Detection**: Intelligent data type inference and conversion for consistent downstream processing
- **PII Detection and Masking**: Privacy-compliant processing with automatic detection and handling of personally identifiable information
- **Quality Validation**: Comprehensive data quality checks including completeness, consistency, and format validation
- **Output Format Optimization**: Parquet file output with CSV fallback for complex data structures

#### Advanced Processing Capabilities
- **Multi-format Support**: Processing of PDF, Excel (.xlsx/.xls), and other document formats with format-specific optimization
- **Content-Aware Processing**: Intelligent selection of transformers based on file content, naming patterns, and metadata
- **Memory-Efficient Processing**: Streaming processing for large documents with memory optimization
- **Parallel Processing**: Concurrent document processing with configurable worker limits
- **CVR Number Extraction**: Automated extraction and validation of Danish company registration numbers

---

## Technical Implementation Details

### Document Processing Architecture

#### Bronze Layer Implementation
```
bronze/{dataset_name}/{timestamp}/
├── document1.pdf
├── document1.pdf.metadata.json
├── document2.xlsx
├── document2.xlsx.metadata.json
├── subfolder/
│   ├── nested_document.pdf
│   └── nested_document.pdf.metadata.json
└── processing_summary.json
```

#### Silver Layer Implementation
```
silver/{dataset_name}/{timestamp}/
├── processed_data.parquet
├── processing_log.json
├── data_quality_report.json
└── cvr_numbers_extracted.json
```

### Advanced Processing Features

#### Work Permits Specialized Processing
- **PDF Table Extraction**: Advanced parsing of Danish agricultural work permit statistics
- **Nationality Processing**: Extraction and standardization of 23+ nationality categories
- **CVR Integration**: Automatic extraction and validation of company registration numbers
- **Temporal Analysis**: Multi-year data processing (2019-2023) with trend analysis capabilities

#### Fertiliser Data Harmonization
- **Multi-source Integration**: Harmonization of GKEA markplan files, Gødningsregnskaber, and Efterafgrøder data
- **Schema Standardization**: Unified schema across diverse fertilizer data sources
- **DuckDB Integration**: High-performance data processing using DuckDB for complex transformations
- **Quality Validation**: Comprehensive validation of fertilizer data completeness and accuracy

#### Error Handling and Recovery
- **Partial Failure Recovery**: Ability to continue processing when individual documents fail
- **Detailed Error Logging**: Comprehensive error tracking with file-specific failure analysis
- **Retry Logic**: Automatic retry mechanisms for transient failures
- **Data Quality Reporting**: Detailed reports on processing success rates and data quality metrics

### Performance and Scalability

#### Processing Optimization
- **Parallel Processing**: Configurable worker pools for concurrent document processing
- **Memory Management**: Streaming processing for large documents to prevent memory overflow
- **Caching Strategy**: Intelligent caching of processed metadata and intermediate results
- **Progress Tracking**: Real-time progress monitoring with detailed statistics

#### Storage and Output Management
- **Flexible Storage**: Support for local filesystem and Google Cloud Storage
- **Format Optimization**: Parquet output for efficient storage and query performance
- **Compression**: Automatic compression for storage efficiency
- **Data Lineage**: Complete tracking of data transformations and processing history

---

## Data Quality and Validation

### Quality Assurance Framework
- **Document Integrity**: Checksum validation and file integrity verification
- **Data Completeness**: Validation of required fields and data completeness
- **Format Consistency**: Standardization of data formats and value representations
- **PII Protection**: Automated detection and appropriate handling of sensitive information

### Processing Statistics and Monitoring
- **Processing Metrics**: Detailed statistics on processing success rates, timing, and data volumes
- **Quality Reporting**: Comprehensive data quality reports with issue identification
- **Error Analysis**: Detailed analysis of processing failures and data quality issues
- **Performance Monitoring**: Real-time monitoring of processing performance and resource usage

---

## Usage and Configuration

### Command-Line Interface
```bash
# Process all data sources
python main.py --verbose

# Process specific subfolders
python main.py --subfolders "fertiliser,work_permits,worker_safety"

# Process specific file types
python main.py --file-types "pdf,xlsx"

# Date-based filtering
python main.py --start-date 2023-01-01 --end-date 2023-12-31

# Bronze layer only
python main.py --bronze-only

# Silver layer only (requires existing Bronze data)
python main.py --silver-only
```

### Environment Configuration
```bash
# Google Drive integration
GOOGLE_DRIVE_FOLDER_ID=your_folder_id
GOOGLE_APPLICATION_CREDENTIALS=path/to/credentials.json

# Storage configuration
STORAGE_TYPE=gcs  # or 'local'
STORAGE_BUCKET=landbruget-data

# Processing configuration
MAX_WORKERS=4
LOG_LEVEL=INFO
```

### Docker Deployment
```bash
# Build and run with Docker Compose
docker-compose build
docker-compose up
```

---

## Data Integration and Downstream Usage

### Integration with Other Pipelines
- **CVR Number Integration**: Extracted CVR numbers are integrated with the Unified Pipeline for company-level analysis
- **Regulatory Compliance Analysis**: Data feeds into compliance monitoring and policy analysis workflows
- **Agricultural Intelligence**: Integration with field-level and farm-level data for comprehensive agricultural insights

### Output Data Products
- **Standardized Parquet Files**: Optimized for analytical queries and integration with data analysis tools
- **Metadata and Lineage**: Complete data provenance tracking for regulatory compliance and audit requirements
- **Quality Reports**: Detailed data quality assessments for informed decision-making
- **Processing Logs**: Comprehensive logging for troubleshooting and process optimization

This pipeline serves as a critical component of Denmark's agricultural data infrastructure, enabling comprehensive monitoring of regulatory compliance, policy effectiveness analysis, and agricultural safety oversight through systematic processing of regulatory documents from multiple Danish authorities.
