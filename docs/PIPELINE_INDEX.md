# Landbruget.dk Data Pipeline Documentation

> **Making Danish Agricultural Data Transparent**: Complete documentation of all data pipelines for journalists, researchers, and citizens.

## Overview

This index provides access to comprehensive documentation for all data pipelines in the Landbruget.dk project. Each pipeline is documented with both technical details and journalist-friendly explanations to ensure transparency and trustworthiness of our data sources.

## Quick Navigation

- [🏛️ **Government Data Pipelines**](#government-data-pipelines) - Official Danish agency data
- [🚜 **Agricultural Monitoring**](#agricultural-monitoring-pipelines) - Farm-level tracking and compliance  
- [🔬 **Environmental Analysis**](#environmental-analysis-pipelines) - Environmental impact and safety
- [🏢 **Business Intelligence**](#business-intelligence-pipelines) - Company and regulatory data
- [📊 **Data Processing Guide**](#data-processing-guide) - How our pipeline system works

---

## Government Data Pipelines

### 🥇 Unified Pipeline - Core Danish Agricultural Data
**Status**: 📝 Documentation needs major update  
**Priority**: 🔴 HIGH - This is our primary data infrastructure  
**Data Sources**: 18+ official Danish government sources  

The Unified Pipeline is our main data collection system, gathering information from multiple Danish government agencies to create a comprehensive picture of Danish agriculture.

**What it collects**:
- Land ownership and boundaries (Cadastral data)
- Agricultural field boundaries and crop types  
- Environmental protection areas
- Soil types and quality
- Administrative boundaries
- Weather and climate data
- Statistical data from Danmarks Statistik

📖 **[Current README](../backend/pipelines/unified_pipeline/README.md)** *(needs comprehensive rewrite)*  
🎯 **Planned Update**: Comprehensive journalist-friendly documentation covering all 18+ data sources

---

## Agricultural Monitoring Pipelines

### 🐄 CHR Pipeline - Animal Health and Movement Tracking  
**Status**: 📝 Good technical docs, needs journalist accessibility  
**Priority**: 🔴 HIGH - Critical for food safety and animal welfare  
**Data Sources**: CHR registry, veterinary records, SPF-SU health data

Tracks all livestock movements, health treatments, and disease surveillance across Danish farms. Essential for food safety, disease control, and animal welfare monitoring.

**What it tracks**:
- Animal movements between farms
- Veterinary treatments and antibiotic usage
- Disease surveillance and health certificates  
- Farm property information
- Stable fires and safety incidents

📖 **[Current README](../backend/pipelines/chr_pipeline/README.md)**  
🎯 **Planned Update**: Add journalist-friendly introduction explaining animal welfare and food safety importance

### 🐷 Svineflytning Pipeline - Pig Movement Tracking
**Status**: 📝 Good technical structure, needs accessibility improvements  
**Priority**: 🟡 MEDIUM - Specialized for pig industry  
**Data Sources**: SvineflytningWS SOAP service

Specialized tracking system for pig movements, complementing the CHR system with detailed pig-specific data.

📖 **[Current README](../backend/pipelines/svineflytning_pipeline/README.md)**  
🎯 **Planned Update**: Add context about pig industry and disease control

---

## Environmental Analysis Pipelines

### 🧪 H3 PFAS Exposure Pipeline - Chemical Contamination Mapping
**Status**: ✅ Excellent journalist-friendly documentation  
**Priority**: 🟢 LOW - Already well documented  
**Data Sources**: BMD pesticide database, field application records

Maps PFAS "forever chemical" contamination from pesticide use across Danish agricultural areas using hexagonal grid analysis.

📖 **[Excellent README](../backend/pipelines/h3_pfas_exposure_pipeline/README.md)** *(model for other pipelines)*

### 🏢 BBR Buildings Pipeline - Farm Infrastructure  
**Status**: 📝 Basic documentation, needs enhancement  
**Priority**: 🟡 MEDIUM  
**Data Sources**: BBR registry, GeoDanmark, INSPIRE

Maps and analyzes farm buildings and agricultural infrastructure across Denmark.

📖 **[Current README](../backend/pipelines/bbr_buildings/README.md)**  
🎯 **Planned Update**: Add context about farm infrastructure and agricultural development

---

## Business Intelligence Pipelines

### 📁 Drive Data Pipeline - Regulatory Compliance Data
**Status**: 📝 Technical docs exist, needs layman version  
**Priority**: 🟡 MEDIUM - Important for compliance tracking  
**Data Sources**: 12+ Google Drive folders with regulatory reports

Processes regulatory compliance reports submitted by farms and agricultural businesses.

**What it includes**:
- Fertilizer usage reports
- Pesticide application records
- Worker safety incidents
- Transportation accidents
- Stable fires and equipment failures
- Subsidies and permits
- Environmental violations

📖 **[Current README](../backend/pipelines/drive_data_pipeline/README.md)**  
🎯 **Planned Update**: Add explanation of regulatory compliance and why this data matters

### 🔍 Web Scraping Pipelines - Regulatory Oversight Data
**Status**: 📝 Mixed documentation quality  
**Priority**: 🟡 MEDIUM  

#### BMD Scraper - Pesticide Registration Database
**What it does**: Tracks approved pesticides, active ingredients, and usage restrictions  
📖 **[Current README](../backend/pipelines/bmd_scraper/README.md)**

#### DMA Scraper - Environmental Authority Companies  
**What it does**: Monitors companies under environmental regulatory oversight  
📖 **[Basic README](../backend/pipelines/dma_scraper/README.md)** *(needs comprehensive rewrite)*

#### Arbejdstilsynet Scraper - Workplace Safety Inspections
**What it does**: Tracks workplace safety inspections and violations in agricultural settings  
📖 **[Current README](../backend/pipelines/arbejdstilsynet_inspections/README.md)**

🎯 **Planned Update**: Create unified documentation explaining regulatory oversight and public safety

---

## Specialized Pipelines

### 🏠 Property Owners SFTP - Land Ownership Data
**Status**: ❌ No documentation found  
**Priority**: 🟡 MEDIUM  
**Data Sources**: Property ownership records via SFTP

🎯 **Planned**: Create comprehensive documentation from scratch

---

## Data Processing Guide

### Understanding Our Three-Layer System

All our pipelines follow a standardized "medallion architecture" that ensures data quality and transparency:

#### 🥉 Bronze Layer - Raw Data Preservation
- **Purpose**: Store data exactly as received from sources
- **Why**: Ensures complete audit trail and ability to reprocess
- **Content**: Unmodified data in original formats

#### 🥈 Silver Layer - Cleaned and Standardized  
- **Purpose**: Clean, validate, and standardize data
- **Why**: Makes data usable while maintaining quality
- **Content**: Consistent formats, validated data, quality checks

#### 🥇 Gold Layer - Analysis-Ready Datasets
- **Purpose**: Create enriched, analysis-ready data products
- **Why**: Enables easy research and visualization
- **Content**: Combined datasets, calculated metrics, research-ready formats

### Data Quality Standards

We maintain transparency about data quality through:
- **Completeness Tracking**: What percentage of expected data we have
- **Accuracy Assessment**: How reliable the data is
- **Timeliness Monitoring**: How current the data is
- **Consistency Validation**: Whether data matches across sources

---

## Pipeline Status Legend

| Symbol | Status | Meaning |
|--------|--------|---------|
| ✅ | Excellent | Complete, journalist-friendly documentation |
| 📝 | Needs Work | Has documentation but needs improvement |
| ❌ | Missing | No documentation found |
| 🔴 | HIGH | Critical pipeline needing immediate attention |
| 🟡 | MEDIUM | Important but not urgent |
| 🟢 | LOW | Lower priority or already complete |

---

## Documentation Standards

All pipeline documentation follows our [standardized template](templates/PIPELINE_README_TEMPLATE.md) which includes:

1. **Plain Language Explanations** - What the pipeline does and why it matters
2. **Data Source Transparency** - Where data comes from and how we collect it  
3. **Processing Documentation** - All transformations and quality checks
4. **Quality Assessment** - Honest discussion of limitations and appropriate uses
5. **Access Information** - How to use the data for research or journalism

---

## Getting Help

### For Journalists and Researchers
- **Data Questions**: Contact pipeline maintainers listed in individual READMEs
- **Access Issues**: [Contact information for data access team]
- **Story Collaboration**: [Information about working with our team]

### For Technical Users  
- **API Documentation**: [Link to technical API docs]
- **Integration Support**: [Technical support contact]
- **Contributing**: [Link to contribution guidelines]

---

## Recent Updates

### January 2025
- ✅ Completed comprehensive audit of all pipeline documentation
- ✅ Created standardized documentation template
- 📝 Identified 18+ data sources in unified pipeline (previously only 4 documented)
- 🎯 Prioritized documentation updates based on pipeline importance

### Planned Updates
- 🎯 **Week 1-2**: Unified Pipeline comprehensive documentation  
- 🎯 **Week 3-4**: CHR Pipeline journalist-friendly enhancements
- 🎯 **Week 5-6**: Drive Data and scraper pipeline documentation
- 🎯 **Week 7-8**: Specialized pipelines and final integration

---

*This documentation index is part of our commitment to transparent, trustworthy agricultural data. Last updated: January 2025*
