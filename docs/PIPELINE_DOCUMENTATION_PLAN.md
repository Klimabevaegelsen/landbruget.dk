# Pipeline Documentation Plan for Journalists and Laymen

## Overview

This document outlines the comprehensive plan to create thorough, journalist-friendly README documentation for all data pipelines in the Landbruget.dk project. The goal is to make our data sources transparent and trustworthy by documenting every step of data collection, processing, and transformation in language accessible to non-technical users.

## Documentation Principles

### Target Audience
- **Journalists** investigating agricultural practices and compliance
- **Researchers** using the data for academic purposes  
- **Policy makers** making decisions based on the data
- **Citizens** wanting to understand agricultural data in Denmark
- **Data users** who need to assess data quality and limitations

### Documentation Standards
- **Plain Language**: Avoid technical jargon, explain complex concepts simply
- **Complete Transparency**: Document all data transformations, limitations, and assumptions
- **Data Lineage**: Clear traceability from original source to final output
- **Quality Assessment**: Honest discussion of data quality, gaps, and reliability
- **Update Frequency**: Clear information about how often data is refreshed
- **Contact Information**: Who to contact for questions or issues

## Pipeline Inventory and Status

### ✅ **Unified Pipeline** (Workflow-Based Architecture)
**Current Status**: Has basic README, needs comprehensive rewrite to reflect workflow structure
**Priority**: HIGH - This is our main data pipeline
**Architecture**: Organized by workflows with different execution schedules

**Workflow Categories**:

#### **Manual Workflows** (On-demand execution) ✅ **COMPLETED**
- ✅ **Soil types** - Danish soil types from Environmental Portal
- ✅ **Wetlands** - Wetlands data processing  
- ✅ **Water typology** - Water typology (lakes, coastal waters, watercourses)
- ✅ **Pesticide disaggregation** - Field-level pesticide analysis (matrix jobs by year)
- ✅ **Pesticide compliance** - Regulatory compliance analysis
- ✅ **Worker safety** - Worker safety incident processing
- ✅ **Work permits** - Work permit data processing
- ✅ **DMI** - Danish Meteorological Institute climate data

#### **Weekly Workflows** (Every Monday 2 AM UTC) ✅ **COMPLETED**
- ✅ **CVR enrichment** - Company data enrichment and geocoding

#### **Monthly Workflows - Foundation Batch** (1st of month, 1 AM UTC) ✅ **COMPLETED**
- ✅ **DST** - Danish Statistics data
- ✅ **FVM WFS** - Agricultural field data (triggers matrix workflow for all years/layers)
- ✅ **Cadastral** - Danish cadastral parcels
- ✅ **BNBO** - Well Protection Areas
- ✅ **DAGI** - Danish Administrative Geographic Division
- ✅ **Water projects** - Water projects data
- ✅ **Arbejdstilsynet inspections** - Work Environment Authority inspections

#### **Monthly Workflows - Dependent Batch** (After foundation completes) ✅ **COMPLETED**
- ✅ **Property cadastral merge** - Merges property and cadastral data
- ✅ **Field production** - Field production estimates (depends on FVM + DST)
- ✅ **Field area analysis** - Complex spatial analysis (depends on FVM + BNBO + Water projects)

**Documentation Needs**:
- [ ] Workflow-based architecture explanation
- [ ] Schedule and dependency documentation
- [ ] Individual workflow documentation for each category
- [ ] Bronze/Silver/Gold layer explanations per workflow
- [ ] Data quality assessments per workflow
- [ ] Resource requirements and performance characteristics
- [ ] Matrix job explanations (FVM WFS, pesticide processing, field area analysis)
- [ ] Usage examples and common queries

### ✅ **CHR Pipeline** (Animal Movement and Health Data)
**Current Status**: Has basic README, needs journalist-friendly rewrite
**Priority**: HIGH - Critical for animal welfare and disease tracking
**Data Sources Include**:
- Animal movements between farms
- Herd registrations and properties
- Veterinary treatments and medicine usage
- Disease surveillance (SPF-SU)
- Property ownership and locations

**Documentation Needs**:
- [ ] Animal welfare and tracking explanation
- [ ] Disease surveillance methodology
- [ ] Privacy protections for farm data
- [ ] Data retention and historical coverage
- [ ] Integration with international tracking systems
- [ ] Antibiotic usage monitoring explanation

### ✅ **Drive Data Pipeline** (12+ Google Drive Sources) ✅ **COMPLETED**
**Current Status**: Comprehensive README completed with detailed coverage of all 12 data sources
**Priority**: MEDIUM - Important for regulatory compliance data
**Data Sources Include**:
- Fertilizer usage reports (Gødning)
- Pesticide application records (Pesticider) 
- Worker safety incidents (Arbejdssikkerhed)
- Transportation accidents (Transportulykker)
- Stable fires (Staldebrande)
- Slurry leaks (Gyllelækager)
- Subsidies (Tilskud)
- Work permits (Arbejdstilladelser)
- Pig tail cutting (Svinehaleafskæring)

**Documentation Needs**:
- [ ] Explanation of regulatory reporting requirements
- [ ] Data collection methodology from Google Drive
- [ ] Quality control for manual data entry
- [ ] Historical data availability
- [ ] Compliance monitoring capabilities

### ✅ **BBR Buildings Pipeline** ✅ **COMPLETED**
**Current Status**: Comprehensive README completed with detailed coverage of building classification system
**Priority**: MEDIUM - Important for farm infrastructure analysis
**Data Sources Include**:
- Building registry data (BBR)
- GeoDanmark building footprints
- INSPIRE building data

**Documentation Needs**:
- [ ] Building classification system explanation
- [ ] Spatial accuracy and resolution
- [ ] Update frequency from official sources
- [ ] Integration methodology between sources

### ✅ **Scraper Pipelines** (3 pipelines) ✅ **COMPLETED**
**Current Status**: Comprehensive documentation completed for all 3 scraper pipelines
**Priority**: MEDIUM - Important for regulatory oversight

#### BMD Scraper (Pesticide Database)
- [ ] Pesticide approval and registration data
- [ ] Active ingredients and restrictions
- [ ] Usage guidelines and safety information

#### DMA Scraper (Environmental Authority Companies)
- [ ] Company environmental compliance data
- [ ] Regulatory oversight information
- [ ] Environmental permits and violations

#### Arbejdstilsynet Scraper (Work Environment Authority)
- [ ] Workplace safety inspections
- [ ] Violation records and follow-up actions
- [ ] Industry-specific safety requirements

### ✅ **Specialized Pipelines** (3 pipelines) ✅ **COMPLETED**
**Current Status**: Comprehensive documentation completed for all 3 specialized pipelines
**Priority**: LOW-MEDIUM - Specialized use cases

#### H3 PFAS Exposure Pipeline
- [ ] PFAS contamination modeling methodology
- [ ] H3 hexagonal grid system explanation
- [ ] Exposure risk assessment calculations
- [ ] Data sources and validation

#### Svineflytning Pipeline (Pig Movement Tracking)
- [ ] Pig movement tracking for disease control
- [ ] Integration with CHR system
- [ ] Traceability and food safety

#### Property Owners SFTP
- [ ] Property ownership data collection
- [ ] SFTP transfer methodology
- [ ] Data privacy and anonymization

## Documentation Template Structure

Each pipeline README should follow this structure:

### 1. **Executive Summary** (2-3 paragraphs)
- What this pipeline does in plain language
- Why this data matters for agriculture/society
- Key statistics (data volume, update frequency, coverage)

### 2. **Data Sources and Collection**
- Official source agencies and their roles
- How data is collected by original sources
- Our collection methodology (APIs, scraping, manual)
- Data quality controls and validation

### 3. **Data Processing Steps**
- **Bronze Layer**: Raw data preservation
- **Silver Layer**: Cleaning and standardization
- **Gold Layer**: Analysis-ready datasets
- All transformations explained in plain language

### 4. **Data Quality and Limitations**
- Known data quality issues
- Missing data and gaps
- Accuracy assessments
- Recommended uses and cautions

### 5. **Update Schedule and Timeliness**
- How often data is refreshed
- Lag time from source to our system
- Historical data availability
- Planned improvements

### 6. **Usage Examples**
- Common questions this data can answer
- Example analyses and visualizations
- Integration with other datasets
- API access information

### 7. **Technical Details** (for advanced users)
- Data formats and schemas
- Storage locations and access methods
- Processing infrastructure
- Performance characteristics

### 8. **Contact and Support**
- Who maintains this pipeline
- How to report issues or request features
- Documentation update schedule

## Implementation Plan

### Workflow Documentation Process
**CRITICAL**: For each workflow, follow this exact process:

1. **Code Review** (30-60 min per workflow):
   - Read the complete workflow implementation files
   - Understand data sources, processing steps, and outputs
   - Note key technical decisions and parameters
   - Identify dependencies and scheduling

2. **Documentation Creation** (60-90 min per workflow):
   - Create workflow-specific README using template
   - Ensure all information is accurate based on code review
   - Include specific technical details from implementation
   - Validate against actual workflow behavior

3. **Validation** (15-30 min per workflow):
   - Cross-check documentation against code
   - Verify all claims and statistics are accurate
   - Ensure no assumptions or guesses are included

### Phase 1: Foundation (Completed)
- [x] Create this planning document
- [x] Develop standardized README template
- [x] Audit existing documentation quality
- [x] Create central documentation index

### Phase 2: Unified Pipeline Workflows (Current)
**Process each workflow individually using the 3-step process above:**

#### Manual Workflows
- [ ] **Soil Types** - Code review → Documentation → Validation
- [ ] **Wetlands** - Code review → Documentation → Validation  
- [ ] **Water Typology** - Code review → Documentation → Validation
- [ ] **Pesticide Disaggregation** - ⚠️ PARTIALLY DONE - needs code review validation
- [ ] **Pesticide Compliance** - Code review → Documentation → Validation
- [ ] **Worker Safety** - Code review → Documentation → Validation
- [ ] **Work Permits** - Code review → Documentation → Validation
- [ ] **DMI** - Code review → Documentation → Validation

#### Weekly Workflows  
- [ ] **CVR Enrichment** - ⚠️ NEEDS CODE REVIEW - current docs may be inaccurate

#### Monthly Foundation Workflows
- [ ] **DST** - Code review → Documentation → Validation
- [ ] **FVM WFS** - ⚠️ NEEDS CODE REVIEW - current docs may be inaccurate
- [ ] **Cadastral** - Code review → Documentation → Validation
- [ ] **BNBO** - Code review → Documentation → Validation
- [ ] **DAGI** - Code review → Documentation → Validation
- [ ] **Water Projects** - Code review → Documentation → Validation
- [ ] **Arbejdstilsynet Inspections** - Code review → Documentation → Validation

#### Monthly Dependent Workflows
- [ ] **Property Cadastral Merge** - Code review → Documentation → Validation
- [ ] **Field Production** - Code review → Documentation → Validation
- [ ] **Field Area Analysis** - Code review → Documentation → Validation

### Phase 3: Other Pipelines
- [ ] CHR Pipeline comprehensive documentation
- [ ] Drive Data Pipeline enhancement
- [ ] BBR Buildings pipeline documentation
- [ ] Scraper pipelines documentation (BMD, DMA, Arbejdstilsynet)
- [ ] Specialized pipelines (H3 PFAS, Svineflytning, Property Owners)

### Phase 4: Integration and Review
- [ ] Cross-reference all documentation for consistency
- [ ] Validate all technical claims against code
- [ ] Review for accessibility and clarity
- [ ] Update central index with all workflow links

## Success Metrics

- **Completeness**: All pipelines have comprehensive READMEs
- **Accessibility**: Non-technical users can understand data sources and limitations
- **Transparency**: All processing steps and quality issues are documented
- **Usability**: Clear examples and use cases for each dataset
- **Maintainability**: Documentation is kept up-to-date with pipeline changes

## README Audit Results

### ✅ **Audit Completed** - Current Documentation Assessment

**High Quality Documentation:**
- **H3 PFAS Exposure Pipeline**: Excellent example of journalist-friendly documentation with clear "What is this pipeline?" section for non-technical readers
- **CHR Pipeline**: Good technical documentation but needs layman-friendly introduction
- **BMD Scraper**: Well-structured technical documentation

**Moderate Quality Documentation:**
- **Unified Pipeline**: Basic technical docs, lists only 4 sources but system has 18+ sources - major gap
- **Svineflytning Pipeline**: Good technical structure, needs journalist accessibility
- **Arbejdstilsynet Inspections**: Detailed technical docs, needs context for non-technical users

**Basic Documentation:**
- **DMA Scraper**: Very basic, needs comprehensive rewrite
- **BBR Buildings**: Minimal documentation

**Missing Documentation:**
- **Property Owners SFTP**: No README found
- Several pipelines have no README files

**Key Findings:**
1. **Technical Focus**: Most READMEs are written for developers, not end users
2. **Missing Context**: Little explanation of why data matters or how it's used
3. **Incomplete Coverage**: Unified pipeline severely under-documents its scope
4. **Best Practice**: H3 PFAS pipeline shows excellent model for journalist-friendly docs
5. **Data Quality**: Limited discussion of data limitations and quality issues

## Questions and Uncertainties

<!-- Comments will be added here as uncertainties arise during documentation -->

### Unified Pipeline Questions
- Need to verify exact number of Gold layer datasets (mentioned as "6" but need confirmation)
- Clarify update schedules for each individual source within unified pipeline
- Confirm data retention policies for historical data

### CHR Pipeline Questions  
- Need to understand privacy protections and anonymization for farm-level data
- Clarify integration points with international animal tracking systems
- Verify antibiotic usage data coverage and accuracy

### Drive Pipeline Questions
- Need to assess data quality controls for manually uploaded files
- Understand validation processes for regulatory compliance data
- Clarify historical data availability across different data types

### General Questions
- Confirm contact information for each pipeline maintainer
- Understand SLA commitments for data freshness and availability
- Clarify legal/compliance requirements for data documentation

## Next Steps

1. **Immediate**: Begin with unified pipeline documentation audit
2. **This Week**: Create standardized template and central index
3. **Ongoing**: Work through pipelines in priority order
4. **Continuous**: Update this plan as new information is discovered

---

**Document Status**: Initial Plan Created  
**Last Updated**: January 2025  
**Next Review**: Weekly during implementation  
**Maintainer**: Pipeline Documentation Team
