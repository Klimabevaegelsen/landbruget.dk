# Field Area Analysis Workflow

> **Monthly Dependent Processing**: Advanced multi-stage spatial analysis of agricultural fields with environmental coverage assessment

---

## What This Workflow Does

The Field Area Analysis workflow performs comprehensive spatial analysis of agricultural fields with environmental coverage assessment through a sophisticated multi-stage architecture. This gold-layer-only workflow combines agricultural field data with multiple environmental and administrative datasets to create detailed field-level spatial analytics, including environmental coverage by water projects, property ownership intersections, and comprehensive spatial relationship analysis.

### Why This Data Matters
- **Comprehensive Spatial Analytics**: Detailed spatial analysis of agricultural fields with multiple environmental and administrative layers
- **Environmental Coverage Assessment**: Advanced analysis of environmental protection coverage by water projects
- **Policy Support**: Critical data for environmental policy development and agricultural land management
- **Water Project Impact Assessment**: Quantitative assessment of water project effectiveness in covering environmental areas
- **Property-Field Relationships**: Detailed analysis of property ownership intersections with agricultural fields
- **Research Foundation**: High-quality spatial analytics for environmental research and land use studies

### Key Statistics
- **Processing Scale**: ~2.8M agricultural fields analyzed with 6+ spatial layers annually
- **Multi-Stage Architecture**: 5-stage pipeline (0-4) with advanced optimization and parallel processing
- **Data Reduction**: Stage 0 pre-filtering achieves 70-90% dataset reduction for optimal performance
- **Environmental Coverage**: Comprehensive coverage analysis of BNBO and wetland areas by water projects
- **Spatial Precision**: Individual field-level analysis with precise environmental and administrative intersections
- **Update Frequency**: Monthly dependent processing with dedicated matrix workflow architecture

---

## Data Sources and Dependencies

### Primary Dependencies
This workflow depends on silver data from multiple foundation sources:

| Source Workflow | Data Type | Dependency | Content |
|-----------------|-----------|------------|---------|
| **FVM WFS Workflow** | Agricultural field boundaries | Monthly foundation | ~2.8M field boundaries with spatial geometries |
| **BNBO Workflow** | Environmental protection areas | Monthly foundation | ~3.7K BNBO polygons (reduced to ~1K via pre-filtering) |
| **Water Projects Workflow** | Water management projects | Monthly foundation | ~2.4K water projects (reduced to ~500 via pre-filtering) |
| **Wetlands Workflow** | Wetland areas | Monthly foundation | ~1.6M wetland polygons (reduced to ~200K via pre-filtering) |
| **Property Cadastral Merge** | Property ownership data | Monthly dependent | ~6.5M properties (reduced to ~500K via pre-filtering) |

### Advanced Multi-Stage Architecture

#### Stage 0: Pre-filtering (Concurrent Processing)
**Purpose**: Massive dataset reduction through intelligent pre-filtering
- **Properties Pre-filtering**: 6.5M → ~500K properties (90% reduction)
- **Wetlands Pre-filtering**: 1.6M → ~200K wetlands (85% reduction)
- **BNBO Pre-filtering**: 3.7K → ~1K BNBO polygons (70% reduction)
- **Water Projects Pre-filtering**: 2.4K → ~500 projects (80% reduction)
- **Soil Types Pre-filtering**: Optimized soil type dataset (40% reduction)

#### Stage 1: Base Intersections (Parallel Processing)
**Purpose**: Fundamental spatial intersections using pre-filtered data
- **Fields × Properties**: Agricultural fields intersected with property boundaries
- **Fields × Soil Types**: Soil type classification for agricultural fields
- **Water Projects × BNBO**: Water project coverage of BNBO areas
- **Water Projects × Wetlands**: Water project coverage of wetland areas

#### Stage 2: Environmental Coverage (Parallel Processing)
**Purpose**: Environmental coverage analysis by water projects
- **Fields × BNBO Water**: BNBO area coverage assessment by water projects
- **Fields × Wetland Water**: Wetland area coverage assessment by water projects

#### Stage 3: Final Analysis (Sequential/Independent Processing)
**Purpose**: Comprehensive property-level environmental analysis
- **Final BNBO Analysis**: Property-level BNBO intersections with water project coverage
- **Final Wetland Analysis**: Property-level wetland intersections with water project coverage

#### Stage 4: Consolidation (Sequential/Independent Processing)
**Purpose**: Final result consolidation and comprehensive analytics
- **Result Consolidation**: Integration of all spatial analysis results into final dataset

### Performance Optimization Features
- **10-15x Performance Improvement**: Stage 0 pre-filtering reduces processing time by order of magnitude
- **Memory Optimization**: Sophisticated memory management for GitHub Actions constraints (16GB RAM, 4 CPU)
- **SPATIAL_JOIN Compliance**: Optimized for DuckDB SPATIAL_JOIN operator performance
- **Parallel Processing**: Concurrent execution of independent stages with proper dependency management
- **Coordinate Order Verification**: Advanced coordinate system validation and consistency checks

---

## Data Processing Steps

### 🥇 Gold Layer: Advanced Multi-Stage Spatial Analysis (Unified Pipeline Only)
**What happens**: We perform comprehensive spatial analysis of agricultural fields through a sophisticated 5-stage pipeline with advanced optimization
**Why**: Environmental policy and agricultural management require detailed spatial relationships between fields and environmental/administrative layers

**Specific processing**:

#### Stage 0: Intelligent Pre-filtering (Concurrent Processing)
- **Properties Pre-filtering**: Reduces 6.5M properties to ~500K intersecting with fields (90% reduction, 13x faster Stage 1)
- **Wetlands Pre-filtering**: Reduces 1.6M wetlands to ~200K intersecting with fields (85% reduction, 8x faster Stage 2)
- **BNBO Pre-filtering**: Reduces 3.7K BNBO areas to ~1K intersecting with fields (70% reduction, 3.7x faster Stage 2)
- **Water Projects Pre-filtering**: Reduces 2.4K projects to ~500 intersecting with fields (80% reduction, 4.8x faster Stage 1)
- **Soil Types Pre-filtering**: Optimizes soil type dataset for field intersection (40% reduction)

#### Stage 1: Fundamental Spatial Intersections (Parallel Processing)
- **Fields × Properties Intersection**: High-precision spatial intersection of agricultural fields with property boundaries
- **Fields × Soil Types Analysis**: Soil type classification and characteristics assignment to agricultural fields
- **Water Projects × BNBO Coverage**: Water project coverage assessment of BNBO environmental protection areas
- **Water Projects × Wetlands Coverage**: Water project coverage assessment of wetland environmental areas

#### Stage 2: Environmental Coverage Analysis (Parallel Processing)
- **Fields × BNBO Water Coverage**: Detailed analysis of BNBO area coverage by water projects at field level
- **Fields × Wetland Water Coverage**: Comprehensive analysis of wetland area coverage by water projects at field level
- **Coverage Percentage Calculations**: Precise calculation of environmental coverage percentages and ratios

#### Stage 3: Advanced Property-Level Analysis (Sequential/Independent Processing)
- **Final BNBO Analysis**: Property-level BNBO intersections with comprehensive water project coverage assessment
- **Final Wetland Analysis**: Property-level wetland intersections with detailed water project coverage analysis
- **Geometric Foundation Integration**: Integration of existing geometries from previous stages without recalculation

#### Stage 4: Comprehensive Result Consolidation (Sequential/Independent Processing)
- **Multi-Source Integration**: Consolidation of all spatial analysis results into comprehensive final dataset
- **Coverage Statistics Generation**: Detailed statistical analysis of environmental coverage and spatial relationships
- **Quality Assessment**: Comprehensive validation of spatial analysis results and coverage completeness

#### Advanced Technical Features
- **SPATIAL_JOIN Optimization**: Leverages DuckDB SPATIAL_JOIN operator for optimal spatial query performance
- **Memory Management**: Sophisticated resource management for GitHub Actions constraints (16GB RAM, 4 CPU, 14GB SSD)
- **Coordinate System Validation**: Advanced coordinate order verification and consistency checks across datasets
- **Parallel Matrix Processing**: Concurrent processing of multiple agricultural field years (2024, 2025)
- **Disk Space Management**: Automated cleanup and resource management for memory-intensive processing

**Output**: Comprehensive field-level spatial analytics with environmental coverage assessment and property relationships

---

## Workflow Schedule and Execution

### Monthly Dependent Processing with Dedicated Matrix Workflow
- **Primary Schedule**: 1st of every month at 1 AM UTC (dependent batch, priority 12)
- **Dedicated Matrix Workflow**: `field_area_analysis_multi_stage.yml` with advanced stage management
- **Processing Duration**: ~3 hours for complete multi-stage analysis (estimated 180 minutes)
- **Dependencies**: Requires FVM WFS, BNBO, Water Projects, Wetlands, and Property Cadastral Merge data
- **Matrix Support**: Parallel processing of multiple agricultural field years (2024, 2025)

### Advanced Multi-Stage Processing Architecture
- **Stage 0**: Concurrent pre-filtering (5 parallel jobs) with massive dataset reduction (70-90%)
- **Stage 1**: Parallel base intersections (4 parallel jobs) using pre-filtered data for optimal performance
- **Stage 2**: Parallel environmental coverage analysis (2 parallel jobs) with detailed coverage calculations
- **Stage 3**: Sequential/independent final analysis with comprehensive property-level integration
- **Stage 4**: Final consolidation with complete result integration and statistical analysis

### Processing Performance and Resource Management
- **Data Volume**: ~2.8M agricultural fields processed with 6+ spatial layers annually
- **Memory Optimization**: Advanced memory management for GitHub Actions constraints (16GB RAM, 4 CPU)
- **Performance Improvement**: 10-15x faster execution through Stage 0 pre-filtering optimization
- **Disk Management**: Automated cleanup and resource management for memory-intensive spatial processing
- **Coordinate Validation**: Advanced coordinate system validation and consistency checks

### Workflow Execution Modes
- **Sequential Mode ('all')**: Complete pipeline execution with proper stage dependencies (Stage 0→1→2→3→4)
- **Independent Stage Mode**: Individual stage execution using latest GCS data for development/debugging
- **Matrix Year Processing**: Parallel processing of multiple agricultural field years with proper resource allocation
- **Fault Tolerance**: Robust error handling with stage-level isolation and recovery strategies

---

## Data Quality and Limitations

### Data Quality Assessment
| Quality Metric | Status | Details |
|----------------|--------|---------|
| **Completeness** | Excellent | >95% field coverage with comprehensive spatial analysis across all environmental layers |
| **Accuracy** | Excellent | High-precision spatial analysis with SPATIAL_JOIN optimization and coordinate validation |
| **Performance** | Excellent | 10-15x performance improvement through advanced pre-filtering and optimization |
| **Spatial Precision** | Excellent | Individual field-level analysis with precise environmental and administrative intersections |

### Known Issues and Limitations

#### Processing and Resource Constraints
- **Memory Requirements**: High memory requirements (16GB) for large-scale spatial processing on GitHub Actions
- **Processing Duration**: Extended processing time (~3 hours) for complete multi-stage analysis
- **Disk Space Requirements**: Significant temporary storage needs (14GB SSD) for intermediate spatial processing
- **Coordinate System Complexity**: Advanced coordinate order validation required for spatial consistency

#### Multi-Stage Architecture Complexity
- **Stage Dependencies**: Complex dependency management between stages requiring proper sequencing
- **Data Consistency**: Temporal alignment between agricultural fields and environmental datasets may vary
- **Matrix Processing**: Multiple year processing requires careful resource allocation and coordination
- **Intermediate Storage**: Significant GCS storage requirements for intermediate stage results

#### Environmental Coverage Limitations
- **Water Project Coverage**: Coverage analysis dependent on water project spatial accuracy and completeness
- **Environmental Data Lag**: Environmental datasets (BNBO, wetlands) may have different update frequencies
- **Property Ownership Alignment**: Property cadastral data may not align perfectly with current field boundaries
- **Spatial Precision Trade-offs**: Balance between processing performance and spatial precision in large-scale analysis

### Advanced Quality Assurance Features
- **Coordinate Order Verification**: Advanced validation of coordinate system consistency across all datasets
- **SPATIAL_JOIN Compliance**: Optimization for DuckDB SPATIAL_JOIN operator with performance monitoring
- **Stage-Level Validation**: Comprehensive validation at each stage with error recovery and continuation
- **Coverage Statistics**: Detailed statistical validation of environmental coverage and spatial relationship completeness

### Recommended Uses
✅ **This data is excellent for**:
- Environmental policy development with detailed field-level environmental coverage analysis
- Water project impact assessment with quantitative coverage evaluation of environmental areas
- Agricultural land management with comprehensive spatial relationship analysis
- Environmental research with high-quality field-level spatial analytics and coverage assessment
- Property-field relationship analysis for administrative and regulatory purposes

⚠️ **Use with caution for**:
- Real-time environmental monitoring - Monthly processing with potential data lag between datasets
- Historical trend analysis - Complex multi-stage architecture may affect historical data consistency
- Small-scale analysis - Pipeline optimized for large-scale processing, may be overkill for small areas

❌ **Not recommended for**:
- Daily environmental monitoring - Monthly updates, not real-time environmental change tracking
- Simple spatial queries - Complex multi-stage architecture not needed for basic spatial intersections
- International comparisons without context - Danish-specific environmental and administrative systems

---

## Usage Examples and Access

### Common Questions This Data Answers
1. **What percentage of BNBO areas in agricultural fields are covered by water projects?** - Environmental coverage analysis with detailed percentage calculations
2. **Which properties have agricultural fields with significant wetland areas?** - Property-field-wetland intersection analysis
3. **How effective are water projects in covering environmental protection areas?** - Water project impact assessment with coverage statistics
4. **What are the soil type distributions across agricultural fields in different regions?** - Soil type analysis with spatial distribution patterns

### Example Analyses
#### Environmental Coverage Assessment
**Question**: What is the effectiveness of water projects in covering BNBO environmental protection areas across Danish agricultural fields?
**Data Used**: Multi-stage spatial analysis results with BNBO-water project coverage calculations
**Method**: Coverage percentage analysis with statistical validation and regional comparison
**Output**: Comprehensive coverage statistics with effectiveness metrics and regional variation analysis
**Limitations**: Coverage analysis dependent on spatial accuracy of water project boundaries; temporal alignment between datasets may affect results

#### Property-Field Environmental Analysis
**Question**: Which properties contain agricultural fields with significant environmental areas (wetlands, BNBO)?
**Data Used**: Property-field intersections combined with environmental area analysis from multi-stage processing
**Method**: Spatial intersection analysis with environmental area calculations and property-level aggregation
**Output**: Property-level environmental profiles with area calculations and coverage statistics
**Limitations**: Property ownership data may not align perfectly with current field boundaries; environmental data temporal consistency varies

#### Water Project Impact Assessment
**Question**: How do water projects impact environmental protection coverage across different agricultural regions?
**Data Used**: Complete multi-stage analysis results with water project coverage of environmental areas
**Method**: Regional aggregation of coverage statistics with water project effectiveness analysis
**Output**: Regional water project impact assessment with coverage improvement metrics and policy recommendations
**Limitations**: Water project effectiveness dependent on spatial accuracy and completeness; regional variations in data quality may affect analysis

### Data Access
- **Research Access**: Comprehensive field-level spatial analytics for environmental and agricultural research
- **Policy Access**: Environmental coverage data for policy development and water project effectiveness assessment
- **Administrative Access**: Property-field relationships for regulatory compliance and land management
- **Regional Access**: Regional environmental coverage statistics for administrative planning and resource allocation

---

## Technical Details

<details>
<summary>Click to expand technical specifications</summary>

### Data Schema
#### Field Area Analysis Final Results (Gold Layer)

**Comprehensive Field-Level Spatial Analytics Dataset**
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
field_id | VARCHAR | FVM field identifier | "12345"
block_id | VARCHAR | FVM block identifier | "67890"
cvr_number | VARCHAR | Company registration number | "12345678"
field_area_m2 | DOUBLE | Field area in square meters | 125000.5
soil_code | VARCHAR | Soil type classification code | "JB3"
soil_description | VARCHAR | Soil type description | "Lerjord, middel bonitet"
bnbo_status | VARCHAR | BNBO status category | "Action Required"
water_project_id | VARCHAR | Water project identifier | "WP_2024_001"
wetland_id | VARCHAR | Wetland identifier | "WL_001"
bfe_number | VARCHAR | Property BFE identifier | "1234567890"
property_area_share | DOUBLE | Property area share percentage | 75.5
total_bnbo_area_m2 | DOUBLE | Total BNBO area in field | 15000.0
bnbo_covered_by_water_projects_m2 | DOUBLE | BNBO area covered by water projects | 12000.0
bnbo_covered_by_water_projects_pct | DOUBLE | BNBO coverage percentage | 80.0
bnbo_not_covered_by_water_projects_pct | DOUBLE | BNBO not covered percentage | 20.0
total_wetland_area_m2 | DOUBLE | Total wetland area in field | 8000.0
wetland_covered_by_water_projects_m2 | DOUBLE | Wetland area covered by water projects | 6400.0
wetland_covered_by_water_projects_pct | DOUBLE | Wetland coverage percentage | 80.0
wetland_not_covered_by_water_projects_pct | DOUBLE | Wetland not covered percentage | 20.0
field_bnbo_coverage_pct | DOUBLE | Field BNBO coverage percentage | 12.0
field_wetland_coverage_pct | DOUBLE | Field wetland coverage percentage | 6.4
```

### Storage Locations
- **Gold Output**: `gs://landbrugsdata-raw-data/gold/field_area_analysis/latest/data.parquet`
- **Stage Outputs**: `gs://landbrugsdata-raw-data/gold/field_area_analysis_stage{N}_{year}/{timestamp}/`
- **Matrix Outputs**: Year-specific paths for parallel processing results

### Processing Infrastructure
- **Platform**: Dedicated GitHub Actions matrix workflow with advanced stage management
- **Resources**: 16GB RAM, 4 CPU, 14GB SSD with sophisticated resource management
- **Dependencies**: FVM WFS, BNBO, Water Projects, Wetlands, Property Cadastral Merge silver data
- **Performance**: ~3 hours for complete multi-stage analysis (180 minutes estimated)

### Multi-Stage Architecture Details
#### Stage 0: Pre-filtering Performance
```
Dataset | Original Size | Filtered Size | Reduction | Performance Impact
Properties | 6.5M | ~500K | 90% | 13x faster Stage 1
Wetlands | 1.6M | ~200K | 85% | 8x faster Stage 2
BNBO | 3.7K | ~1K | 70% | 3.7x faster Stage 2
Water Projects | 2.4K | ~500 | 80% | 4.8x faster Stage 1
```

#### Advanced Spatial Processing Features
- **SPATIAL_JOIN Optimization**: Leverages DuckDB SPATIAL_JOIN operator for optimal performance
- **Coordinate Order Verification**: Advanced validation of coordinate system consistency
- **Memory Management**: Sophisticated resource management for GitHub Actions constraints
- **Parallel Processing**: Concurrent execution with proper dependency management
- **Error Recovery**: Stage-level isolation with robust error handling and recovery

### Quality Assurance Features
- **Coordinate System Validation**: Advanced coordinate order verification across all datasets
- **Coverage Statistics Validation**: Comprehensive validation of environmental coverage calculations
- **Stage-Level Quality Checks**: Quality validation at each stage with error recovery
- **Spatial Precision Monitoring**: Continuous monitoring of spatial analysis accuracy and performance
- **Resource Usage Monitoring**: Real-time monitoring of memory and disk usage with automatic cleanup

### Matrix Workflow Features
- **Parallel Year Processing**: Concurrent processing of multiple agricultural field years (2024, 2025)
- **Stage Dependency Management**: Proper sequencing of stages with dependency validation
- **Independent Stage Execution**: Individual stage execution using latest GCS data for development
- **Resource Scaling**: Dynamic resource allocation based on stage-specific requirements
- **Fault Tolerance**: Stage-level isolation with robust error handling and continuation strategies

</details>

---

## Contact and Support

### Workflow Maintainer
- **Primary Contact**: Spatial Analytics Data Team
- **Response Time**: 1-2 business days

### Reporting Issues
- **Data Quality Issues**: Report via GitHub issues with "Field Area Analysis" label
- **Performance Issues**: Contact infrastructure team for multi-stage pipeline optimization
- **Environmental Coverage Issues**: Contact environmental data team for coverage analysis improvements

### Documentation Updates
- **Last Updated**: January 2025
- **Update Schedule**: Reviewed when environmental or agricultural data structures change
- **Version**: 1.0

---

*This documentation is part of the Landbruget.dk transparency initiative to make environmental and spatial analytics data accessible and trustworthy for policy development, research, and environmental management.*
