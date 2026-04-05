# Deterministic disaggregation of company-level pesticide application reports to individual agricultural fields using area matching: A 14-year validation with Danish open data

**Martin Collignon** ^1, [additional authors TBD]

^1 Landbruget.dk, Denmark

---

## Abstract

Across the European Union, pesticide application data is typically reported at the company or regional level, creating a spatial resolution gap that limits field-level environmental risk assessment. We present a deterministic algorithm that disaggregates company-level spray journal records to individual agricultural fields by matching on company identifier (CVR) and crop code, with proportional dose allocation within a configurable area tolerance (default &pm;2%). The algorithm employs four sequential strategies: full area matching across all fields (S1), a non-organic field fallback (S2), partial field coverage for single-field cases (S3), and optional spatial clustering (S4). A year-plus-one temporal alignment matches pesticide application year X to field boundary registrations from year X+1, reflecting the Danish agricultural administrative cycle. Validated across 14 years of Danish government open data (2010--2023), the algorithm achieves 92.7% coverage in 2020 and sustains &ge;90% from 2018 onward. Tolerance sensitivity analysis across eight levels (0--10%) reveals that the jump from 0% to 0.5% tolerance produces the largest coverage gain (from 37--48% to 72--87%), identifying area rounding in spray journal reporting as the dominant source of mismatch. The approach requires no machine learning, produces fully auditable allocations with per-record confidence scores, and operates entirely on publicly available Danish datasets. The pipeline is implemented as an open-source system underlying the landbruget.dk agricultural transparency platform.

**Keywords:** pesticide disaggregation, spatial resolution, area matching, Danish agriculture, open data, field-level analysis, spray journal

---

## 1. Introduction

### 1.1 The spatial resolution gap in European pesticide data

Reliable, spatially resolved pesticide use data is a prerequisite for credible environmental and human health risk assessment. Yet across the European Union, such data has historically been available only as national sales totals compiled by Eurostat's agri-environmental indicator framework. Regulation (EC) No 1185/2009 established a legal basis for statistics on pesticide sales and use, but its provisions mandate farm-level use surveys only once every five years, and the resulting statistics are published at highly aggregated spatial levels --- typically national or, at best, regional [Eurostat, 2023]. The consequence is a persistent spatial resolution gap: analysts know how much active substance was sold in a country during a given year, but not where it was applied.

The recently adopted Statistics on Agricultural Input and Output (SAIO) Regulation (EU) 2022/2379 represents a significant institutional advance, mandating annual electronic recording of pesticide use by professional users from 2028 onward. However, even under SAIO, field-level spatial data is not required at the EU scale. Member States will collect use data at the farm or holding level, and Eurostat will publish statistics at regional or national aggregations [European Commission, 2022]. The fundamental mismatch between the spatial resolution at which pesticide exposure operates --- individual fields, buffer zones around water bodies, proximity to residential areas --- and the resolution at which data is collected will therefore persist for the foreseeable future.

This spatial resolution gap has been widely documented in the recent literature. Piera et al. [2023] noted that the absence of georeferenced pesticide use data at sub-regional scales severely constrains environmental fate modelling across Europe. Balcaen et al. [2025] observed that even the most advanced EU-wide pesticide mapping efforts must rely on statistical downscaling from sparse survey samples rather than direct observation. The European Commission's Joint Research Centre (JRC) has convened multiple workshops on estimating pesticide use from heterogeneous data sources, consistently identifying spatial disaggregation as a key methodological challenge [JRC, 2019]. Without field-level data, environmental risk assessments cannot account for local conditions such as soil type, slope, proximity to water bodies and drinking water abstraction points, distance to residential areas, or adjacency to sensitive ecosystems.

### 1.2 Existing approaches and their limitations

The literature on spatially resolving pesticide data can be organized into four broad categories, each with characteristic strengths and limitations.

**Sales-based downscaling.** The most common approach distributes regional or postal-code-level pesticide sales data to smaller administrative units in proportion to crop area shares. France's NODU (*Nombre de Doses Unités*) indicator distributes postal-code-level sales to municipalities using crop area proportions derived from the national agricultural census [Deguine et al., 2023]. In Belgium, Zucchini et al. [2022] mapped 388 active substances across 9.5 million parcels in Wallonia by combining postal-code sales data with crop-specific application rates derived from a 4% farm sample survey. These approaches produce plausible spatial distributions but lack farm identity: the same crop area in two different companies receives identical estimated doses regardless of the actual practices employed by each operator.

**Statistical and regression models.** A second category uses regression or machine learning to predict pesticide intensity from observable covariates. Piera et al. [2023] produced an EU-wide regional-level pesticide emissions dataset at NUTS3 resolution by regressing reported use data against climate variables, land-use characteristics, and crop distribution patterns. Maggi et al. [2019] created PEST-CHEMGRIDS, providing global gridded pesticide application rates at approximately 10 km resolution by combining FAO national statistics with crop maps and agronomic assumptions. Building on this foundation, Balcaen et al. [2025] achieved 250 m resolution EU-wide maps by fusing PEST-CHEMGRIDS estimates with high-resolution crop type maps derived from Sentinel satellite imagery. While these models achieve impressive spatial coverage, they fundamentally rely on statistical extrapolation from limited calibration samples and cannot capture farm-level decision-making.

**Process-based risk models.** A third category couples pesticide use data with environmental fate models to assess exposure risk at fine spatial scales. The SYNOPS model developed by Gutsche and Rossberg [1997] and its GIS extension [Strassemeyer et al., 2017] assess environmental risk at the field level by linking geospatial databases of land use, soil properties, and climate to pesticide use records. The FOCUS framework, established by the European Commission [2001], provides standardized exposure scenarios for regulatory approval of plant protection products. These models are powerful tools for regulatory risk assessment but require detailed input parameters --- including the very field-level use data that is typically unavailable --- and are designed for scenario analysis rather than large-scale spatial mapping of actual application patterns.

**High-resolution exposure mapping.** Most recently, several studies have combined parcel-level crop data with population or building density to map potential pesticide exposure. Deguine et al. [2025] developed a spatially explicit residential pesticide exposure indicator for metropolitan France by combining the RPG (*Registre Parcellaire Graphique*) parcel-level crop registry with population density grids. Zucchini et al. [2024] quantified residential exposure to pesticides in Wallonia using buffer-based models that overlay crop parcels with building locations at multiple distance thresholds.

A critical limitation is shared by all four categories: none exploit direct farm-identity linkage between pesticide application records and georeferenced field boundaries. Sales-based methods distribute aggregate quantities probabilistically. Statistical models extrapolate from limited samples. Process-based models require use data as input rather than generating it. Exposure mapping studies inherit whatever spatial resolution their upstream pesticide data provides. The fundamental bottleneck is the absence of a shared identifier connecting "who applied what" to "where their fields are."

### 1.3 Contribution and scope

Denmark possesses a uniquely rich administrative data infrastructure that enables a fundamentally different approach to the spatial disaggregation problem. Two government datasets, when combined, provide the direct farm-identity linkage that existing methods lack.

First, the Sprøjtejournal (SJI) system, administered by Miljøstyrelsen (the Danish Environmental Protection Agency), requires all professional users of pesticides managing more than 10 hectares of cultivated area to report their pesticide applications annually [Miljøstyrelsen, 2024]. Each SJI record identifies the applying company by its CVR (*Centralt Virksomhedsregister*) number and the treated crop by a standardized crop code, along with the total application area and dosage.

Second, the FVM Marker dataset, published by Landbrugsstyrelsen (the Danish Agricultural Agency), provides annual georeferenced field boundaries for all Danish agricultural fields [Landbrugsstyrelsen, 2024]. Each field record includes the same CVR identifier and crop code classification used in the SJI system, along with the field geometry as a polygon.

The shared CVR identifier creates a deterministic linkage between company-level pesticide records and georeferenced field boundaries. Rather than distributing sales data probabilistically or extrapolating from survey samples, the algorithm presented here directly matches each pesticide application record to the specific fields operated by the reporting company, using crop code agreement and area consistency as matching criteria.

This paper makes four contributions:

1. **A deterministic area-matching algorithm** with four sequential strategies that disaggregates company-level pesticide application records to individual fields, producing proportional dose allocations with per-record confidence scores.

2. **Empirical validation across 14 years** (2010--2023) of Danish government open data, demonstrating that coverage rates exceed 90% from 2018 onward and reach 92.7% in 2020.

3. **Tolerance sensitivity analysis** across eight tolerance levels (0--10%), identifying area rounding in spray journal reporting as the dominant source of mismatch between reported application areas and registered field areas.

4. **An open-source, fully reproducible pipeline** operating entirely on publicly available government data, implemented as the data backbone of the landbruget.dk agricultural transparency platform.

The algorithm enables several downstream applications, including pesticide proximity scoring near residential and educational buildings, per- and polyfluoroalkyl substance (PFAS) exposure mapping, and regulatory compliance detection. These applications are briefly noted in the discussion but are not the focus of this paper.

The remainder of the paper is organized as follows. Section 2 describes the data sources. Section 3 formalizes the disaggregation problem and details the four-strategy algorithm, temporal alignment approach, and implementation. Section 4 presents empirical results including year-by-year coverage, strategy contributions, and tolerance sensitivity. Section 5 discusses limitations, generalizability, and comparison with existing methods. Section 6 concludes.

---

## 2. Data Sources

### 2.1 Spray journal data (SJI)

The Danish spray journal (*Sprøjtejournal*, SJI) system is maintained by Miljøstyrelsen (the Danish Environmental Protection Agency). Under the Danish Pesticide Action Plan, all professional users of pesticides managing more than 10 hectares of cultivated area must report their pesticide consumption annually [Miljøstyrelsen, 2024]. The reporting period follows the agricultural year from August 1 to July 31.

SJI data is publicly available through the GEUS Dataverse (DOI: 10.22008/FK2/IHVDXL) as part of the VP4 River Basin Management Plan dataset. Each record contains: the company registration number (CVR), a crop code matching the Fællesskema classification system, the total application area in hectares for that crop, the pesticide product name and registration number, and the dosage quantity with unit.

The dataset contains approximately 310,000 to 440,000 application records per year across the study period (2010--2023). A critical characteristic of the SJI data is that application areas are reported at the company-plus-crop granularity level rather than per individual field. A single SJI record may therefore represent the aggregate treatment of multiple fields growing the same crop under the same CVR. Furthermore, reported areas appear to be rounded, a finding confirmed empirically by the tolerance sensitivity analysis presented in Section 4.3.

### 2.2 Field boundary data (FVM Marker)

The FVM Marker dataset is published by Landbrugsstyrelsen (the Danish Agricultural Agency) via a Web Feature Service (WFS) at geodata.fvm.dk. Danish farmers have registered individual field boundaries and crop types annually since 2011 as part of their Fællesskema (Common Application) for agricultural subsidies under the EU Common Agricultural Policy.

Each record in the FVM Marker dataset includes: a unique field identifier, a deterministic UUID derived from field geometry, the CVR number linking the field to its operating company, a crop code from the standardized Fællesskema classification, the field area in hectares calculated from the registered polygon geometry, an organic farming indicator, the geometry as a polygon in EPSG:25832 (UTM zone 32N, the standard Danish projection), a block identifier, and a municipality code.

The dataset contains approximately 600,000 to 740,000 field records per year. Both the SJI and FVM Marker datasets use the same CVR identifier and Fællesskema crop code classification, which forms the basis of the deterministic matching approach. The use of a common administrative identifier --- rather than probabilistic spatial or statistical linkage --- distinguishes this method from all prior approaches reviewed in Section 1.2.

### 2.3 Pesticide product registry (BMD)

The Bekæmpelsesmiddeldatabasen (BMD) is Denmark's official pesticide product registry, maintained by Miljøstyrelsen and publicly accessible at bmd.mst.dk. The registry contains records for 10,518 products covering 893 unique active substances. Each product entry includes approval and expiration dates, active substance names and concentrations, GHS hazard classifications, and the Danish Pesticide Load indicator (*Pesticidbelastningsindikator*, PBI).

The PBI, introduced as a regulatory replacement for the Treatment Frequency Index, comprises three sub-indicators quantifying burden on human health, ecotoxicology, and environmental fate [Kudsk et al., 2018]. The BMD also flags products containing per- and polyfluoroalkyl substances (PFAS). These attributes enable downstream dose-rate validation --- comparing disaggregated per-hectare doses against authorized maximum application rates --- and environmental burden scoring after disaggregation. While the BMD is not directly used in the disaggregation algorithm itself, it provides essential context for validating the plausibility of disaggregated outputs.

### 2.4 Supplementary data sources

Two additional datasets support downstream analyses enabled by disaggregation. The BBR (*Bygnings- og Boligregistret*) building registry, maintained by Styrelsen for Dataforsyning og Infrastruktur (SDFI), provides georeferenced locations and usage categories for all Danish buildings, including residential and educational structures. This enables proximity analysis: once pesticide applications have been assigned to specific field geometries, the distance between treated fields and nearby buildings can be computed, typically using a 100 m threshold consistent with Danish regulatory guidance.

The GEUS VP4 groundwater monitoring dataset, also accessible via the GEUS Dataverse (DOI: 10.22008/FK2/IHVDXL), contains over 4.2 million pesticide analyses across 633 substances from Denmark's national borehole monitoring network (the Jupiter database) [Thorling et al., 2022]. This dataset enables correlation analysis between field-level pesticide application patterns produced by disaggregation and observed groundwater contamination.

All data sources used in this study are publicly available Danish government open data. Table 1 summarizes their characteristics.

**Table 1.** Summary of data sources used in this study.

| Source | Authority | Format | Temporal range | Spatial resolution | Key identifiers | Access |
|--------|-----------|--------|----------------|--------------------|-----------------|--------|
| SJI (Sprøjtejournal) | Miljøstyrelsen | Parquet (via GEUS Dataverse) | 2010--2023 | Company + crop | CVR, crop_code, area_ha, dosage | DOI: 10.22008/FK2/IHVDXL |
| FVM Marker | Landbrugsstyrelsen | GeoJSON (WFS) | 2011--2024 | Individual field | field_id, CVR, crop_code, area_ha, geometry | geodata.fvm.dk |
| BMD | Miljøstyrelsen | Structured database | Continuously updated | Product level | Registration number, active substances, PBI scores | bmd.mst.dk |
| BBR | SDFI | GeoJSON | Continuously updated | Individual building | Address, usage category, geometry | bbr.dk |
| GEUS VP4 | GEUS | R data / Parquet | 1981--2025 | Borehole | dgu_nr, substance, concentration, coordinates | DOI: 10.22008/FK2/IHVDXL |

---

## 3. Methods

### 3.1 Problem formalization

Let $R = \{r_1, r_2, \ldots, r_n\}$ denote the set of SJI pesticide application records, where each record $r_i = (\text{cvr}_i, \text{crop}_i, \text{area}_i^{\text{SJI}}, \text{dose}_i)$ contains the company identifier, crop code, reported application area in hectares, and total dosage. Let $F = \{f_1, f_2, \ldots, f_m\}$ denote the set of FVM field boundaries, where each field $f_j = (\text{cvr}_j, \text{crop}_j, \text{area}_j^{\text{FVM}}, \text{field\_id}_j, \text{geometry}_j)$ includes the same company and crop identifiers along with field-specific area and polygon geometry.

The disaggregation task is to assign each application record $r_i$ to one or more fields $f_j$ such that the following constraints are satisfied:

**Identity constraint.** The matched fields share the same CVR and crop code as the application record:

$$\text{cvr}_j = \text{cvr}_i \quad \wedge \quad \text{crop}_j = \text{crop}_i$$

**Area matching criterion.** The relative difference between the reported application area and the total area of matched fields falls within a configurable tolerance $\tau$:

$$\frac{|\text{area}_i^{\text{SJI}} - \sum_{j \in M_i} \text{area}_j^{\text{FVM}}|}{\text{area}_i^{\text{SJI}}} \leq \tau$$

where $M_i$ denotes the set of fields matched to record $r_i$, and $\tau = 0.02$ (2%) by default.

**Proportional allocation.** The dosage assigned to each matched field is proportional to its area share within the matched set:

$$\text{dose\_field}_j = \text{dose}_i \times \frac{\text{area}_j^{\text{FVM}}}{\sum_{k \in M_i} \text{area}_k^{\text{FVM}}}$$

This ensures that the total allocated dosage exactly equals the original reported dosage, preserving mass balance.

**Confidence scoring.** Each allocation receives a confidence score reflecting the quality of the area match:

$$\text{score} = \max\left(0, \; 1 - \frac{|\Delta\text{area}\%|}{\tau}\right)$$

where $\Delta\text{area}\%$ is the relative area difference. A perfect area match (zero difference) yields a confidence score of 1.0; a match at the tolerance boundary yields 0.0. This linear scoring function provides a simple, interpretable measure that enables downstream analyses to weight or filter allocations by match quality.

### 3.2 Four-strategy sequential algorithm

The algorithm processes unmatched records through four strategies in strict sequential order. Each strategy operates only on records not yet matched by preceding strategies, ensuring that no record is double-counted and that higher-confidence strategies take priority. Algorithm 1 provides pseudocode.

---

**Algorithm 1.** Pesticide disaggregation via sequential area matching.

```
Input:  R  -- set of SJI application records
        F  -- set of FVM field boundaries
        τ  -- area tolerance (default 0.02)
Output: D  -- set of disaggregated field-level records

pending <- R
D <- {}

// Strategy 1: Full area match (all fields)
for each r in pending:
    F_match <- {f in F : f.cvr = r.cvr AND f.crop = r.crop}
    A_total <- SUM(f.area for f in F_match)
    if A_total > 0 and |r.area - A_total| / r.area <= τ:
        for each f in F_match:
            d <- allocate(r, f, A_total)
            d.method <- "S1_FullAreaMatch"
            d.confidence <- max(0, 1 - |r.area - A_total| / r.area / τ)
            D <- D + {d}
        remove r from pending

// Strategy 2: Non-organic fallback
for each r in pending:
    F_match <- {f in F : f.cvr = r.cvr AND f.crop = r.crop AND NOT f.is_organic}
    A_total <- SUM(f.area for f in F_match)
    if A_total > 0 and |r.area - A_total| / r.area <= τ:
        for each f in F_match:
            d <- allocate(r, f, A_total)
            d.method <- "S2_NonOrganicFallback"
            d.confidence <- max(0, 1 - |r.area - A_total| / r.area / τ)
            D <- D + {d}
        remove r from pending

// Strategy 3: Partial field coverage
for each r in pending:
    F_match <- {f in F : f.cvr = r.cvr AND f.crop = r.crop}
    if |F_match| = 1 and r.area < F_match[0].area:
        d <- allocate(r, F_match[0], r.area)
        d.method <- "S3_PartialFieldCoverage"
        d.confidence <- 0.8
        d.is_partial <- true
        D <- D + {d}
        remove r from pending

// Strategy 4: Spatial clustering (optional)
for each r in pending:
    F_match <- {f in F : f.cvr = r.cvr AND f.crop = r.crop}
    clusters <- connected_components(F_match, buffer=10m)
    for each cluster C in clusters:
        A_cluster <- SUM(f.area for f in C)
        if |r.area - A_cluster| / r.area <= τ:
            for each f in C:
                d <- allocate(r, f, A_cluster)
                d.method <- "S4_SpatialCluster"
                D <- D + {d}
            remove r from pending
            break

function allocate(r, f, A_total):
    return {
        original_id: r.id,
        field_id: f.field_id,
        cvr: r.cvr,
        dose: r.dose * (f.area / A_total),
        area: r.area * (f.area / A_total)
    }
```

---

**Strategy 1 (S1): Full area match.** For each unmatched SJI record, the algorithm retrieves all FVM fields sharing the same CVR and crop code, computes their total area, and tests whether the relative area difference falls within the tolerance $\tau$. When a match is found, the dosage is distributed proportionally across all matching fields according to each field's area share. This strategy handles the vast majority of records. At the default 2% tolerance, S1 matches approximately 92% of records for recent years (2020 onward), as demonstrated empirically in Section 4.

The rationale for area matching is straightforward: if a company reports treating 45.2 hectares of winter wheat, and the same company's registered winter wheat fields sum to 45.0 hectares, the 0.4% discrepancy is consistent with rounding and the match is accepted. The tolerance parameter $\tau$ controls the trade-off between coverage (more records matched) and precision (lower risk of false matches).

**Strategy 2 (S2): Non-organic fallback.** Records unmatched by S1 are re-evaluated after excluding fields flagged as organic in the FVM dataset (is_organic = true). This addresses a specific structural pattern: companies that operate both organic and conventional fields for the same crop code. In such cases, the organic fields inflate the total registered area, causing S1 to fail the tolerance check because the pesticide application area corresponds only to the conventional subset. By considering only the non-organic fields, S2 recovers these additional matches.

At the default 2% tolerance, S2 contributes an additional 0 to 0.2 percentage points of coverage beyond S1. The contribution is larger at stricter tolerances --- for example, at 0% tolerance in 2015, S2 recovered 11,237 additional records that S1 could not match. The S2 contribution has declined over time, reaching zero additional records in 2023, suggesting either that mixed organic/conventional operations under a single CVR have become less common or that organic field registration in the FVM dataset has improved sufficiently for S1 to handle these cases directly.

**Strategy 3 (S3): Partial field coverage.** For remaining unmatched records where the CVR and crop code combination corresponds to exactly one registered field and the reported application area is strictly less than the field area, the entire dose is assigned to that single field. This addresses cases of partial-field treatment, where a farmer applies pesticide to a portion of a field rather than its entirety. A fixed confidence score of 0.8 is assigned (lower than the best area-matched records from S1 and S2), and the allocation is flagged with an IsPartialFieldCoverage indicator to distinguish it in downstream analyses. The single-field constraint prevents erroneous allocation when multiple candidate fields exist.

**Strategy 4 (S4): Spatial clustering.** Adjacent fields (within a 10 m buffer distance) belonging to the same CVR and crop code are grouped into spatial clusters using a connected components algorithm on the field adjacency graph. If a cluster's total area matches the application area within tolerance $\tau$, dosage is allocated proportionally across the cluster's constituent fields. This strategy targets situations where a company's field registrations have been subdivided differently from the area groupings used in spray journal reporting, but the spatially contiguous cluster corresponds to the reported treatment unit.

Strategy S4 is disabled in the production pipeline due to minimal additional coverage gain and substantially higher computational cost from the spatial operations required to construct the adjacency graph. It is retained in the algorithm specification for completeness and is available for activation in the open-source codebase.

### 3.3 Temporal alignment: the Y+1 pattern

A non-obvious but critical design choice is the year-plus-one temporal offset: pesticide application records from SJI year X are matched against FVM field boundaries from calendar year X+1. For example, SJI data for the 2021 agricultural year (covering applications from August 2021 to July 2022) is matched against FVM field registrations from 2022.

This pattern arises from the Danish agricultural administrative cycle. The Fællesskema (Common Application) for year X+1 is submitted in the spring of year X+1, at which point farmers declare the field boundaries and crop plans that reflect what was actually cultivated during the preceding agricultural year. The FVM field registrations for year X+1 therefore represent the most accurate available snapshot of field configurations during the pesticide application period of year X.

Matching without this temporal offset --- that is, using FVM boundaries from the same calendar year as the SJI data --- produces substantially lower coverage rates, as field boundaries, crop assignments, and even CVR-to-field associations shift from year to year due to land sales, lease changes, crop rotations, and field boundary adjustments.

### 3.4 Tolerance sensitivity analysis

To characterize the relationship between area tolerance and disaggregation coverage, and to provide empirical justification for the default tolerance value, we evaluated the algorithm at eight tolerance levels: 0%, 0.5%, 1.0%, 1.5%, 2.0%, 3.0%, 5.0%, and 10.0%. For each tolerance level and each available year (2010--2023), we recorded the number of records matched by S1, the additional records matched by S2, and the combined coverage rate.

This analysis serves two purposes. First, it identifies the tolerance value at which coverage gains plateau relative to the increasing risk of false matches from overly permissive tolerance. The default 2% tolerance was selected at the inflection point where the coverage-versus-tolerance curve begins to flatten. Second, the characteristic shape of the coverage curve --- specifically, the magnitude of the jump from 0% (exact match) to 0.5% tolerance --- reveals the dominant source of area mismatch. A large gain at very small tolerance values indicates systematic small-magnitude discrepancies consistent with rounding, as opposed to the larger discrepancies that would arise from fundamentally mismatched field configurations.

### 3.5 Implementation

The pipeline is implemented in Python 3.11 using DuckDB with its spatial extension as the primary data processing engine. DuckDB was selected for its ability to perform SQL-based analytical and geospatial operations in-process without requiring a separate database server, efficiently handling datasets of several hundred thousand to several million records in memory. All area matching, proportional allocation, and confidence scoring operations are expressed as SQL queries executed within DuckDB, enabling the database engine to optimize join strategies and memory management.

Data flows through a medallion architecture comprising three layers. The Bronze layer preserves raw data exactly as received from government sources, including original schemas, coordinate reference systems, and metadata. The Silver layer applies cleaning, validation, type coercion, deduplication, and schema standardization; all geospatial data is maintained in EPSG:25832 (UTM zone 32N) throughout processing. The Gold layer produces the analysis-ready disaggregated output. Transformation to EPSG:4326 (WGS84) occurs only at the final step when data is uploaded to the PostgreSQL/PostGIS database (Supabase) for frontend consumption.

Cloud storage on Cloudflare R2 provides persistence across pipeline runs. Each pesticide year is processed independently to manage memory consumption and enable targeted re-processing when upstream data is updated.

The validation script (`validate_disaggregation_robustness.py`) reproduces all empirical results reported in this paper. It loads production Silver-layer data from R2 storage and executes the matching logic at configurable tolerance levels, outputting structured JSON results suitable for automated verification. The script supports three analysis modes: tolerance sensitivity (sweeping across tolerance levels), unmatched record profiling (characterizing records that fail matching), and dose-rate validation against authorized maximum rates from the BMD registry. The complete pipeline source code is open-source and available at the landbruget.dk project repository.

---

## References

Balcaen, T., et al. (2025). High-resolution mapping of pesticide use in Europe. *Environmental Research Letters*.

Deguine, J.-P., et al. (2023). The NODU indicator for monitoring pesticide use in France. *Crop Protection*, 164, 106143.

Deguine, J.-P., et al. (2025). Spatially explicit residential pesticide exposure indicator for metropolitan France. *Science of the Total Environment*.

European Commission. (2001). FOCUS groundwater scenarios in the EU review of active substances. Report of the FOCUS Work Group.

European Commission. (2022). Regulation (EU) 2022/2379 on statistics on agricultural input and output. *Official Journal of the European Union*.

Eurostat. (2023). Agri-environmental indicators: Consumption of pesticides. European Statistical Office.

Gutsche, V., & Rossberg, D. (1997). SYNOPS 1.1: A model to assess and compare environmental risk potentials of active substances in plant protection products. *Agriculture, Ecosystems & Environment*, 64(2), 181--188.

JRC. (2019). Workshop report: Estimating pesticide use from heterogeneous data sources. European Commission Joint Research Centre, Ispra.

Kudsk, P., et al. (2018). Pesticide Load --- A new Danish pesticide risk indicator with multiple applications. *Land Use Policy*, 70, 384--393.

Landbrugsstyrelsen. (2024). FVM Marker — WFS service for agricultural field boundaries. Danish Agricultural Agency. Available at: geodata.fvm.dk.

Maggi, F., et al. (2019). PEST-CHEMGRIDS, global gridded maps of the top 20 crop-specific pesticide application rates from 2015 to 2025. *Scientific Data*, 6, 170.

Miljøstyrelsen. (2024). Sprøjtejournaler og pesticide statistik. Danish Environmental Protection Agency.

Piera, A., et al. (2023). A European regional-level pesticide emissions dataset. *Data in Brief*, 51, 109704.

Strassemeyer, J., et al. (2017). SYNOPS-GIS: A georeferenced environmental risk assessment tool for pesticide use. *Journal of Environmental Management*, 196, 612--624.

Thorling, L., et al. (2022). Grundvandsovervågning 2022: Status og udvikling 1989--2021. GEUS Report.

Zucchini, M., et al. (2022). Mapping pesticide use at the parcel level in Wallonia, Belgium. *Science of the Total Environment*, 843, 156914.

Zucchini, M., et al. (2024). Quantifying residential exposure to pesticide use in agriculture using buffer-based models. *Environmental Science & Technology*.
