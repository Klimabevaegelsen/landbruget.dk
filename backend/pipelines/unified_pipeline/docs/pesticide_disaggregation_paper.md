# Matching company-level pesticide reports to agricultural fields using administrative identifiers: A 14-year coverage analysis with Danish government data

**Martin Collignon** ^1, [additional authors TBD]

^1 Landbruget.dk, Denmark

---

## Abstract

Pesticide application data in the European Union is typically reported at company or regional level, limiting field-level environmental risk assessment. We present a method that matches company-level spray journal records to individual georeferenced agricultural fields by exploiting a shared administrative identifier (the Danish CVR company number) and crop code, with proportional dose allocation within a configurable area tolerance (default ±2%). Validated across 14 years of Danish government data (2010--2023), the method achieves 92.7% matching coverage in 2020, sustaining ≥90% from 2018 onward. Matching coverage measures the fraction of records successfully linked to georeferenced fields --- not allocation accuracy: only 19.6% of matched records receive unambiguous single-field allocations, while 80.4% are distributed proportionally across multiple fields. Tolerance sensitivity analysis reveals that relaxing tolerance from 0% to 0.5% produces the largest coverage gain (37--48% to 72--87%), identifying area rounding in spray journal reporting as the dominant mismatch source. The method requires no machine learning and produces auditable allocations, but the proportional dose distribution step relies on an unvalidated uniform-intensity assumption. All input data are Danish government administrative records; spray journal data was obtained through freedom of information requests and all other sources are publicly available.

**Keywords:** pesticide disaggregation, spatial resolution, area matching, Danish agriculture, field-level analysis, spray journal, administrative data linkage

---

## 1. Introduction

### 1.1 The spatial resolution gap

Spatially resolved pesticide use data is a prerequisite for environmental and health risk assessment. Across the EU, such data has historically been available only as national sales totals under Regulation (EC) No 1185/2009, which mandates farm-level use surveys only every five years at national or regional aggregation [Eurostat, 2023]. The Statistics on Agricultural Input and Output (SAIO) Regulation (EU) 2022/2379 mandates electronic recording of pesticide use from 1 January 2026, with first data transmission to Eurostat by end of 2028. However, even under SAIO, field-level spatial data is not required; Member States will collect data at the farm or holding level [European Commission, 2022]. The European Commission's Farm to Fork Strategy target of 50% reduction in pesticide use by 2030 [European Commission, 2020] further underscores the need for spatially resolved monitoring data to track progress at meaningful scales.

This gap has been widely documented. Piera et al. [2023] noted that absent georeferenced use data severely constrains environmental fate modelling. Balcaen et al. [2025] observed that even advanced EU-wide mapping efforts rely on statistical downscaling rather than direct observation. The JRC has consistently identified spatial disaggregation as a key methodological challenge [JRC, 2019].

### 1.2 Existing approaches

The literature on spatially resolving pesticide data spans three broad categories, each sharing a fundamental limitation.

**Sales-based and statistical downscaling.** France's NODU indicator distributes postal-code-level sales to municipalities using crop area proportions [Deguine et al., 2023]. Zucchini et al. [2022] mapped 388 active substances across 9.5 million parcels in Wallonia by combining postal-code sales with crop-specific application rates from a 4% farm survey. At broader scales, Maggi et al. [2019] produced global gridded maps (PEST-CHEMGRIDS) at ~10 km resolution, and Balcaen et al. [2025] achieved 250 m EU-wide maps by fusing PEST-CHEMGRIDS with Sentinel-derived crop type maps. These approaches lack farm identity: the same crop area in different companies receives identical estimated doses regardless of actual practices.

**Exposure mapping and risk models.** Deguine et al. [2025] combined France's RPG parcel registry with population density for residential exposure mapping. Zucchini et al. [2024] quantified residential pesticide exposure in Wallonia using buffer-based models. Process-based models such as SYNOPS [Gutsche & Rossberg, 1997; Strassemeyer et al., 2017] assess risk at the field level but require the very field-level use data that is typically unavailable. California's Pesticide Use Reporting (PUR) program, operational since 1990, requires reporting at ~2.6 km² sections [California DPR, 2024], but specific fields within sections are not identified, requiring statistical downscaling [Larsen & Noack, 2017; Nuckols et al., 2007].

**Farm-level monitoring.** Möhring et al. [2020] demonstrated farm-level pesticide use quantification in Switzerland using central registry linkage, though without the field-level spatial disaggregation component presented here. EFSA has published technical reports on harmonizing pesticide use monitoring across Member States, consistently identifying the lack of georeferenced field-level data as a barrier [EFSA, 2023].

None of these approaches exploit direct farm-identity linkage between application records and georeferenced field boundaries. The fundamental bottleneck is the absence of a shared identifier connecting "who applied what" to "where their fields are."

### 1.3 Contribution

Denmark possesses two government datasets that, combined, provide the direct linkage that existing methods lack. The Sprøjtejournal (SJI) system requires all professional users managing more than 10 hectares to report pesticide applications annually, identifying each company by CVR number and crop code [Miljøstyrelsen, 2024]. The FVM Marker dataset provides georeferenced field boundaries using the same CVR and crop code classification [Landbrugsstyrelsen, 2024].

This paper makes three contributions: (1) a data integration methodology that matches company-level pesticide records to individual fields using shared administrative identifiers, with proportional dose allocation and per-record quality scores; (2) empirical characterization across 14 years (2010--2023), demonstrating matching coverage exceeding 90% from 2018 onward; and (3) tolerance sensitivity analysis identifying area rounding as the dominant mismatch source. The pipeline is open-source, underlying the landbruget.dk transparency platform.

---

## 2. Data Sources

### 2.1 Spray journal data (SJI)

The SJI system is maintained by Miljøstyrelsen under the Danish Pesticide Action Plan. SJI data is not open data; access was obtained through freedom of information requests (*aktindsigt*) under the Danish Environmental Information Act. Each record contains: CVR number, Fællesskema (Common Application) crop code, total application area (hectares) for that crop, pesticide product name and registration number, and dosage quantity with unit. The reporting period follows the agricultural year (August 1 to July 31). The dataset contains 310,000--440,000 records per year (2010--2023). Areas are reported at company-plus-crop granularity, not per individual field, and appear to be rounded (confirmed by tolerance sensitivity analysis in Section 4.3). SJI-reported consumption is consistently lower than independently measured sales totals [Miljøstyrelsen, 2023], particularly for glyphosate; disaggregated outputs therefore represent a lower bound on actual application.

### 2.2 Field boundary data (FVM Marker)

Published by Landbrugsstyrelsen via WFS at geodata.fvm.dk, FVM Marker contains 600,000--740,000 georeferenced field records per year. Each record includes: field identifier, CVR number, Fællesskema crop code, field area (hectares), organic farming indicator, and polygon geometry in EPSG:25832. The crop code system has been stable across the study period, with no wholesale reclassification.

### 2.3 Supporting datasets

The BMD pesticide product registry (bmd.mst.dk) provides product attributes including the Pesticide Load Indicator [Kudsk et al., 2018] and PFAS flags, enabling post-hoc dose-rate plausibility checks via the shared registration number. The BBR building registry and GEUS VP4 groundwater monitoring dataset support downstream proximity and contamination analyses.

**Table 1.** Data sources.

| Source | Authority | Temporal range | Spatial resolution | Access |
|--------|-----------|----------------|--------------------|--------|
| SJI | Miljøstyrelsen | 2010--2023 | Company + crop | FOI request |
| FVM Marker | Landbrugsstyrelsen | 2011--2024 | Individual field | geodata.fvm.dk (open) |
| BMD | Miljøstyrelsen | Continuously updated | Product level | bmd.mst.dk (open) |
| BBR | SDFI | Continuously updated | Individual building | bbr.dk (open) |
| GEUS VP4 | GEUS | 1981--2025 | Borehole | DOI: 10.22008/FK2/IHVDXL |

---

## 3. Methods

### 3.1 Problem formalization

Let $R = \{r_1, \ldots, r_n\}$ denote SJI records, where each $r_i = (\text{cvr}_i, \text{crop}_i, \text{area}_i^{\text{SJI}}, \text{dose}_i)$. Let $F = \{f_1, \ldots, f_m\}$ denote FVM fields, where each $f_j = (\text{cvr}_j, \text{crop}_j, \text{area}_j^{\text{FVM}}, \text{geometry}_j)$. The disaggregation task assigns each $r_i$ to fields $M_i \subseteq F$ satisfying:

**Identity constraint:** $\text{cvr}_j = \text{cvr}_i \wedge \text{crop}_j = \text{crop}_i$ for all $j \in M_i$.

**Area matching:** $\frac{|\text{area}_i^{\text{SJI}} - \sum_{j \in M_i} \text{area}_j^{\text{FVM}}|}{\text{area}_i^{\text{SJI}}} \leq \tau$, with default $\tau = 0.02$ (2%).

**Proportional allocation:** $\text{dose\_field}_j = \text{dose}_i \times \frac{\text{area}_j^{\text{FVM}}}{\sum_{k \in M_i} \text{area}_k^{\text{FVM}}}$, preserving mass balance.

**Area-match quality score:** $\text{score} = \max(0, 1 - |\Delta\text{area}\%| / \tau)$. This linear function measures area-match precision only; it does not reflect allocation reliability, which depends on the number of fields in the matched set. In 2021, the median score across 1,671,454 disaggregated records is 0.98 (mean 0.93), with 27.1% receiving the maximum score of 1.0.

### 3.2 Matching algorithm

The algorithm processes unmatched records through three strategies in strict sequential order (Figure 3). Each strategy operates only on records not yet matched by preceding strategies.

**Algorithm 1.** Pesticide disaggregation via sequential area matching.

```
Input:  R (SJI records), F (FVM fields), τ (tolerance, default 0.02)
Output: D (disaggregated field-level records)

pending ← R;  D ← ∅

// Strategy 1: Full area match (all fields)
for each r in pending:
    F_match ← {f ∈ F : f.cvr = r.cvr AND f.crop = r.crop}
    A_total ← Σ f.area for f in F_match
    if A_total > 0 and |r.area − A_total| / r.area ≤ τ:
        allocate r proportionally across F_match
        remove r from pending

// Strategy 2: Non-organic fallback
for each r in pending:
    F_match ← {f ∈ F : f.cvr = r.cvr AND f.crop = r.crop AND NOT f.organic}
    (same area-match test and allocation as S1)

// Strategy 3: Partial field coverage
for each r in pending:
    F_match ← {f ∈ F : f.cvr = r.cvr AND f.crop = r.crop}
    if |F_match| = 1 and r.area < F_match[0].area:
        assign entire dose to the single field
        flag as partial coverage (quality score = 0.8)
```

**Strategy 1 (S1)** retrieves all FVM fields sharing the same CVR and crop code, and tests whether their total area matches the reported application area within tolerance. When matched, dosage is distributed proportionally by field area. S1 handles ~92% of records for recent years.

**Strategy 2 (S2)** re-evaluates unmatched records after excluding organic fields, addressing companies operating both organic and conventional fields for the same crop. S2 contributes 0--0.2 additional percentage points at 2% tolerance, declining to zero by 2023.

**Strategy 3 (S3)** handles cases where a single registered field exists for a CVR+crop combination and the reported area is less than the field area, representing partial-field treatment.

### 3.3 Temporal alignment

SJI year X is matched against FVM field boundaries from year X+1. The Fællesskema submitted in spring of year X+1 declares field boundaries and crops for the *preceding* agricultural year. Under Y+0 alignment (matching SJI year X against FVM year X), the FVM dataset instead describes the growing season *before* the one in which the pesticides were applied. Because Danish farms practice crop rotation, Y+0 alignment produces systematic crop code mismatches: a field registered as winter wheat in FVM 2021 (growing season 2020) may have been sown with spring barley in 2021 (the year captured by SJI 2021).

Table 2a quantifies this effect. With Y+0 alignment, coverage at 2% tolerance drops from 82--93% to 6--9% across all tested years --- an 82--84 percentage point reduction that is consistent and stable. The near-total failure of Y+0 matching confirms that the temporal offset is not a calibration choice but a structural requirement of the Danish administrative reporting cycle.

**Table 2a.** Y+1 versus Y+0 matching coverage (S1) at 2% area tolerance. Y+0 matches SJI year X against FVM year X (wrong growing season); Y+1 matches against FVM year X+1 (correct growing season).

| SJI Year | Y+1 coverage | Y+0 coverage | Difference |
|----------|-------------|-------------|------------|
| 2018 | 90.4% | 8.2% | +82.2 pp |
| 2019 | 91.5% | 9.4% | +82.1 pp |
| 2020 | 92.6% | 9.3% | +83.3 pp |
| 2021 | 91.9% | 9.3% | +82.6 pp |
| 2022 | 91.2% | 6.8% | +84.4 pp |
| 2023 | 90.0% | 6.3% | +83.7 pp |

*Note: SJI 2015 is excluded because FVM 2015 (Y+0 target) contains NULL CVR values (the 2014 anomaly). The 6--9% Y+0 coverage represents farmers who happen to grow the same crop on the same total area in consecutive years. The Y+1 coverage values differ slightly from Table 2 because Table 2 reports S1+S2 combined coverage while Table 2a reports S1 only.*

### 3.4 Implementation

The pipeline is implemented in Python using DuckDB with spatial extensions for in-process SQL-based data processing. Data flows through bronze (raw), silver (cleaned), and gold (analysis-ready) layers. The complete source code and a validation script reproducing all reported results are open-source.

---

## 4. Results

### 4.1 Longitudinal coverage (2010--2023)

Table 2 and Figure 2 present coverage across 14 years at the default 2% tolerance.

**Table 2.** Disaggregation coverage by year at 2% area tolerance. Coverage is the fraction of SJI records matched to at least one georeferenced field.

| Year | SJI Records | S1 matched | S2 additional | Coverage |
|------|------------|------------|---------------|----------|
| 2010 | 390,956 | 217,819 | 0 | 55.7% |
| 2011 | 407,352 | 256,043 | 994 | 63.1% |
| 2012 | 404,289 | 252,161 | 909 | 62.6% |
| 2013 | 422,795 | 280,606 | 800 | 66.6% |
| 2014 | 440,059 | 0 | 0 | 0.0% |
| 2015 | 423,483 | 345,674 | 901 | 81.8% |
| 2016 | 414,297 | 360,332 | 875 | 87.2% |
| 2017 | 338,842 | 293,792 | 564 | 86.9% |
| 2018 | 375,588 | 339,358 | 606 | 90.5% |
| 2019 | 347,564 | 317,937 | 506 | 91.6% |
| 2020 | 358,128 | 331,663 | 479 | 92.7% |
| 2021 | 342,302 | 314,740 | 375 | 92.1% |
| 2022 | 310,997 | 283,518 | 138 | 91.2% |
| 2023 | 313,317 | 281,987 | 0 | 90.0% |

![Figure 2. Longitudinal disaggregation coverage (2010--2023) at 2% area tolerance. Three phases are annotated: sparse FVM registration (2010--2013), improving coverage (2015--2019), and stable high coverage (2020--2023). The 2014 anomaly (0% due to NULL CVR in FVM 2015) is marked.](./figures/figure_2_longitudinal_coverage.png)

Three phases emerge. **Phase 1 (2010--2013):** 56--67% coverage, reflecting early-stage adoption of electronic field registration. **Phase 2 (2015--2019):** coverage rises from 82% to 92%, corresponding to increasingly stringent CAP requirements for digital field registration. **Phase 3 (2020--2023):** stable plateau at 90--93%, representing the practical ceiling of deterministic area matching. The improvement reflects FVM data completeness, not algorithmic refinement --- the same algorithm was applied uniformly across all years. Across the full period (excluding 2014), approximately 3.88 million of 4.85 million SJI records were matched.

### 4.2 The 2014 anomaly

Year 2014 produced 0% coverage. Under the Y+1 alignment, SJI 2014 matches against FVM 2015, which contains 741,882 records with plausible field counts but NULL CVR values for all records. The crop code scheme is compatible; the failure is entirely attributable to missing company identifiers in the upstream dataset. We investigated whether cross-year journal number matching could recover CVR values: of 17,259 shared numeric identifiers between FVM 2016 and FVM 2017, zero had matching CVR numbers, confirming that journal numbers are year-specific application identifiers unsuitable for cross-year CVR recovery.

### 4.3 Tolerance sensitivity

Figure 1 and Table 3 present coverage across seven tolerance levels for years with consistent FVM quality (2015--2023).

**Table 3.** Combined (S1+S2) coverage (%) at seven area tolerance levels.

| Tolerance | 2015 | 2018 | 2020 | 2021 | 2023 |
|-----------|------|------|------|------|------|
| 0.0% | 43.7 | 47.7 | 45.2 | 42.0 | 37.8 |
| 0.5% | 72.0 | 84.6 | 86.7 | 85.5 | 78.2 |
| 1.0% | 77.5 | 88.3 | 90.8 | 89.9 | 85.5 |
| 2.0% | 81.8 | 90.5 | 92.7 | 92.1 | 90.0 |
| 3.0% | 83.7 | 91.5 | 93.5 | 92.9 | 91.8 |
| 5.0% | 85.5 | 92.5 | 94.4 | 93.8 | 93.3 |
| 10.0% | 87.4 | 93.8 | 95.4 | 94.8 | 94.8 |

![Figure 1. Tolerance sensitivity: matching coverage versus area tolerance for selected years, with cross-crop-code ambiguity rate (right axis, 2021). The sharp elbow between 0% and 0.5% identifies area rounding as the dominant mismatch source. The 2% default balances coverage against rising ambiguity.](./figures/figure_1_tolerance_sensitivity.png)

**Area rounding is the dominant mismatch source.** The 0% to 0.5% jump produces the largest coverage gain in every year --- from 28 pp (2015) to 42 pp (2020) --- consistent with systematic rounding of reported areas to the nearest 0.1 or 0.5 hectare.

**Diminishing returns above 2%.** Moving from 2% to 10% adds only 3--5 pp of coverage while the cross-crop-code ambiguity rate (records matching multiple crop groups under the same CVR) rises from 6.5% at 2% to 27.6% at 10% (2021). The 2% default represents the inflection point where coverage gains flatten and ambiguity costs accelerate.

### 4.4 Allocation plausibility

The coverage figures above measure matching success, not allocation accuracy. No independent ground-truth dataset of field-level pesticide applications exists in Denmark. The distinction between coverage and accuracy is critical.

**Field-count distribution.** For 2021 (314,740 S1-matched records at 2%): 19.6% are single-field matches (unambiguous allocation), 31.9% match 2--3 fields, 17.9% match 4--5 fields, 18.7% match 6--10 fields, and 11.9% match 11+ fields. The median allocation set contains three fields. Only the 19.6% single-field matches receive allocations free of the uniform-intensity assumption; the remaining 80.4% are distributed proportionally by area.

**Cross-crop ambiguity.** At 2% tolerance, 6.5% of matched records (20,647 of 315,114) could match multiple crop groups, rising to 27.6% at 10%.

**Dose-rate plausibility.** Of 1,671,295 checkable disaggregated records (2021), 289 (0.017%) exceeded 10 times the product-specific median dose rate (L/ha). The distribution has a mean of 0.42 L/ha and median of 0.22 L/ha. This low outlier rate suggests disaggregation does not systematically produce implausible allocations, though it cannot detect errors within the normal dose range.

**Unmatched characterization.** Of 27,188 unmatched records (2021): 67% have a matching CVR+crop but area deviation exceeding 2%; 24% have a CVR entirely absent from FVM; 8% have CVR present but no matching crop code.

**Summary.** True allocation accuracy is bounded above by matching coverage (92.1%) and below by the single-field unambiguous fraction (19.6%). Narrowing this wide bound through validation against independent data sources remains important future work.

---

## 5. Discussion

### 5.1 Coverage reflects data infrastructure, not algorithmic improvement

The rising coverage from 56% (2010) to a plateau above 90% (2018--2023) reflects the progressive completeness of Denmark's FVM field registration, not algorithmic refinement. Phase 1 (2010--2013) corresponds to early electronic adoption; Phase 2 (2015--2019) to CAP-mandated digital registration; Phase 3 (2020--2023) to steady-state completeness. The remaining ~8% residual represents structurally unmatchable cases: farms below the 10-hectare reporting threshold, company restructurings, or area discrepancies beyond tolerance.

### 5.2 Deterministic matching versus statistical downscaling

The method's primary advantage over statistical approaches [Balcaen et al., 2025; Deguine et al., 2023; Maggi et al., 2019] is auditability: every allocation traces to a specific CVR-crop-area match with no hidden weights or learned parameters. The single tolerance parameter is transparently characterized (Section 4.3). These properties suit regulatory applications requiring verifiability.

An important qualification: the proportional allocation step is itself a statistical assumption --- it distributes doses uniformly by area, functionally similar to area-proportional downscaling applied at farm rather than regional level. The genuine distinction is that the matching step narrows the geographic scope from a postal code or region to a single farm's fields, preserving the identity linkage. For 19.6% of records (single-field matches), the allocation is deterministic. For the remaining 80.4%, it is an approximation whose accuracy depends on unobserved within-farm heterogeneity of application intensity. The coefficient of variation of within-farm dose intensity across fields of the same crop is unknown and likely substantial; agronomic factors including pest pressure, soil type, and microclimate all influence field-level application decisions. Future work should quantify this uncertainty through simulation with synthetic dose heterogeneity at plausible within-farm variation levels.

### 5.3 Limitations

*Uniform-intensity assumption.* Proportional allocation averages over within-farm heterogeneity. Per-field doses are correct in aggregate (mass balance is preserved) but potentially inaccurate for individual fields, particularly in large operations with many fields.

*Temporal boundary stability.* The Y+1 pattern assumes field boundaries registered in year X+1 correspond to fields cultivated during year X. Boundary changes from splits, mergers, or sales between growing season and registration introduce mismatches of unknown magnitude.

*Reporting threshold.* Farms under 10 hectares are exempt from SJI reporting. These represent 25--30% of Danish farms but only 2--4% of cultivated area [Danmarks Statistik, 2023]. Their exclusion is disproportionately relevant in peri-urban areas where proximity to residential buildings is greatest.

*Systematic under-reporting.* SJI consumption is consistently below sales totals, particularly for glyphosate inter-crop applications [Miljøstyrelsen, 2023]. Disaggregated outputs represent a lower bound.

*CVR consistency.* Company restructuring, mergers, or registration errors can break the CVR linkage that is foundational to the method.

*2014 data gap.* The NULL CVR column in FVM 2015 leaves one year unrecoverable without re-processing upstream data.

### 5.4 Transferability and future applications

The approach is applicable to any jurisdiction with farm-identity-linked pesticide reporting, georeferenced field boundaries under the same identifier, and a shared crop classification. The Netherlands (Basisregistratie Gewaspercelen), Belgium [Zucchini et al., 2022], and Switzerland [Möhring et al., 2020] partially meet these prerequisites. Most significantly, SAIO (EU 2022/2379) will create the data infrastructure across all EU member states, with electronic recording from 2026. The algorithm's simplicity --- a single parameter, no training data --- makes adaptation straightforward.

The disaggregated dataset enables downstream applications including proximity scoring near residential buildings, PFAS exposure mapping, and regulatory compliance analysis. These are implemented as preliminary capabilities within the landbruget.dk platform but require independent validation before policy-relevant conclusions can be drawn.

### 5.5 Ethics and data protection

The method links identifiable companies (CVR) to specific pesticide applications on georeferenced fields. Under Danish and EU law, CVR numbers identify registered businesses, not natural persons. GDPR Recital 14 explicitly excludes data concerning legal persons — including company name, form, and contact details — from the regulation's scope. For sole proprietorships (*enkeltmandsvirksomheder*), where the CVR is linked to a natural person, the data nonetheless pertains to professional agricultural activity rather than private life, and the CJEU *Schecke* proportionality framework (C-92/09) supports publication where environmental transparency objectives are at stake.

Critically, Denmark implements the Aarhus Convention through the *miljøoplysningsloven* (Environmental Information Act), which establishes an affirmative right of public access to environmental information with narrowly construed exceptions. Pesticide application data falls squarely within the Convention's definition of environmental information (emissions and discharges of substances into the environment). The CVR register itself is public by statute under the CVR-loven (LBK nr. 1052/2019). The SJI data was obtained through FOI requests (*aktindsigt*), and the landbruget.dk platform publishes CVR-level application records in its public-facing interfaces, consistent with Denmark's strong tradition of environmental data transparency. California's PUR system, by comparison, excludes grower identity from public releases — a design choice reflecting different legal traditions rather than a universal standard. Any replication of this approach in other jurisdictions must consider local data protection frameworks, but the Danish legal basis for publication is robust.

---

## 6. Conclusion

This paper presented a method for matching company-level pesticide application records to individual agricultural fields using shared administrative identifiers (CVR) and crop codes, with proportional dose allocation within a 2% area tolerance. The matching step is deterministic and auditable; the proportional allocation step relies on an unvalidated uniform-intensity assumption.

Empirical characterization across 14 years of Danish data (2010--2023) demonstrated matching coverage exceeding 90% from 2018 onward, peaking at 92.7% in 2020. Coverage measures matching success, not allocation accuracy: 19.6% of matched records receive unambiguous single-field allocations, while 80.4% are distributed across multiple fields. Tolerance sensitivity analysis confirmed that area rounding in spray journal reporting is the dominant matching failure mode: the 0% to 0.5% tolerance jump produces the largest coverage gain in every year.

The method's principal limitation is the approximately 8--10% residual of records that cannot be matched within tolerance, and the unquantified uncertainty in proportional allocation for multi-field matches. Future work should quantify allocation error through simulation with synthetic within-farm dose heterogeneity, investigate temporal stability of field boundaries across consecutive years with formal Y+0/Y+1 comparison, and extend the approach to other EU member states as SAIO-mandated electronic reporting becomes operational.

---

## Data Availability Statement

Input data are Danish government administrative records. Spray journal data (SJI) was obtained through freedom of information requests (*aktindsigt*) to Miljøstyrelsen and is not publicly available. Field boundary data (FVM Marker) is publicly available via geodata.fvm.dk. The BMD pesticide registry is at bmd.mst.dk. The GEUS VP4 dataset is accessible via DOI: 10.22008/FK2/IHVDXL. The pipeline and validation script are open-source at [repository URL].

---

## References

Balcaen, T., Pinsard, C., Koehl, A., Pernin, C., & Hedde, M. (2025). Pesticides application rate maps in the European Union at a 250 m spatial resolution. *Scientific Data*, 12, Article 234. https://doi.org/10.1038/s41597-025-04502-x

California Department of Pesticide Regulation. (2024). *Pesticide Use Reporting*. https://www.cdpr.ca.gov/pesticide-use-in-california/pesticide-use-reporting/

Danmarks Statistik. (2023). *Statistikbanken: Bedrifter efter areal*. https://www.statistikbanken.dk/

Deguine, O., Thiour-Mauprivez, C., Kesse-Guyot, E., Allès, B., Cordeau, S., & Reboud, X. (2023). Modelling the spatialisation of pesticide sales to monitor environmental policies in France. *Journal of Cleaner Production*, 414, Article 137543. https://doi.org/10.1016/j.jclepro.2023.137543

Deguine, O., Reboud, X., Kesse-Guyot, E., Allès, B., & Cordeau, S. (2025). From parcels to people: Development of a spatially explicit risk indicator for monitoring residential pesticide exposure. *Scientific Reports*, 15, Article 5678.

EFSA. (2023). Technical report on harmonized pesticide use monitoring across EU Member States. *EFSA Supporting Publications*. https://www.efsa.europa.eu/

European Commission. (2001). *FOCUS Surface Water Scenarios in the EU Evaluation Process* (SANCO/4802/2001-rev.2).

European Commission. (2020). *A Farm to Fork Strategy for a fair, healthy and environmentally-friendly food system*. COM(2020) 381 final.

European Parliament & Council. (2009). Regulation (EC) No 1185/2009 concerning statistics on pesticides. *Official Journal of the European Union*, L 324, 1--22.

European Parliament & Council. (2022). Regulation (EU) 2022/2379 on statistics on agricultural input and output. *Official Journal of the European Union*, L 315, 1--72.

Eurostat. (2023). Agri-environmental indicator --- consumption of pesticides. *Statistics Explained*. https://ec.europa.eu/eurostat/statistics-explained/

Gutsche, V., & Rossberg, D. (1997). SYNOPS 1.1: A model to assess the environmental risk potential of active ingredients. *Agriculture, Ecosystems & Environment*, 64(2), 175--192. https://doi.org/10.1016/S0167-8809(97)00037-4

Joint Research Centre. (2019). *Workshop report: Estimating pesticide use from heterogeneous data sources*. European Commission JRC, Ispra.

Kudsk, P., Jørgensen, L. N., & Ørum, J. E. (2018). Pesticide Load --- A new Danish pesticide risk indicator. *Land Use Policy*, 70, 384--393. https://doi.org/10.1016/j.landusepol.2017.11.010

Landbrugsstyrelsen. (2024). *FVM Marker --- WFS service for agricultural field boundaries*. https://geodata.fvm.dk

Larsen, A. E., & Noack, F. (2017). Identifying the landscape drivers of agricultural insecticide use leveraging evidence from 100,000 fields. *Proceedings of the National Academy of Sciences*, 114(21), 5473--5478. https://doi.org/10.1073/pnas.1620674114

Maggi, F., Tang, F. H. M., la Cecilia, D., & McBratney, A. (2019). PEST-CHEMGRIDS, global gridded maps of the top 20 crop-specific pesticide application rates. *Scientific Data*, 6, Article 170. https://doi.org/10.1038/s41597-019-0169-4

Miljøstyrelsen. (2023). *Bekæmpelsesmiddelstatistik 2022*. https://mst.dk/erhverv/sikker-kemi/pesticider/statistik/

Miljøstyrelsen. (2024). Indberetning og føring af sprøjtejournal (SJI). https://mst.dk/erhverv/sikker-kemi/pesticider/

Möhring, N., Gaba, S., & Finger, R. (2020). Quantity based indicators fail to identify extreme pesticide risks. *Science of the Total Environment*, 741, Article 140249. https://doi.org/10.1016/j.scitotenv.2020.140249

Nuckols, J. R., Gunier, R. B., Riggs, P., Miller, R., Reynolds, P., & Ward, M. H. (2007). Linkage of the California Pesticide Use Reporting Database with spatial land use data. *Environmental Health Perspectives*, 115(5), 684--689. https://doi.org/10.1289/ehp.9518

Piera, A., Cerofolini, M., Quaglia, G., Fumagalli, D., & Lugato, E. (2023). Emissions of pesticides in the European Union: A new regional-level dataset. *Scientific Data*, 10, Article 862. https://doi.org/10.1038/s41597-023-02762-7

Strassemeyer, J., Gutsche, V., & Ahrends, H. (2017). SYNOPS-WEB, an online tool for environmental risk assessment. *Crop Protection*, 97, 28--44. https://doi.org/10.1016/j.cropro.2016.11.036

Thorling, L., et al. (2022). National assessment of long-term groundwater response to pesticide regulation. *Environmental Science & Technology*, 56(17), 12445--12455. https://doi.org/10.1021/acs.est.2c03465

Zucchini, E., et al. (2022). Mapping agricultural use of pesticides to enable research and environmental health actions in Belgium. *Environmental Pollution*, 315, Article 120322. https://doi.org/10.1016/j.envpol.2022.120322

Zucchini, E., et al. (2024). Quantifying residents' exposure to agricultural pesticides using new geospatial approaches. *Heliyon*, 10(3), Article e25234. https://doi.org/10.1016/j.heliyon.2024.e25234

---

## AI Disclosure Statement

This paper was drafted with assistance from Claude (Anthropic, Claude Opus 4.6). All empirical analyses, algorithm design, and validation were performed by the authors. The AI assistant was used for literature search, prose drafting, and manuscript preparation.
