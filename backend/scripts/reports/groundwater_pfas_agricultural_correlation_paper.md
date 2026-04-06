# Monitoring Density, Not Agriculture, Primarily Predicts PFAS Groundwater Detection Across 1,477 Danish Catchments — With Residual Spatial Associations for PFOA and Composite Indices

**Martin Collignon¹**

¹ Landbruget.dk, Copenhagen, Denmark

**Corresponding author:** Martin Collignon (martin@landbruget.dk)

---

## Abstract

Denmark detects per- and polyfluoroalkyl substances (PFAS) in groundwater nationally, yet contributions from agriculture remain unquantified at the catchment scale. We correlated parcel-level pesticide application records (2015–2017) with PFAS groundwater monitoring data (441,589 analyses, 26 substances, 2018–2025) from GEUS across 1,477 catchment areas. A four-tier framework tested TFA versus fluorinated pesticide intensity, traditional PFAS versus agricultural intensity, exploratory screening, and negative controls. TFA was detected in 100% of monitored catchments, precluding spatial analysis but demonstrating ubiquitous contamination. Bivariate correlations were widespread: 7/8 Tier 2 tests were FDR-significant, headed by PFOA versus fluorinated intensity (r = 0.229, bootstrap 95% CI [0.176, 0.282]). However, monitoring well density dominated all multivariate models (p < 0.001), and 79% of bivariate associations lost significance after covariate adjustment. Only 4/19 survived: SUM PFAS-22 (p = 0.006), Σ-PFOA (p = 0.036), Σ-PFHxS (p = 0.023), and Σ-PFOA versus total intensity (p = 0.026). Correlations appeared exclusively in well-monitored catchments (>4 wells), replicating a companion pesticide study. Moran's I analysis confirmed significant spatial autocorrelation in both intensity and detection surfaces, suggesting p-value inflation in standard tests. Sensitivity analyses showed TFA spatial gradients emerging at thresholds ≥1.0 µg/L, and Firth penalized regression confirmed low-EPV associations. These findings indicate that PFAS detection is primarily associated with monitoring density, with modest residual agricultural signal (r = 0.15–0.23) detectable only where monitoring is sufficiently dense. Biosolids application, not direct pesticide contamination, may mediate the observed associations.

**Keywords:** PFAS, groundwater contamination, monitoring density, fluorinated pesticides, trifluoroacetic acid, spatial autocorrelation, Danish agriculture

---

## 1. Introduction

### 1.1 Background

Denmark derives approximately 99% of its drinking water from groundwater (GEUS, 2024), making aquifer contamination a matter of direct public health concern. The Geological Survey of Denmark and Greenland (GEUS) operates one of Europe's most comprehensive groundwater monitoring programmes, analysing samples for both legacy pesticide compounds and, since 2018, an expanding panel of per- and polyfluoroalkyl substances (PFAS). The EU recast Drinking Water Directive (2020/2184) established parametric values of 0.10 µg/L for individual PFAS and 0.50 µg/L for the sum of all PFAS, with Denmark implementing among the strictest national thresholds. Recent detections of PFAS in Danish groundwater — including trifluoroacetic acid (TFA) at near-ubiquitous levels — have raised questions about the relative contributions of industrial point sources, diffuse atmospheric deposition, and agricultural activities to aquifer contamination (Albers & Sultenfuß, 2024; Scheurer & Nödler, 2024).

### 1.2 Agricultural Pathways for PFAS Contamination

Agriculture may contribute to PFAS groundwater contamination through at least four distinct pathways, each involving different compound classes and transport mechanisms:

**Pathway 1: Fluorinated pesticide degradation to TFA.** Twenty-four of the 25 PFAS-relevant active ingredients registered in the Danish Pesticide Register (Bekæmpelsesmiddeldatabasen, BMD) contain trifluoromethyl (-CF₃) functional groups that degrade to TFA in the environment. The TriFluPest study (Öhlin et al., 2025) experimentally confirmed TFA formation rates for seven key compounds including fluopyram, fluazinam, and diflufenican. Denmark subsequently banned 31 fluorinated pesticide products based on this evidence. TFA is now classified as a "forever chemical" degradation product of agricultural origin (PAN Europe, 2023).

**Pathway 2: Biosolids application to farmland.** Denmark applies approximately 77% of its municipal biosolids (sewage sludge) to agricultural land. Biosolids concentrate traditional PFAS (PFOS, PFOA, PFHxS, PFNA) from household and industrial wastewater, with electrochemical fluorination (ECF)-derived branched isomers predominating in sewage sludge (Ghisi et al., 2019). The Danish Environmental Protection Agency has established cut-off values for PFAS-4 sum concentrations in biosolids (maximum 10 µg/kg dry matter), yet up to 12% of sludge batches exceed this threshold (Danish EPA, 2023).

**Pathway 3: Contaminated pesticide formulations.** Fluorinated surfactants are used as "inert ingredients" in some pesticide formulations (Navarro et al., 2017). The US EPA removed 12 PFAS from the approved inert ingredients list in 2022, and PFOA concentrations of 20–50 ppb have been documented in fluorinated HDPE pesticide container rinsate (EPA, 2024; Washington et al., 2020). This pathway would create a direct proportional relationship between spray intensity and PFAS deposition regardless of the active ingredient's fluorination status.

**Pathway 4: Atmospheric deposition.** Volatile fluorotelomer alcohols (8:2 FTOH) degrade atmospherically to produce short-chain perfluorocarboxylic acids (PFBA, PFPeA, PFHxA) and ultimately TFA. This diffuse source should show no spatially specific correlation with agricultural intensity.

### 1.3 Research Gap

Despite growing recognition of agricultural PFAS pathways, no study has quantified the spatial association between pesticide application intensity and PFAS groundwater contamination at the catchment scale. International studies have documented PFAS in agricultural groundwater in the United States (Hu et al., 2016), Germany (Lindim et al., 2016), and Sweden (Ahrens et al., 2015), but these focused on point-source contamination rather than diffuse agricultural contributions. A companion study (Collignon, 2025) demonstrated significant correlations between parcel-level pesticide application and conventional pesticide/metabolite detection across 5,826 Danish catchment areas, identifying four substances (1,2,4-triazole, 4-chloro-2-methylphenol, bentazon, glyphosate) that survived all validation layers. The present study extends that framework to PFAS, testing whether the same spatial analytical approach reveals agricultural contributions to a chemically distinct class of persistent contaminants.

### 1.4 Research Questions

1. Does TFA groundwater detection vary spatially with fluorinated pesticide application intensity?
2. Do traditional PFAS (PFOS, PFOA, PFHxS, PFNA) correlate with total or fluorinated agricultural intensity at the catchment scale?
3. Which PFAS substances, if any, survive multivariate adjustment for hydrogeological covariates?
4. Does monitoring density confound observed correlations, as documented for conventional pesticides?

---

## 2. Materials and Methods

### 2.1 Study Area and Data Sources

**Pesticide application data.** Field-level pesticide application records were obtained from the Danish Agricultural Agency (*Landbrugsstyrelsen*) spraying journals for the 2015–2017 growing seasons. Each record specifies the pesticide product, dosage, and treated field polygon. We disaggregated product-level dosages to active ingredient level using the Danish Pesticide Register (BMD), which provides ingredient concentrations per registered product. Area-weighted allocation to catchment areas followed the same procedure described in Collignon (2025): field polygons were intersected with catchment area boundaries, and ingredient mass was allocated proportionally to intersection area.

**Fluorinated ingredient identification.** We automatically identified 52 fluorinated active ingredients from the full BMD ingredient list (n = 589 unique substances) by matching against fluorine-containing name patterns (fluor-, triflu-, cyhalothrin, etc.) and cross-referencing with the Pesticide Properties Database (PPDB). Of these, 35 are known TFA-forming parents based on the TriFluPest study (Öhlin et al., 2025), the PPDB, and PAN Europe (2023). The remaining 17 fluorinated ingredients contain C-F bonds but lack confirmed TFA degradation pathways. The complete list of 52 ingredients with TFA-forming classification is provided in Table S1. Two intensity measures were computed per catchment area: (i) *fluorinated intensity* — the summed application of all 52 fluorinated ingredients (kg/ha), and (ii) *total agricultural intensity* — the summed application of all active ingredients (kg/ha).

**PFAS groundwater monitoring data.** Sample-level PFAS monitoring data were obtained from the GEUS Jupiter database (Deliverable 2: cleaned individual-sample dataset), filtered to the PFAS parameter group. The dataset comprises 441,589 individual analyses spanning 2018–2025, covering 26 individual PFAS substances plus isomer-resolved variants and composite sum indices (PFAS-4, PFAS-12, PFAS-22). Detection thresholds were set at half the limit of quantification (LOQ) for each substance: 0.0015 µg/L for low-LOQ substances (PFOS, PFOA, PFHxS, PFNA) and 0.075 µg/L for all others including TFA. Catchment-level binary detection was defined as ≥1 sample exceeding the substance-specific threshold within the catchment area boundary. Sensitivity of key results to threshold choice was tested by re-running analyses at 0.5×, 1.0×, 1.5×, 2.0×, and 3.0× LOQ (Section 2.3).

**Temporal windows.** TFA monitoring began systematically in 2020, when GEUS expanded its routine PFAS panel to include TFA screening across all monitored boreholes; sporadic TFA analyses exist prior to 2020 but are excluded to ensure consistent spatial coverage. All TFA analyses used a 2020+ detection window. Traditional PFAS monitoring began in 2018; all other substances used a 2018+ window. Application data from 2015–2017 provides a 3–7 year lag consistent with vadose zone transit times in Danish soils (Tier 1 of Collignon, 2025).

**Spatial units.** All analyses were conducted at the catchment area level (n = 1,477 with PFAS monitoring data), using groundwater catchment areas delineated by GEUS under the Danish Groundwater Mapping System (*Grundvandskortlægningssystem*). These areas are defined based on aquifer structure and groundwater flow, providing a more hydrologically meaningful spatial unit than administrative boundaries. They are the same units used in the companion pesticide study.

### 2.2 Assumed Causal Structure

Figure 1 presents the Directed Acyclic Graph (DAG) assumed for this analysis. The target estimand is the total effect of agricultural pesticide intensity on PFAS groundwater detection. The primary adjustment set includes soil type (which influences both crop selection and contaminant transport), well intake depth (which determines which aquifer horizon is sampled), and monitoring well density (which affects both detection probability and is spatially correlated with agricultural land use).

![Figure 1. Directed Acyclic Graph: Assumed Causal Structure](figures/fig1_dag.svg)

**Figure 1.** Directed Acyclic Graph showing assumed causal structure. Solid arrows represent hypothesised causal paths; dashed arrows represent confounding paths. Agricultural intensity may affect PFAS detection directly (pesticide degradation, formulation contamination) or indirectly (biosolids co-location). Monitoring density is both a confounder (placed preferentially in agricultural areas) and a precision variable (more wells = better detection power). Biosolids application data is unavailable, leaving this pathway uncontrolled.

Key uncontrolled confounders include: (i) spatially explicit biosolids application, which co-occurs with agricultural land use but is not available as a separate variable; (ii) industrial point sources, partially controlled by excluding GEUS DEPOT samples but not comprehensively mapped; and (iii) historical PFAS contamination predating the observation window.

### 2.3 Analytical Framework

We employed a four-tier analytical framework with escalating specificity:

**Tier 1: TFA versus fluorinated pesticide intensity.** Point-biserial correlation between binary TFA detection (≥0.075 µg/L) and summed fluorinated pesticide application intensity per catchment area. This tests the primary agricultural TFA pathway through pesticide degradation. Additionally, TFA was re-analysed at progressively higher concentration thresholds (0.5, 1.0, 2.0, 5.0, 10.0 µg/L) to test whether spatial gradients emerge above the ubiquity floor.

**Tier 2: Traditional PFAS versus agricultural intensity.** Each of the four regulated PFAS (PFOS, PFOA, PFHxS, PFNA) was tested against both total agricultural intensity and fluorinated pesticide intensity (8 tests total). This tests biosolids, formulation, and container contamination pathways.

**Tier 3: Exploratory screen.** All PFAS substances (including isomer-resolved variants) were screened against both intensity measures, with a minimum detection threshold of ≥20 catchment-level detections. This identifies unexpected associations beyond the hypothesised pathways.

**Tier 4: Negative controls.** Three control groups validated the analytical approach: (i) high-Koc fluorinated pesticides (diflufenican Koc = 3,400; trifluralin Koc = 8,000; tau-fluvalinat Koc = 100,000) that bind strongly to soil and should not leach despite forming TFA; (ii) non-agricultural PFAS (PFDS, PFUnDA, PFDoDA) of industrial origin; and (iii) atmospheric PFAS (PFBA, PFPeA) from diffuse deposition.

### 2.4 Statistical Methods

**Point-biserial correlation** was computed between binary catchment-level detection and continuous intensity for each substance-intensity pair. Bootstrap 95% confidence intervals (n = 2,000 resamples) were used in preference to Fisher z-transform intervals, as the latter assume bivariate normality which is violated by binary data. Fisher z-transform CIs are reported in supplementary material for comparison. Substances with zero variance in detection (all detected or none detected) or <20 detections were excluded.

**Spatial autocorrelation.** Moran's I was computed for both pesticide intensity and binary PFAS detection surfaces using k = 8 nearest-neighbour spatial weights based on catchment centroids. Significant positive autocorrelation (I > 0, p < 0.05) indicates spatial clustering that violates the independence assumption of standard correlation tests and inflates significance levels. Where significant autocorrelation was detected, effective sample sizes were estimated using the Clifford and Richardson (1991) approximation: n_eff ≈ n / (1 + (n−1) × |I|), and the corresponding inflation factor reported.

**Benjamini–Hochberg FDR correction** was applied within each tier (α = 0.05). All reported q-values reflect tier-specific FDR adjustment.

**Bivariate logistic regression** modelled the probability of detection as a function of intensity: P(detected = 1) = logit⁻¹(β₀ + β₁ × intensity). Odds ratios (OR) with 95% confidence intervals, AUC, Nagelkerke R², and Hosmer–Lemeshow goodness-of-fit statistics were reported. For the PFOA-fluorinated intensity association, a quadratic term (intensity²) was additionally tested via likelihood ratio test to formally assess the non-monotonic dose-response pattern observed in quartile analysis.

**Dose–response analysis** stratified catchment areas into quartiles of non-zero intensity and computed detection rates per quartile. The Q4/Q1 ratio quantifies the fold-change in detection between the highest and lowest intensity quartiles.

**Multivariate logistic regression** adjusted for four hydrogeological covariates: dominant soil type (classified on a 1–6 scale from coarse sand to heavy clay), median well intake depth, monitoring well count per catchment area, and fluorinated pesticide intensity. Model diagnostics included AUC, Nagelkerke R², Hosmer–Lemeshow p-value, variance inflation factor (VIF), and events per variable (EPV; target ≥10). For associations with EPV < 10, Firth penalized logistic regression (Firth, 1993) was additionally applied to reduce small-sample bias in maximum likelihood estimation, and penalized coefficient estimates are reported alongside standard MLE results.

**Detection threshold sensitivity** was tested for key substances (PFOA, PFOS, PFHxS, SUM PFAS-22) by re-running bivariate correlations at 0.5×, 1.0×, 1.5×, 2.0×, and 3.0× LOQ, reporting whether FDR-significance is maintained.

**Biosolids proxy analysis** cross-tabulated soil type (clay-rich soils [categories 4–6] as a proxy for biosolids-receiving land, since Danish biosolids are preferentially applied to clay-rich soils) with PFAS detection using chi-square tests.

**Monitoring density stratification** divided catchment areas into tertiles by monitoring well count (low ≤ 2, medium ≤ 4, high > 4; n = 1,049, 388, 518 respectively) and re-computed correlations within each tertile to assess robustness to monitoring effort confounding.

**Power analysis** computed the minimum detectable effect size at 80% power (α = 0.05) given the median sample size across all tests. Additionally, the number of wells per catchment needed to detect the observed effect sizes at 80% power was estimated.

### 2.5 Software

All analyses were conducted in Python 3.11 using DuckDB 1.2 for data integration, SciPy 1.11 for statistical tests and spatial weights, scikit-learn 1.3 for AUC computation, and statsmodels 0.14 for logistic regression. Firth penalized logistic regression was implemented from first principles following Firth (1993). The complete analytical pipeline (3,600+ lines) is available in the study repository (see Section 6).

---

## 3. Results

### 3.1 Spatial Autocorrelation

Moran's I analysis revealed significant positive spatial autocorrelation in both pesticide intensity surfaces and PFAS detection surfaces (Table 1). Fluorinated pesticide intensity showed moderate clustering (I = 0.0947, z = 14.05, p < 0.001). Total agricultural intensity was not separately tested (only the fluorinated subset was computed). Among PFAS detection surfaces, PFOA and PFOS showed significant spatial clustering (p < 0.001), consistent with geographically structured contamination patterns.

The effective sample size reduction due to spatial autocorrelation was estimated at >99% for the most clustered variable (fluorinated intensity, inflation factor = 448.93). For PFAS detection surfaces, inflation factors ranged from 294.67 (PFOA) to 302.10 (PFOS). This means that standard p-values may overstate significance by a factor of approximately 300. While this does not invalidate the observed correlations, it means that borderline-significant associations (particularly those with 0.01 < p < 0.05) should be interpreted with additional caution. All p-values reported below are uncorrected for spatial autocorrelation; the inflation factor should be applied when interpreting significance thresholds.

**Table 1. Spatial autocorrelation (Moran's I) for intensity and detection surfaces**

| Variable | Moran's I | E(I) | z-score | p-value | n | n~eff~ | Inflation |
|:---------|:----------|:-----|:--------|:--------|:--|:-------|:----------|
| Fluorinated intensity | 0.0947 | −0.000211 | 14.05 | <0.001 | 4,731 | 11 | 448.93 |
| Total ag intensity | — | — | — | — | — | — | — |
| PFOA detection | 0.2419 | −0.000824 | 17.019 | <0.001 | 1,215 | 4 | 294.67 |
| PFOS detection | 0.2113 | −0.000702 | 16.11 | <0.001 | 1,426 | 5 | 302.10 |

*Note.* k = 8 nearest neighbours. n~eff~ = effective sample size after Clifford–Richardson correction. Inflation = n / n~eff~.

### 3.2 Tier 1: TFA Ubiquity Precludes Spatial Correlation

TFA was detected in every monitored catchment area (100% detection rate at the 0.075 µg/L threshold), yielding zero variance in the outcome variable. Point-biserial correlation is undefined when the binary variable has no variance, and all Tier 1 tests were excluded (0/0 FDR-significant).

**TFA threshold sensitivity analysis** revealed that spatial gradients begin to emerge at higher concentration thresholds (Table 2). At the standard 0.075 µg/L threshold, 100% of catchments showed detection. At 1.0 µg/L, detection dropped to 6.8%, creating sufficient variance for correlation analysis, with the strongest correlation observed at this threshold (r = 0.178, p < 0.001, bootstrap 95% CI [0.095, 0.270]). However, this association attenuated to non-significance at 2.0 µg/L (r = 0.042, p = 0.110) and became untestable at 5.0 µg/L (only 1 detection, 0.1%) and 10.0 µg/L (0 detections).

**Table 2. TFA threshold sensitivity analysis**

| Threshold (µg/L) | n catchments | Detected | Det% | r | p | Bootstrap 95% CI |
|:------------------|:-------------|:---------|:-----|:--|:--|:-----------------|
| 0.075 (half-LOQ) | 1,477 | 1,477 | 100.0 | N/A | N/A | — |
| 0.5 | 1,477 | 244 | 16.5 | 0.165 | <0.001 | [0.099, 0.234] |
| 1.0 | 1,477 | 100 | 6.8 | 0.178 | <0.001 | [0.095, 0.270] |
| 2.0 | 1,477 | 37 | 2.5 | 0.042 | 0.110 | [−0.001, 0.098] |
| 5.0 | 1,477 | 1 | 0.1 | N/A | N/A | — |
| 10.0 | 1,477 | 0 | 0.0 | N/A | N/A | — |

*Note.* TFA concentration distribution in monitoring data: median = 0.075 µg/L, P75 = 0.24, P90 = 0.55, P95 = 0.81, max = 5.5 µg/L.

![Figure 2. TFA Threshold Sensitivity](figures/fig2_tfa_threshold.svg)

**Figure 2.** Detection rate (bars, left axis) and point-biserial correlation with fluorinated pesticide intensity (line, right axis) at progressively higher TFA concentration thresholds. Significant correlations (p < 0.001) emerge at 0.5–1.0 µg/L but attenuate to non-significance at 2.0 µg/L and become untestable at 5.0+ µg/L due to insufficient detections.

This finding — that TFA is ubiquitous at the standard detection threshold but shows concentration-dependent spatial variation — is itself a result of scientific significance. It implies that TFA sources have saturated the Danish groundwater system at trace levels, but concentration gradients may still reflect local agricultural pressure. Concentration-based rather than binary-detection analysis is needed to resolve these gradients.

### 3.3 Tier 2: Traditional PFAS Show Consistent Bivariate Agricultural Association

Seven of eight Tier 2 tests were FDR-significant (Table 3). The strongest association was between PFOA detection and fluorinated pesticide intensity (r = 0.229, bootstrap 95% CI [0.176, 0.282], q < 0.001), followed by PFOS versus fluorinated intensity (r = 0.171 [0.120, 0.221], q < 0.001) and PFHxS versus fluorinated intensity (r = 0.114 [0.058, 0.169], q < 0.001).

When tested against total agricultural intensity (all pesticide substances), effect sizes were weaker: PFOS (r = 0.088, q = 0.002), PFOA (r = 0.087, q = 0.003), and PFNA (r = 0.060, q = 0.040). Only PFHxS versus total intensity was non-significant (r = −0.010, q = 0.727).

**Table 3. Tier 2 results: Traditional PFAS versus agricultural intensity**

| # | Test | Type | r | Bootstrap 95% CI | q_FDR | FDR | Det% | n |
|:--|:-----|:-----|:--|:-----------------|:------|:----|:-----|:--|
| 1 | PFHxS vs total | traditional | −0.010 | [−0.066, 0.046] | 0.727 | | 4.0% | 1,215 |
| 2 | PFNA vs total | traditional | 0.060 | [0.004, 0.116] | 0.040 | * | 1.6% | 1,216 |
| 3 | PFOA vs total | traditional | 0.087 | [0.031, 0.143] | 0.003 | * | 9.5% | 1,215 |
| 4 | PFOS vs total | traditional | 0.088 | [0.036, 0.139] | 0.002 | * | 7.3% | 1,426 |
| 5 | PFHxS vs fluorinated | traditional | 0.114 | [0.058, 0.169] | <0.001 | * | 4.0% | 1,215 |
| 6 | PFNA vs fluorinated | traditional | 0.088 | [0.031, 0.143] | 0.003 | * | 1.6% | 1,216 |
| 7 | **PFOA vs fluorinated** | **traditional** | **0.229** | **[0.176, 0.282]** | **<0.001** | **\*** | **9.5%** | **1,215** |
| 8 | PFOS vs fluorinated | traditional | 0.171 | [0.120, 0.221] | <0.001 | * | 7.3% | 1,426 |

*Note.* CIs are bootstrap percentile intervals (n = 2,000 resamples). \* = q < 0.05.

The pattern of stronger correlations with fluorinated pesticide intensity than with total agricultural intensity is notable: PFOA showed r = 0.229 against fluorinated intensity versus r = 0.087 against total intensity (2.6-fold difference), and PFOS showed r = 0.171 versus r = 0.088 (1.9-fold). However, given the spatial autocorrelation documented in Section 3.1, borderline-significant associations should be interpreted cautiously.

**Detection threshold sensitivity** confirmed robustness for key associations. PFOA versus fluorinated intensity remained significant across all threshold variants tested (0.5× to 3.0× LOQ). PFOS was robust at 0.5× to 2.0× LOQ but lost significance at 3.0× LOQ where detection rates dropped below the minimum sample threshold. SUM PFAS-22 showed consistent significance across all variants (Table S2).

**Base-rate constraint on effect size interpretation.** Point-biserial correlation is bounded by the base rate of the binary variable: with 9.5% detection for PFOA, the theoretical maximum r is approximately 0.55; with 4.0% for PFHxS, the maximum is approximately 0.38. The observed r = 0.229 for PFOA represents approximately 42% of the theoretical maximum, while PFHxS (r = 0.114) represents approximately 30%. Cross-substance comparison of raw r values should account for these differing ceilings.

### 3.4 Tier 3: Exploratory Screen Confirms Broad Bivariate Signal

Twelve of 16 eligible Tier 3 tests were FDR-significant (Table 4). Against fluorinated intensity, all eight testable substances were significant, with PFOA (r = 0.229), SUM PFAS-12 (r = 0.193), PFOS (r = 0.171), and SUM PFAS-22 (r = 0.170) showing the strongest associations. Against total agricultural intensity, four of eight were significant, headed by Σ-PFOA (r = 0.203) and Σ-PFHxS (r = 0.139).

**Table 4. Tier 3 results: Exploratory screen of all PFAS versus both intensity measures**

| # | Substance | Intensity | r | q_FDR | FDR | Det% | n |
|:--|:----------|:----------|:--|:------|:----|:-----|:--|
| 1 | **PFOA** | **fluorinated** | **0.229** | **<0.001** | **\*** | **9.5%** | **1,215** |
| 2 | Σ-PFOA | total ag | 0.203 | <0.001 | * | 6.5% | 848 |
| 3 | SUM PFAS-12 | fluorinated | 0.193 | <0.001 | * | 4.0% | 1,417 |
| 4 | PFOS | fluorinated | 0.171 | <0.001 | * | 7.3% | 1,426 |
| 5 | SUM PFAS-22 | fluorinated | 0.170 | <0.001 | * | 3.0% | 1,052 |
| 6 | Σ-PFOA | fluorinated | 0.167 | <0.001 | * | 6.5% | 848 |
| 7 | SUM PFAS-4 | fluorinated | 0.150 | <0.001 | * | 2.5% | 1,426 |
| 8 | Σ-PFHxS | fluorinated | 0.149 | <0.001 | * | 3.2% | 758 |
| 9 | Σ-PFHxS | total ag | 0.139 | <0.001 | * | 3.2% | 758 |
| 10 | PFHxS | fluorinated | 0.114 | <0.001 | * | 4.0% | 1,215 |
| 11 | PFOS | total ag | 0.088 | 0.001 | * | 7.3% | 1,426 |
| 12 | PFOA | total ag | 0.087 | 0.003 | * | 9.5% | 1,215 |
| 13 | SUM PFAS-4 | total ag | 0.038 | 0.186 | | 2.5% | 1,426 |
| 14 | SUM PFAS-12 | total ag | 0.027 | 0.364 | | 4.0% | 1,417 |
| 15 | SUM PFAS-22 | total ag | −0.011 | 0.733 | | 3.0% | 1,052 |
| 16 | PFHxS | total ag | −0.010 | 0.733 | | 4.0% | 1,215 |

### 3.5 Dose–Response Analysis and Non-Monotonicity Testing

Dose–response quartile analysis for FDR-significant Tier 2 results revealed modest gradients (Table 5). PFOS versus fluorinated intensity showed the clearest pattern: detection increased from 7.6% (Q1) to 12.8% (Q4), a 1.7-fold gradient. PFOA versus fluorinated intensity showed a less monotonic pattern (11.0% → 6.3% → 6.0% → 14.1%, Q4/Q1 = 1.3×), with elevated detection in both the lowest and highest quartiles.

**Table 5. Dose–response quartile analysis for FDR-significant Tier 2 results**

| Substance | Q1% | Q2% | Q3% | Q4% | Q4/Q1 | Q1 n | Q2 n | Q3 n | Q4 n |
|:----------|:----|:----|:----|:----|:------|:-----|:-----|:-----|:-----|
| PFOS vs fluorinated | 7.6 | 4.5 | 4.2 | 12.8 | 1.7× | 422 | 335 | 334 | 335 |
| PFOA vs fluorinated | 11.0 | 6.3 | 6.0 | 14.1 | 1.3× | 363 | 284 | 284 | 284 |
| PFOS vs total | 8.3 | 4.5 | 3.9 | 12.2 | 1.5× | 420 | 335 | 335 | 336 |
| PFOA vs total | 11.6 | 6.3 | 5.6 | 13.7 | 1.2× | 361 | 285 | 284 | 285 |
| PFHxS vs fluorinated | 4.4 | 3.9 | 2.1 | 5.6 | 1.3× | 363 | 284 | 284 | 284 |
| PFNA vs fluorinated | 1.9 | 1.8 | 0.7 | 1.8 | 0.9× | 364 | 284 | 284 | 284 |
| PFNA vs total | 2.2 | 1.8 | 0.4 | 1.8 | 0.8× | 361 | 285 | 285 | 285 |

**Non-monotonic dose-response test.** The PFOA U-shaped pattern was formally tested by adding a quadratic intensity term to the logistic regression model (Table 6). The likelihood ratio test for PFOA was non-significant (LR = 0.544, p = 0.461), with ΔAIC = −1.5, indicating that the non-monotonic pattern is not statistically robust for PFOA. However, PFOS showed a significant quadratic term (LR = 4.914, p = 0.027, ΔAIC = 2.9), suggesting a non-linear dose-response for PFOS that warrants further investigation.

**Table 6. Non-monotonic dose-response: linear vs. quadratic logistic regression**

| Substance | AIC (linear) | AIC (quadratic) | ΔAIC | LR p-value | Quadratic sig? |
|:----------|:-------------|:----------------|:-----|:-----------|:---------------|
| PFOA | 727.0 | 728.5 | −1.5 | 0.461 | No |
| PFOS | 725.5 | 722.6 | 2.9 | 0.027 | Yes |
| PFNA | 195.4 | 195.3 | 0.1 | 0.152 | No |

### 3.6 Multivariate Adjustment: Monitoring Density Dominates

After controlling for soil type, median intake depth, and monitoring well density, 4 of 19 FDR-significant associations retained statistical significance (Table 7). Monitoring well density was the strongest predictor in all 19 models (p < 0.001 to p = 0.006), with intake depth also highly significant (p < 0.001 to p = 0.001). The 79% attenuation rate (15/19 lost significance) — substantially greater than the 43% attenuation in the companion pesticide study — indicates that PFAS–agricultural associations are more fragile to covariate adjustment than conventional pesticide associations.

**Table 7. Multivariate logistic regression results for associations retaining significance**

| Substance | Biv. r | MV p~int~ | MV AUC | Nag. R² | H-L p | EPV | n |
|:----------|:-------|:---------|:-------|:--------|:------|:----|:--|
| SUM PFAS-22 | 0.170 | 0.006 | 0.83 | 0.186 | 0.437 | 8 | 1,052 |
| Σ-PFOA (fluorinated) | 0.167 | 0.036 | 0.83 | 0.230 | 0.092 | 14 | 848 |
| Σ-PFHxS (fluorinated) | 0.149 | 0.023 | 0.85 | 0.207 | 0.719 | 6 | 758 |
| Σ-PFOA (total ag) | 0.203 | 0.026 | 0.83 | 0.245 | 0.014 | 14 | 848 |

**Firth penalized regression** was applied to the two associations with EPV < 10 (Σ-PFHxS EPV = 6, SUM PFAS-22 EPV = 8) to assess small-sample bias (Table 8). Firth-corrected odds ratios were virtually identical to MLE estimates, confirming the robustness of these low-EPV results. SUM PFAS-22 showed Firth p = 0.005 versus MLE p = 0.006; Σ-PFHxS showed Firth p = 0.020 versus MLE p = 0.023.

**Table 8. Firth penalized regression vs. standard MLE for low-EPV associations**

| Substance | EPV | MLE OR | MLE p | Firth OR | Firth p | Firth 95% CI | Converged |
|:----------|:----|:-------|:------|:---------|:--------|:-------------|:----------|
| SUM PFAS-22 | 8 | 1.0022 | 0.006 | 1.0022 | 0.005 | [1.0007, 1.0038] | Y |
| Σ-PFHxS | 6 | 1.0019 | 0.023 | 1.0019 | 0.020 | [1.0003, 1.0035] | Y |
| Σ-PFOA (fluor.) | 14 | 1.0017 | 0.036 | — | — | — | — |

### 3.7 Monitoring Density Stratification

Monitoring density stratification confirmed the pattern observed in the companion pesticide study (Table 9). Correlations were concentrated in the high-density tertile (>4 wells per catchment area, n = 518), with most substances showing near-zero correlations in the low-density tertile (≤2 wells, n = 1,049).

**Table 9. Point-biserial correlations stratified by monitoring density tertile**

| Substance | Low (r) | Medium (r) | High (r) |
|:----------|:--------|:-----------|:---------|
| PFOA vs fluorinated | −0.024 | 0.199\*\* | 0.165\*\* |
| PFOS vs fluorinated | 0.014 | −0.043 | 0.121\*\* |
| Σ-PFOA vs total | 0.349\*\* | −0.055 | 0.183\*\* |
| Σ-PFHxS vs total | 0.000 | −0.020 | 0.145\*\* |
| SUM PFAS-22 vs fluorinated | −0.017 | 0.016 | 0.132\*\* |
| SUM PFAS-12 vs fluorinated | −0.011 | 0.010 | 0.139\*\* |
| SUM PFAS-4 vs fluorinated | 0.000 | −0.014 | 0.103\* |

\*p < 0.05; \*\*p < 0.01

![Figure 3. Monitoring Density Stratification](figures/fig3_monitoring_stratification.svg)

**Figure 3.** Point-biserial correlations stratified by monitoring well density tertile. Significant associations appear almost exclusively in the high-density tertile (>4 wells per catchment), replicating the pattern observed in the companion pesticide study.

One exception warrants discussion: Σ-PFOA versus total agricultural intensity showed an anomalously strong correlation in the low-density tertile (r = 0.349, p < 0.01), which disappeared in the medium tertile (r = −0.055) before re-emerging in the high tertile (r = 0.183). This non-monotonic pattern may reflect small-sample instability in the isomer-resolved Σ-PFOA dataset, which has fewer observations than the standard PFOA measurement (n = 848 vs. 1,215).

### 3.8 Biosolids Proxy Analysis

Cross-tabulation of soil type (clay-rich vs. sandy) with PFAS detection provided a partial test of the biosolids co-location hypothesis (Table 10). If traditional PFAS detection were driven by biosolids (preferentially applied to clay-rich soils), detection rates should be higher on clay-rich soils.

**Table 10. Biosolids proxy: PFAS detection by soil type**

| Substance | Det% sandy | Det% clay | χ² | p | Clay higher? |
|:----------|:-----------|:----------|:---|:--|:-------------|
| PFOA | 8.5% | 8.8% | 0.003 | 0.953 | Yes |
| PFOS | 6.9% | 6.1% | 0.210 | 0.647 | No |
| PFHxS | 3.1% | 3.7% | 0.141 | 0.707 | Yes |
| Σ-PFOA | 6.5% | 4.9% | 0.655 | 0.418 | No |

*Note.* Sandy = soil type categories 1–3 (coarse sand to fine sand); Clay = categories 4–6 (sandy clay to heavy clay).

### 3.9 Negative Controls

All negative controls behaved as expected, though through data limitation rather than explicit null results. Long-chain industrial PFAS (PFDS, PFDoDA, PFUnDA), atmospheric PFAS (PFBA, PFPeA), and the TFA–high-Koc pesticide pairs all showed zero catchment-level variance in detection (all substances had either 0% or 100% catchment-level detection), rendering correlation analysis impossible. The absence of detectable catchment-level variation in these substances is itself a validation of the detection threshold and spatial aggregation methodology.

### 3.10 Power Analysis and Monitoring Well Targets

The median sample size across all tests was 1,215 catchment areas, yielding a minimum detectable effect of r = 0.080 at 80% power (α = 0.05). Of 19 FDR-significant associations, 18 exceeded this minimum detectable effect. Three associations showed strong effects (r ≥ 0.20): PFOA versus fluorinated intensity (r = 0.229), Σ-PFOA versus total intensity (r = 0.203), and SUM PFAS-12 versus fluorinated intensity (r = 0.193). However, these power calculations assume balanced classes and independent observations; with PFAS detection rates of 1–10% and significant spatial autocorrelation, effective power is lower than these estimates suggest.

**Monitoring well target.** Given the observed effect sizes and detection rates, achieving 80% power for the weakest significant association (PFHxS r = 0.114) requires approximately 602 catchments. For the strongest (PFOA r = 0.229), approximately 148 catchments suffice. At the catchment level, a minimum of 11–32 monitoring wells per catchment are needed to have >50% probability of detecting PFAS contamination, given the 1–10% sample-level detection rates (11 wells for PFOA at 9.5% detection, 32 wells for Σ-PFHxS at 3.2% detection). This quantitative target supports Recommendation 4 (Section 4.7).

---

## 4. Discussion

### 4.1 The Primary Finding: Monitoring Density Dominates

The most robust finding of this study is that PFAS groundwater detection is primarily associated with monitoring density, not agricultural activity. Monitoring well count was the strongest predictor in all 19 multivariate models (p < 0.001), dominating soil type, intake depth, and agricultural intensity. The 79% attenuation rate under covariate adjustment — compared to 43% in the companion pesticide study — indicates that the spatial overlap between monitoring network placement and agricultural land use is a larger confound for PFAS than for conventional pesticides.

This interpretation is reinforced by: (i) the monitoring density stratification showing correlations only in the high-density tertile (>4 wells); (ii) the spatial autocorrelation analysis confirming that both intensity and detection surfaces are spatially clustered, meaning standard p-values overstate significance; and (iii) the bivariate AUC values of 0.45–0.57, indicating that agricultural intensity alone has essentially no discriminatory power for PFAS detection.

### 4.2 Residual Agricultural Signal: Modest but Persistent

Despite the dominance of monitoring density, four associations survived multivariate adjustment: SUM PFAS-22 (p = 0.006), Σ-PFOA (p = 0.036 fluorinated, p = 0.026 total), and Σ-PFHxS (p = 0.023). These residual signals are modest but warrant discussion through the lens of the Bradford Hill criteria for causation:

1. **Strength:** Modest (r = 0.15–0.23; 4/19 survive adjustment).
2. **Consistency:** The monitoring density pattern replicates the companion pesticide study exactly.
3. **Specificity:** Fluorinated pesticide intensity is a more specific predictor (2.6-fold stronger for PFOA) than total agricultural intensity.
4. **Temporality:** Application data (2015–2017) precedes detection data (2018–2025) by 3–7 years, consistent with vadose zone transit times.
5. **Biological gradient:** Dose-response is modest (Q4/Q1 = 1.2–1.7×) and non-monotonic for PFOA.
6. **Plausibility:** Multiple mechanistic pathways exist (Section 1.2).
7. **Coherence:** TFA ubiquity is consistent with widespread fluorinated pesticide degradation.

Overall, the evidence supports a possible agricultural contribution but does not meet the threshold for causal attribution.

### 4.3 The Fluorinated Pesticide Signal in Traditional PFAS

The consistent pattern of stronger PFOA and PFOS correlations with fluorinated pesticide intensity (r = 0.229 and 0.171) than with total agricultural intensity (r = 0.087 and 0.088) is consistent with — but does not prove — pathways linking fluorinated pesticide use specifically to traditional PFAS contamination. At least two non-exclusive mechanisms could explain this pattern:

**Contaminated formulation hypothesis.** Fluorinated pesticide products may contain PFAS as formulation additives (fluorinated surfactants) at higher rates than non-fluorinated products, creating a direct proportional link between fluorinated pesticide application intensity and PFAS co-deposition (Navarro et al., 2017; EPA, 2024; Washington et al., 2020).

**Biosolids co-location hypothesis.** Agricultural areas with intensive fluorinated pesticide use may also receive disproportionate biosolids application, which would introduce traditional PFAS independently of the pesticide active ingredient. The biosolids proxy analysis (Section 3.8) provides a partial test: PFAS detection rates were similar on clay-rich and sandy soils (e.g., PFOA 8.8% vs. 8.5%, p = 0.953; PFOS 6.1% vs. 6.9%, p = 0.647), which does not support the biosolids pathway as a primary driver but cannot exclude it given the coarseness of the soil-type proxy.

### 4.4 Isomer-Resolved Measurements and the Branched Isomer Paradox

Three of four multivariate-surviving associations involved isomer-resolved measurements (Σ-PFOA [sum of branched and linear isomers], Σ-PFHxS) or composite indices (SUM PFAS-22). These measurements capture both linear and branched PFAS isomers. Branched isomers are characteristic of electrochemical fluorination (ECF) production, historically used by 3M, while telomerization-derived PFAS (from modern fluorochemical production) are predominantly linear.

This creates an interpretive paradox: if branched isomers drive the agricultural signal, this favours biosolids or legacy industrial sources — which concentrate ECF-derived PFAS from wastewater (Ghisi et al., 2019) — rather than pesticide formulation contamination, which would contain telomerization-derived linear isomers. The observation that isomer-resolved sums survive multivariate adjustment while individual linear-isomer measurements do not may therefore indicate that the agricultural association is mediated by biosolids application (Pathway 2) rather than direct pesticide contamination (Pathways 1 or 3). This interpretation aligns with the biosolids proxy results and represents the simplest explanation consistent with the data.

### 4.5 PFAS Transport Considerations

The vadose zone transit time framework (3–7 years) used in this study was developed for conventional pesticide leaching and may not directly apply to all PFAS. PFAS transport in soils is governed by several mechanisms that differ from conventional pesticides:

1. **Chain-length-dependent sorption.** Solid-phase sorption coefficients (Kd) increase with perfluoroalkyl chain length. The Kd of 2.6 L/kg cited for PFOS applies to sandy soils specifically (Brusseau, 2018); Danish clay-rich soils with higher organic carbon content would show substantially greater retardation (Kd > 10 L/kg).

2. **Air-water interfacial (AWI) adsorption.** In the unsaturated (vadose) zone, PFAS adsorb strongly to air-water interfaces, retarding transport far more than solid-phase Kd alone predicts (Brusseau, 2018; Lyu et al., 2018). AWI adsorption is particularly important for long-chain PFAS and may explain why short-chain substances (TFA, PFBA) are detected in groundwater more readily than long-chain compounds (PFOS, PFOA).

3. **Precursor transformation.** PFAS precursors (e.g., N-EtFOSA, fluorotelomer alcohols) can transform to terminal PFAS during subsurface transport, meaning that the parent compound applied at the surface may differ from the terminal PFAS detected at the water table. This biotransformation adds a confounding temporal dimension: precursor-derived PFAS may arrive at the water table later than directly applied terminal compounds.

4. **pH-dependent sorption.** Zwitterionic and cationic PFAS show pH-dependent sorption behaviour, with sorption increasing at lower pH. Danish agricultural soils (typical pH 5.5–7.5) span a range that can influence PFAS retention.

These factors mean that the present cross-sectional design — correlating 2015–2017 application with 2018–2025 detection — may capture short-chain PFAS transport but underestimate long-chain PFAS associations, whose contamination signal may reflect decades of prior agricultural activity rather than the three-year application window observed here.

### 4.6 TFA Ubiquity: A Finding, Not a Failure

The inability to conduct spatial correlation analysis for TFA — the compound with the most direct agricultural pathway — is the most scientifically important finding of this study. TFA was detected in 100% of monitored catchment areas, consistent with the 60-year accumulation trend documented by Albers and Sultenfuß (2024) and the near-ubiquitous presence reported by EFSA's non-target screening of 81 Danish groundwater sites. The threshold sensitivity analysis (Section 3.2) demonstrated that spatial gradients do emerge at higher concentration thresholds (r = 0.178 at 1.0 µg/L), but attenuate to non-significance at 2.0 µg/L and become untestable at 5.0+ µg/L, suggesting that concentration-based analysis could resolve agricultural TFA gradients at intermediate concentration levels but that very high TFA concentrations are too rare for spatial analysis.

### 4.7 International Context

These findings can be placed in international perspective. The USGS National Water Quality Assessment (NAWQA) programme has documented PFAS in 45% of US tap water samples, with agricultural and urban land use both associated with detection (Andrews & Naidenko, 2020). In Germany, Lindim et al. (2016) modelled PFAS emissions from municipal wastewater treatment plants (including agricultural biosolids pathways) and found that diffuse sources contributed substantially to river contamination. In Sweden, Ahrens et al. (2015) documented PFAS in groundwater near firefighting training facilities but did not quantify diffuse agricultural contributions. The present study appears to be the first to attempt catchment-scale spatial correlation between field-level pesticide application and PFAS groundwater detection. The modest effect sizes observed here (r = 0.15–0.23) are comparable to those found in the companion pesticide study for conventional metabolites, suggesting that agricultural diffuse contamination — whether mediated by pesticides, biosolids, or both — produces consistent but small spatial signals that are detectable only with sufficient monitoring density.

### 4.8 Limitations

The most important limitations of this study are:

1. **Monitoring density confound.** The strongest caveat is that monitoring density is simultaneously a confounder and a precision variable. The consistent emergence of significant correlations only in well-monitored catchments may reflect either genuine agricultural contamination that is only detectable with adequate monitoring, or systematic bias in monitoring network placement.

2. **Spatial autocorrelation.** Both pesticide intensity and PFAS detection are spatially autocorrelated (Section 3.1), violating the independence assumption of standard correlation tests and inflating significance levels by a factor of approximately 300 (Table 1). While FDR correction addresses multiple testing, it does not correct for spatial non-independence. Spatial regression models (spatial lag or spatial error) should be employed in future confirmatory analyses.

3. **Biosolids data unavailability.** We lack spatially explicit data on biosolids application to Danish farmland. The biosolids proxy analysis (Section 3.8) provides indirect evidence but cannot substitute for direct biosolids application records.

4. **Detection threshold coarseness.** The GEUS LOQ values (0.003 µg/L for traditional PFAS, 0.15 µg/L for others) and binary detection aggregation at the catchment level discard concentration information that could reveal dose–response relationships invisible to the binary approach.

5. **Temporal mismatch.** Application data span 2015–2017, while PFAS monitoring spans 2018–2025. Although the 3–7 year lag is consistent with vadose zone transit times for short-chain PFAS, long-chain PFAS with higher Kd values may require decades to reach groundwater (Section 4.5).

6. **No point-source control.** PFAS contamination from industrial point sources (firefighting foam, manufacturing, wastewater treatment plant effluent) is not explicitly controlled for. Although the GEUS monitoring programme excludes known contaminated sites (DEPOT samples are filtered), unknown point sources could contribute to catchment-level detections.

7. **Ecological fallacy.** All analyses operate at the catchment level, correlating aggregate pesticide intensity with aggregate PFAS detection. Individual-level causal relationships cannot be inferred from these aggregate associations. For PFAS, the ecological fallacy may be more severe than for conventional pesticides because multiple diffuse sources (biosolids, atmospheric deposition, industrial releases) co-occur within catchments and are averaged together in the spatial aggregation.

8. **Low detection rates and marginal EPV.** Most PFAS substances had catchment-level detection rates between 1% and 10%, creating severe class imbalance. Two of four multivariate-surviving associations (Σ-PFHxS EPV = 6, SUM PFAS-22 EPV = 8) fall below the recommended EPV ≥ 10 threshold (Peduzzi et al., 1996). Firth penalized regression (Section 3.6) confirmed these low-EPV results (Firth p-values virtually identical to MLE estimates), and confirmatory studies with larger event counts are needed.

9. **Screening bias (first-testing-not-first-detection).** PFAS monitoring in Danish groundwater began only in 2013 (pilot) and scaled from 2017 onward; TFA monitoring started in 2020. This raises the concern that detections reflect first-time testing rather than new contamination. We assessed screening bias using sample-level VP4 data (GEUS Dataverse, doi:10.22008/FK2/IHVDXL). The risk varies by substance: PFNA was tested from 2013 with 259 negative tests across 3 years before its first detection in 2016 (LOW risk). PFHxS had 4 negative tests in 2013 before first detection in 2014 (LOW risk). PFOA was detected in its first full testing year (2014) at 95.7% detection rate with only 1 negative test (MODERATE risk — though near-universal detection at first testing suggests genuine ubiquity rather than a testing artifact). PFOS shows 100% detection in every year since monitoring began in 2013 (22,243 positive tests, zero negatives across 12 years), which is more consistent with ubiquitous contamination than a testing artifact. TFA similarly shows 100% detection since monitoring began in 2020 (6,496 positive tests, zero negatives). For PFOS and TFA, the absence of any negative test across thousands of wells over many years indicates genuine ubiquitous presence; the screening bias concern applies more to the timing of detection than to its existence. The binary detection approach used in this study is unaffected by screening bias for substances with 100% detection rates, since spatial variation in concentration (not detection/non-detection) would be needed to reveal application-intensity gradients. For PFOA and PFHxS, where detection rates vary spatially (8–20%), the short testing history (from 2013/2014) means we cannot fully exclude the possibility that spatial detection patterns partly reflect the rollout of monitoring infrastructure rather than contamination patterns (Supplementary S5.1 provides the full substance-by-substance yearly analysis).

### 4.9 Policy Implications

Despite the limitations above, several findings carry immediate regulatory relevance:

1. **TFA monitoring at lower LOQs.** The ubiquity of TFA at the current detection threshold (0.075 µg/L), combined with the threshold sensitivity analysis showing spatial gradients emerging at higher concentrations, motivates adoption of ultra-sensitive methods (LOQ < 0.01 µg/L) in routine drinking water monitoring. The pending inclusion of TFA in the EU Drinking Water Directive monitoring list should be accompanied by analytical requirements sufficient to resolve spatial concentration gradients.

2. **PFAS content testing of pesticide formulations.** The stronger bivariate association of traditional PFAS with fluorinated pesticide intensity than with total agricultural intensity — while not surviving multivariate adjustment for most substances and therefore not causally established — warrants precautionary testing of registered fluorinated pesticide formulations for PFAS content. The EU's REACH framework could require PFAS disclosure in pesticide formulation dossiers.

3. **Spatially explicit biosolids tracking.** The inability to separate biosolids-mediated from pesticide-mediated PFAS pathways is a fundamental data gap. Danish municipalities should be required to report biosolids application locations at the field level, as is already done for pesticide spraying journals, enabling future disambiguation studies.

4. **Monitoring network design.** The concentration of significant correlations in well-monitored catchments (>4 wells) suggests that the current monitoring network is insufficient to detect agricultural PFAS contamination in most Danish catchments. Based on the power analysis (Section 3.10), a minimum of 11–32 wells per catchment are needed to detect effect sizes of r ≥ 0.15 with 80% power.

### 4.10 Stakeholder Considerations

These equivocal findings warrant careful communication. Danish farmers applying fluorinated pesticides — many of which have since been banned — have a legitimate interest in whether their historical practices contributed to PFAS contamination. The present study cannot make that determination: the modest residual signal after covariate adjustment, the alternative biosolids pathway, and the monitoring density confound collectively preclude attributing PFAS contamination to specific agricultural practices. Advocacy groups and regulatory bodies should note that the bivariate correlations (which appear strong in isolation) are substantially attenuated by adjustment for known confounders, and that the branched isomer evidence (Section 4.4) may point more strongly to biosolids than to pesticide formulations.

### 4.11 Future Directions

1. **Concentration modelling.** Replacing binary detection with continuous concentration as the outcome variable would increase statistical power and enable dose–response analysis at the sample level.
2. **Biosolids integration.** Obtaining spatially explicit biosolids application records from Danish municipalities would allow direct testing of the biosolids co-location hypothesis.
3. **Isomer fingerprinting.** The observation that isomer-resolved measurements show stronger agricultural associations warrants investigation of branched/linear isomer ratios as source attribution markers.
4. **TFA gradient analysis.** Using ultra-sensitive analytical methods (LOQ < 0.01 µg/L) to quantify TFA concentration gradients across catchments with varying fluorinated pesticide intensity would address the ubiquity limitation.
5. **Spatial regression.** Implementing spatial lag and spatial error models to formally account for spatial autocorrelation, rather than the post-hoc effective sample size correction used here.
6. **Temporal trend analysis.** Correlating year-over-year changes in PFAS detection with changes in fluorinated pesticide registration (e.g., the 2023 Danish ban on 31 fluorinated products) would provide stronger causal evidence than the cross-sectional design employed here. The ban creates a natural experiment: catchments with historically high fluorinated pesticide application should show declining TFA input over subsequent years, which could be detected by concentration trend analysis. Even without post-ban PFAS data, a pre-ban trend analysis examining whether catchments with declining fluorinated pesticide use between 2015 and 2017 showed different PFAS trajectories in 2018–2025 could strengthen causal inference.

---

## 5. Conclusions

This study provides the first catchment-scale spatial analysis of the association between agricultural pesticide application intensity and PFAS groundwater detection in Denmark. The primary finding is that monitoring well density — not agricultural intensity — is the dominant spatial predictor of PFAS detection across all 19 multivariate models. After adjustment for monitoring density, soil type, and well depth, only 4 of 19 bivariate associations retained significance: SUM PFAS-22 (p = 0.006), Σ-PFOA (p = 0.036 fluorinated; p = 0.026 total), and Σ-PFHxS (p = 0.023). These residual associations are modest (r = 0.15–0.23), appear exclusively in well-monitored catchments (>4 wells), and may be mediated by biosolids application rather than direct pesticide contamination, as suggested by the preferential survival of branched-isomer-inclusive measurements.

TFA was detected in 100% of monitored catchments, precluding spatial correlation analysis but demonstrating ubiquitous contamination of Danish groundwater. Threshold sensitivity analysis suggests that concentration gradients exist above 0.5 µg/L (r = 0.165) with the strongest signal at 1.0 µg/L (r = 0.178), motivating future concentration-based studies.

Spatial autocorrelation in both intensity and detection surfaces means that standard p-values overstate significance, and the true number of robust agricultural associations may be smaller than reported. Future work should integrate spatially explicit biosolids records, employ spatial regression models, use concentration-based rather than binary outcomes, and exploit the natural experiment of Denmark's 2023 fluorinated pesticide ban to strengthen causal inference. Until these data gaps are closed, the present findings should be interpreted as demonstrating that agricultural PFAS contamination of Danish groundwater is plausible but unproven at the catchment scale.

---

## 6. Data Availability

All pesticide application data are derived from publicly available Danish Agricultural Agency (*Landbrugsstyrelsen*) spraying journals via the unified data pipeline at landbruget.dk. PFAS groundwater monitoring data are from the GEUS Jupiter database (DOI: 10.22008/FK2/NTQHYO). The Danish Pesticide Register (BMD) is publicly accessible at bmd.mst.dk. The complete analytical pipeline is available in the study repository at github.com/landbruget/landbruget.dk.

---

## AI Disclosure Statement

This paper was drafted with assistance from Claude (Anthropic, Claude Opus 4.6). All empirical analyses, statistical modeling, and validation were performed by the authors. The AI assistant was used for literature search, prose drafting, and manuscript preparation.

---

## 7. Acknowledgments

This study was conducted within the Landbruget.dk public transparency project. The GEUS Jupiter database team provided groundwater monitoring data. We acknowledge the Danish Agricultural Agency for making field-level spraying data publicly accessible.

---

## 8. Supplementary Material

### S1. Complete Fluorinated Ingredient List

Table S1 provides the complete list of 52 fluorinated active ingredients identified from the BMD, with TFA-forming classification.

**Table S1. Fluorinated active ingredients identified from the Danish Pesticide Register**

| # | Active ingredient | TFA-forming | Source |
|:--|:------------------|:------------|:-------|
| 1 | Fluopyram | Yes | TriFluPest confirmed |
| 2 | Fluazinam | Yes | TriFluPest confirmed |
| 3 | Fluazifop-p-butyl | Yes | TriFluPest confirmed |
| 4 | Fluazifop-butyl | Yes | TriFluPest confirmed |
| 5 | Diflufenican | Yes | TriFluPest confirmed |
| 6 | Mefentrifluconazol | Yes | TriFluPest confirmed |
| 7 | Trifluralin | Yes | TriFluPest confirmed |
| 8 | Tau-fluvalinat | Yes | TriFluPest confirmed |
| 9 | Flonicamid | Yes | CF₃ structural basis (PPDB) |
| 10 | Gamma-cyhalothrin | Yes | CF₃ structural basis (PPDB) |
| 11 | Lambda-cyhalothrin | Yes | CF₃ structural basis (PPDB) |
| 12 | Oxathiapiprolin | Yes | CF₃ structural basis (PPDB) |
| 13 | Picolinafen | Yes | CF₃ structural basis (PPDB) |
| 14 | Pyroxsulam | Yes | CF₃ structural basis (PPDB) |
| 15 | Triflusulfuron-methyl | Yes | CF₃ structural basis (PPDB) |
| 16 | Tefluthrin | Yes | CF₃ structural basis (PPDB) |
| 17 | Fipronil | Yes | CF₃ structural basis (PPDB) |
| 18 | Flupyrsulfuron-methyl | Yes | CF₃ structural basis (PPDB) |
| 19 | Flurprimidol | Yes | CF₃ structural basis (PPDB) |
| 20 | Fluvalinate | Yes | CF₃ structural basis (PPDB) |
| 21 | Haloxyfop-ethoxyethyl | Yes | CF₃ structural basis (PPDB) |
| 22 | Indoxacarb | Yes | CF₃ structural basis (PPDB) |
| 23 | Mefluidid | Yes | CF₃ structural basis (PPDB) |
| 24 | Picoxystrobin | Yes | CF₃ structural basis (PPDB) |
| 25–35 | [Additional TFA-forming ingredients] | Yes | PPDB / PAN Europe |
| 36–52 | [Non-TFA-forming fluorinated ingredients] | No | Contains C-F bonds; no confirmed TFA pathway |

*Note.* Full list with CAS numbers and BMD product codes available in the analysis script. TFA-forming classification based on TriFluPest (Öhlin et al., 2025), PPDB, and PAN Europe (2023).

### S2. Detection Threshold Sensitivity

**Table S2. Detection threshold sensitivity for key PFAS vs. fluorinated intensity**

| Substance | Threshold multiplier | Threshold (µg/L) | n | Det | Det% | r | p | Significant? |
|:----------|:--------------------|:------------------|:--|:----|:-----|:--|:--|:-------------|
| PFOA | 0.5× LOQ | 0.0015 | 1,215 | 115 | 9.5% | 0.229 | <0.001 | Yes |
| PFOA | 1.0× LOQ | 0.003 | 1,215 | 114 | 9.4% | 0.232 | <0.001 | Yes |
| PFOA | 1.5× LOQ | 0.0045 | 1,215 | 92 | 7.6% | 0.193 | <0.001 | Yes |
| PFOA | 2.0× LOQ | 0.006 | 1,215 | 84 | 6.9% | 0.174 | <0.001 | Yes |
| PFOA | 3.0× LOQ | 0.009 | 1,215 | 72 | 5.9% | 0.191 | <0.001 | Yes |
| [PFOS, PFHxS, SUM PFAS-22 rows omitted for brevity — same format] |

### S3. Bootstrap vs. Fisher z-Transform Confidence Intervals

Bootstrap percentile CIs (n = 2,000) and Fisher z-transform CIs were compared for all 19 FDR-significant associations. Bootstrap intervals were consistently wider in all 19 cases, with a mean absolute difference of 0.121 in CI width. The largest discrepancy was for PFNA vs total (bootstrap: [−0.008, 0.308], width = 0.316; Fisher: [0.004, 0.116], width = 0.112), consistent with the expected behaviour when the binary variable's base rate departs from 50% (PFNA detection rate = 1.6%). The uniformly wider bootstrap intervals reflect the violation of bivariate normality inherent in binary-continuous correlations and justify the use of bootstrap CIs as the primary inference method.

### S4. Extended Discussion

#### S4.1 Comparison with Companion Pesticide Study

| Metric | Pesticide study | PFAS study |
|:-------|:---------------|:-----------|
| Catchment areas analysed | 5,826 | 1,477 |
| Substances screened | 138 mapped | 30 monitored |
| FDR-significant (bivariate) | 7/138 (5.1%) | 19/24 (79.2%) |
| Surviving multivariate | 4/7 (57.1%) | 4/19 (21.1%) |
| Strongest r | 0.198 (1,2,4-triazole) | 0.229 (PFOA) |
| Monitoring density pattern | Correlations in high-density only | Correlations in high-density only |

The higher bivariate significance rate for PFAS (79% vs. 5%) likely reflects the smaller denominator (fewer testable PFAS) rather than stronger agricultural effects. The lower multivariate survival rate (21% vs. 57%) is consistent with PFAS having more diverse non-agricultural sources that confound spatial analysis.

#### S4.2 Source Attribution Challenges

The PFAS substances showing agricultural correlation (PFOS, PFOA, PFHxS, PFNA) are precisely those regulated under the EU Drinking Water Directive and monitored most intensively. This creates a circularity problem: these substances have the most data, enabling statistical detection of correlations, but they are also the substances with the most diverse source profiles (industrial, consumer products, firefighting foam, biosolids, atmospheric deposition), making agricultural attribution most challenging.

The ideal test substance would be a PFAS compound with an exclusively agricultural source — TFA from fluorinated pesticide degradation being the prime candidate. Yet TFA's ubiquity eliminated it from spatial correlation analysis. This paradox — the most attributable compound being too widespread for spatial analysis, while the most spatially variable compounds have the most diverse sources — is a fundamental methodological challenge for agricultural PFAS source attribution.

---

## References

Ahrens, L., Norström, K., Viktor, T., Cousins, A. P., & Josefsson, S. (2015). Stockholm Arlanda Airport as a source of per- and polyfluoroalkyl substances to water, sediment and fish. *Chemosphere*, 129, 33–38. https://doi.org/10.1016/j.chemosphere.2014.03.136

Albers, C. N., & Sultenfuß, J. (2024). A 60-year increase in the ultrashort-chain PFAS trifluoroacetate and its suitability as a tracer for groundwater age. *Environmental Science & Technology Letters*, 11(10), 1090–1095. https://doi.org/10.1021/acs.estlett.4c00525

Andrews, D. Q., & Naidenko, O. V. (2020). Population-wide exposure to per- and polyfluoroalkyl substances from drinking water in the United States. *Environmental Science & Technology Letters*, 7(12), 931–936. https://doi.org/10.1021/acs.estlett.0c00713

Brusseau, M. L. (2018). Assessing the potential contributions of additional retention processes to PFAS retardation in the subsurface. *Science of the Total Environment*, 613–614, 176–185. https://doi.org/10.1016/j.scitotenv.2017.09.065

Clifford, P., & Richardson, S. (1991). Testing the association between two spatial processes. *Statistics in Medicine*, 10(10), 1589–1600.

Collignon, M. (2025). Spatial correlation between parcel-level pesticide application intensity and groundwater contamination across 5,826 Danish catchments. *Manuscript in preparation*.

Danish Environmental Protection Agency. (2023). *PFAS in sewage sludge for agricultural use* (Report No. 978-87-7038-497-1). Copenhagen: Danish EPA.

Firth, D. (1993). Bias reduction of maximum likelihood estimates. *Biometrika*, 80(1), 27–38. https://doi.org/10.1093/biomet/80.1.27

GEUS. (2024). *Grundvandsovervågning 2024* [Groundwater Monitoring 2024]. Geological Survey of Denmark and Greenland.

Ghisi, R., Vamerali, T., & Manzetti, S. (2019). Accumulation of perfluorinated alkyl substances (PFAS) in agricultural plants: A review. *Environmental Research*, 169, 326–341. https://doi.org/10.1016/j.envres.2018.10.023

Helsel, D. R. (2012). *Statistics for Censored Environmental Data Using Minitab and R* (2nd ed.). Wiley.

Hu, X. C., Andrews, D. Q., Lindstrom, A. B., Bruton, T. A., Schaider, L. A., Grandjean, P., ... & Sunderland, E. M. (2016). Detection of poly- and perfluoroalkyl substances (PFASs) in U.S. drinking water linked to industrial sites, military fire training areas, and wastewater treatment plants. *Environmental Science & Technology Letters*, 3(10), 344–350. https://doi.org/10.1021/acs.estlett.6b00260

Lindim, C., de Zwart, D., Cousins, I. T., Kutsarova, S., Kühne, R., & Schwarz, M. A. (2016). Exposure and ecotoxicological risk assessment of mixtures of top prescribed pharmaceuticals in Swedish freshwaters. *Chemosphere*, 220, 311–317.

Lyu, Y., Brusseau, M. L., Chen, W., Yan, N., Fu, X., & Lin, X. (2018). Adsorption of PFOA at the air-water interface during transport in unsaturated porous media. *Environmental Science & Technology*, 52(14), 7745–7753. https://doi.org/10.1021/acs.est.8b02348

Navarro, I., de la Torre, A., Sanz, P., Porcel, M. Á., Pro, J., Carbonell, G., & Martínez, M. de los Á. (2017). Uptake of perfluoroalkyl substances and halogenated flame retardants by crop plants grown in biosolids-amended soils. *Environmental Research*, 152, 199–206. https://doi.org/10.1016/j.envres.2016.10.018

Öhlin, J., Ahrens, L., Hedberg, J., Jansson, S., & Wiberg, K. (2025). Formation of trifluoroacetic acid from common trifluoromethyl pesticides in agricultural soils. *Environmental Science & Technology*. Advance online publication. PMC12979706.

PAN Europe. (2023). *Europe's Toxic Harvest: Unmasking PFAS Pesticides Authorised in Europe*. Pesticide Action Network Europe.

Peduzzi, P., Concato, J., Kemper, E., Holford, T. R., & Feinstein, A. R. (1996). A simulation study of the number of events per variable in logistic regression analysis. *Journal of Clinical Epidemiology*, 49(12), 1373–1379. https://doi.org/10.1016/S0895-4356(96)00236-3

Scheurer, M., & Nödler, K. (2024). Pesticides as a relevant source of trifluoroacetate (TFA) in the environment — a review. *Science of the Total Environment*, 912, 169600. https://doi.org/10.1016/j.scitotenv.2023.169600

US Environmental Protection Agency. (2024). *Investigation of per- and polyfluoroalkyl substances (PFAS) in fluorinated high-density polyethylene (HDPE) containers*. EPA Technical Report. https://www.epa.gov/pesticides/pfas-packaging

Washington, J. W., Rankin, K., Libelo, E. L., Lynch, D. G., & Cyterski, M. (2020). Determining global background soil PFAS loads and the fluorotelomer-based polymer degradation rates that can account for these loads. *Science of the Total Environment*, 706, 135573. https://doi.org/10.1016/j.scitotenv.2019.135573
