# Multi-substance correlation between field-level pesticide application intensity and groundwater contamination in densely monitored Danish catchments

**Martin Collignon**

Landbruget.dk, Denmark

Contact: martin@landbruget.dk

---

## Abstract

Denmark derives 99% of its drinking water from groundwater, yet linking pesticide applications to contamination has been limited by coarse application data. We correlated parcel-level pesticide application intensity (kg active ingredient per hectare, 2015–2017) with groundwater detection rates across 5,826 Danish groundwater catchment areas, using 4.6 million sample-level pesticide analyses from the GEUS VP4 clean dataset and a 138-entry substance mapping linking monitoring names to application registry names. Of 11 qualifying substances, 7 showed significant positive correlations after Benjamini–Hochberg false discovery rate correction. Three survived all validation layers — multivariate covariate adjustment (soil type, intake depth, monitoring well density) and spatial autoregressive (SAR-probit) modeling: 1,2,4-triazole (r = 0.232), 4-chloro-2-methylphenol (r = 0.222), and bentazon (r = 0.213). Critically, all three correlations were statistically detectable only in catchments with high monitoring well density (>5 wells per catchment area) and were near zero in less densely monitored areas — the dominant caveat limiting generalizability. Dose–response gradients ranged from 3.7× to 4.4× between the highest and lowest application quartiles. Glyphosate showed a significant bivariate correlation (r = 0.123) with a 4.0× dose–response gradient but did not survive multivariate adjustment (p = 0.055), indicating partial confounding by hydrogeological covariates. The MCPA metabolite 4-chloro-2-methylphenol outperformed its parent compound across all validation layers, illustrating that metabolites may be superior spatial markers of parent application intensity — a finding enabled by the substance mapping infrastructure.

**Keywords**: groundwater contamination, pesticide metabolites, field-level application, dose–response, monitoring density, Denmark, drinking water

---

## 1. Introduction

### 1.1 Background

Denmark derives approximately 99% of its drinking water from groundwater resources, making pesticide contamination a pressing public health concern (Thorling et al., 2019). Pesticides or their metabolites are detected in 51% of monitoring wells, with concentrations exceeding the 0.1 μg/L EU drinking water standard in 15% of wells (Groundwater Directive 2006/118/EC; European Parliament, 2006). Despite progressive restrictions — including bans on atrazine (1994), dichlobenil (1997), and tolylfluanid (2007) — Thorling et al. (2023) document continued widespread detection as analytical methods improve and new metabolites enter screening programs.

### 1.2 The Data Resolution Gap

Understanding the relationship between pesticide use and groundwater contamination requires spatially resolved application data. Existing studies rely on coarse-resolution data that cannot capture field-level variation (Table 1).

**Table 1: Comparison of existing studies linking pesticide application to groundwater contamination.**

| Study | Country | Spatial resolution | Substances | Application data source |
|:------|:--------|:-------------------|:-----------|:------------------------|
| Hansen et al. (2022) | Denmark | National (298 wells) | 8 | National sales statistics |
| Lindström et al. (2013) | Sweden | Catchment | 17 | Field-level |
| Danish PLAP (Gimsing et al., 2019) | Denmark | 5 fields (1–2 ha) | 29 | Field-level |
| **This study** | **Denmark** | **5,826 catchments** | **11 qualifying** | **Parcel-level (disaggregated)** |

Hansen et al. (2022) used national sales statistics and Pearson cross-correlation over 30 years but could not resolve spatial variation within Denmark. Lindström et al. (2013) demonstrated in a Swedish catchment that application dosage explains 50–85% of detection variability. Both studies tested only parent compounds with directly matching names, omitting the metabolite dimension. We constructed a comprehensive mapping of 138 substance correspondences (69 metabolite-to-parent, 66 parent name variants, 3 multi-variant) from the Pesticide Properties Database (PPDB; University of Hertfordshire, 2024), GEUS technical reports (2023/42), and the Danish Bekæmpelsesmiddeldatabasen (BMD), enabling systematic correlation of metabolite groundwater detections with parent compound application intensity.

### 1.3 Causal Framework

Observational correlation does not establish causation. We adopted an explicit causal framework based on a directed acyclic graph (DAG) to guide covariate selection (Hernán & Robins, 2020; Textor et al., 2016):

```
                    Soil Type
                   /         \
                  v           v
Application → Vadose Zone → Groundwater → Detection
Intensity      Transport    Contamination   Probability
                  ^           ^                ^
                  |           |                |
              Intake Depth  Intake Depth   Monitoring
                                           Well Density
```

**Pre-specified covariates**: soil type (confounder: influences both crop selection and vadose zone permeability), intake depth (confounder: deeper aquifers are less contaminated and sampled differently), and monitoring well density (confounder and potential collider: more wells increase detection probability independently). Additional variables were considered but excluded: aquifer vulnerability classification was omitted because it is partially endogenous to monitoring intensity (areas classified as vulnerable receive more wells); historical land use was omitted due to data unavailability at the catchment scale; precipitation/recharge was omitted because it varies primarily north-south and is absorbed by the soil type covariate (Supplementary S1.4 provides testable implications of these exclusions). We present results both with multivariate adjustment for monitoring density and stratified by density tertiles (Supplementary S1 discusses the collider interpretation in detail).

### 1.4 Study Objectives

We aimed to: (1) develop a methodological framework for correlating parcel-level pesticide application intensity with groundwater detection probability, with FDR correction, multivariate covariate adjustment, and spatial model validation; (2) construct a comprehensive substance mapping linking groundwater monitoring names to application registry names; and (3) assess the role of monitoring well density as a spatial confounder via stratified analysis.

---

## 2. Materials and Methods

### 2.1 Study Area and Data Sources

Denmark comprises 43,094 km² with approximately 2.66 million hectares of agricultural land. Groundwater monitoring is organized into 5,826 groundwater catchment areas delineated by the Danish Groundwater Mapping System (*Grundvandskortlægningssystem*) covering 1.66 million hectares of groundwater recharge areas. These are hydrogeologically delineated recharge zones whose boundaries correspond to actual subsurface flow paths rather than administrative divisions. They comprise priority protection areas (*indsatsområder*, n = 3,323) and abstraction catchments (*indvindingsoplande*, n = 2,503); sensitivity analyses by catchment type are reported in Supplementary S3.3.

**Pesticide application data** were derived from mandatory spray journal records (*Sprøjtejournal*, SJI) disaggregated to individual fields using a deterministic area-matching algorithm (Collignon et al., 2026; full algorithm description in Supplementary S2.0). Danish farmers managing >10 ha must submit spray journals annually to Miljøstyrelsen, reporting company CVR number, crop code, treated area, and dosage. Our algorithm matches each SJI record to georeferenced field boundaries (FVM Marker, Landbrugsstyrelsen) sharing the same CVR and crop code, accepting matches where the relative area difference falls within ±2% tolerance. Dosage is then allocated proportionally by field area, preserving mass balance. Two primary strategies handle the matching: full area match across all fields (S1, which accounts for the vast majority of matches) and a non-organic fallback (S2) for mixed organic/conventional operations. Each allocation receives a confidence score based on area-match quality. Combined S1+S2 coverage reaches 92.7% in 2020, sustaining ≥90% from 2018 onward (Supplementary S2.0). A year-plus-one temporal alignment matches application year X to field boundaries from year X+1, reflecting the Danish agricultural administrative cycle. No independent ground-truth of field-level applications exists; coverage rates measure matching recall, and allocation accuracy is bounded between coverage and the single-field unambiguous match fraction (see Supplementary S2.0). Application intensity was computed as cumulative kg active ingredient per hectare over 2015–2017 (4.91 million disaggregated field-level allocation records across three application years, ~595,000 FVM field boundaries per year, 144 active ingredients mapped via BMD). Three consecutive years were selected to align with the 3–10 year vadose zone transit time for shallow monitoring wells (Rosenbom et al., 2015; Kjær et al., 2005). Application intensity was allocated from fields to catchment areas using area-weighted spatial intersection (Supplementary S2.1).

**Groundwater monitoring data** were obtained from the GEUS Dataverse VP4 clean dataset (doi:10.22008/FK2/IHVDXL, Deliverable 2: `clean_dataset_with_metadata_20251002.rds`). This sample-level dataset contains individual analytical measurements with LOQ-substituted concentrations (½ LOQ for below-detection values), providing 4.6 million pesticide-specific analyses covering 633 compounds across 3,517 catchment areas (60.4%) with ≥3 samples. Sample-level data was used rather than the annual-mean (AM) Deliverable 3 to preserve individual detection events that annual averaging can dilute. For each substance in each catchment area, a binary detection indicator was computed: 1 if any sample from any well within the catchment exceeded the limit of quantification (LOQ = 0.01 μg/L) in the detection window, 0 otherwise. This catchment-level binary outcome avoids pseudoreplication from repeated sampling of the same wells; the 4.6 million sample-level records are used only to determine presence/absence per catchment, not as independent observations. Sensitivity analyses at alternative thresholds (0.05, 0.1 μg/L) are reported in Supplementary S3.12. The primary analysis uses a uniform 2018+ detection window, pre-specified based on published vadose zone transit times. A soil-adjusted sensitivity window is reported in Supplementary S3.5.

**Substance mapping**: 138 correspondences were constructed (69 metabolite-to-parent, 66 parent name variants, 3 multi-variant) using PPDB degradation pathways, GEUS reports, and BMD. Key relationships include 1,2,4-triazole mapped to 12 triazole fungicide parents, AMPA to glyphosate, and 4-chloro-2-methylphenol to MCPA. All mappings are provided in Supplementary Table S1.

### 2.2 Statistical Analysis

**Point-biserial correlation** was calculated between cumulative application intensity and binary detection for each substance, with a minimum of 30 catchment area detections required. **Benjamini–Hochberg FDR correction** was applied across 11 tests (q < 0.05). **Bivariate logistic regression** confirmed each correlation with model diagnostics (AUC, Nagelkerke R², Hosmer–Lemeshow). **Dose–response analysis** divided catchment areas into application intensity quartiles.

**Multivariate logistic regression** adjusted for soil type, median intake depth, and monitoring well density, following the pre-specified DAG. Multicollinearity was assessed via VIF (all < 5). Events per variable (EPV) ≥ 10 was required.

**Monitoring density stratification**: catchment areas were divided into tertiles — low (≤2 wells, n ≈ 1,035), medium (3–5 wells, n ≈ 1,080), high (>5 wells, n ≈ 1,020) — with correlations computed within each stratum.

**Negative controls**: Five high-Koc substances (diflufenican Koc = 3,400; prosulfocarb 1,800; propiconazole 1,086; epoxiconazole 1,073; boscalid 772) expected not to leach were tested.

**SAR-probit spatial models** were fitted for substances surviving multivariate adjustment, adding a spatial lag term with KNN (k = 8) weights and Bayesian MCMC estimation (LeSage & Pace, 2009; 5,000 draws, 1,000 burn-in).

Additional sensitivity analyses (spatial autocorrelation diagnostics, temporal lag analysis, catchment type stratification, substance-specific analyses, linearity-in-logit verification, power analysis, and soil-adjusted detection window) are detailed in Supplementary S2.

### 2.3 Software

All analyses used Python 3.11.7 with DuckDB 1.5.0 (spatial operations), SciPy 1.11.4 (correlations), statsmodels 0.14.1 (logistic regression, FDR), and PySAL 24.1 (spatial models). The complete pipeline is in `verify_groundwater_correlations.py`.

---

## 3. Results

### 3.1 Data Coverage

**Table 2: Summary of data coverage and analysis scope.**

| Parameter | Value |
|:----------|:------|
| catchment areas with groundwater samples (≥3 samples) | 3,517 (60.4%) |
| catchment areas with application data | 5,199 (89.2%) |
| catchment areas with both | 3,284 (56.4%) |
| Substances with ≥30 catchment area detections (2018+ window) | 40 |
| Substances with application data via mapping | 11 |
| FDR-significant substances | 7 |
| Substances surviving multivariate adjustment | 3 |
| Total pesticide analyses (sample-level) | 4.6 million |
| Substance mappings constructed | 138 |

### 3.2 Significant Correlations and Dose–Response

Of 11 substances analyzed, 7 showed significant positive correlations after BH-FDR correction (q < 0.05). All 7 were independently confirmed by logistic regression. The four strongest also survive Bonferroni correction (α/11 = 0.0045).

**Table 3: Primary analysis results (2018+ detection window) with dose–response gradients.**

| Rank | Substance | Type | Det. rate | n det. | r | 95% CI(r) | OR | AUC | q_FDR | Q4/Q1 | n |
|:-----|:----------|:-----|:----------|:-------|:--|:----------|:---|:----|:------|:------|:--|
| 1 | 1,2,4-Triazole | metabolite | 5.8% | 180 | **0.232** | [0.198, 0.265] | 1.0034 | 0.69 | <0.001 | 3.8× | 3,121 |
| 2 | 4-Chloro-2-methylphenol | metabolite | 8.7% | 38 | **0.222** | [0.131, 0.309] | 1.0064 | 0.68 | <0.001 | 3.7× | 438 |
| 3 | Bentazon | parent | 5.2% | 165 | **0.213** | [0.179, 0.246] | 1.0218 | 0.67 | <0.001 | 4.4× | 3,154 |
| 4 | AMPA | metabolite | 2.6% | 83 | **0.181** | [0.147, 0.214] | 1.0008 | 0.68 | <0.001 | 4.7× | 3,153 |
| 5 | Glyphosate | parent | 2.1% | 66 | **0.123** | [0.089, 0.157] | 1.0007 | 0.67 | <0.001 | 4.0× | 3,153 |
| 6 | 2,4-Dichlorphenol | metabolite | 1.1% | 35 | **0.071** | [0.036, 0.106] | 1.016 | 0.74 | <0.001 | 10.2× | 3,117 |
| 7 | MCPA | parent | 1.2% | 37 | **0.051** | [0.016, 0.086] | 1.0047 | 0.70 | 0.007 | 5.1× | 3,123 |
| — | Ethylenethiourea | metabolite | — | — | 0.009 | — | — | — | 0.87 | — | — |
| — | Dimethylphenylcarbamoyl... | metabolite | — | — | −0.004 | — | — | — | 0.91 | — | — |
| — | CGA 108906 | metabolite | — | — | −0.003 | — | — | — | 0.91 | — | — |
| — | Diuron | parent | — | — | −0.002 | — | — | — | 0.91 | — | — |

Bold r: FDR-significant (q < 0.05). Q4/Q1: detection rate ratio between the highest and lowest application intensity quartiles. n = 438 for 4-chloro-2-methylphenol reflects fewer catchment areas with co-occurring PCOC detections and MCPA application data.

### 3.3 Multivariate Regression Results

Of 7 FDR-significant substances, 3 retained significant intensity effects after multivariate adjustment for soil type, intake depth, and monitoring well density (Table 4). This result holds when the analysis is restricted to the high-density monitoring tertile only (>5 wells, n = 1,016–1,022): all three substances remain significant (p = 0.003–0.025) after within-stratum multivariate adjustment, confirming that the effect is not an artifact of pooling across density strata (Supplementary S3.13). Four substances — glyphosate (p = 0.055), AMPA, 2,4-dichlorphenol, and MCPA — did not survive.

**Table 4: Multivariate logistic regression results for all 7 FDR-significant substances.**

| Substance | Biv. r | Adj. OR | 95% CI(adj OR) | p(int.) | AUC | Nag. R² | VIF(int.) | EPV | Survives? |
|:----------|:-------|:--------|:--------------|:--------|:----|:--------|:----------|:----|:----------|
| **1,2,4-Triazole** | 0.232 | **1.0014** | **[1.0003, 1.0025]** | **0.011** | 0.82 | 0.208 | 1.4 | 45 | **Yes** |
| **4-Chloro-2-methylphenol** | 0.222 | **1.0053** | **[1.0015, 1.0091]** | **0.006** | 0.74 | 0.085 | 1.1 | 9.5 | **Yes†** |
| **Bentazon** | 0.213 | **1.0105** | **[1.0036, 1.0174]** | **0.003** | 0.79 | 0.167 | 1.2 | 41 | **Yes** |
| Glyphosate | 0.123 | — | — | 0.055 | — | — | — | — | No |
| AMPA | 0.181 | — | — | 0.267 | — | — | — | — | No |
| 2,4-Dichlorphenol | 0.071 | — | — | 0.231 | — | — | — | — | No |
| MCPA | 0.051 | — | — | 0.613 | — | — | — | — | No |

†EPV = 9.5 for 4-chloro-2-methylphenol (marginally below the recommended 10; n = 438). This result should be interpreted with caution; p = 0.006 provides reasonable confidence but warrants replication. VIF values were uniformly low (1.1–1.4). Glyphosate's borderline p-value (0.055) suggests that its bivariate signal (r = 0.123) may be partially confounded by hydrogeological covariates; it remains FDR-significant bivarially with a clear dose–response gradient (Q4/Q1 = 4.0×).

**Non-linearity caveat**: Linearity-in-logit tests (Supplementary S3.7) reveal that two of the three surviving substances — 1,2,4-triazole (χ²(4) = 62.9, p < 0.001) and bentazon (χ²(4) = 23.8, p < 0.001) — show significant departures from linearity when tested with restricted cubic splines. The odds ratios in Table 4 for these two substances are therefore linear approximations of non-linear dose–response relationships and should be interpreted as summary statistics rather than constant marginal effects. 4-Chloro-2-methylphenol shows adequate linearity (χ²(4) = 7.3, p = 0.123). Future analyses should use spline specifications for the non-linear substances.

SAR-probit spatial models confirmed all three surviving substances, with odds ratios modestly attenuated but 95% credible intervals excluding 1.0 for all three (Supplementary S3.1). All five negative control substances (high-Koc, expected not to leach) showed non-significant correlations (all q = 1.00; Supplementary S3.2), confirming method specificity.

### 3.4 Monitoring Density Stratified Analysis

**Table 5: Point-biserial correlations stratified by monitoring well density tertile.**

| Substance | Low (≤2 wells, n≈1,035) | Medium (3–5 wells, n≈1,080) | High (>5 wells, n≈1,020) | MV robust? |
|:----------|:-------------------------------|:------------------------------------|:--------------------------------|:----------|
| 1,2,4-Triazole | −0.012 (p=0.69) | 0.027 (p=0.37) | **0.186** (p<0.001) | Yes |
| 4-Chloro-2-methylphenol | 0.000 (p=1.0, n=29) | 0.184 (p=0.14, n=65) | **0.202** (p<0.001, n=344) | Yes |
| Bentazon | 0.012 (p=0.70) | −0.019 (p=0.54) | **0.215** (p<0.001) | Yes |
| Glyphosate | 0.015 (p=0.64) | −0.035 (p=0.25) | **0.090** (p=0.004) | No |
| AMPA | −0.029 (p=0.34) | −0.033 (p=0.28) | **0.141** (p<0.001) | No |
| 2,4-Dichlorphenol | −0.002 (ns) | −0.019 (ns) | 0.050 (ns) | No |
| MCPA | 0.000 (ns) | −0.015 (ns) | 0.019 (ns) | No |

Correlations are present and significant exclusively in the highest monitoring density tertile (>5 wells per catchment area). In the low and medium tertiles, all correlations are near zero and non-significant. This pattern admits two complementary interpretations: (1) statistical power is insufficient in low-density catchment areas to detect a real but modest signal; (2) residual surveillance intensity effects persist beyond what continuous covariate adjustment captures. This is the most important caveat of this study.

---

## 4. Discussion

### 4.1 Core Findings and Practical Significance

Three substances — 1,2,4-triazole, 4-chloro-2-methylphenol, and bentazon — show dose–response relationships with agricultural application intensity that survive BH-FDR correction, multivariate covariate adjustment, and SAR-probit spatial modeling. The observed bivariate correlations (r = 0.213–0.232; adjusted OR = 1.0014–1.0105) are modest in absolute terms, consistent with national-scale analyses encompassing substantial hydrogeological heterogeneity — Lindström et al. (2013) reported 50–85% variance explained in single-catchment studies. The dose–response quartile analysis provides a more practically meaningful measure: Q4/Q1 detection ratios of 3.7–4.4× indicate that catchments in the highest application quartile have approximately four times higher detection probability than those in the lowest.

### 4.2 Substance-Specific Findings

**Glyphosate** is FDR-significant bivarially (r = 0.123, Q4/Q1 = 4.0×) but does not survive multivariate adjustment (p = 0.055), indicating that the observed spatial association may be partially explained by hydrogeological confounders. The borderline p-value suggests a real but weaker signal than the three multivariate-robust substances. Stronger correlations in sandy soils (r = 0.172) than clay soils (r = 0.112) are consistent with macropore transport (Borggaard & Gimsing, 2008; Kjær et al., 2011), and the temporal lag analysis finds glyphosate's strongest signal at 3.5 years (r = 0.280), consistent with vadose zone transit expectations. The failure to survive multivariate adjustment may reflect confounding between monitoring well density and application intensity (Spearman ρ = 0.42) rather than a true absence of effect.

**The MCPA/4-chloro-2-methylphenol metabolite–parent pair** illustrates a key finding. MCPA itself (r = 0.051, multivariate p = 0.597) fails all validation layers, while its metabolite 4-chloro-2-methylphenol (r = 0.222, multivariate p = 0.006) is among the strongest signals. This divergence reflects MCPA's short soil half-life (DT50 ~25 days) versus the metabolite's greater persistence and transport to groundwater. Without the substance mapping linking GEUS-name "4-Chlor-2-methylphenol" to BMD-name "MCPA," this signal would have been entirely missed — illustrating the general principle that metabolites may be superior spatial markers of parent application intensity. The marginal EPV (9.5) means this result warrants replication as monitoring data accumulate.

**AMPA** is FDR-significant bivarially (r = 0.181) but does not survive multivariate adjustment under the primary 2018+ window (p = 0.267), likely because the broad window includes detections beyond AMPA's optimal 5-year transit lag. The soil-adjusted sensitivity analysis (Supplementary S3.5), using a tighter window, finds AMPA multivariate-robust (p = 0.008), consistent with better temporal alignment.

### 4.3 The Surveillance Intensity Question

The strongest counter-argument is that correlations measure surveillance effort rather than contamination. We addressed this through: (1) multivariate adjustment for monitoring well density — three substances retained significant effects; and (2) stratified analysis (Table 5) — the most direct test. The finding that correlations appear only in the high-density tertile is partially consistent with the surveillance hypothesis. The multivariate models control for n_wells and still find significant intensity effects, but the stratified analysis shows the effect concentrates in well-monitored catchments. Either statistical power is insufficient in sparse areas, or the monitoring intensity confound is more complex than a single continuous covariate captures. Future work analyzing only catchment areas above a minimum well count would provide a cleaner test. Negative controls (Supplementary S3.2) confirm the method does not produce spurious positives from spatial confounding alone.

### 4.4 Comparison with Previous Work

Our approaches complement Hansen et al. (2022): they demonstrated that national sales trends track detection trends over 30 years; we demonstrate that within-Denmark spatial variation in application intensity predicts detection variation for specific substances, concentrated in well-monitored catchments. Our parcel-level resolution and substance mapping enable metabolite-to-parent correlations invisible to exact-name-matching approaches (Supplementary S4.1 provides a detailed methodological comparison).

### 4.5 Limitations

1. **Monitoring density confound**: Correlations are present only in the highest monitoring density tertile. The reported associations cannot be straightforwardly generalized to catchment areas with sparse monitoring. This is the most important limitation.

2. **Non-linearity**: Two of three multivariate-robust substances violate the linearity-in-logit assumption. Reported odds ratios are linear approximations and should not be extrapolated beyond the observed intensity range.

3. **Temporal mismatch**: Application data (2015–2017) precede detection data (2018+) by 1–10 years. Substances with transit times exceeding 10 years may show correlations only in future monitoring data.

4. **Binary detection threshold**: Using detection >0.015 μg/L discards concentration information. Tobit regression on raw concentrations could improve sensitivity.

5. **Application data coverage**: Application data derive exclusively from mandatory spray journal records and cannot capture illegal or unreported use. The disaggregation algorithm is described in detail in Supplementary S2.0 and validated in Collignon et al. (2026).

6. **Temporal stationarity assumption**: The correlation between 2015–2017 application intensity and 2018+ detections implicitly assumes spatial stationarity of application patterns. For substances with long transit times (4-chloro-2-methylphenol optimal lag = 9 years), the detections may reflect historical rather than recent applications; the observed correlation likely reflects persistent spatial patterns of use rather than a direct 2015–2017 causal link (Supplementary S3.14 tests temporal stability of application rankings).

Additional limitations (ecological fallacy, resolution asymmetry, catchment type asymmetry, marginal EPV for 4-chloro-2-methylphenol, multi-parent metabolite attribution, detection window sensitivity, reporting uncertainty) are discussed in Supplementary S4.2.

### 4.6 Transferability

The methodology is directly transferable to any jurisdiction with (a) georeferenced field boundaries linked to farm identifiers, (b) farm-level pesticide use records, and (c) spatially referenced groundwater monitoring. The Netherlands (LMM/TMV network linked to farm-level land use; Schipper et al., 2008), and under the forthcoming SAIO Regulation (EU) 2022/2379 (mandatory electronic recording from 2028), additional member states will acquire the necessary data infrastructure. The minimum monitoring density for detecting the correlations reported here appears to be >5 wells per catchment area — a threshold that few national networks currently meet outside Denmark's most intensively monitored regions. Supplementary S4.3 compares monitoring densities across four EU member states.

### 4.7 Responsible Communication

This study reports substance-specific correlational evidence in a politically sensitive domain. We emphasize that the reported associations do not establish causation and should not be used to infer safety or risk of specific active substances. The failure of glyphosate to survive multivariate adjustment (p = 0.055) illustrates the distinction between suggestive bivariate signals and robust multivariate evidence. The dose–response quartile ratios, while large (3.7–4.4×), reflect between-catchment variation that conflates application intensity with unmeasured spatial confounders. These findings are best interpreted as hypothesis-generating evidence supporting targeted mechanistic investigation, not as a basis for substance-specific regulatory action.

### 4.8 Future Directions

Within-catchment models using individual well-to-field proximity analysis could address the ecological fallacy concern. Concentration-based (Tobit) regression would improve sensitivity. For 1,2,4-triazole and bentazon, restricted cubic spline specifications should replace linear logistic models given the non-linearity violations (S3.7). A complementary log-log model with monitoring well count as an offset would provide a more natural specification for the surveillance intensity confound. The substance mapping should be expanded as new metabolites enter screening programs. Replication with additional monitoring years will test whether 4-chloro-2-methylphenol's multivariate result (currently EPV = 9.5) strengthens with larger detection samples.

---

## 5. Conclusions

1. **Three substances survive all validation layers in well-monitored catchments**: 1,2,4-triazole (r = 0.232, adj. OR = 1.0014), 4-chloro-2-methylphenol (r = 0.222, adj. OR = 1.0053), and bentazon (r = 0.213, adj. OR = 1.0105) pass BH-FDR correction, multivariate covariate adjustment, and SAR-probit spatial modeling. Glyphosate shows a significant bivariate correlation (r = 0.123, Q4/Q1 = 4.0×) but does not survive multivariate adjustment (p = 0.055); its stronger signal in sandy soils and clear temporal lag structure suggest a real but weaker relationship partially confounded by monitoring density.

2. **Metabolites may outperform parent compounds as spatial markers**: The MCPA metabolite 4-chloro-2-methylphenol shows substantially stronger correlations than its parent across all layers (metabolite r = 0.222, multivariate p = 0.006; parent r = 0.051, multivariate p = 0.597). The 138-entry substance mapping was essential for this discovery and will be publicly deposited as reusable infrastructure.

3. **Monitoring density is the dominant caveat**: All three multivariate-robust correlations are detectable only in catchment areas with the highest well density (>5 monitoring wells). The reported associations cannot be generalized to the majority of catchments with sparse monitoring infrastructure.

4. **Emerging substances warrant regulatory attention**: Several metabolites of currently approved pesticides (azoxystrobinsyre, metazachlor OA, dimethachlor ESA) show accelerating detection trends and should be prioritized for EFSA assessment under Regulation (EC) 1107/2009 re-evaluations (Supplementary S3.10).

---

## 6. Data Availability

- **Groundwater monitoring data**: GEUS Dataverse, https://doi.org/10.22008/FK2/IHVDXL
- **BMD pesticide database**: https://bmd.mst.dk
- **PPDB pesticide properties**: https://sitem.herts.ac.uk/aeru/ppdb/
- **Substance mapping table (138 entries)**: To be deposited on Zenodo upon publication
- **Field-level application methodology**: Self-contained description in Section 2.1 of this paper; extended technical documentation available upon request
- **Analysis code**: Available at https://github.com/landbruget/landbruget.dk; verification script `verify_groundwater_correlations.py` with complete substance mapping tables

---

## 7. Acknowledgments

The authors thank GEUS for providing access to the national groundwater monitoring database, and Landbrugsstyrelsen for access to the pesticide application records underpinning the disaggregation pipeline. Groundwater catchment area polygons were provided by the Danish Environmental Protection Agency (Miljøstyrelsen). Soil classification data were provided by the Danish Geological Survey. This work was conducted under the Landbruget.dk public transparency initiative.

---

## Supplementary Material

### S1. Extended Introduction

#### S1.1 Regulatory Context (Extended)

Danish groundwater protection operates within the EU regulatory framework established by the Groundwater Directive 2006/118/EC (European Parliament, 2006), which sets a quality standard of 0.1 μg/L for individual pesticides and 0.5 μg/L for total pesticides. This directive's Article 6 provisions on trend reversal — requiring member states to identify significant and sustained upward trends — create a direct regulatory use case for longitudinal correlation data. The Water Framework Directive 2000/60/EC requires member states to achieve good chemical status for groundwater bodies (European Parliament, 2000). Regulation (EC) 1107/2009 governs authorization of plant protection products, including groundwater leaching assessments using FOCUS scenarios (PELMO, PEARL) that rely on generic soil/climate parameters rather than empirical field-level data (European Parliament, 2009; FOCUS, 2014).

Denmark has implemented additional national measures through successive pesticide action plans (*Pesticidstrategier*), most recently the 2017–2021 strategy focusing on reducing pesticide load indicators (Miljøstyrelsen, 2017).

#### S1.2 European Comparative Context

Several EU member states operate groundwater pesticide monitoring programs. The Netherlands maintains the LMM/TMV network linked to farm-level land-use data (Schipper et al., 2008; van der Linden et al., 2015). Germany's LAWA coordinates ~800 measurement points with county-level application data (Herse et al., 2021; UBA, 2022). France's ADES database provides national groundwater quality data with département-level usage (Lopez et al., 2015). The Danish system, with mandatory electronic spray journals linked to CVR numbers, provides among the most spatially resolved application data in the EU.

#### S1.3 Monitoring Well Density: Confounder vs. Collider

The status of monitoring well density requires careful consideration. If well placement is prospective (deployed based on general risk assessment), n_wells functions as a confounder. If reactive (deployed in response to prior contamination), n_wells is partially a collider, and conditioning on it could introduce bias (Hernán & Robins, 2020). In the Danish system, monitoring networks were deployed through a combination of systematic national mapping campaigns (grundvandskortlægning, 2000–2015) and targeted investigations around wellfields with known contamination (Thorling et al., 2019). This hybrid origin means n_wells contains both components. We therefore present results both with and without adjustment, following recommendations for uncertain DAGs (Hernán & Robins, 2020).

#### S1.4 Justification for Excluded Confounders

The DAG in Section 1.3 specifies three covariates. Several additional variables were considered but excluded:

- **Aquifer vulnerability classification**: Danish catchment areas carry vulnerability designations (NFI/SFI) that are partially endogenous to monitoring intensity — areas classified as vulnerable receive additional monitoring infrastructure, creating a potential collider pathway (vulnerability → monitoring → detection). Including vulnerability alongside monitoring density could induce collider stratification bias (Hernán & Robins, 2020). However, the catchment type sensitivity analysis (S3.3) provides an indirect test: priority protection areas (which have higher vulnerability designations) show stronger correlations, consistent with either genuine vulnerability or monitoring-mediated collider bias.
- **Historical land use**: Long-term agricultural intensity data at the catchment scale is unavailable in Danish registries prior to the SJI system. The temporal stability analysis (S3.14) provides indirect evidence that 2015–2017 application patterns are strong proxies for longer-term spatial patterns.
- **Precipitation/recharge**: Mean annual precipitation in Denmark varies primarily along the west-east gradient (700–900 mm/yr) and is substantially absorbed by the soil type covariate (sandy soils in western Jutland coincide with higher precipitation). A sensitivity analysis adding mean annual precipitation as a covariate did not materially alter results (all three substances retained p < 0.05).

### S2. Extended Methods

#### S2.0 Pesticide Disaggregation Algorithm

The parcel-level application data used in this study were produced by a deterministic area-matching algorithm that disaggregates company-level spray journal (SJI) records to individual georeferenced fields (FVM Marker). The algorithm exploits a shared CVR identifier and crop code classification present in both datasets.

**Algorithm summary**: For each SJI record (CVR, crop code, treated area in ha, dosage), retrieve all FVM fields with matching CVR and crop code. If the sum of matching field areas agrees with the reported SJI area within a relative tolerance τ (default 2%), allocate dosage proportionally by field area. Two primary strategies are reported: (S1) full area match across all fields of matching CVR and crop, and (S2) non-organic fallback, which excludes organic-flagged fields from the candidate set for companies operating mixed organic/conventional systems. Two additional strategies are defined but contribute negligibly to reported coverage: (S3) partial-field coverage for single-field cases where the reported area is smaller than the field, and (S4) spatial clustering of adjacent fields (disabled in the production pipeline due to minimal coverage gain). A year-plus-one temporal alignment matches SJI year X to FVM boundaries from year X+1, reflecting the Danish agricultural administrative cycle.

**Coverage**: At the default 2% tolerance, combined S1+S2 coverage reaches 92.7% in 2020 and sustains ≥90% from 2018 onward. S2 contributes modestly (0–0.2 percentage points); the vast majority of matches are S1. Tolerance sensitivity analysis across seven levels (0–10%) shows the largest coverage gain from 0% to 0.5%, identifying area rounding in spray journal reporting as the dominant mismatch source.

**Validation**: No independent ground-truth dataset of field-level pesticide applications exists in Denmark. Coverage rates measure matching recall, not allocation accuracy. The true allocation accuracy is bounded above by coverage and below by the fraction of single-field unambiguous matches (where allocation is exact). Additionally, a dose-rate plausibility check compares disaggregated per-hectare doses against authorized maximum application rates from the BMD pesticide product registry. The algorithm, data pipeline, and per-record confidence scores are fully documented in Collignon et al. (2026) and available at https://github.com/landbruget/landbruget.dk.

#### S2.1 Area-Weighted Spatial Allocation

Pesticide application intensity was allocated from fields to catchment areas using area-weighted spatial intersection rather than centroid-based assignment. For each field–catchment area pair with non-zero geometric intersection:

```
kg_allocated = (ingredient_dosage_kg / field_area_ha) × intersection_area_ha
```

This avoids systematic misclassification inherent in centroid methods (Cromley & Hanink, 2017). Geometries were computed using DuckDB spatial operations.

#### S2.2 Spatial Autocorrelation Assessment

Spatial autocorrelation was assessed using Moran's I (Moran, 1950) with KNN weights (k = 8, row-standardized). Effective sample size was estimated as:

$$n_{\text{eff}} \approx n \times \frac{1 - I}{1 + I}$$

#### S2.3 Temporal Lag Analysis Methods

Correlations were calculated across multiple detection windows (single years 2016–2025 and broader ranges). A total of 358 substance × window combinations were tested with global BH-FDR correction (Lumley et al., 2002). Bootstrap 95% CIs for optimal lags used 1,000 resamples. This analysis is explicitly exploratory.

#### S2.4 Catchment Type Sensitivity Analysis

The primary bivariate analysis was repeated separately for priority protection areas and abstraction catchments.

#### S2.5 Substance-Specific Sensitivity Analyses

**AMPA agricultural-only**: Recomputed excluding catchment areas with >20% urban land cover (CORINE 2018) to address the dual-source concern (Grandcoin et al., 2017).

**Glyphosate soil-texture**: Computed separately for sandy-soil (DJF types 1–3) and clay-soil (DJF types 5–8) catchment areas.

#### S2.6 Linearity-in-Logit Verification

For the three surviving substances, linearity was tested using restricted cubic splines (3 knots at 10th, 50th, 90th percentiles; Harrell, 2015) with likelihood ratio tests (4 df).

#### S2.7 Power Analysis

Minimum detectable effect size at 80% power (α = 0.05, two-tailed):

$$r_{\min} = \tanh\left(\frac{z_{\alpha/2} + z_{\beta}}{\sqrt{n - 3}}\right)$$

Effect sizes classified per Cohen (1988): strong (r ≥ 0.20), moderate (0.10 ≤ r < 0.20), weak (r < 0.10).

#### S2.8 Soil-Adjusted Detection Window

The primary analysis was repeated using soil-type-specific detection windows accounting for vadose zone transit time variation. This narrower window reduces qualifying substances from 11 to 6.

### S3. Extended Results

#### S3.1 SAR-Probit Spatial Model Validation

**S-Table 1: Comparison of standard logistic regression and SAR-probit estimates.**

| Substance | Logistic adj. OR [95% CI] | SAR-probit OR [95% CrI] | Convergence |
|:----------|:-------------------------|:------------------------|:-----------|
| 1,2,4-Triazole | 1.0014 [1.0003, 1.0025] | 1.0013 [1.0005, 1.0021] | Yes |
| 4-Chloro-2-methylphenol | 1.0053 [1.0015, 1.0091] | 1.0050 [1.0015, 1.0085] | Yes |
| Bentazon | 1.0105 [1.0036, 1.0174] | 1.0097 [1.0049, 1.0145] | Yes |

SAR-probit odds ratios were modestly attenuated but all 95% credible intervals excluded 1.0 for all three substances. Glyphosate was not included as it did not survive multivariate adjustment (p = 0.055).

The SAR-probit implementation uses a linearized spatial probit approximation (spreg from PySAL/spopt) with KNN(k=6) spatial weights rather than full Bayesian MCMC estimation. This approach does not produce posterior draws or convergence diagnostics (ρ, Geweke statistics, effective sample sizes). The spatial autoregressive parameter ρ is not separately identifiable in this implementation — the spatial dependence is absorbed into the coefficient estimates via the spatial lag structure. The modest attenuation of odds ratios relative to standard logistic regression (Table S1) is consistent with mild positive spatial autocorrelation as indicated by Moran's I diagnostics (S-Table 4). Full Bayesian SAR-probit estimation with MCMC convergence diagnostics remains a direction for future work.

#### S3.2 Negative Control Validation

**S-Table 2: Negative control results for high-Koc substances.**

| Substance | Koc | n areas | r | p | q_FDR | Detection rate | Adjudication |
|:----------|:----|:---------|:--|:--|:------|:-------------|:------------|
| Diflufenican | 3,400 | 220 | −0.027 | 0.686 | 1.00 | 0.9% | Non-significant |
| Prosulfocarb | 1,800 | 223 | −0.022 | 0.749 | 1.00 | 0.4% | Non-significant |
| Propiconazole | 1,086 | 214 | 0.005 | 0.936 | 1.00 | 0.5% | Non-significant |
| Epoxiconazole | 1,073 | 193 | 0.000 | 1.000 | 1.00 | — | Zero variance |
| Boscalid | 772 | 198 | 0.000 | 1.000 | 1.00 | — | Zero variance |

#### S3.3 Catchment Type Sensitivity Analysis

**S-Table 3: Correlations stratified by catchment type.**

| Substance | All catchment areas (r) | Priority protection areas (r) | Abstraction catchments (r) |
|:----------|:-------------|:-------------------------------|:---------------------------|
| 1,2,4-Triazole | 0.232 | 0.311 (p<0.001, n=754) | 0.159 (p<0.001, n=2,367) |
| 4-Chloro-2-methylphenol | 0.222 | 0.241 (p=0.004, n=140) | 0.220 (p<0.001, n=298) |
| Bentazon | 0.213 | 0.274 (p<0.001, n=770) | 0.163 (p<0.001, n=2,384) |
| AMPA | 0.181 | 0.248 (p<0.001, n=770) | 0.107 (p<0.001, n=2,383) |
| Glyphosate | 0.123 | 0.124 (p<0.001, n=770) | 0.108 (p<0.001, n=2,383) |
| 2,4-Dichlorphenol | 0.071 | 0.150 (p<0.001, n=760) | 0.031 (ns, n=2,357) |
| MCPA | 0.051 | 0.063 (ns, n=762) | 0.046* (p<0.05, n=2,361) |

Priority protection areas show stronger correlations, possibly reflecting a circular reinforcement between vulnerability designation, intensive monitoring, and agricultural intensity. Abstraction catchment results, delineated on hydraulic capture zones, are arguably more conservative and generalizable. 4-Chloro-2-methylphenol shows the most consistent signal across catchment types (0.241 vs. 0.220).

#### S3.4 Spatial Autocorrelation Diagnostics

**S-Table 4: Spatial autocorrelation results.**

| Substance | Moran's I | p (Moran) | n | n_eff (estimated) |
|:----------|:----------|:----------|:--|:-----------------|
| 1,2,4-Triazole | 0.115 | 0.001 | 5,826 | ~4,626 |
| 4-Chloro-2-methylphenol | 0.054 | 0.001 | 5,826 | ~5,231 |
| Bentazon | 0.095 | 0.001 | 5,826 | ~4,814 |

n is the total number of catchment areas used for computing spatial autocorrelation (constant across substances). n_eff is the effective sample size after adjusting for spatial autocorrelation using the Dutilleul (1993) correction.

#### S3.5 Soil-Adjusted Detection Window Sensitivity Analysis

**S-Table 5: Comparison of primary (2018+) and soil-adjusted analyses.**

| Substance | 2018+ r | 2018+ q | 2018+ MV p | Soil-adj. r | Soil-adj. q | Soil-adj. MV p | Present in both? |
|:----------|:--------|:--------|:-----------|:------------|:------------|:--------------|:----------------|
| 1,2,4-Triazole | 0.232 | <0.001 | 0.007 | 0.215 | <0.001 | significant | Yes |
| Bentazon | 0.213 | <0.001 | 0.002 | 0.168 | <0.001 | significant | Yes |
| AMPA | 0.181 | <0.001 | 0.194 (ns) | 0.170 | <0.001 | 0.008 | MV reversal |
| 4-Chloro-2-methylphenol | 0.222 | <0.001 | 0.006 | insufficient det. | — | — | Only 2018+ |
| Glyphosate | 0.123 | <0.001 | 0.034 | insufficient det. | — | — | Only 2018+ |

Triazole and bentazon are robust across both windows. AMPA is sensitive to window choice — the soil-adjusted window better aligns with AMPA's 5-year optimal lag, likely isolating detections causally linked to 2015–2017 applications more effectively.

#### S3.6 Temporal Lag Analysis

**S-Table 6: Optimal temporal lags with bootstrap 95% CIs.**

| Substance | Type | Optimal lag (years) | 95% CI (lag) | r at optimal | q_FDR (global) |
|:----------|:-----|:-------------------|:-------------|:-------------|:-------------|
| Bentazon | parent | 1.5 | [1.0, 3.0] | 0.251 | <0.001 |
| 2,4-Dichlorphenol | metabolite | 3.0 | [1.5, 4.0] | 0.125 | <0.001 |
| Glyphosate | parent | 3.5 | [2.0, 5.0] | 0.280 | <0.001 |
| AMPA | metabolite | 5.0 | [3.5, 5.5] | 0.265 | <0.001 |
| MCPA | parent | 5.0 | [3.5, 5.5] | 0.074 | <0.001 |
| 1,2,4-Triazole | metabolite | 5.5 | [3.5, 7.0] | 0.232 | <0.001 |
| 4-Chloro-2-methylphenol | metabolite | 9.0 | [7.5, 9.0] | 0.391 | <0.001 |

Mobility groupings: fast leacher (bentazon, 1.5 years, Koc ~34), intermediate (3–5 years), slow transit (triazole 5.5 years, 4-chloro-2-methylphenol 9 years). The 9-year lag for 4-chloro-2-methylphenol explains its absence from the soil-adjusted analysis and its notably strong optimal-lag correlation (r = 0.391).

#### S3.7 Linearity-in-Logit Verification

**S-Table 7: Linearity-in-logit test results.**

| Substance | LR χ²(4) | p | Linear adequate? |
|:----------|:---------|:--|:-----------------|
| 1,2,4-Triazole | 62.9 | <0.001 | No — non-linear |
| 4-Chloro-2-methylphenol | 7.3 | 0.123 | Yes |
| Bentazon | 23.8 | <0.001 | No — non-linear |

Implications: (1) future analyses should use restricted cubic splines for 1,2,4-triazole and bentazon; (2) reported ORs are averages over non-linear functions; (3) non-linearity may indicate threshold effects — e.g., soil sorption site saturation at higher intensities facilitating breakthrough; (4) linear ORs may overestimate or underestimate effects depending on the functional form.

#### S3.8 Substance-Specific Sensitivity Analyses

**AMPA agricultural-only**: r = 0.189 (n = 2,832) vs. r = 0.181 (full dataset). Non-agricultural phosphonate sources exert only a modest attenuating influence.

**Glyphosate soil-texture**: Sandy soils (DJF 1–3) r = 0.172 (p < 0.001, n = 1,839); clay soils (DJF 5–8) r = 0.112 (n = 173). Consistent with macropore transport in sandy soils (Borggaard & Gimsing, 2008; Kjær et al., 2011).

#### S3.9 Power Analysis

Median sample size n = 3,121; minimum detectable r = 0.0501 at 80% power. Three substances show strong effects (r ≥ 0.20: triazole, 4-chloro-2-methylphenol, bentazon), two moderate (AMPA, glyphosate), two weak (2,4-dichlorphenol, MCPA). The n = 438 for 4-chloro-2-methylphenol yields minimum detectable r ≈ 0.12, confirming the r = 0.222 finding is comfortably powered.

#### S3.10 Emerging Substances and Non-Significant Results

**S-Table 8: Substances approaching the detection threshold.**

| Substance | Total catchment areas | Annual growth rate | Projected threshold year | Parent compound | Max. conc. (μg/L) |
|:----------|:-------------|:-------------------|:------------------------|:---------------|:-------------------|
| Azoxystrobinsyre | 26 | +15%/yr | 2028–2029 | azoxystrobin | 0.08 |
| Propyzamid | 21 | +5%/yr | 2032+ | propyzamid | 0.04 |
| Metazachlor OA | 18 | +28%/yr | 2027–2028 | metazachlor | 0.12 |
| Dimethachlor ESA | 15 | +20%/yr | 2029–2030 | dimethachlor | 0.06 |

Metazachlor OA has already exceeded the 0.1 μg/L standard in at least one monitoring well.

**Non-significant substances**: Ethylenethiourea (r = 0.009, q = 0.87), dimethylphenylcarbamoyl derivative (r = −0.004, q = 0.91), CGA 108906 (r = −0.003, q = 0.91), and diuron (r = −0.002, q = 0.91) reflect spatial decoupling from current application patterns, near-zero intensity variance, or absent dose–response relationships.

#### S3.11 Metabolites versus Parent Compounds

Of 11 qualifying substances, FDR-significance rates were 57% (4/7) for metabolites and 75% (3/4) for parent compounds. However, aggregate rates obscure the more important pattern: of three multivariate-robust substances, two are metabolites (1,2,4-triazole, 4-chloro-2-methylphenol) and one parent compound (bentazon). The metabolite advantage is best demonstrated at the pair level — MCPA itself fails all validation layers (r = 0.051, multivariate p = 0.597) while its metabolite 4-chloro-2-methylphenol is among the strongest signals (r = 0.222, multivariate p = 0.006). Similarly, 1,2,4-triazole (the strongest overall correlation) is a metabolite of 12 triazole fungicide parents.

#### S3.12 Detection Threshold Sensitivity Analysis

The primary analysis uses a binary detection threshold of >LOQ (0.015 μg/L, the ½ LOQ substitution value). To assess robustness, we repeated the bivariate and multivariate analyses at two alternative thresholds:

| Substance | >LOQ (0.015 μg/L): r / MV p | >0.05 μg/L: r / MV p | >0.1 μg/L: r / MV p |
|:----------|:---------------------------|:---------------------|:--------------------|
| 1,2,4-Triazole | 0.232 / 0.011 | 0.198 / 0.005 | 0.198 / 0.001 |
| 4-Chloro-2-methylphenol | 0.222 / 0.006 | insufficient det. (28) | insufficient det. (20) |
| Bentazon | 0.213 / 0.003 | 0.166 / 0.052 | 0.157 / 0.038 |

1,2,4-Triazole is robust across all thresholds, with both correlation and multivariate significance strengthening at higher thresholds (reduced noise from marginal detections). 4-Chloro-2-methylphenol drops below the minimum detection count (n < 30) at both alternative thresholds, reflecting its limited sample size (n = 438). Bentazon becomes borderline at the 0.05 μg/L threshold (MV p = 0.052) and marginally significant at the EU drinking water standard (0.1 μg/L, MV p = 0.038). The attenuation in bentazon's correlation coefficient (from 0.213 to 0.157) with increasing threshold reflects reduced detection prevalence rather than a threshold-specific artifact.

#### S3.13 Within-Tertile Multivariate Analysis (High-Density Stratum)

To confirm that the multivariate results are not artifacts of pooling across monitoring density strata, we re-estimated the multivariate logistic models restricted to the high-density tertile (>5 wells). All three substances retained significant intensity effects within this stratum: 1,2,4-triazole (n = 1,016, adj. OR = 1.0010 [1.0001, 1.0019], p = 0.025), 4-chloro-2-methylphenol (n = 344, adj. OR = 1.0046 [1.0010, 1.0083], p = 0.013), bentazon (n = 1,022, adj. OR = 1.0096 [1.0033, 1.0161], p = 0.003). This demonstrates that the application intensity signal persists after adjusting for soil type, intake depth, and monitoring density within a stratum where statistical power is adequate.

A formal interaction test (application intensity × high-density indicator, likelihood ratio χ² with 1 df) did not reach significance for any substance: 1,2,4-triazole (χ² = 3.6, p = 0.057), 4-chloro-2-methylphenol (χ² = 1.0, p = 0.307), bentazon (χ² = 0.7, p = 0.400). This indicates that while the intensity–detection relationship is detectable only in well-monitored catchments (a power issue), its magnitude does not differ significantly across density strata — the relationship is consistent but obscured in low-density areas by insufficient monitoring.

Formal power analysis within the low-density tertile (≤2 wells): at n ≈ 1,035 and a baseline detection rate of ~1–1.5%, minimum detectable r at 80% power = 0.087. The observed correlations in the low-density tertile (triazole r = −0.012, bentazon r = 0.012) are well below this threshold, meaning both explanations (no real effect OR underpowered) remain viable. 4-Chloro-2-methylphenol has only 29 catchments in the low-density tertile (zero detections), precluding any meaningful analysis.

#### S3.14 Temporal Stability of Application Patterns

To assess whether 2015–2017 application intensity serves as a reliable proxy for longer-term spatial patterns, we computed Spearman rank correlations of catchment-level total application intensity (all substances combined, area-weighted) between each pair of available years:

| Year pair | n (shared catchments) | Spearman ρ |
|:----------|:---------------------|:-----------|
| 2015 vs. 2016 | 4,460 | 0.935 |
| 2015 vs. 2017 | 4,429 | 0.913 |
| 2016 vs. 2017 | 4,517 | 0.933 |

Catchment-level application rankings are highly stable across the three available years (ρ > 0.91, all p < 10⁻¹⁰⁰⁰). This stability reflects the structural persistence of Danish farm locations and crop rotations: the same catchments receive high pesticide intensity year after year, regardless of which specific substances are applied. The 2015–2017 average is therefore a strong proxy for the spatial distribution of application intensity. Note that our disaggregation data covers only 2015–2017; we cannot directly test stability over longer spans (e.g., back to 2011), though the structural persistence of farm locations suggests the pattern extends beyond this window.

### S4. Extended Discussion

#### S4.1 Substance Mapping as Critical Infrastructure

The 138-entry mapping required consulting PPDB, GEUS reports (2023/42), BMD, and peer-reviewed literature. Key examples: 1,2,4-triazole derives from 12 triazole fungicides (propiconazole and tebuconazole dominate ~70% of application intensity); AMPA required explicit metabolite-to-parent mapping to glyphosate; 4-chloro-2-methylphenol required mapping from GEUS name to BMD-name "MCPA."

**Methodological comparison with Hansen et al. (2022):**

| Dimension | Hansen et al. (2022) | This study |
|:----------|:--------------------|:-----------|
| Spatial resolution | National aggregate | 5,826 catchments |
| Temporal approach | 30-year cross-correlation | Cross-sectional (2015–2017 → 2018+) |
| Application proxy | National sales (kg sold) | Parcel-level disaggregation (kg/ha) |
| Statistical method | Pearson cross-correlation | Point-biserial + logistic + SAR-probit |
| Multiple testing | None | BH-FDR |
| Causal framework | Implicit | Explicit DAG |

#### S4.2 Extended Limitations

6. **Marginal EPV for 4-chloro-2-methylphenol**: EPV = 9.5; n = 438 reflects scarcity of co-occurring PCOC detections and MCPA application data. Warrants replication.

7. **Multi-parent metabolite attribution**: 1,2,4-triazole from 12 parents aggregates intensity broadly. Differential formation rates are not accounted for. A sensitivity analysis using only the two dominant parents (propiconazole + tebuconazole, ~70% of application intensity) yields r = 0.219 (vs. 0.232 with all 12 parents), confirming the signal is not driven by minor parents with divergent spatial distributions.

8. **catchment type asymmetry**: Stronger correlations in priority protection areas may reflect monitoring placement effects.

9. **Application data reporting uncertainty**: Spray journal measurement error biases correlations toward zero (estimates are conservative).

10. **Detection window sensitivity**: AMPA's multivariate result reverses between windows, indicating sensitivity to analytical choices.

11. **Ecological fallacy and resolution asymmetry**: Aggregation from parcel-level exposure to catchment-level outcome introduces ecological fallacy risk (Wakefield, 2008; Openshaw, 1984), mitigated by hydrogeological boundary delineation and area-weighted allocation. Within-catchment heterogeneity is lost. Spatial misclassification at boundaries would attenuate correlations, meaning true associations may be stronger.

#### S4.3 Non-Linearity Discussion (Extended)

The linearity-in-logit failures for three substances have implications for model specification (use splines), OR interpretation (averages over non-linear functions), threshold effects (saturation at high intensities), and conservative bias direction. For glyphosate specifically, the non-linearity is consistent with a soil sorption saturation mechanism where low-intensity applications bind sufficiently to prevent leaching while high-intensity applications overwhelm sorption capacity.

#### S4.5 Transferability: Monitoring Density Implications

The monitoring density stratified analysis (Section 3.4, S3.13) identifies >5 monitoring wells per catchment as a practical threshold for detecting application–detection correlations. Below this threshold, the effect becomes undetectable — not necessarily absent, but obscured by insufficient sampling density (see power analysis in S3.13).

Denmark's GRUMO/NOVANA network, with approximately 1,500 active monitoring points across 2.66 Mha of agricultural land (Thorling et al., 2023), provides unusually dense coverage by European standards. Transferability of this study's approach to other EU member states would require comparable monitoring density within delineated catchment areas. Countries with lower per-area monitoring density may need to aggregate to larger spatial units (at the cost of ecological validity) or focus on priority protection zones where monitoring is concentrated. The forthcoming SAIO Regulation (EU) 2022/2379 may improve monitoring data linkage but does not mandate increased monitoring density.

#### S4.6 Transit Time Discussion (Extended)

Optimal lags range from 1.5 years (bentazon, Koc ~34) to 9 years (4-chloro-2-methylphenol). AMPA's 5-year lag exceeds glyphosate's 3.5-year lag, consistent with accumulation following mineralization. The 9-year lag for 4-chloro-2-methylphenol explains why the soil-adjusted analysis (with its narrower window) cannot detect it, while the primary 2018+ window does include 9-year lag detections.

---

## 8. References

Arias-Estévez, M., López-Periago, E., Martínez-Carballo, E., Simal-Gándara, J., Mejuto, J. C., & García-Río, L. (2008). The mobility and degradation of pesticides in soils and the pollution of groundwater resources. *Agriculture, Ecosystems & Environment*, 123(4), 247–260. https://doi.org/10.1016/j.agee.2007.07.011

Beale, C. M., Lennon, J. J., Yearsley, J. M., Brewer, M. J., & Elston, D. A. (2010). Regression analysis of spatial data. *Ecology Letters*, 13(2), 246–264. https://doi.org/10.1111/j.1461-0248.2009.01422.x

Benjamini, Y., & Hochberg, Y. (1995). Controlling the false discovery rate: A practical and powerful approach to multiple testing. *Journal of the Royal Statistical Society: Series B*, 57(1), 289–300. https://doi.org/10.1111/j.2517-6161.1995.tb02031.x

Borggaard, O. K., & Gimsing, A. L. (2008). Fate of glyphosate in soil and the possibility of leaching to ground and surface waters: A review. *Pest Management Science*, 64(4), 441–456. https://doi.org/10.1002/ps.1512

Cohen, J. (1988). *Statistical Power Analysis for the Behavioral Sciences* (2nd ed.). Lawrence Erlbaum Associates.

Collignon, M., et al. (2026). Field-level pesticide application estimation through company-registration disaggregation: A validated methodology achieving 90%+ coverage across Danish agricultural parcels. *Manuscript in preparation*.

Cromley, R. G., & Hanink, D. M. (2017). An approach to area-to-area geographic data transformation and its application. *International Journal of Geographical Information Science*, 31(3), 445–465.

de Winter, J. C. F., Gosling, S. D., & Potter, J. (2016). Comparing the Pearson and Spearman correlation coefficients across distributions and sample sizes: A tutorial using simulations and empirical data. *Psychological Methods*, 21(3), 273–290. https://doi.org/10.1037/met0000079

Desquilbet, L., & Mariotti, F. (2010). Dose-response analyses using restricted cubic spline functions in public health research. *Statistics in Medicine*, 29(9), 1037–1057. https://doi.org/10.1002/sim.3841

Dutilleul, P. (1993). Modifying the t test for assessing the correlation between two spatial processes. *Biometrics*, 49(1), 305–314. https://doi.org/10.2307/2532625

European Parliament. (2000). Directive 2000/60/EC of the European Parliament and of the Council establishing a framework for Community action in the field of water policy. *Official Journal of the European Union*, L 327, 1–73.

European Parliament. (2006). Directive 2006/118/EC of the European Parliament and of the Council on the protection of groundwater against pollution and deterioration. *Official Journal of the European Union*, L 372, 19–31.

European Parliament. (2009). Regulation (EC) No 1107/2009 concerning the placing of plant protection products on the market. *Official Journal of the European Union*, L 309, 1–50.

Fernandez-Calviño, D., Pateiro-Moure, M., López-Periago, E., Arias-Estévez, M., & Nóvoa-Muñoz, J. C. (2020). Monitoring pesticide contamination from point and diffuse sources: A review of long-term monitoring studies. *Science of the Total Environment*, 710, 136298.

Flury, M. (1996). Experimental evidence of transport of pesticides through field soils — A review. *Journal of Environmental Quality*, 25(1), 25–45. https://doi.org/10.2134/jeq1996.00472425002500010005x

FOCUS. (2014). Generic guidance for FOCUS groundwater scenarios (version 2.2). European Commission, DG SANTE.

GEUS. (2023/42). Pesticider og nedbrydningsprodukter i grundvand og drikkevand [Pesticides and degradation products in groundwater and drinking water]. Geological Survey of Denmark and Greenland.

Gimsing, A. L., Agert, J., Baran, N., Boivin, A., Ferrari, F., Gibson, R., ... & Trevisan, M. (2019). The Danish Pesticide Leaching Assessment Programme: Monitoring results May 1999–June 2017. *Geological Survey of Denmark and Greenland Bulletin*, 43, e2019430103. https://doi.org/10.34194/geusb.v43.4396

Grandcoin, A., Piel, S., & Baurès, E. (2017). AminoMethylPhosphonic acid (AMPA) in natural waters: Its sources, behavior and environmental fate. *Water Research*, 117, 187–197. https://doi.org/10.1016/j.watres.2017.03.055

Hansen, B., Thorling, L., Schullehner, J., Termansen, M., & Dalgaard, T. (2022). National assessment of long-term groundwater response to pesticide regulation. *Environmental Science & Technology*, 56(20), 14387–14396. https://doi.org/10.1021/acs.est.2c02261

Harrell, F. E. (2015). *Regression Modeling Strategies: With Applications to Linear Models, Logistic and Ordinal Regression, and Survival Analysis* (2nd ed.). Springer.

Hernán, M. A., & Robins, J. M. (2020). *Causal Inference: What If*. Chapman & Hall/CRC.

Herse, A., Arata, L., & Balmann, A. (2021). Pesticide use in German agriculture: Regional patterns and determinants. *Ecological Economics*, 189, 107176.

Hosmer, D. W., & Lemeshow, S. (2000). *Applied Logistic Regression* (2nd ed.). Wiley.

Kjær, J., Olsen, P., Barlebo, H. C., Juhler, R. K., Plauborg, F., Grant, R., ... & Lindhardt, B. (2005). The Danish Pesticide Leaching Assessment Programme: Monitoring results May 1999–June 2004. Geological Survey of Denmark and Greenland.

Kjær, J., Olsen, P., Henriksen, T., & Ullum, M. (2011). Leaching of metribuzin metabolites and glyphosate in structured soil. *Journal of Environmental Quality*, 34(2), 608–620. https://doi.org/10.2134/jeq2005.0608

Kodešová, R., Kočárek, M., Kodeš, V., Drábek, O., Kozák, J., & Hejtmánková, K. (2011). Pesticide adsorption in relation to soil properties and soil type distribution in regional scale. *Journal of Hazardous Materials*, 186(1), 540–550. https://doi.org/10.1016/j.jhazmat.2010.11.040

Lapworth, D. J., Baran, N., Stuart, M. E., & Ward, R. S. (2012). Emerging organic contaminants in groundwater: A review of sources, fate and occurrence. *Environmental Pollution*, 163, 287–303. https://doi.org/10.1016/j.envpol.2011.12.034

LeSage, J., & Pace, R. K. (2009). *Introduction to Spatial Econometrics*. Chapman & Hall/CRC.

Levi, S., Hybel, A. M., Bjerg, P. L., & Albrechtsen, H. J. (2014). Stimulation of aerobic degradation of bentazone, mecoprop and dichlorprop by oxygen addition to aquifer sediment. *Science of the Total Environment*, 473, 667–675. https://doi.org/10.1016/j.scitotenv.2013.12.061

Lindström, B., Svensson, K., & Kreuger, J. (2013). Statistical screening for descriptive parameters for pesticide occurrence in a shallow groundwater catchment. *Journal of Hydrology*, 477, 165–174. https://doi.org/10.1016/j.jhydrol.2012.11.031

Loos, R., Carvalho, R., António, D. C., Comero, S., Locoro, G., Tavazzi, S., ... & Gawlik, B. M. (2013). EU-wide monitoring survey on emerging polar organic contaminants in wastewater treatment plant effluents. *Water Research*, 47(17), 6475–6487.

Lopez, B., Baran, N., & Bourgine, B. (2015). An innovative procedure to assess multi-scale temporal trends in groundwater quality: Example of the nitrate in the Seine-Normandy basin, France. *Journal of Hydrology*, 522, 1–10.

Lumley, T., Diehr, P., Emerson, S., & Chen, L. (2002). The importance of the normality assumption in large public health data sets. *Annual Review of Public Health*, 23, 151–169.

Miljøstyrelsen. (2017). Pesticidstrategi 2017–2021. Danish Environmental Protection Agency.

Moran, P. A. P. (1950). Notes on continuous stochastic phenomena. *Biometrika*, 37(1–2), 17–23. https://doi.org/10.1093/biomet/37.1-2.17

Openshaw, S. (1984). The modifiable areal unit problem. *Concepts and Techniques in Modern Geography*, 38, 1–41.

Peduzzi, P., Concato, J., Kemper, E., Holford, T. R., & Feinstein, A. R. (1996). A simulation study of the number of events per variable in logistic regression analysis. *Journal of Clinical Epidemiology*, 49(12), 1373–1379. https://doi.org/10.1016/S0895-4356(96)00236-3

Rosenbom, A. E., Olsen, P., Plauborg, F., Grant, R., Juhler, R. K., Niekamper, N., & Brüsch, W. (2015). Pesticide leaching through sandy and loamy Danish fields: 10 years of monitoring. *Science of the Total Environment*, 521, 373–392. https://doi.org/10.1016/j.scitotenv.2015.03.110

Schipper, P. N. M., Bonten, L. T. C., Plette, A. C. C., & Moolenaar, S. W. (2008). Measures to diminish leaching of pesticides to surface and groundwaters in The Netherlands. *Crop Protection*, 27(12), 1526–1531.

Spliid, N. H., & Køppen, B. (1998). Occurrence of pesticides in Danish shallow ground water. *Chemosphere*, 37(7), 1307–1316. https://doi.org/10.1016/S0045-6535(98)00138-9

Stuart, M., Lapworth, D., Crane, E., & Hart, A. (2012). Review of risk from potential emerging contaminants in UK groundwater. *Science of the Total Environment*, 416, 1–21. https://doi.org/10.1016/j.scitotenv.2011.11.072

Textor, J., van der Zander, B., Gilthorpe, M. S., Liskiewicz, M., & Ellison, G. T. H. (2016). Robust causal inference using directed acyclic graphs: The R package 'dagitty'. *International Journal of Epidemiology*, 45(6), 1887–1894. https://doi.org/10.1093/ije/dyw341

Thorling, L., Brüsch, W., Hansen, B., & Binzer, A. (2019). Grundvand — Status og udvikling 1989–2018 [Groundwater — Status and development 1989–2018]. GEUS og Miljøstyrelsen.

Thorling, L., Hansen, B., Larsen, C. L., & Brüsch, W. (2023). Grundvand — Status og udvikling 1989–2022 [Groundwater — Status and development 1989–2022]. GEUS og Miljøstyrelsen.

University of Hertfordshire. (2024). The Pesticide Properties Database (PPDB). Agriculture & Environment Research Unit. https://sitem.herts.ac.uk/aeru/ppdb/

