# Scientific Robustness Review: Groundwater Correlation Script

## Executive Summary

Our `verify_groundwater_correlations.py` script uses point-biserial correlation to test whether field-level pesticide application intensity (kg a.i. per GRUKO catchment) predicts binary groundwater detection. While the core research question is scientifically valid and the application-intensity signal is well-supported by literature, the script has **three significant methodological gaps** that would need addressing before publication:

1. **No correction for spatial autocorrelation** — neighboring GRUKOs are not independent, inflating p-values by up to 7x [1]
2. **No multiple-comparison correction** — testing 57+ substances at a=0.05 expects ~3 false positives
3. **Point-biserial correlation is suboptimal** — logistic regression is the established method for binary groundwater detection outcomes [2][3]

The results are **directionally correct and useful for internal verification** of the draft paper, but would not survive peer review in their current form. Below, we detail each issue and provide concrete fixes.

---

## 1. Statistical Method: Point-Biserial Correlation

### What we do

```python
r, p = scipy_stats.pointbiserialr(detected, intensity)
```

We correlate a binary variable (detected=0/1 per GRUKO) against a continuous variable (total kg of parent compound applied in fields within that GRUKO). This is mathematically equivalent to Pearson's r when one variable is dichotomous [4].

### Is this valid?

**Partially.** Point-biserial correlation is a legitimate measure of association between a binary and continuous variable. However, the literature on pesticide-groundwater analysis overwhelmingly uses **logistic regression** for this type of problem:

- **Teso et al. (1996)** established logistic regression + GIS as the standard for predicting groundwater vulnerability to pesticides, modeling detection probability as a function of soil particle-size composition [2].
- **Groundwater Vulnerability to Pesticides: Statistical Approaches** (Worrall & Kolpin, 2004) uses logistic regression with explanatory variables including hydrogeology, land use, and well geometry [3].
- **Swedish catchment screening study** (Lindstrom et al., 2013) tested 17 parameters for discriminating detected/non-detected pesticides, finding dosage applied as the most significant predictor -- accounting for 50-85% of variability [5].
- **Hansen et al. (2022)** -- the closest Danish comparator -- used **Pearson cross-correlation** between pesticide sales time-series and detection frequency time-series, *not* spatial cross-sectional correlation [6].

### Recommendation

**Replace point-biserial with logistic regression.** This is a straightforward change:

```python
import statsmodels.api as sm

X = sm.add_constant(intensity)
model = sm.Logit(detected, X).fit(disp=0)
p_value = model.pvalues[1]  # p-value for intensity coefficient
odds_ratio = np.exp(model.params[1])
```

Logistic regression also allows adding covariates later (soil type, aquifer depth, precipitation) without changing the framework, and produces interpretable odds ratios.

**If keeping point-biserial**, acknowledge it explicitly as a screening metric and report it alongside logistic regression results for validation.

---

## 2. Spatial Autocorrelation -- The Critical Gap

### The problem

Our 5,826 GRUKOs are not independent observations. Neighboring catchments share:
- Similar geology and soil types
- Similar agricultural practices and crop rotations
- Overlapping hydrogeological flow paths
- Similar groundwater monitoring intensity

Standard statistical tests (including `scipy.stats.pointbiserialr`) assume **independent observations**. When spatial autocorrelation is present, this assumption is violated, and **p-values are systematically too small** -- the test reports significance where none exists.

### How bad is the inflation?

A rigorous study using Monte Carlo simulation with spatially autocorrelated data found that:

> "An inflation in type I error rate is an increasing function of the degree of spatial autocorrelation. At moderate autocorrelation, an erroneous significant association would be concluded over **36% of the time instead of the expected 5%**." [1]

This means our p<0.05 threshold may effectively be p<0.36 -- a 7x inflation. Several of our "significant" substances with p-values between 0.001 and 0.05 could be false positives driven by spatial structure.

### What the literature recommends

**Option A: Dutilleul's modified t-test (recommended)**

Dutilleul (1993) / Clifford & Richardson (1989) proposed adjusting the degrees of freedom based on the spatial autocorrelation structure of both variables [7]:

1. Compute Moran's I for both the detection variable and the intensity variable across GRUKOs
2. Estimate the effective degrees of freedom (EDF) -- always <= actual n
3. Use EDF instead of n-2 for the t-test on the correlation coefficient

When zero spatial autocorrelation prevails, EDF = n; when perfect positive autocorrelation prevails, EDF = 1 [8]. For our data with ~5,826 GRUKOs, the effective sample size could be dramatically smaller.

**Option B: Permutation test with spatial structure preservation**

Generate null distributions by randomly permuting detection labels while preserving the spatial autocorrelation structure, then compare observed correlations against this null [1]. Computationally expensive but assumption-free.

**Option C: Spatial regression models**

Use spatial lag or spatial error models (via PySAL/spreg) that explicitly model the spatial dependence structure:

```python
from spreg import GM_Error  # or GM_Lag
model = GM_Error(detected, intensity, w=spatial_weights)
```

### Concrete diagnostic for the script

At minimum, add Moran's I diagnostic:

```python
from libpysal.weights import Queen  # or KNN
from esda.moran import Moran

# Build spatial weights from GRUKO adjacency
w = Queen.from_dataframe(gruko_gdf)
mi_detect = Moran(detected_array, w)
mi_intensity = Moran(intensity_array, w)
log.info(f"Moran's I (detection): {mi_detect.I:.3f}, p={mi_detect.p_sim:.4f}")
log.info(f"Moran's I (intensity): {mi_intensity.I:.3f}, p={mi_intensity.p_sim:.4f}")
```

If Moran's I is significant (likely), then report "spatially-corrected significance requires further analysis" rather than claiming p<0.05.

---

## 3. Multiple Comparisons

### The problem

We test 57 substances at a=0.05. Without correction, the expected number of false positives is 57 x 0.05 = **2.85 substances**. We report 10 significant -- but 2-3 of those could be chance.

### What to do

**Benjamini-Hochberg FDR correction** is standard for screening studies in environmental chemistry [9][10]. It controls the false discovery rate (proportion of discoveries that are false) rather than the family-wise error rate (Bonferroni), giving more power:

```python
from statsmodels.stats.multitest import multipletests

p_values = [r["p_value"] for r in results]
reject, p_adjusted, _, _ = multipletests(p_values, alpha=0.05, method='fdr_bh')

for r, p_adj, sig in zip(results, p_adjusted, reject):
    r["p_adjusted"] = p_adj
    r["significant_fdr"] = sig
```

**Bonferroni** (a/57 = 0.00088) would be too conservative for exploratory screening. BH-FDR is the right balance.

Note: Hansen et al. (2022) did **not** apply multiple comparison corrections either [6], so our script is at least consistent with the closest comparable published study. But best practice requires it.

---

## 4. Ecological Fallacy and Spatial Aggregation

### Risk assessment

We aggregate individual field-level pesticide applications to GRUKO polygons (sum of kg a.i. across all fields with centroids in a GRUKO). This creates an ecological-level analysis where the unit is the GRUKO, not the individual field or borehole.

The ecological fallacy literature warns that **aggregate correlations can differ from -- and even reverse direction compared to -- individual-level correlations** [11][12]. Two sources of bias:

1. **Pure specification bias**: Nonlinear relationships at the field level become distorted when averaged across a GRUKO
2. **Confounding by aggregation**: Within-GRUKO variation in soil type, depth to water table, and aquifer connectivity is lost

### How serious is this for us?

**Moderate.** Our use case is less vulnerable than classic ecological fallacy examples because:
- The GRUKO boundaries are *hydrogeologically meaningful* (they delineate actual groundwater catchment zones), not arbitrary administrative units
- The exposure (pesticide application) and outcome (groundwater detection) are causally linked through the same hydrological system bounded by the GRUKO polygon
- We're looking for *zone-level* correlations, which is inherently an ecological question (does more spraying in a catchment lead to more detection in that catchment?)

But we should acknowledge:
- Smaller GRUKOs with fewer fields have more variable intensity estimates (noisier)
- The MAUP (Modifiable Areal Unit Problem) means results could differ with different polygon boundaries [12]
- Within-GRUKO heterogeneity (e.g., sandy vs. clay areas within one GRUKO) is lost

### Mitigation

Report the number of fields per GRUKO as a covariate and/or weight the correlation by GRUKO sample size (number of fields or boreholes). This partially addresses the varying reliability of the aggregate measure.

---

## 5. Detection Threshold (Binary vs. Concentration)

### Current approach

We use a binary indicator: detected = (maengde > 0.015 ug/L). This discards quantitative concentration information.

### Is this appropriate?

**Yes, for this analysis.** Binary detection is standard in groundwater vulnerability studies [2][3][5] because:
- Many measurements are below the limit of detection (censored data)
- Concentration values near the LOD are unreliable
- The policy question is "does the pesticide reach groundwater?" not "how much?"
- Logistic/correlation methods handle binary outcomes naturally

Using raw concentrations would require Tobit regression or survival analysis for left-censored data -- valid but more complex and not standard in screening studies.

---

## 6. Temporal Alignment

### Current approach

Application years: 2015-2016. Detection modes: all years / 2018+ / soil-adjusted transit.

### Assessment

The `--detection-mode all` approach (no year filter) gives the strongest results but is methodologically problematic -- it correlates 2015-2016 applications with detections from 1981-2025, which includes decades of legacy contamination unrelated to recent applications.

Hansen et al. (2022) handled this elegantly with **cross-correlation and lag estimation** -- correlating time series shifted by 0-40 years to find the optimal lag [6]. Our `--detection-mode soil` approach (soil-dependent transit times) is a reasonable approximation but reduces power by restricting the detection window.

### Recommendation

The `--detection-mode 2018` (uniform 2018+ window) is the most defensible for a cross-sectional analysis. Acknowledge that `mode=all` inflates correlations by including legacy detections.

---

## Summary of Issues and Fixes

| Issue | Severity | Fix | Effort |
|-------|----------|-----|--------|
| No spatial autocorrelation correction | **Critical** | Add Moran's I diagnostic; use Dutilleul correction or permutation test | Medium |
| No multiple-comparison correction | **High** | Add BH-FDR to existing p-values (3 lines of code) | Low |
| Point-biserial instead of logistic regression | **Medium** | Add logistic regression as primary; keep rpb as secondary | Medium |
| No ecological fallacy acknowledgment | **Low** | Add discussion noting GRUKO boundaries are hydrogeologically meaningful | Low |
| `mode=all` includes legacy detections | **Low** | Default to `mode=2018`; report `mode=all` as sensitivity analysis | Low |

---

## What We Got Right

1. **Application intensity as predictor** -- literature confirms dosage applied is the strongest single predictor of detection [5]
2. **Binary detection approach** -- standard in the field, appropriate for censored GEUS data
3. **Metabolite-parent mapping** -- correct approach to pool parent compound intensities for metabolite detection
4. **GRUKO-level aggregation** -- hydrogeologically meaningful spatial units (not arbitrary admin boundaries)
5. **Soil-dependent transit times** -- defensible proxy for Hansen et al.'s lag estimation
6. **Q4/Q1 ratio** -- good non-parametric supplement to correlation coefficient

---

## References

[1] PLOS ONE: Testing Pairwise Association Between Spatially Autocorrelated Variables. https://journals.plos.org/plosone/article?id=10.1371/journal.pone.0048766

[2] Teso et al. (1996) "Use of Logistic Regression and GIS Modeling to Predict Groundwater Vulnerability to Pesticides." J. Environ. Qual. https://acsess.onlinelibrary.wiley.com/doi/10.2134/jeq1996.00472425002500030007x

[3] Groundwater Vulnerability to Pesticides: Statistical Approaches. https://www.researchgate.net/publication/229694537

[4] SciPy docs: scipy.stats.pointbiserialr. https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.pointbiserialr.html

[5] Lindstrom et al. (2013) "Statistical screening for descriptive parameters for pesticide occurrence in a shallow groundwater catchment." J. Hydrology. https://www.sciencedirect.com/science/article/abs/pii/S0022169412010049

[6] Hansen et al. (2022) "National Assessment of Long-Term Groundwater Response to Pesticide Regulation." Environ. Sci. Technol. https://pmc.ncbi.nlm.nih.gov/articles/PMC9583610/

[7] Dutilleul et al. (1993) / Clifford & Richardson (1989). Effective degrees of freedom under autocorrelation. https://pmc.ncbi.nlm.nih.gov/articles/PMC6693558/

[8] Griffith (2005) "Effective Geographic Sample Size in the Presence of Spatial Autocorrelation." https://www.academia.edu/25157865

[9] Benjamini & Hochberg (1995) "Controlling the False Discovery Rate." J. Royal Stat. Soc. B. https://rss.onlinelibrary.wiley.com/doi/10.1111/j.2517-6161.1995.tb02031.x

[10] Use and misuse of corrections for multiple testing. https://www.sciencedirect.com/science/article/pii/S2590260123000115

[11] Wakefield (2004). Ecological Inference / Spatial Aggregation and the Ecological Fallacy. https://pmc.ncbi.nlm.nih.gov/articles/PMC4209486/

[12] Openshaw (1984). The Modifiable Areal Unit Problem. https://en.wikipedia.org/wiki/Modifiable_areal_unit_problem
