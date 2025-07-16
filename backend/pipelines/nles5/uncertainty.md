# NLES5 Model: Parameter Estimates and Uncertainties

This document summarizes the key parameter estimates, standard errors (SE), and model uncertainties for the NLES5 empirical model, as detailed in DCA Report No. 163. The values are primarily sourced from Tables 3.2 and 3.3, and the uncertainty analysis in Chapter 6 of the report.

### Model Parameters

The NLES5 model's parameters are divided into two main categories: those related to nitrogen, soil, and water percolation, and those related to crop and vegetation cover effects.

#### Nitrogen, Soil, and Percolation Parameters (from Table 3.2)

These parameters form the core of the model's response to nitrogen inputs, soil characteristics, and water movement.

| Parameter | Description | Estimate | SE |
| :--- | :--- | :--- | :--- |
| \\(\tau\\) | Effect of leaching trend over years (kg N/ha/yr) | -0.1108 | - |
| \\(\kappa\\) | The power to which the effect of N input and crops is raised | 1.5 | - |
| \\(\rho\\) | A scaling factor to account for bias from back-transformation | 1.085 | - |
| \\(\mu\\) | Intercept | 23.51000 | 4.341800 |
| \\(\beta_{NT}\\) | Total N in the top 25 cm soil layer (Mg N/ha) | 0.456793 | 0.202200 |
| \\(\beta_{CS}\\) | Mineral N application in spring in current year (kg N/ha) | 0.049570 | 0.007000 |
| \\(\beta_{CA}\\) | Mineral N application in autumn in current year (kg N/ha) | 0.157044 | 0.034257 |
| \\(\beta_{udb}\\) | Mineral N deposited by grazing animals in current year (kg N/ha) | 0.038245 | 0.011056 |
| \\(\beta_{m1}\\) | Effect of mineral and organic N in the previous two years (kg N/ha) | 0.026499 | 0.006121 |
| \\(\beta_{f0}\\) | Biological N fixation in the current year (kg N/ha) | 0.016314 | 0.005530 |
| \\(\beta_{f1}\\) | Biological N fixation in the previous two years (kg N/ha) | 0.026499 | 0.006121 |
| \\(\beta_{g0}\\) | Organic N in animal manure in current year (kg N/ha) | 0.014099 | 0.008799 |
| \\(\theta_1\\) | Winter crop group 1 (WC1) | 1.000000 | - |
| \\(\theta_2\\) | Winter crop group 2 (WC2) | 1.205144 | 0.110679 |
| \\(\zeta\\) | Soil clay content in the topsoil (0-25 cm) (%) | 0.001849 | 0.004557 |
| \\(\delta_{1s}\\) | Percolation in the period April-August on sandy soils (mm) | 0.001194 | 0.000437 |
| \\(\delta_{2s}\\) | Percolation in the period September-March on sandy soils (mm) | 0.001107 | 0.000306 |
| \\(\nu_{2s}\\) | Percolation in the period Sep-Mar in the previous year on sandy soils (mm) | 0.000856 | 0.000163 |
| \\(\delta_{1c}\\) | Percolation in the period April-August on loamy soils (mm) | 0.000798 | 0.000233 |
| \\(\delta_{2c}\\) | Percolation in the period September-March on loamy soils (mm) | 0.000745 | 0.000180 |
| \\(\nu_{2c}\\) | Percolation in the period Sep-Mar in the previous year on loamy soils (mm) | 0.000638 | 0.000144 |

#### Crop and Vegetation Cover Parameters (from Table 3.3)

These parameters quantify the effect of different crops and vegetation covers on nitrate leaching. Negative values indicate lower leaching compared to the reference crop (parameter value of 0).

| Code | Description | Estimate | SE |
| :--- | :--- | :--- | :--- |
| **Main crop** | | | |
| M1 | Winter cereal (reference) | 0 | - |
| M2 | Spring cereal | -6.744 | 2.725 |
| M3 | Grain-legume mixtures | -7.279 | 3.089 |
| M4 | Grass or grass-clover | -13.493 | 4.183 |
| M5 | Grass for seed | -17.478 | 5.388 |
| M6 | Set-aside | -11.192 | 4.821 |
| M7 | Sugar beet, fodder beet | -0.640 | 3.196 |
| M8 | Silage maize and potato | 3.534 | 2.973 |
| M9 | Winter oilseed rape | -7.319 | 2.295 |
| M10 | Winter cereal after grass | -1.248 | 8.049 |
| M11 | Maize after grass | 19.524 | 9.745 |
| M12 | Spring cereal after grass | -6.229 | 9.154 |
| M13 | Grain legumes and spring oilseed rape | -2.866 | 3.201 |
| **Winter vegetation cover** | | | |
| W1 | Winter cereal (reference) | 0 | - |
| W2 | Bare soil | -2.055 | 1.185 |
| W3 | Autumn cultivation | -0.456 | 1.499 |
| W4 | Cover crops, undersown grass and set-aside | -15.959 | 2.674 |
| W5 | Weeds and volunteers | -3.792 | 1.700 |
| W6 | Grass and grass-clover | -14.596 | 2.569 |
| W7 | Winter cereal after grass | -1.049 | 0.000 |
| W8 | Grass ploughed late autumn or winter | -21.060 | 6.208 |
| **Main previous crop** | | | |
| MP1 | Winter cereal (reference) | 0 | - |
| MP2 | Other crops than winter cereals and grass or grass-clover | 2.847 | 1.031 |
| MP3 | Grass or grass-clover | 0.664 | 2.000 |
| MP4 | Spring or winter crops after grass or grass-clover | 1.160 | 9.838 |
| **Winter previous crop** | | | |
| WP1 | Winter cereal (reference) | 0 | - |
| WP2 | Bare soil | 9.704 | 2.864 |
| WP3 | Grass-clover | 10.601 | 3.447 |
| WP4 | Cover crops | 9.354 | 2.902 |
| WP5 | Grass for seed and set aside | 13.241 | 5.101 |
| WP6 | Beets and hemp | 5.483 | 3.094 |
| WP7 | Bare soil after maize or potatoes | -1.572 | 2.963 |
| WP8 | Winter oilseed rape | 7.413 | 0.000 |
| WP9 | Bare soil or winter cereal following grass-clover ploughed in spring | 7.396 | 7.976 |
| WP10 | Bare soil or winter cereal following grass-clover ploughed in autumn | 10.975 | 9.318 |

### Model Uncertainties

The report provides several quantifications of the model's uncertainty at different scales and for different outputs.

#### General and National Scale Uncertainty
*   **Overall Model Uncertainty:** The general uncertainty of the model, quantified by the coefficient of variation, is approximately **10%**.
*   **National Nitrate Leaching Uncertainty:** The uncertainty for national-level predictions, expressed as a standard deviation, is approximately **6 kg N/ha**.

#### Marginal N Leaching Uncertainty
*   **National Average:** The average marginal N leaching for farmland in Denmark was predicted to be **17%**.
*   **National Uncertainty:** The uncertainty for this national average, expressed as a standard deviation, is **2.6 percentage points**.
*   **Regional Uncertainty:** The uncertainty in marginal N leaching varies regionally with the level of leaching:
    *   For farmland with low leaching levels, the uncertainty is approximately **1 percentage point**.
    *   For areas with high leaching levels, the uncertainty increases to **4 percentage points**.