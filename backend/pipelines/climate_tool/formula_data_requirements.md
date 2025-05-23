# Formula Data Inputs

This document outlines the data input that is expected in each function.

## `marker_afgroederester.py`

### Function: `calculate_k_graes(n_graes_kg_n_ha: float) -> float`

- **Purpose**: Calculates correction factor `k_græs` based on N from grazing.
- **Constants to be Sourced from JSON**:
  - Thresholds for `n_graes_kg_n_ha` (50, 10) and resulting `k_graes` values (1.49, 1.24, 1.0) are part of the defined logic in `formulas.md`. Not direct "Tabelværdi" lookups, but could be structured in a JSON if this logic becomes more complex or varies by other factors.
  - Proposed: `poultry_factors.json` or `crop_residue_factors.json` under a `k_graes_rules` key.
- **Non-Constant Data Required**:
  - `n_graes_kg_n_ha`: Mængden af N afsat under afgræsning [kg N/ha] (Source: MO or User input).

### Function: `calculate_A_over_kg_ts_ha(...)`

- **Purpose**: Calculates dry matter in above-ground crop residues.
- **Constants to be Sourced from JSON (passed as arguments, crop-specific)**:
  - `h_u_fast_halmudbytte_kg_ts_ha`: Fast halmudbytte i frøgræs [kg ts/ha].
  - `formulas.md` Link: `Afgrøder_data_gødningsår...xlsx 'Data'!S1`.
  - Proposed: `crop_data.json` (keyed by crop code), under e.g., `[crop_code].fast_halmudbytte_froegraes_kg_ts_ha`.
  - `s_slope`: Slope for residue calculation.
  - `formulas.md` Link: `Afgrøder_data_gødningsår...xlsx 'Data'!L1`.
  - Proposed: `crop_data.json`, under e.g., `[crop_code].residue_over_slope`.
  - `i_intercept`: Intercept for residue calculation.
  - `formulas.md` Link: `Afgrøder_data_gødningsår...xlsx 'Data'!M1`.
  - Proposed: `crop_data.json`, under e.g., `[crop_code].residue_over_intercept`.
  - `h_f_halmfraktion`: Halmfraktion ift. udbytte.
  - `formulas.md` Link: `Afgrøder_data_gødningsår...xlsx 'Data'!R1`.
  - Proposed: `crop_data.json`, under e.g., `[crop_code].halmfraktion`.
- **Non-Constant Data Required**:
  - `x1_halmnedmulding`: Boolean, halm incorporated (Source: MO or User input).
  - `x2_udbytte_nedmuldes`: Boolean, yield incorporated (Source: `Afgrøder_data...xlsx 'Data'!Y1`, crop-specific lookup).
  - `t_torstof_total_kg_ts_ha`: Total dry matter of main yield [kg ts/ha] (Source: MO or User input).
  - `k_graes`: Output from `calculate_k_graes`.

### Function: `calculate_A_under_kg_ts_ha(...)`

- **Purpose**: Calculates dry matter in below-ground crop residues.
- **Constants to be Sourced from JSON (passed as arguments, crop-specific)**:
  - `h_u_fast_halmudbytte_kg_ts_ha`: (As above).
  - `s_slope`: (As above).
  - `i_intercept`: (As above).
  - `f_forhold_under_over_biomasse`: Ratio below-ground to above-ground biomass.
  - `formulas.md` Link: `Afgrøder_data_gødningsår...xlsx 'Data'!Q1`.
  - Proposed: `crop_data.json`, under e.g., `[crop_code].ratio_biomass_below_over`.
- **Non-Constant Data Required**:
  - `t_torstof_total_kg_ts_ha`: Total dry matter of main yield [kg ts/ha] (Source: MO or User input).
  - `k_graes`: Output from `calculate_k_graes`.

### Function: `calculate_n_afgroederester_kg_n_ha(...)`

- **Purpose**: Calculates N in crop residues per ha.
- **Constants to be Sourced from JSON (passed as arguments, crop-specific)**:
  - `n_over_kg_n_pr_kg_ts`: N content in above-ground residues [kg N/kg ts].
  - `formulas.md` Link: `Afgrøder_data_gødningsår...xlsx 'Data'!O1`.
  - Proposed: `crop_data.json`, under e.g., `[crop_code].n_content_residue_over`.
  - `n_under_kg_n_pr_kg_ts`: N content in below-ground residues [kg N/kg ts].
  - `formulas.md` Link: `Afgrøder_data_gødningsår...xlsx 'Data'!P1`.
  - Proposed: `crop_data.json`, under e.g., `[crop_code].n_content_residue_below`.
- **Non-Constant Data Required**:
  - `a_over_kg_ts_ha`: Output from `calculate_A_over_kg_ts_ha`.
  - `a_under_kg_ts_ha`: Output from `calculate_A_under_kg_ts_ha`.

### Function: `calculate_co2e_afgroederester_kg_co2e_ha(...)`

- **Purpose**: Calculates CO2e from N in crop residues per ha.
- **Constants to be Sourced from JSON**:
  - `EF_N2O_AFGROEDERESTER`: `0.01` (N2O emission factor for crop residues).
  - `formulas.md` Link: `Afgrøder_data_gødningsår...xlsx 'Coefficients'!B2`.
  - Proposed: `gwp_factors.json` (as `indirect_n2o_factors.atmospheric_deposition` or a more specific key like `ef_n2o.crop_residue`). Current match: `indirect_n2o_factors.atmospheric_deposition: 0.01`.
  - `THETA_N2O_CO2`: `265.0` (GWP for N2O).
  - `formulas.md` Link: `GlobalCoefficients.xlsx 'Coefficients'!B2`.
  - Proposed: `gwp_factors.json` as `gwp.n2o`. **Note discrepancy**: File has `273.0`, Python/C# code use `265.0`. Excel is master.
  - `MOL_WEIGHT_N2O_N_FACTOR`: `44.0 / 28.0`. (Standard molar mass conversion). Could be in `gwp_factors.json -> molecular_weights` if standardized approach is taken.
- **Non-Constant Data Required**:
  - `n_total_afgroederester_kg_n_ha`: Output from `calculate_n_afgroederester_kg_n_ha`.
  - `o_omlaegningsfrekvens`: Crop turnover frequency (crop-specific).
  - `formulas.md` Link: `Afgrøder_data_gødningsår...xlsx 'Data'!N1`.
  - Proposed: `crop_data.json`, under e.g., `[crop_code].omlaegningsfrekvens`. (Passed as argument).

## `marker_kulstofbalance.py`

### Function: `calculate_C_afgroederest_kg_c_ha(...)`

- **Purpose**: Calculates carbon (C) from crop residues per ha.
- **Constants to be Sourced from JSON**:
  - `C_FRAK_TORSTOF_TIL_C`: `0.45` (Assumed carbon fraction in dry matter).
  - `formulas.md` Link: `Afgrøder_data_gødningsår...xlsx 'Coefficients'!B27`.
  - Proposed: `global_coefficients.json` or `crop_residue_factors.json` as `carbon_fraction_in_dry_matter`.
- **Non-Constant Data Required**:
  - `a_over_kg_ts_ha`: Dry matter in above-ground residues (kg ts/ha) (Source: `marker_afgroederester.py`).
  - `a_under_kg_ts_ha`: Dry matter in below-ground residues (kg ts/ha) (Source: `marker_afgroederester.py`).

### Function: `calculate_C_organisk_goedning_kg_c_ha(...)`

- **Purpose**: Calculates carbon (C) from organic fertilizer per ha.
- **Constants to be Sourced from JSON**:
  - `F_HUS_N_TIL_C_ORGANISK`: `8.0` (Conversion factor N to C for manure).
  - `formulas.md` Link: `Afgrøder_data_gødningsår...xlsx 'Coefficients'!B26`.
  - Proposed: `manure_factors.json` as `n_to_c_conversion.husdyrgoedning` (or specific manure types as per `Tabel 33`). `Tabel 33` in `tables.md` has `faktor_kg_C_kg_N: 8` for "kvæggylle" and "blandet gylle".
- **Non-Constant Data Required**:
  - `n_hus_plus_afg_kg_n_ha`: N in manure/organic fertilizer + grazing (kg N/ha) (Source: MO or User input).

### Function: `calculate_co2e_kulstofbalance_mark(...)`

- **Purpose**: Calculates CO2e from carbon balance for a field.
- **Constants to be Sourced from JSON**:
  - `MU_DK_GENNEMSNIT_INPUT_C`: `4093.0` (Average Danish carbon input from a crop, kg C/ha).
  - `formulas.md` Link: `Afgrøder_data_gødningsår...xlsx 'Coefficients'!B24`.
  - Proposed: `global_coefficients.json` or `soil_carbon_factors.json` as `avg_danish_crop_carbon_input_kg_c_ha`.
  - `MOL_WEIGHT_CO2_C_FACTOR`: `44.0 / 12.0` (Standard molar mass conversion C to CO2). Could be in `gwp_factors.json -> molecular_weights` if standardized.
  - `STABILIZATION_FACTOR`: `0.097` (Factor for C stabilization in soil).
  - `formulas.md`: Directly in formula. Not explicitly linked to an Excel cell in the list.
  - Proposed: `soil_carbon_factors.json` as `carbon_stabilization_factor`.
- **Non-Constant Data Required**:
  - `r_relativ_faktor`: Factor indicating if crop is relativized to DK average (0, 1, or 2) (Source: `Afgrøder_data...xlsx 'Data'!W1`, crop-specific lookup).
  - `areal_ha`: Field area (ha) (Source: MO or User input).
  - `c_afgroederest_kg_c_ha`: Output from `calculate_C_afgroederest_kg_c_ha`.
  - `c_organisk_kg_c_ha`: Output from `calculate_C_organisk_goedning_kg_c_ha`.

## `marker_goedning_og_nitrifikationshaemmer.py`

### Function: `calculate_n2o_components(...)`

- **Purpose**: Calculates N2O_jord, N2O_NH3, N2O_NOx components.
- **Constants to be Sourced from JSON**:
  - `EF_N2O_GENERAL`: `0.01` (General N2O EF for NH3/NOx deposition).
  - `formulas.md` Link: `Afgrøder_data_gødningsår...xlsx 'Coefficients'!B2`.
  - Proposed: `gwp_factors.json -> indirect_n2o_factors.atmospheric_deposition: 0.01` or specific `ef_n2o.deposition`.
  - `EF_NOX`: `0.04` (EF for N-loss as NOx).
  - `formulas.md` Link: `Afgrøder_data_gødningsår...xlsx 'Coefficients'!B4`.
  - Proposed: `global_coefficients.json` or `nitrogen_loss_factors.json` as `ef_nox_from_fertilizer`.
  - `MOL_WEIGHT_FACTOR_N2O_N`: `44.0 / 28.0`. (Standard).
  - `MOL_WEIGHT_FACTOR_NOX_N`: `46.0 / 14.0`. (Standard, for NO2 assumed).
- **Non-Constant Data Required**:
  - `n_total_kg_ha`: Total N applied (kg N/ha) (Source: MO or User input).
  - `areal_ha`: Field area (ha) (Source: MO or User input).
  - `ef_n2o_jord`: EF for direct N2O from soil (depends on fertilizer type, see below) (Source: Constant for type).
  - `ef_nh3`: EF for NH3 loss (depends on fertilizer type, see below) (Source: Constant for type).

### Function: `calculate_n2o_goedning(...)`

- **Purpose**: Calculates total N2O and CO2e from fertilizer, considering type and nitrification inhibitor.
- **Constants to be Sourced from JSON**:
  - `THETA_N2O_CO2`: `265.0` (GWP for N2O).
  - `formulas.md` Link: `GlobalCoefficients.xlsx 'Coefficients'!B2`.
  - Proposed: `gwp_factors.json -> gwp.n2o`. (Discrepancy: file has 273.0).
  - `EF_N2O_GENERAL`: `0.01` (N2O EF for soil for handelsgoedning/husdyrgoedning).
  - `formulas.md` Link: `Afgrøder_data_gødningsår...xlsx 'Coefficients'!B2`.
  - Proposed: `gwp_factors.json -> indirect_n2o_factors.atmospheric_deposition: 0.01` or specific key.
  - `EF_N2O_JORD_AFGRAESNING`: `0.004` (N2O EF for soil for grazing).
  - `formulas.md` Link: `Afgrøder_data_gødningsår...xlsx 'Coefficients'!B3`.
  - Proposed: `manure_factors.json` or `grazing_factors.json` as `ef_n2o_direct_grazing_soil`.
  - `EF_NH3_HANDELSGOEDNING`: `0.05`.
  - `formulas.md` Link: `Afgrøder_data_gødningsår...xlsx 'Coefficients'!B5`.
  - Proposed: `fertilizer_factors.json` or `nitrogen_loss_factors.json` as `ef_nh3.handelsgoedning`.
  - `EF_NH3_HUSDYRGOEDNING`: `0.08`.
  - `formulas.md` Link: `Afgrøder_data_gødningsår...xlsx 'Coefficients'!B6`.
  - Proposed: `manure_factors.json` or `nitrogen_loss_factors.json` as `ef_nh3.husdyrgoedning_field`.
  - `EF_NH3_AFGRAESNING`: `0.084` (for kvæg).
  - `formulas.md` Link: `Afgrøder_data_gødningsår...xlsx 'Coefficients'!B7`.
  - Proposed: `grazing_factors.json` as `ef_nh3_grazing.kvaeg`.
  - `NITRIFICATION_INHIBITOR_EFFECTIVENESS`: `0.4` (40% reduction).
  - `formulas.md`: Directly in formula.
  - Proposed: `mitigation_factors.json` as `nitrification_inhibitor_effectiveness`.
- **Non-Constant Data Required**:
  - `n_total_kg_ha`: Total N applied (kg N/ha) (Source: MO or User input).
  - `areal_ha`: Field area (ha) (Source: MO or User input).
  - `goedningstype`: String ("handelsgoedning", "husdyrgoedning", "afgraesning") (Source: User input/MO).
  - `n_nitri_kg_ha`: Amount of N with inhibitor (kg N/ha) (Source: User input).

## `marker_organogene_jorde.py`

### Function: `calculate_co2_organogene_jorde(...)`

- **Purpose**: Calculates CO2e emissions from organogenic soils based on land characteristics.
- **Constants to be Sourced from JSON**:
  - Factors for CO2 (kulstof), N2O (CO2e), and CH4 (CO2e) based on omdrift, vandstand, and kulstof_percentage.
  - Rule 1a (i omdrift, lav vand, 6-12% C): `co2_kulstof_factor = 21.08` (tons/ha)
    - `formulas.md` Link: Explicitly in rules. Source `Tabel 31`.
    - Proposed: `organic_soil_factors.json -> co2_emission.omdrift_6_12c_lav_vand` (or similar structure based on `Tabel 31` content from `tables.md`).
  - Rule 2a (i omdrift, lav vand, >12% C): `co2_kulstof_factor = 42.17` (tons/ha)
    - `formulas.md` Link: Explicitly in rules. Source `Tabel 31`.
    - Proposed: `organic_soil_factors.json -> co2_emission.omdrift_gt12c_lav_vand`.
  - Rule 2b (i omdrift, lav vand, >12% C): `n2o_co2e_factor = 3.87` (tons CO2e/ha)
    - `formulas.md` Link: Explicitly in rules. (Not directly from `Tabel 30` + GWP, seems pre-converted).
    - Proposed: `organic_soil_factors.json -> n2o_co2e_emission.omdrift_gt12c_lav_vand`.
  - Rule 3a (ikke omdrift, lav vand, >12% C): `co2_kulstof_factor = 30.8` (tons/ha)
    - `formulas.md` Link: Explicitly in rules. Source `Tabel 31`.
    - Proposed: `organic_soil_factors.json -> co2_emission.ikke_omdrift_gt12c_lav_vand`.
  - Rule 3b (ikke omdrift, lav vand, >12% C): `n2o_co2e_factor = 2.44` (tons CO2e/ha)
    - `formulas.md` Link: Explicitly in rules. (Pre-converted).
    - Proposed: `organic_soil_factors.json -> n2o_co2e_emission.ikke_omdrift_gt12c_lav_vand`.
  - Rule 4a (ikke omdrift, lav vand, 6-12% C): `co2_kulstof_factor = 15.4` (tons/ha)
    - `formulas.md` Link: Explicitly in rules. Source `Tabel 31`.
    - Proposed: `organic_soil_factors.json -> co2_emission.ikke_omdrift_6_12c_lav_vand`.
  - Rule 5c (ikke omdrift, høj vand): `ch4_co2e_factor = 6.8` (tons CO2e/ha)
    - `formulas.md` Link: Explicitly in rules. Source `Tabel 32` (magnitude match).
    - Proposed: `organic_soil_factors.json -> ch4_co2e_emission.ikke_omdrift_hoej_vand`.
  - **Note**: The existing `organic_soil_factors.json` has a different structure and values (e.g., `omdrift_gt12c: 13.0` for N2O-N and `omdrift_gt12c: 20.0` for CO2). This needs reconciliation with `Tabel 30` and `Tabel 31` and the specific factors (like 3.87, 2.44) used in the Python code which are directly from `formulas.md` rules.
- **Non-Constant Data Required**:
  - `h`: Field area (ha) (Source: User input/MO).
  - `i_omdrift`: Boolean, is field in rotation (Source: User input/MO).
  - `lav_vandstand`: Boolean, is water level low (Source: User input/MO).
  - `kulstof_percentage`: String ("6-12%" or ">12%") (Source: User input/MO).

## `marker_kalkning.py`

### Function: `calculate_co2_kalkning_bedrift(...)`

- **Purpose**: Calculates kg CO2 from liming for the entire farm per year.
- **Constants to be Sourced from JSON**:
  - `M_CACO3`: `100.09` (Molar mass of CaCO3, g/mol).
  - `formulas.md` Link: `Afgrøder_data_gødningsår...xlsx 'Coefficients'!B31`.
  - Proposed: `gwp_factors.json -> molecular_weights.caco3` (or a new `chemical_constants.json`).
  - `M_C`: `12.01` (Molar mass of C, g/mol).
  - `formulas.md` Link: `Afgrøder_data_gødningsår...xlsx 'Coefficients'!B32`.
  - Proposed: `gwp_factors.json -> molecular_weights.c`.
  - `S_CACO3_PER_HA`: `170.0` (Standard CaCO3 application rate, kg/ha/year).
  - `formulas.md` Link: `Afgrøder_data_gødningsår...xlsx 'Coefficients'!B15`.
  - Proposed: `crop_management_factors.json` or `liming_factors.json` as `standard_caco3_application_kg_ha_yr`.
  - Factor `44.0 / 12.0`: (Molar mass CO2 / Molar mass C). Standard.
- **Non-Constant Data Required**:
  - `a_total_kalket_areal`: Total limed area on farm (ha) (Source: User input).

### Function: `calculate_co2_kalkning_mark(...)`

- **Purpose**: Allocates farm-level CO2 from liming to a specific field.
- **Constants to be Sourced from JSON**: None directly in this function other than those used by `calculate_co2_kalkning_bedrift`.
- **Non-Constant Data Required**:
  - `co2_bedrift`: Output from `calculate_co2_kalkning_bedrift`.
  - `a_mark_areal`: Area of the specific field (ha) (Source: MO or User input).
  - `a_total_kalket_areal`: Total limed area on farm (ha) (Source: User input).

## `marker_nitratudvaskning.py`

### Function: `calculate_n2o_nitratudvaskning(t: float, h: float)`

- **Purpose**: Calculates N2O (kg) and CO2e (kg) from nitrate leaching.
- **Constants to be Sourced from JSON**:
  - `THETA_N2O_CO2`: `265.0` (GWP for N2O).
  - `formulas.md` Link: `GlobalCoefficients.xlsx 'Coefficients'!B2`.
  - Proposed: `gwp_factors.json -> gwp.n2o`. (Discrepancy: file has 273.0, Python/C# code use 265.0. Excel is master).
  - Leaching N2O EF: `0.0075` (kg N2O-N / kg N leached).
  - `formulas.md`: Directly in formula `T * 0.0075 * (44/28) * H`.
  - Proposed: `gwp_factors.json -> indirect_n2o_factors.leaching: 0.0075`. **Matches existing.**
  - Factor `44.0 / 28.0`: (Molar mass N2O / Molar mass N2). Standard.
- **Non-Constant Data Required**:
  - `t`: Typetal for nitratudvaskning (kg N/ha) (Source: `Afgrøder_data...xlsx 'Data'!Y1`, crop-specific).
  - `h`: Field area (ha) (Source: MO or User input).

## `kvaeg_stald_og_lager.py`

### Function: `calculate_co2_stald_lager(...)`

- **Purpose**: Calculates CO2e from cattle housing and manure storage.
- **Constants to be Sourced from JSON**:
  - `phi`: `1.05` (Assumed waste percentage on farm).
  - `formulas.md` Link: `DairyCoefficients.xlsx 'Coefficients'!B4`.
  - Proposed: `dairy_coefficients.json` as `waste_percentage_on_farm` (or similar).
- **Non-Constant Data Required**:
  - `s_co2e`: CO2e from housing and storage per kg FPCM (kg CO2e/kg FPCM) (Source: ARLA API `Farm_KPIManureStorageCO2eq`).
  - `theta_maelk`: Milk allocation factor (Source: ARLA API `Farm_KPIAllocKeyMilk`).
  - `fpcm`: Fat and protein corrected milk per cow (kg) (Source: ARLA API `Farm_KPIFatProteinCorrectedMilkPerCow`).
  - `n_ko`: Number of cows (Source: ARLA API `Farm_KPICowsNHeads`).

## `kvaeg_indkoebt_foder.py`

- **Note on Discrepancy**: The Python implementation `beregn_co2e_indkoebt_foder_kvaeg(fodermidler_data: List[Dict[str, Any]])` expects a list of feedstuffs, each with its own `maengde_kg` and `co2e_faktor_kg`. The `formulas.md` for `Kvaeg/Indkøbt foder.ipynb` describes a different formula: `CO2_importeret_foder = (F_CO2e / theta_maelk) * FPCM * phi * N_ko`, where `F_CO2e` is an aggregated value from ARLA API (`Farm_KPIPurchasedFeedCO2eq`).

### Function: `beregn_co2e_indkoebt_foder_kvaeg(...)` (as per Python file)

- **Purpose**: Calculates total CO2e from a list of purchased feedstuffs.
- **Constants to be Sourced from JSON**:
  - `co2e_faktor_kg` for each feedstuff type would be sourced from a comprehensive feed database JSON.
  - Proposed: `feed_database.json` with entries like `[feed_name].co2e_per_kg`.
- **Non-Constant Data Required**:
  - `fodermidler_data`: List of dicts, each with `maengde_kg` and `co2e_faktor_kg` for a feedstuff (Source: User input, potentially mapped from detailed farm records).

### Based on `formulas.md` (Kvaeg/Indkøbt foder.ipynb) - if this is the target logic:

- **Purpose**: Calculates CO2e from imported feed using aggregated ARLA API data.
- **Constants to be Sourced from JSON**:
  - `phi`: `1.05` (Assumed waste percentage on farm).
  - `formulas.md` Link: `DairyCoefficients.xlsx 'Coefficients'!B4`.
  - Proposed: `dairy_coefficients.json` as `waste_percentage_on_farm`.
- **Non-Constant Data Required**:
  - `f_co2e`: CO2e from imported feed per kg FPCM (kg CO2e/kg FPCM) (Source: ARLA API `Farm_KPIPurchasedFeedCO2eq`).
  - `theta_maelk`: Milk allocation factor (Source: ARLA API `Farm_KPIAllocKeyMilk`).
  - `fpcm`: Fat and protein corrected milk per cow (kg) (Source: ARLA API `Farm_KPIFatProteinCorrectedMilkPerCow`).
  - `n_ko`: Number of cows (Source: ARLA API `Farm_KPICowsNHeads`).

## `kvaeg_enterisk_metan.py`

### Function: `beregn_co2e_enterisk_kvaeg(...)`

- **Purpose**: Calculates CO2e from enteric methane for cattle.
- **Constants to be Sourced from JSON**:
  - `phi`: `1.05` (Assumed waste percentage on farm).
  - `formulas.md` Link: `DairyCoefficients.xlsx 'Coefficients'!B4`.
  - Proposed: `dairy_coefficients.json` as `waste_percentage_on_farm`.
- **Non-Constant Data Required**:
  - `e_co2e`: Enteric methane CO2e per kg FPCM (kg CO2e/kg FPCM) (Source: ARLA API `Farm_KPICH4EntericCO2eq`).
  - `theta_maelk`: Milk allocation factor (Source: ARLA API `Farm_KPIAllocKeyMilk`).
  - `fpcm`: Fat and protein corrected milk per cow (kg) (Source: ARLA API `Farm_KPIFatProteinCorrectedMilkPerCow`).
  - `n_ko`: Number of cows (Source: ARLA API `Farm_KPICowsNHeads`).