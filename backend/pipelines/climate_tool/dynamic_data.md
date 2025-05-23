# Dynamic data needed for the climate tool

This file describes the data for the climate tool that needs to be fetchd from other pipelines / data sources when calculating.

## marker_produktregnskab.py

The `calculate_produktaftryk_afgroede_kg_co2e_pr_ha` function requires `fields_data` as input.
The `co2e_sources` dictionary within each element of `fields_data` is expected to contain pre-calculated CO2e contributions from various emission sources for each field. These values are derived from other calculation modules or external data sources.
Specifically, the following keys (and their corresponding values) within `co2e_sources` are dynamically determined:

- `goedning`
- `kalkning`
- `afgroederester`
- `nitratudvaskning`
- `organogene_jorde`
- `import_goedning` (illustrative)
- `import_diesel` (illustrative)

## marker_afgroederester.py

The calculations in this module depend on several crop-specific and scenario-specific parameters that are currently represented by placeholder values in the test cases. These need to be fetched from external data tables or other pipelines based on inputs like `afgroede_kode` (crop code) and specific field conditions.

Dynamically fetched parameters include:

- `s_slope`: Slope for calculating above-ground residue (A_over). Found in `tabel_27_hældning_og_intercept_ton_tørstof_til_beregning_af_overjordisk_afgrøderest_samt_n-indhold_i.json` as "Slope".
- `i_intercept`: Intercept for calculating A_over. Found in `tabel_27_hældning_og_intercept_ton_tørstof_til_beregning_af_overjordisk_afgrøderest_samt_n-indhold_i.json` as "Intercept".
- `h_f_halmfraktion`: Straw fraction relative to yield. Likely in `tabel_23_udsnit_af_ipcc_2006_værdier_for_beregning_af_kg_ts_i_afgrøderester_i_forhold_til_udbytte_si.json` (needs lookup logic based on crop).
- `f_forhold_under_over_biomasse`: Ratio of below-ground to above-ground biomass. Found in `tabel_25_faktorer_for_underjordisk_afgrøderest_i_forhold_til_total_ipcc_20062019_side_88.json` or `tabel_28_faktor_til_beregning_af_underjordisk_afgrøderest_samt_n-indhold_i_underjordisk_afgrøderest_.json` based on crop type.
- `n_over_kg_n_pr_kg_ts`: Nitrogen content in above-ground crop residue. Found in `tabel_24_kgn_kg_tørstof_indhold_for_overjordiske_og_underjordiske_dele_af_planten_ipcc_2006_side_87.json` or `tabel_27_hældning_og_intercept_ton_tørstof_til_beregning_af_overjordisk_afgrøderest_samt_n-indhold_i.json`.
- `n_under_kg_n_pr_kg_ts`: Nitrogen content in below-ground crop residue. Found in `tabel_24_kgn_kg_tørstof_indhold_for_overjordiske_og_underjordiske_dele_af_planten_ipcc_2006_side_87.json` or `tabel_28_faktor_til_beregning_af_underjordisk_afgrøderest_samt_n-indhold_i_underjordisk_afgrøderest_.json`.
- `o_omlaegningsfrekvens`: Crop rotation frequency factor. Source needs to be determined, potentially from farm management data.

Inputs that are also dynamic:

- `t_torstof_total_kg_ts_ha`: Total dry matter yield (main harvest). This comes from yield data for the specific crop on the field.
- `h_u_fast_halmudbytte_kg_ts_ha`: Fixed straw yield (e.g., for seed grass). Crop-specific, potentially zero for many crops.
- `n_graes_kg_n_ha`: Amount of N deposited during grazing, used to calculate `k_graes`. This is dynamic based on grazing activity.

Constants status:

- `EF_N2O_AFGROEDERESTER`: Loaded from `tabel_19_ammoniak-emissionerne_fra_udbringning_af_organisk_gødning_side_75-76.json`.
- `THETA_N2O_CO2`: Currently hardcoded as `265.0`. The documentation mentions a discrepancy with some C# outputs using `298.0`. This value might need to be configurable or loaded from a definitive source if identified.
- `MOL_WEIGHT_N2O_N_FACTOR`: Universal chemical constant (`44.0 / 28.0`), remains hardcoded.

The boolean flags `x1_halmnedmulding` (straw incorporation) and `x2_udbytte_nedmuldes` (yield incorporation) are also dynamic inputs based on farm practices for the specific field.

## marker_kulstofbalance.py

This module calculates CO2e from carbon balance. It uses several constants:

- `C_FRAK_TORSTOF_TIL_C = 0.45`: Assumed carbon fraction in dry matter. This is currently hardcoded. It might be a general constant, but its source should be verified if more specific values per crop type exist.
- `MU_DK_GENNEMSNIT_INPUT_C = 4093.0`: Average Danish input of carbon from a crop (kg C/ha). This is a specific Danish average value and is hardcoded. Its source is likely a national inventory or specific study.
- `F_HUS_N_TIL_C_ORGANISK = 8.0`: Conversion factor from N to C for organic fertilizer (kg C/kg N). Currently hardcoded. `tabel_33_normtal_for_kvælstof_tons_gødning_og_ts_kulstofbalancen_i_jorden_side_102.json` shows this factor varies by manure type (e.g., 8 for "kvæggylle", 6 for "svinegylle"). The calculation should ideally use a value specific to the `n_hus_plus_afg_kg_n_ha` input, which represents N from animal manure and grazing.
- `MOL_WEIGHT_CO2_C_FACTOR = 44.0 / 12.0`: Universal chemical constant for converting C to CO2. Remains hardcoded.
- `STABILIZATION_FACTOR = 0.097`: Factor for stabilization of C in soil. This is currently hardcoded. The origin or potential variability of this factor should be clarified (e.g., dependency on soil type, climate).

Dynamic inputs to this module:

- `a_over_kg_ts_ha`: Above-ground crop residue in kg dry matter/ha (from `marker_afgroederester.py`).
- `a_under_kg_ts_ha`: Below-ground crop residue in kg dry matter/ha (from `marker_afgroederester.py`).
- `n_hus_plus_afg_kg_n_ha`: Nitrogen from animal manure and grazing (kg N/ha). This is an input to the function `calculate_C_organisk_goedning_kg_c_ha`.
- `r_relativ_faktor`: Integer (0, 1, or 2) indicating if the calculation should be relative to the Danish average, absolute, or zeroed out. This is a scenario/logic input.
- `areal_ha`: Area in hectares.

The placeholder functions `calculate_A_over_placeholder` and `calculate_A_under_placeholder` must be replaced with actual calls to `marker_afgroederester.py`.

## marker_goedning_og_nitrifikationshaemmer.py

This module calculates N2O emissions from fertilizer application and the effect of nitrification inhibitors.

Constants loaded from JSON:

- `EF_N2O_GENERAL`: Loaded from `tabel_19_ammoniak-emissionerne_fra_udbringning_af_organisk_gødning_side_75-76.json` (value `0.01`). This is used as the general N2O emission factor for N deposition (from NH3 and NOx) and direct soil emissions for non-grazing scenarios.

Hardcoded constants (to be verified or made dynamic):

- `THETA_N2O_CO2 = 265.0`: Conversion factor N2O to CO2e. Status discussed previously (see `marker_afgroederester.py`).
- `EF_N2O_JORD_AFGRAESNING = 0.004`: N2O emission factor for soil under grazing. `tabel_11` shows `N2O_pct_af_N_Dybstrøelse` for "Afgræsning" as `2` (i.e., 0.02 if it is a direct factor for N to N2O-N), which differs. The source/basis for `0.004` needs clarification.
- `EF_NH3_HANDELSGOEDNING = 0.05`: NH3 emission factor for commercial fertilizer. `tabel_19` shows `0.025`. `tabel_22` shows this varies significantly by specific fertilizer type. The `0.05` might be an intended average or a standard from a different source.
- `EF_NH3_HUSDYRGOEDNING = 0.08`: NH3 emission factor for animal manure. `tabel_19` shows this varies by manure type (e.g., `0.107` for pig slurry, `0.132` for cattle slurry). The `0.08` needs to be mapped to specific manure types or confirmed as a general average.
- `EF_NH3_AFGRAESNING = 0.084`: NH3 emission factor for grazing. The Python comment specifies this for cattle. `tabel_20` shows `0.14` for "Afgræsning kvæg". The discrepancy needs resolution.
- `EF_NOX = 0.04`: NOx emission factor. Not found in the provided tables. Its source and applicability (e.g., general or fertilizer-specific) should be determined.
- `MOL_WEIGHT_FACTOR_N2O_N = 44.0 / 28.0`: Universal chemical constant. Remains hardcoded.
- `MOL_WEIGHT_FACTOR_NOX_N = 46.0 / 14.0`: Universal chemical constant (assuming NOx is NO2 for this ratio). Remains hardcoded.
- `NITRIFICATION_INHIBITOR_EFFECTIVENESS = 0.4`: Effectiveness of nitrification inhibitors (40% reduction). Not found in tables. Source/basis should be confirmed.

Dynamic inputs to this module:

- `n_total_kg_ha`: Total N applied per ha for a given fertilizer type.
- `areal_ha`: Area in hectares.
- `goedningstype`: String indicating fertilizer type ("handelsgoedning", "husdyrgoedning", "afgraesning"). This input determines which EF_NH3 and EF_N2O_JORD factor is used from the hardcoded set.
- `n_nitri_kg_ha`: Amount of N per ha treated with nitrification inhibitor.

Future improvements:

- The selection of `EF_NH3_...` and `EF_N2O_JORD_AFGRAESNING` should be driven by more granular input (e.g., specific commercial fertilizer type, specific animal manure type, specific grazing animal type) and fetched from the tables rather than using a few hardcoded general values.

## marker_organogene_jorde.py

This module calculates CO2, N2O (as CO2e), and CH4 (as CO2e) emissions from organogene jorde based on land characteristics (area, cultivation status, water level, carbon percentage).

Constants and their sources:

- **CO2 emissions from carbon (tons CO2/ha):** These are conditional and based on cultivation status (`i_omdrift`), water level (`lav_vandstand`), and carbon percentage (`kulstof_percentage`).
  - Values like `21.08`, `42.17`, `30.8`, `15.4` are used.
  - These correspond to values in `tabel_31_emission_af_co2_fra_nedbrydning_af_organisk_stof_på_organogen_jord_ton_co2_pr_ha_side_96.json`.
    - `i_omdrift = True` maps to the "Omdrift" column in the table.
    - `i_omdrift = False` (and `lav_vandstand = True`) maps to the "Permanent_græs_og_afvandet" column.
- **N2O emissions (tons CO2e/ha):**
  - `3.87` (for `i_omdrift=True`, `lav_vandstand=True`, `kulstof_percentage=">12%C"`)
  - `2.44` (for `i_omdrift=False`, `lav_vandstand=True`, `kulstof_percentage=">12%C"`)
  - In other cases for `lav_vandstand`, N2O is `0.0`.
  - The source of these specific CO2e values is not directly derivable from `tabel_30_emission_af_n2o_fra_nedbrydning_af_organisk_stof_på_organogen_jord_kg_n2o-n_pr_ha_side_96.json` using standard GWP factors (265 or 298 for N2O). These values are currently hardcoded in the Python script as `N2O_CO2E_OMDRIFT_GT12C` and `N2O_CO2E_PERMGRAS_GT12C` after an attempt to load them. Their precise origin or calculation method needs to be documented or found.
- **CH4 emissions (tons CO2e/ha):**
  - `6.8` (for `i_omdrift=False`, `lav_vandstand=False` (i.e., høj vandstand)). Kulstof percentage is irrelevant for this rule.
  - This value corresponds to `CH4_CO2e_kg_ha_aar / 1000.0` from `tabel_32_effekter_af_udtagning_af_organogen_jord_olesen_et_al_2018_dca_rapport_nr_130_side_97.json` for "Vådområde (ikke dyrket)".
  - In other cases, CH4 is `0.0`.

Dynamic inputs:

- `h` (float): Area in hectares.
- `i_omdrift` (bool): Cultivation status.
- `lav_vandstand` (bool): Water level status.
- `kulstof_percentage` (str): Carbon content range (e.g., "6-12%C", ">12%C"). This needs to match the keys used for lookup in `tabel_31` (e.g. "6-12%C", ">12%C").

Implementation notes:

- The Python script was intended to be updated to load CO2 (kulstof) and CH4 values from `tabel_31...json` and `tabel_32...json` respectively.
- The N2O CO2e values (3.87 and 2.44) remain effectively hardcoded as their calculation from base N2O-N values in `tabel_30` is not straightforward with available GWP.
- The case `i_omdrift=True` and `lav_vandstand=False` (omdrift med høj vandstand) is not explicitly covered by the rules in the script and results in zero emissions. This should be verified if it's a possible scenario with non-zero emissions.

## marker_kalkning.py

This module calculates CO2 emissions from liming (kalkning).

Constants used:

- `M_CACO3 = 100.09`: Molar mass of CaCO3 (g/mol). Universal chemical constant.
- `M_C = 12.01`: Molar mass of Carbon (g/mol). Universal chemical constant.
- `S_CACO3_PER_HA = 170.0`: Standard application rate of CaCO3 per hectare per year (kg/ha/år). This is currently hardcoded. Its source (e.g., national guidelines, specific study) should be documented. A search in the provided tables did not directly locate this value.
- The factor `(44.0 / 12.0)` is used to convert mass of C to mass of CO2, based on molar masses. This is also a standard chemical conversion factor.

Dynamic inputs:

- `a_total_kalket_areal` (float): Total area of limed fields on the farm (ha). Used in `calculate_co2_kalkning_bedrift`.
- `a_mark_areal` (float): Area of the specific field being calculated (ha). Used in `calculate_co2_kalkning_mark`.

The calculation first determines total CO2 for the farm based on `a_total_kalket_areal` and `S_CACO3_PER_HA`, then allocates it to a specific mark based on `a_mark_areal` relative to `a_total_kalket_areal`.

## marker_nitratudvaskning.py

This module calculates N2O emissions from nitrate leaching and its CO2 equivalent.

The formula used is: `N2O_kg = t * 0.0075 * (44/28) * h`
Then: `CO2e_kg = N2O_kg * THETA_N2O_CO2`

Constants:

- `THETA_N2O_CO2 = 265.0`: Conversion factor from N2O to CO2e. Hardcoded, consistent with previous discussions. The file notes that `formulas.md` examples might use `298`, but `265` is used here based on notebook cell definitions.
- `0.0075`: This is the IPCC emission factor `EF1` for N2O emissions from N leaching and runoff (kg N2O-N / kg N leached or run-off). This is a standard IPCC value and remains hardcoded.
- `(44.0 / 28.0)`: Molar mass ratio of N2O to N in N2O (M_N2O / M_N2). Universal chemical constant, remains hardcoded.

Dynamic inputs:

- `t` (float): "Typetal for nitratudvaskning" (Typical value for nitrate leaching). The comments state this is a "Tabelværdi" (table value) and will depend on factors like crop type, soil type, and potentially other conditions (e.g., use of catch crops). This value needs to be fetched from an appropriate data table. In the test cases, values like `63.0` (for Vårbyg), `74.0` (for Solsikke), and `-30.0` (for pligtige efterafgrøder) are used.
- `h` (float): Area of the field in hectares.

## kvaeg_stald_og_lager.py

This module calculates CO2e from cattle housing (stald) and manure storage (lager).

The formula is: `CO2_stald_lager = (S_CO2e / theta_maelk) * FPCM * phi * N_ko`

Dynamic inputs (primarily from "ARLA API"):

- `s_co2e` (float): Emission from housing and storage per kg FPCM (kg CO2e/kg FPCM). From ARLA API (`Farm_KPIManureStorageCO2eq`).
- `theta_maelk` (float): Allocation factor for milk. From ARLA API (`Farm_KPIAllocKeyMilk`).
- `fpcm` (float): Fat and protein corrected milk production per cow, delivered (kg). From ARLA API (`Farm_KPIFatProteinCorrectedMilkPerCow`).
- `n_ko` (int): Number of cows. From ARLA API (`Farm_KPICowsNHeads`).

Table-derived or standard value input:

- `phi` (float): Assumed on-farm wastage percentage (e.g., 1.05 for 5% wastage). The comments state this is a "Tabelværdi". The specific table and key for this value need to be identified. A search for "spildprocent" did not directly locate it in the provided tables. Test cases use `1.05`.

## fjerkrae_produkt_og_bedriftsregnskab_aeg.py

This module calculates product and farm-level CO2e footprints for egg production.

Functions:

- `beregn_produktaftryk_aeg_pr_kg`:
  - Inputs like `v_aeg_gram` (average egg weight) and `a_aeg_pr_holdhoene` (eggs per hen) are dynamic farm/flock-specific data.
  - `co2e_total_pr_holdhoene` and `co2e_slagt_pr_holdhoene` are results from other calculations.
- `beregn_co2e_total_holdhoene`:
  - All inputs (`co2e_el`, `co2e_enterisk_metan`, etc.) are summed CO2e values from other specific emission source modules for poultry.
- `beregn_bedriftsaftryk_aeg`:
  - Takes a list of `hold_data`, where each item contains results like `p_h` (total CO2e per hen before slaughter allocation), `co2e_hjemme` (CO2e from home-grown feed per hen), `a_aars` (number of annual hens), and `co2e_slagt` (CO2e allocated to slaughter per hen).

There are no hardcoded constants within this module that need to be fetched from the JSON tables. All calculation inputs are expected to be provided dynamically, either as farm-specific data or as results from other emission calculation modules.

## kvaeg_bedriftsaftryk.py

This module calculates the total farm-level CO2e footprint for cattle production by summing contributions from various sources.

Inputs:

- `farmahead_data` (dict): Contains CO2e values from ARLA's FarmAhead calculations for:
  - `stald_og_lager` (housing and manure storage)
  - `fordoejelse` (digestion/enteric fermentation)
  - `importerede_dyr` (imported animals)
  - `importeret_foder` (imported feed)
- `mark_data` (dict): Contains CO2e values from the ESGreenTool field module for:
  - `grovfoder` (home-grown roughage/fodder)
  - `salgsafgroeder` (cash crops)

There are no hardcoded constants within this module that need to be fetched from the JSON tables. All inputs are aggregated results from other data sources or calculation modules.

## import_importeret_goedning.py

This module calculates CO2e from imported commercial fertilizer based on N, P, and K content.

Formula: `co2e = (n_total * NK_KONSTANT + p_total * PK_KONSTANT + k_total * KK_KONSTANT) * areal`

Constants loaded from JSON:

- `NK_KONSTANT`: Carbon footprint for Nitrogen (N) in commercial fertilizer (kg CO2e/kg N). Loaded from `gødning_carbon_footprint_side_82.json` (value `6.6`).
- `PK_KONSTANT`: Carbon footprint for Phosphorus (P) in commercial fertilizer (kg CO2e/kg P). Loaded from `gødning_carbon_footprint_side_82.json` (value `3.6`).
- `KK_KONSTANT`: Carbon footprint for Potassium (K) in commercial fertilizer (kg CO2e/kg K). Loaded from `gødning_carbon_footprint_side_82.json` (value `0.7`).

Dynamic inputs:
- `n_total` (float): Total kg N/ha from commercial fertilizer.
- `p_total` (float): Total kg P/ha from commercial fertilizer.
- `k_total` (float): Total kg K/ha from commercial fertilizer.
- `areal` (float): Field area in hectares.

## kvaeg_indkoebte_dyr.py

This module calculates CO2e emissions associated with purchased cattle.

The formula is: `co2e_indkoebte_dyr = (i_co2e / theta_maelk) * fpcm * phi * n_ko`

Dynamic inputs (primarily from "ARLA API"):

- `i_co2e` (float): CO2e from imported animals per kg FPCM (kg CO2e/kg FPCM). Source from ARLA API is noted as '???' in the comments, indicating potential uncertainty or a placeholder name.
- `theta_maelk` (float): Allocation factor for milk. From ARLA API.
- `fpcm` (float): Fat and protein corrected milk production per cow, delivered (kg). From ARLA API.
- `n_ko` (float): Number of cows. From ARLA API.

Table-derived or standard value input:

- `phi` (float): Assumed on-farm wastage percentage (e.g., 1.05 for 5% wastage). This is stated as a "Tabelværdi". It is the same `phi` as used in `kvaeg_stald_og_lager.py`. The specific table and key for this value need to be identified if it varies or if a more specific source is required than a general assumption.

No new constants are introduced in this file that require fetching from JSON tables, assuming `phi` is handled consistently.

## kvaeg_indkoebt_foder.py

This module calculates CO2e emissions from purchased feed for cattle.

Formula: `total_co2e = sum(foder_item['maengde_kg'] * foder_item['co2e_faktor_kg'])` for all feed items.

Dynamic inputs:

- `fodermidler_data` (List[Dict]): A list where each dictionary represents a purchased feed item and contains:
  - `maengde_kg` (float): The amount of the feed item in kg.
  - `co2e_faktor_kg` (float): The CO2e emission factor per kg of the specific feed item (kg CO2e / kg feed). This factor itself is a form of constant for that feed type and needs to be sourced from appropriate data (e.g., derived from tables like `tabel_3_indkøbte_fodermidler_har_følgende_klimaværdi_udtryk_i_g_co2_ækv_per_kg_tørstof_side_24-25.json`, ensuring units and dry matter conversions are handled before being passed to this function).

There are no hardcoded constants within this specific calculation module that require fetching from the JSON tables. The emission factors for feed are expected to be provided as part of the input data.

## kvaeg_enterisk_metan.py

This module calculates CO2e emissions from enteric methane for cattle.

The formula is: `co2_enterisk = (e_co2e / theta_maelk) * fpcm * phi * n_ko`

Dynamic inputs (primarily from "ARLA API"):

- `e_co2e` (float): CO2e from enteric methane per kg FPCM (kg CO2e/kg FPCM).
- `theta_maelk` (float): Allocation factor for milk.
- `fpcm` (float): Fat and protein corrected milk production per cow, delivered (kg).
- `n_ko` (float): Number of cows.

Table-derived or standard value input:

- `phi` (float): Assumed on-farm wastage percentage. This is the same `phi` as used in `kvaeg_stald_og_lager.py` and `kvaeg_indkoebte_dyr.py`. Its specific table source needs identification if a general assumption is not sufficient.

No new constants are introduced in this file that require fetching from JSON tables, assuming `phi` is handled consistently.

## kvaeg_el.py

This module calculates CO2e emissions from electricity consumption for cattle.

The formula is: `co2e_el = n_ko * e_ko * o_el`

Dynamic inputs and table values:

- `n_ko` (float): Number of cows. This is a dynamic input.
- `e_ko` (float): Standard electricity consumption per cow (kWh/cow). This is a standard factor that needs to be sourced, likely from a table or specific study (e.g., national agricultural statistics or benchmarks).
- `o_el` (float): Emission factor for electricity (kg CO2e/kWh). This is the grid electricity emission factor and needs to be sourced, typically from national energy statistics or a relevant database (e.g., `tabel_35a` might be intended to hold this, but currently doesn't list a direct grid factor in kWh, or it might be in a more general import module like `import_el.py`).

No hardcoded constants within this module need to be changed to load from the provided JSONs, but the sources for `e_ko` and `o_el` must be clearly defined and implemented in the data pipeline that calls this function.

## import_el.py

This module calculates CO2e emissions from electricity consumption at the farm level, for field irrigation, and for other field-related electricity uses (e.g., drying).

Common dynamic input/table value for all functions:

- `o_el_kg_co2e_pr_kwh` (float): Emission factor for electricity (kg CO2e/kWh). This is the grid electricity emission factor. As discussed for `kvaeg_el.py`, this needs to be sourced from national energy statistics or a relevant data table (e.g., potentially a more specific entry in `tabel_35a_emissions-_og_omregningsfaktorer_energiforbrug_side_108-109.json` or a dedicated table for electricity grid factors). The functions in this module expect this value as an argument.

Other dynamic inputs specific to functions:

- `beregn_co2e_el_bedrift`:
  - `e_ind_kwh` (float): Purchased electricity (kWh).
  - `e_egen_kwh` (float): Self-produced electricity (kWh).
- `beregn_co2e_el_vanding_mark`:
  - `f_v_kwh` (float): Total farm electricity consumption for irrigation (kWh).
  - `sum_ha_v` (float): Total farm area that can be irrigated (ha).
  - `ha_a` (float): Area of the specific field being calculated (ha).
- `beregn_co2e_el_andet_mark`:
  - `f_t_kwh` (float): Total farm electricity consumption for drying, etc. (kWh).
  - `sum_ha_m` (float): Total farm area with crops harvested to maturity (ha).
  - `ha_a` (float): Area of the specific field being calculated (ha).

No hardcoded constants within this module require fetching from the provided JSON tables; the critical emission factor `o_el_kg_co2e_pr_kwh` is an input parameter.

## import_diesel_maskinarbejde.py

This module calculates diesel consumption related to machine work and the resulting Scope 1 and Scope 3 CO2e emissions, as well as allocating diesel CO2e to specific crops.

Function `beregn_diesel_fra_maskinarbejde`:

- Formula: `m = (p_k_kr * theta_m_l_pr_kr) - (p_s_kr * theta_m_l_pr_kr)`
- Table value / Dynamic input:
  - `theta_m_l_pr_kr` (float): Conversion factor from DKK machine work to Liters of diesel (L/kr). This economic conversion factor needs to be sourced from relevant data (e.g., standard costs for machine work, diesel prices).
- Dynamic inputs:
  - `p_k_kr` (float): Price of purchased machine work (kr).
  - `p_s_kr` (float): Price of sold machine work (kr).

Function `beregn_co2e_diesel_scope1`:

- Formula: `d_scope1 = (d_total_liter_bedrift + m_maskinarbejde_liter) * O_SCOPE1_DIESEL`
- Constant loaded from JSON:
  - `O_SCOPE1_DIESEL`: Emission factor for diesel combustion (Scope 1, kg CO2e/L). Loaded from `tabel_36_emissioner_fra_transportsektoren_er_baseret_på_følgende_værdier_baseret_på_den_nationale_op.json` (value `2.654`).
- Dynamic inputs:
  - `d_total_liter_bedrift` (float): Total diesel consumption on the farm (L).
  - `m_maskinarbejde_liter` (float): Diesel from machine work (L), result of `beregn_diesel_fra_maskinarbejde`.

Function `beregn_co2e_diesel_scope3`:

- Formula: `d_scope3 = (d_total_liter_bedrift + m_maskinarbejde_liter) * theta_d3_kg_co2e_pr_l`
- Table value / Dynamic input:
  - `theta_d3_kg_co2e_pr_l` (float): Emission factor for diesel production (Scope 3, kg CO2e/L). Needs to be sourced from LCA data or specific tables (e.g., `tabel_35a...` might contain elements if converted and allocated, or a dedicated upstream emission factor table).
- Dynamic inputs:
  - `d_total_liter_bedrift` (float): Total diesel consumption on the farm (L).
  - `m_maskinarbejde_liter` (float): Diesel from machine work (L).

Function `beregn_produktaftryk_diesel_afgroede`:

- Allocates total diesel CO2e (Scope 1+3) to a specific crop based on typical diesel use per hectare.
- Table values / Dynamic inputs:
  - `t_a_typetal_diesel_pr_ha` (float): Typical diesel consumption for the specific crop `a` (L/ha). Crop-specific table value.
  - `alle_afgroeder_data` (List[Dict]): Data for all crops, each containing `h_i_hektar` (area) and `t_i_typetal_diesel_pr_ha` (typical diesel L/ha for that crop). These `t_i` values are also crop-specific table values.
  - `theta_d_total_kg_co2e_pr_l` (float): Total CO2e emission factor for diesel (Scope 1 + Scope 3) in kg CO2e/L. This would be `O_SCOPE1_DIESEL + theta_d3_kg_co2e_pr_l`.
- Dynamic inputs:
  - `h_a_hektar` (float): Area of crop `a` (ha).
  - `d_total_liter_bedrift_korrigeret` (float): Total farm diesel consumption corrected for machine work (L).