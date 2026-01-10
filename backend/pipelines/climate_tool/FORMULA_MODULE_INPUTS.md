# Climate Tool Formula Module Input Documentation

This document provides comprehensive documentation of input requirements, function signatures, and expected data types for all formula modules in the climate tool pipeline.

**Last Updated**: 2026-01-10

---

## Table of Contents

1. [kvaeg/enterisk_metan.py](#kvægenterisk_metanpy)
2. [kvaeg/stald_og_lager.py](#kvægstald_og_lagerpy)
3. [marker/goedning_og_nitrifikationshaemmer.py](#markergoedning_og_nitrifikationshaemmerpy)
4. [marker/afgroederester.py](#markerafgroederesterpy)
5. [marker/organogene_jorde.py](#markerorganogene_jordepy)
6. [Integration Status Summary](#integration-status-summary)

---

## kvaeg/enterisk_metan.py

**Status**: ✅ Ready to integrate

### Purpose
Calculates CO2e emissions from enteric methane for cattle based on feed intake, fat content, and fiber content.

### Constants
- `THETA_CH4_CO2 = 25.0` - GWP factor for CH4 to CO2e conversion
- `ENTERIC_CH4_YOUNGSTOCK_DEFAULTS_KG_CH4_PER_PERIOD` - Default CH4 values for young cattle (0-6 months)

### Functions

#### 1. `calculate_ch4_enteric_malkeko_tung_race_pr_aar()`
Calculates annual CH4 emissions for heavy breed dairy cows.

**Parameters**:
- `foderoptag_kg_ts_pr_dag` (float): Daily feed intake in kg dry matter per day
- `fedtsyre_g_pr_kg_ts` (float): Fat content in g per kg dry matter
- `ndf_g_pr_kg_ts` (float): NDF (Neutral Detergent Fiber) in g per kg dry matter

**Returns**: `float` - kg CH4 per cow per year

**Example**:
```python
ch4_mktr = calculate_ch4_enteric_malkeko_tung_race_pr_aar(
    foderoptag_kg_ts_pr_dag=24.0,
    fedtsyre_g_pr_kg_ts=20.0,
    ndf_g_pr_kg_ts=350.0
)
# Returns: ~150.5 kg CH4/cow/year
```

---

#### 2. `calculate_ch4_enteric_malkeko_jersey_pr_aar()`
Calculates annual CH4 emissions for Jersey dairy cows.

**Parameters**:
- `foderoptag_kg_ts_pr_dag` (float): Daily feed intake in kg dry matter per day
- `fedtsyre_g_pr_kg_ts` (float): Fat content in g per kg dry matter
- `ndf_g_pr_kg_ts` (float): NDF in g per kg dry matter

**Returns**: `float` - kg CH4 per cow per year

**Formula Difference**: Uses different dry period factor (0.207 vs 0.304 for heavy breeds)

---

#### 3. `calculate_ch4_enteric_opdraet_og_tyre_aeldre_pr_aar()`
Calculates annual CH4 emissions for older heifers (>6 months) and bulls (>6 months to slaughter).

**Parameters**:
- `foderoptag_kg_ts_pr_dag` (float): Daily feed intake in kg dry matter per day
- `kraftfoderandel_procent` (float): Concentrate feed percentage of total intake (0-100)
- `fedtsyre_g_pr_kg_ts` (float): Fat content in g per kg dry matter

**Returns**: `float` - kg CH4 per animal per year

**Note**: Ash intake is fixed at 860 g/animal/day internally

**Example**:
```python
ch4_heifer = calculate_ch4_enteric_opdraet_og_tyre_aeldre_pr_aar(
    foderoptag_kg_ts_pr_dag=7.7,
    kraftfoderandel_procent=9.0,
    fedtsyre_g_pr_kg_ts=19.0
)
# Returns: ~44.5 kg CH4/animal/year
```

---

#### 4. `beregn_co2e_enterisk_kvaeg_total()`
**Main integration function** - Calculates total CO2e emissions from enteric methane for all cattle on the farm.

**Parameters**:
- `dyretype_counts` (dict): Dictionary mapping animal types to their counts and feed parameters

**Dictionary Structure**:
```python
{
    "malkeko_tung_race": {
        "count": int,  # Number of animals
        "foderoptag_kg_ts_pr_dag": float,
        "fedtsyre_g_pr_kg_ts": float,
        "ndf_g_pr_kg_ts": float
    },
    "malkeko_jersey": {
        "count": int,
        "foderoptag_kg_ts_pr_dag": float,
        "fedtsyre_g_pr_kg_ts": float,
        "ndf_g_pr_kg_ts": float
    },
    "opdraet_aeldre_tung": {  # Heifers >6 months, heavy breed
        "count": int,
        "foderoptag_kg_ts_pr_dag": float,
        "kraftfoderandel_procent": float,
        "fedtsyre_g_pr_kg_ts": float
    },
    "opdraet_aeldre_jersey": {  # Heifers >6 months, Jersey
        "count": int,
        "foderoptag_kg_ts_pr_dag": float,
        "kraftfoderandel_procent": float,
        "fedtsyre_g_pr_kg_ts": float
    },
    "tyre_aeldre_tung": {  # Bulls >6 months, heavy breed
        "count": int,
        "foderoptag_kg_ts_pr_dag": float,
        "kraftfoderandel_procent": float,
        "fedtsyre_g_pr_kg_ts": float
    },
    "tyre_aeldre_jersey": {  # Bulls >6 months, Jersey
        "count": int,
        "foderoptag_kg_ts_pr_dag": float,
        "kraftfoderandel_procent": float,
        "fedtsyre_g_pr_kg_ts": float
    },
    # For young stock (0-6 months), only count is needed - uses defaults
    "opdraet_0_6mdr_tung": {"count": int},
    "opdraet_0_6mdr_jersey": {"count": int},
    "tyre_0_6mdr_tung": {"count": int},
    "tyre_0_6mdr_jersey": {"count": int}
}
```

**Returns**: `float` - Total CO2e in kg for entire farm

**Example**:
```python
test_bedrift_dyr = {
    "malkeko_tung_race": {
        "count": 100,
        "foderoptag_kg_ts_pr_dag": 23.5,
        "fedtsyre_g_pr_kg_ts": 22.0,
        "ndf_g_pr_kg_ts": 340.0
    },
    "opdraet_0_6mdr_tung": {"count": 20}
}
total_co2e = beregn_co2e_enterisk_kvaeg_total(test_bedrift_dyr)
# Returns: Total CO2e in kg
```

---

## kvaeg/stald_og_lager.py

**Status**: ⚠️ Requires ARLA API data

### Purpose
Calculates CO2e emissions from barn and manure storage systems. **This module depends on data from ARLA API**.

### Function

#### `calculate_co2_stald_lager()`
Calculates CO2e from barn and storage based on ARLA metrics.

**Parameters**:
- `s_co2e` (float): Emissions from barn and storage in kg CO2e per kg FPCM (from ARLA API: `Farm_KPIManureStorageCO2eq`)
- `theta_maelk` (float): Milk allocation factor (from ARLA API: `Farm_KPIAllocKeyMilk`)
- `fpcm` (float): Fat-protein corrected milk per cow in kg (from ARLA API: `Farm_KPIFatProteinCorrectedMilkPerCow`)
- `phi` (float): Assumed waste percentage on farm (table value, typically 1.05 = 5% waste)
- `n_ko` (int): Number of cows (from ARLA API: `Farm_KPICowsNHeads`)

**Returns**: `float` - CO2e in kg

**Formula**: `CO2_stald_lager = (s_co2e / theta_maelk) * fpcm * phi * n_ko`

**Example**:
```python
result = calculate_co2_stald_lager(
    s_co2e=0.05,        # From ARLA API
    theta_maelk=0.87,   # From ARLA API
    fpcm=10456.95,      # From ARLA API
    phi=1.05,           # Table value
    n_ko=109            # From ARLA API
)
# Returns: 68781.49 kg CO2e
```

**Integration Notes**:
- Requires ARLA API integration to obtain: `s_co2e`, `theta_maelk`, `fpcm`, `n_ko`
- `phi` (waste percentage) should come from lookup tables based on farm type
- This is the only formula module that depends on external API data

---

## marker/goedning_og_nitrifikationshaemmer.py

**Status**: ✅ Ready to integrate

### Purpose
Calculates N2O emissions from fertilizer application and nitrification inhibitors on fields.

### Constants
- `THETA_N2O_CO2 = 265.0` - N2O to CO2e conversion factor
- `EF_N2O_JORD_AFGRAESNING = 0.004` - Emission factor for grazing
- `EF_NH3_HANDELSGOEDNING = 0.05` - Default NH3 emission factor for commercial fertilizer
- `EF_NH3_HUSDYRGOEDNING = 0.08` - NH3 emission factor for manure
- `EF_NH3_AFGRAESNING = 0.084` - NH3 emission factor for grazing (cattle)
- `EF_NOX = 0.04` - NOx emission factor
- `NITRIFICATION_INHIBITOR_EFFECTIVENESS = 0.4` - 40% reduction

### Data Loading
- Loads `EF_N2O_GENERAL` from `tabel_19` (default 0.01)
- Loads specific NH3 emission factors for commercial fertilizers from `tabel_22`

### Functions

#### 1. `get_ef_nh3_for_handelsgoedning()`
Retrieves NH3 emission factor for specific commercial fertilizer types.

**Parameters**:
- `handelsgoedning_type` (str | None): Fertilizer type name (e.g., "Urea*", "NPK*"), or None for default

**Returns**: `float` - NH3 emission factor (kg NH3-N per kg N)

**Available Types** (from tabel_22):
- "Urea*"
- "NPK*"
- "NK*"
- "NP*"
- And others from tabel_22

**Falls back to** `DEFAULT_EF_NH3_HANDELSGOEDNING` (0.05) if type not found.

---

#### 2. `calculate_n2o_components()`
Internal helper function - calculates the three N2O emission components.

**Parameters**:
- `n_total_kg_ha` (float): Total N in fertilizer per ha [kg N/ha]
- `areal_ha` (float): Field area [ha]
- `ef_n2o_jord` (float): Soil N2O emission factor
- `ef_nh3` (float): NH3 emission factor

**Returns**: `Tuple[float, float, float]`
- `n2o_jord_kg` - N2O from soil
- `n2o_nh3_kg` - N2O from NH3 deposition
- `n2o_nox_kg` - N2O from NOx deposition

---

#### 3. `calculate_n2o_goedning()`
**Main integration function** - Calculates N2O emissions from fertilizer application.

**Parameters**:
- `n_total_kg_ha` (float): Total N in fertilizer per ha [kg N/ha]
- `areal_ha` (float): Field area [ha]
- `goedningstype` (str): Fertilizer type - one of:
  - `"handelsgoedning"` - Commercial fertilizer
  - `"husdyrgoedning"` - Manure
  - `"afgraesning"` - Grazing
- `n_nitri_kg_ha` (float, optional): Amount of N with nitrification inhibitor per ha [kg N/ha] (default: 0.0)
  - Only applies to "handelsgoedning" and "husdyrgoedning"
  - Ignored for "afgraesning"
- `handelsgoedning_detail_type` (str | None, optional): Specific commercial fertilizer type (default: None)
  - Only used when `goedningstype="handelsgoedning"`
  - Example: "Urea*"

**Returns**: `Tuple[float, float]`
- `total_n2o_kg` - Total N2O in kg
- `total_co2e_kg` - Total CO2e in kg

**Example 1 - Commercial Fertilizer with Inhibitor**:
```python
n2o_kg, co2e_kg = calculate_n2o_goedning(
    n_total_kg_ha=122.0,
    areal_ha=10.0,
    goedningstype="handelsgoedning",
    n_nitri_kg_ha=12.0,  # 12 kg N/ha has inhibitor
    handelsgoedning_detail_type="Urea*"
)
# Returns: (n2o_kg, co2e_kg)
```

**Example 2 - Manure without Inhibitor**:
```python
n2o_kg, co2e_kg = calculate_n2o_goedning(
    n_total_kg_ha=100.0,
    areal_ha=5.0,
    goedningstype="husdyrgoedning",
    n_nitri_kg_ha=0.0
)
# Returns: (n2o_kg, co2e_kg)
```

**Example 3 - Grazing**:
```python
n2o_kg, co2e_kg = calculate_n2o_goedning(
    n_total_kg_ha=100.0,
    areal_ha=1.0,
    goedningstype="afgraesning"
)
# Returns: (n2o_kg, co2e_kg)
# Note: n_nitri_kg_ha is automatically set to 0 for grazing
```

---

## marker/afgroederester.py

**Status**: ⚠️ Requires lookup tables

### Purpose
Calculates N and CO2e from crop residues (above and below ground).

### Constants
- `THETA_N2O_CO2 = 265.0` - N2O to CO2e conversion factor
- `MOL_WEIGHT_N2O_N_FACTOR = 44.0 / 28.0` - Molecular weight ratio
- `EF_N2O_AFGROEDERESTER` - Loaded from tabel_19 (default 0.01)

### Functions

#### 1. `calculate_k_graes()`
Calculates correction factor for grass residues based on grazing intensity.

**Parameters**:
- `n_graes_kg_n_ha` (float): Amount of N deposited during grazing [kg N/ha]

**Returns**: `float` - k_græs factor
- Returns 1.0 if N < 10 kg/ha
- Returns 1.24 if 10 ≤ N < 50 kg/ha
- Returns 1.49 if N ≥ 50 kg/ha

---

#### 2. `calculate_A_over_kg_ts_ha()`
Calculates dry matter in above-ground crop residues.

**Parameters**:
- `x1_halmnedmulding` (bool): True if straw is incorporated
- `x2_udbytte_nedmuldes` (bool): True if harvest yield is incorporated
- `t_torstof_total_kg_ts_ha` (float): Total dry matter in main harvest [kg ts/ha]
- `h_u_fast_halmudbytte_kg_ts_ha` (float): Fixed straw yield for seed grass [kg ts/ha]
- `s_slope` (float): Slope coefficient (crop-specific)
- `i_intercept` (float): Intercept coefficient (crop-specific)
- `k_graes` (float): Grass correction factor (from `calculate_k_graes()`)
- `h_f_halmfraktion` (float): Straw fraction relative to yield

**Returns**: `float` - Above-ground residue dry matter [kg ts/ha]

**Example**:
```python
k_graes = calculate_k_graes(n_graes_kg_n_ha=30.0)  # Returns 1.24
a_over = calculate_A_over_kg_ts_ha(
    x1_halmnedmulding=True,
    x2_udbytte_nedmuldes=False,
    t_torstof_total_kg_ts_ha=4930.0,
    h_u_fast_halmudbytte_kg_ts_ha=0.0,
    s_slope=0.98,
    i_intercept=590.0,
    k_graes=k_graes,
    h_f_halmfraktion=0.55
)
# Returns: ~5421.4 kg ts/ha
```

---

#### 3. `calculate_A_under_kg_ts_ha()`
Calculates dry matter in below-ground crop residues.

**Parameters**:
- `t_torstof_total_kg_ts_ha` (float): Total dry matter in main harvest [kg ts/ha]
- `h_u_fast_halmudbytte_kg_ts_ha` (float): Fixed straw yield [kg ts/ha]
- `s_slope` (float): Slope coefficient
- `i_intercept` (float): Intercept coefficient
- `k_graes` (float): Grass correction factor
- `f_forhold_under_over_biomasse` (float): Ratio of below-ground to above-ground biomass

**Returns**: `float` - Below-ground residue dry matter [kg ts/ha]

---

#### 4. `calculate_n_afgroederester_kg_n_ha()`
Calculates N in crop residues per ha.

**Parameters**:
- `a_over_kg_ts_ha` (float): Above-ground residue dry matter [kg ts/ha]
- `n_over_kg_n_pr_kg_ts` (float): N content in above-ground residues [kg N/kg ts]
- `a_under_kg_ts_ha` (float): Below-ground residue dry matter [kg ts/ha]
- `n_under_kg_n_pr_kg_ts` (float): N content in below-ground residues [kg N/kg ts]

**Returns**: `Tuple[float, float, float]`
- `n_a_over_kg_n_ha` - N in above-ground residues [kg N/ha]
- `n_a_under_kg_n_ha` - N in below-ground residues [kg N/ha]
- `n_total_afgroederester_kg_n_ha` - Total N [kg N/ha]

---

#### 5. `calculate_co2e_afgroederester_kg_co2e_ha()`
**Main integration function for main crops** - Calculates CO2e from crop residues.

**Parameters**:
- `n_a_over_kg_n_ha` (float): N in above-ground residues [kg N/ha]
- `n_a_under_kg_n_ha` (float): N in below-ground residues [kg N/ha]
- `o_omlaegningsfrekvens` (float): Rotation frequency (years)
  - 1.0 for annual crops
  - Cycle length for perennials (e.g., 3 for 3-year grass)
- `is_perennial_and_ploughed_this_year` (bool): True if perennial and ploughed this year

**Returns**: `float` - CO2e per ha [kg CO2e/ha]

**Example - Annual Crop**:
```python
co2e_ha = calculate_co2e_afgroederester_kg_co2e_ha(
    n_a_over_kg_n_ha=37.95,
    n_a_under_kg_n_ha=31.88,
    o_omlaegningsfrekvens=1.0,
    is_perennial_and_ploughed_this_year=False
)
# Returns: CO2e in kg/ha for annual crop
```

---

#### 6. `calculate_stub_mv_efterafgroede_kg_ts_ha()`
Calculates dry matter in stubble for catch crops.

**Parameters**:
- `udbytte_efterafgroede_kg_ts_ha` (float): Harvested/harvestable yield [kg ts/ha]
- `haeldning_table27` (float): Slope from Table 27
- `intercept_table27` (float): Intercept from Table 27

**Returns**: `float` - Stubble dry matter [kg ts/ha]

---

#### 7. `calculate_n_efterafgroede_kg_n_ha()`
Calculates N in catch crop residues.

**Parameters**:
- `udbytte_efterafgroede_kg_ts_ha` (float): Harvested/harvestable yield [kg ts/ha]
- `nedmuld_flag` (bool): True if yield itself is incorporated
- `haeldning_table27` (float): Slope from Table 27
- `intercept_table27` (float): Intercept from Table 27
- `n_indhold_over_table27` (float): Above-ground N content from Table 27 [kg N/kg ts]
- `faktor_under_table28` (float): Below-ground factor from Table 28 (ratio)
- `n_indhold_under_table28` (float): Below-ground N content from Table 28 [kg N/kg ts]

**Returns**: `Tuple[float, float, float]`
- `n_over_nedmuldet_kg_n_ha` - N in incorporated above-ground [kg N/ha]
- `n_under_efterafgroede_kg_n_ha` - N in below-ground [kg N/ha]
- `n_total_efterafgroede_kg_n_ha` - Total N [kg N/ha]

---

#### 8. `calculate_co2e_efterafgroede_kg_co2e_ha()`
**Main integration function for catch crops** - Calculates CO2e from catch crop residues.

**Parameters**:
- `n_total_efterafgroede_kg_n_ha` (float): Total N in catch crop residues [kg N/ha]

**Returns**: `float` - CO2e per ha [kg CO2e/ha]

---

### Lookup Tables Required

This module requires the following lookup tables (crop-specific values):

**Main Crops (Tables 24, 25, 26)**:
- `o_omlaegningsfrekvens` - Rotation frequency
- `n_over_kg_n_pr_kg_ts` - Above-ground N content
- `n_under_kg_n_pr_kg_ts` - Below-ground N content
- `h_f_halmfraktion` - Straw fraction
- `s_slope` - Slope coefficient
- `i_intercept` - Intercept coefficient
- `f_forhold_under_over_biomasse` - Below/above biomass ratio

**Catch Crops (Tables 27, 28)**:
- `haeldning_table27` - Slope for stubble calculation
- `intercept_table27` - Intercept for stubble calculation
- `n_indhold_over_table27` - Above-ground N content
- `faktor_under_table28` - Below-ground factor
- `n_indhold_under_table28` - Below-ground N content

**Integration Priority**: High - Requires building crop code → table value lookup system

---

## marker/organogene_jorde.py

**Status**: ✅ Ready to integrate

### Purpose
Calculates CO2 emissions from organic soils based on land use characteristics.

### Data Loading
- Loads CO2 emission factors from `tabel_31` (CO2 from organic matter breakdown)
- Loads CH4 CO2e factor from `tabel_32` (wetland scenario)

### Hardcoded Values
- `N2O_CO2E_OMDRIFT_GT12C = 3.87` - N2O CO2e for rotation, >12% C
- `N2O_CO2E_PERMGRAS_GT12C = 2.44` - N2O CO2e for permanent grass, >12% C

### Function

#### `calculate_co2_organogene_jorde()`
**Main integration function** - Calculates CO2 emissions from organic soils.

**Parameters**:
- `h` (float): Field area [ha]
- `i_omdrift` (bool): True if field is "in rotation" (actively cultivated), False for permanent grass
- `lav_vandstand` (bool): True if low water table, False if high water table
- `kulstof_percentage` (str): Carbon content - one of:
  - `"6-12%"` - 6-12% carbon
  - `">12%"` - More than 12% carbon

**Returns**: `Tuple[float, float, float, float]`
- `co2_tons_kulstof` - CO2 from carbon [tons CO2]
- `co2_tons_n2o` - CO2e from N2O [tons CO2e]
- `co2_tons_ch4` - CO2e from CH4 [tons CO2e]
- `co2_tons_total` - Total [tons CO2e]

**Decision Rules**:

| Rule | i_omdrift | lav_vandstand | kulstof % | CO2 Kulstof | N2O | CH4 |
|------|-----------|---------------|-----------|-------------|-----|-----|
| 1 | True | True | 6-12% | 21.08 t/ha | 0 | 0 |
| 2 | True | True | >12% | 42.17 t/ha | 3.87 t/ha | 0 |
| 3 | False | True | >12% | 30.8 t/ha | 2.44 t/ha | 0 |
| 4 | False | True | 6-12% | 15.4 t/ha | 0 | 0 |
| 5 | False | False | any | 0 | 0 | 6.8 t/ha |

**Example 1 - Cultivated, Low Water, High Carbon**:
```python
kulstof, n2o, ch4, total = calculate_co2_organogene_jorde(
    h=10.0,
    i_omdrift=True,
    lav_vandstand=True,
    kulstof_percentage=">12%"
)
# Returns: (421.7, 38.7, 0.0, 460.4) tons CO2e
```

**Example 2 - Permanent Grass, High Water**:
```python
kulstof, n2o, ch4, total = calculate_co2_organogene_jorde(
    h=10.0,
    i_omdrift=False,
    lav_vandstand=False,
    kulstof_percentage=">12%"  # Carbon % irrelevant for this rule
)
# Returns: (0.0, 0.0, 68.0, 68.0) tons CO2e (CH4 only)
```

**Example 3 - Permanent Grass, Low Water, Medium Carbon**:
```python
kulstof, n2o, ch4, total = calculate_co2_organogene_jorde(
    h=5.0,
    i_omdrift=False,
    lav_vandstand=True,
    kulstof_percentage="6-12%"
)
# Returns: (77.0, 0.0, 0.0, 77.0) tons CO2e
```

---

## Integration Status Summary

### ✅ Ready to Integrate (Can be used immediately)

1. **kvaeg/enterisk_metan.py**
   - Complete function: `beregn_co2e_enterisk_kvaeg_total()`
   - Requires: Animal counts and feed parameters
   - No external dependencies

2. **marker/goedning_og_nitrifikationshaemmer.py**
   - Complete function: `calculate_n2o_goedning()`
   - Requires: N application rates, field areas, fertilizer types
   - Uses lookup tables (tabel_19, tabel_22) - already loaded

3. **marker/organogene_jorde.py**
   - Complete function: `calculate_co2_organogene_jorde()`
   - Requires: Field area, land use type, water table level, carbon content
   - Uses lookup tables (tabel_31, tabel_32) - already loaded

---

### ⚠️ Requires External Data

4. **kvaeg/stald_og_lager.py**
   - Complete function: `calculate_co2_stald_lager()`
   - **Blocker**: Requires ARLA API integration for 4 parameters
   - Priority: Medium (can be implemented after ARLA API is integrated)

---

### ⚠️ Requires Lookup Table Development

5. **marker/afgroederester.py**
   - Complete functions: Multiple functions for main crops and catch crops
   - **Blocker**: Requires building crop code → table value lookup system
   - Tables needed: 24, 25, 26 (main crops), 27, 28 (catch crops)
   - Priority: High (core calculation for field emissions)

---

## Recommended Integration Order

1. **Phase 1 - Immediate** (No blockers):
   - `kvaeg/enterisk_metan.py` - Enteric methane
   - `marker/goedning_og_nitrifikationshaemmer.py` - Fertilizer N2O
   - `marker/organogene_jorde.py` - Organic soils

2. **Phase 2 - Crop Residue Tables** (After table system built):
   - `marker/afgroederester.py` - Crop residues
   - Build crop code → parameters lookup
   - Implement main crop and catch crop calculations

3. **Phase 3 - ARLA Integration** (After API access):
   - `kvaeg/stald_og_lager.py` - Barn and storage
   - Requires ARLA API credentials and data pipeline

---

## Common Data Types

### Field Input
```python
field_input = {
    "areal_ha": float,              # Field area in hectares
    "afgroede_kode": str,           # Crop code
    "udbytte_kg_ts_ha": float,      # Yield in kg dry matter per ha
    "n_total_kg_ha": float,         # Total N applied in kg N per ha
    "goedningstype": str            # "handelsgoedning", "husdyrgoedning", "afgraesning"
}
```

### Cattle Input
```python
cattle_input = {
    "dyretype": str,                # e.g., "malkeko_tung_race"
    "count": int,                   # Number of animals
    "foderoptag_kg_ts_pr_dag": float,  # Feed intake
    "fedtsyre_g_pr_kg_ts": float,   # Fat content
    "ndf_g_pr_kg_ts": float         # Fiber content (for dairy cows)
}
```

### Organic Soil Input
```python
organic_soil_input = {
    "areal_ha": float,              # Field area
    "i_omdrift": bool,              # In rotation?
    "lav_vandstand": bool,          # Low water table?
    "kulstof_percentage": str       # "6-12%" or ">12%"
}
```

---

## Notes on Units

All modules use consistent units:
- **Area**: hectares (ha)
- **Mass**: kilograms (kg) or tons (t) as specified
- **N content**: kg N per unit
- **CO2e**: kilograms (kg) or tons (t) CO2 equivalents
- **Feed intake**: kg dry matter (ts = tørstof) per day
- **Percentages**: 0-100 scale (not 0-1 decimal)

---

## Error Handling

All modules raise `ValueError` for invalid inputs:
- Unknown animal types
- Invalid fertilizer types
- Out-of-range parameters
- Missing required parameters

Calculator should validate inputs before calling formula functions.

---

**End of Document**
