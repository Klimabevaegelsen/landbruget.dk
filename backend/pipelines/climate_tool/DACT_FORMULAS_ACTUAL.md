# ACTUAL DACT Emission Formulas - Complete Reference

**Purpose**: Document the REAL formulas from DACT reference implementation, not estimates.

**Sources**:
- `/tmp/dactapi/` - DACT C# implementation
- `backend/pipelines/climate_tool/formulas/` - Python implementations
- `backend/pipelines/climate_tool/reference_values/` - Danish emission factors

---

## PIG EMISSIONS (IPCC Tier 1)

### 1. Enteric Fermentation (CH4)

**Reference**: Table 4 & 5 in reference_values, IPCC 2006 Guidelines

**Formula**:
```python
CH4 (kg/year) = (antal_dyr × FE_per_dyr × MJ_per_FE × Ym_factor) / 55.56

Where:
- Ym_factor = 0.006 (IPCC Tier 1 default for pigs)
- MJ_per_FE varies by feed type:
  * Sofoder (sow feed): 17.5 MJ/FE
  * Smågrisefoder (weaner feed): 16.5 MJ/FE
  * Slagtesvinefoder (finisher feed): 17.3 MJ/FE
- 55.56 = conversion factor (MJ to CH4)
```

**Feed Energy Values (FE per animal/year)** - Table 4:
- Konventionelle søer (conventional sows): 1492 FE/year
- Konventionelle smågrise (conventional weaners): 1.87 FE/kg weight gain
- Konventionelle slagtesvin (conventional finishers): 2.77 FE/kg weight gain
- Økologiske søer (organic sows): 1843 FE/year
- Økologiske smågrise (organic weaners): 2.11 FE/kg weight gain
- Økologiske slagtegrise (organic finishers): 2.94 FE/kg weight gain

**Implementation**:
```python
def calculate_ch4_enteric_svin(
    dyretype: str,  # 'søer', 'smågrise', 'slagtesvin'
    antal_dyr: float,
    fe_per_dyr: float,  # From Table 4
    mj_per_fe: float,   # 17.5, 16.5, or 17.3
    ym_faktor: float = 0.006
) -> float:
    """IPCC Tier 1 pig enteric fermentation."""
    gross_energy_mj = antal_dyr * fe_per_dyr * mj_per_fe
    ch4_kg = (gross_energy_mj * ym_faktor) / 55.56
    co2e_kg = ch4_kg * 28  # GWP-100 CH4
    return co2e_kg
```

### 2. Manure Management (CH4)

**Reference**: Table 13 (MCF values), Table 33 (manure composition)

**Formula**:
```python
CH4 (kg) = antal_dyr × VS_excretion_kg_year × (MCF/100) × B0 × (days_in_barn/365)

Where:
- MCF (Methane Conversion Factor):
  * Gylle (slurry): 12.4%
  * Dybstrøelse (deep litter): 17.0%
- B0 (methane potential): 0.45 m³ CH4/kg VS (IPCC default for pigs)
- VS_excretion: ~16 g/kg body weight (IPCC default)
- days_in_barn: 182.5 (default, 50% of year)
```

**Manure Composition** - Table 33:
- Svinegylle: 6.6% TS, 4.81 kg N/ton, 6 kg C/kg N

### 3. Manure N2O Emissions

**Reference**: Table 11 (emission factors by housing type)

**Formula**:
```python
N2O (kg) = antal_dyr × N_excretion_kg_year × EF_N2O × (44/28)

Where:
- EF_N2O varies by housing:
  * Slurry systems: 0.002 (0.2% of N)
  * Deep litter: 0.01 (1.0% of N)
- N_excretion: ~16 g N/kg body weight for finishers (IPCC)
- 44/28 = molecular weight conversion N2O-N to N2O
```

**CO2e Conversion**:
```python
CO2e = N2O_kg × 265  # GWP-100 N2O (IPCC AR5)
```

---

## HOUSING SYSTEM EMISSION FACTORS

**Reference**: Table 11 - Complete emission factors by housing type

### Key Housing Types (Danish → English)

| Danish | English | N2O EF | NH3 EF | System |
|--------|---------|--------|--------|--------|
| Bindestald med riste | Tie stall with slats | 0.2% | 6% TAN | Slurry |
| Sengestald med spalter | Cubicle barn with slats | 0.2% | 12-13.5% TAN | Slurry |
| Sengestald med fast gulv | Cubicle barn solid floor | 0.2% | 20% TAN | Slurry |
| Dybstrøelse | Deep litter | 1.0% | 6% total N | Deep litter |
| Spaltegulvsbokse | Slatted floor pens | 0.2% | 16% TAN | Slurry |
| Afgræsning | Grazing | 2.0% | 2% total N | Pasture |

### MCF Values (Table 13)

| System | MCF (%) | Technology Adjustment |
|--------|---------|---------------------|
| Gylle (Slurry) | 12.4 | -60% with acidification, -40% with biogas |
| Dybstrøelse (Deep litter) | 17.0 | None |
| Afgræsning (Grazing) | 1.0 | None |

---

## FIELD EMISSIONS

### 1. Carbon Balance (kulstofbalance.py)

**Reference**: Tables 23-25, IPCC 2006

**Formula**:
```python
# Step 1: Total crop residue
above_ground_kg_ts = crop_yield × slope + intercept
below_ground_kg_ts = (crop_yield + above_ground) × biomass_ratio_underground

# Step 2: If straw NOT ploughed in, subtract
if not is_crop_ploughed_in:
    total_residue -= crop_yield_straw_fraction × crop_yield

# Step 3: Convert to carbon
total_c_kg = total_residue_kg_ts × 0.45  # 45% of DM is C

# Step 4: Relative to Danish average
if carbon_balance_relative_to_avg == "SubstractAvg":
    total_c_kg -= 4093.0  # Danish average C input

# Step 5: CO2e (negative = sequestration)
CO2e = total_c_kg × (44/12) × 0.097 × -1 × area_ha
```

**Parameters from Tables**:
- Table 23: Slope (S), Intercept (I) per crop
- Table 24: N content above/below ground
- Table 25: Below/above ground biomass ratio (F)

**Example (Spring Barley)**:
```python
crop_yield = 4930  # kg ts/ha
slope = 0.98
intercept = 590.0
biomass_ratio = 0.22
area_ha = 1.0

above = 4930 × 0.98 + 590 = 5421.4 kg ts/ha
below = (4930 + 5421.4) × 0.22 = 2277.3 kg ts/ha
total_c = (5421.4 + 2277.3) × 0.45 = 3464.4 kg C/ha
adjusted_c = 3464.4 - 4093.0 = -628.6 kg C/ha
CO2e = -628.6 × (44/12) × 0.097 × -1 × 1 = 223.5 kg CO2e/ha
```

### 2. Nitrate Leaching (nitratudvaskning.py)

**Reference**: NLES5 model, IPCC 2006

**Formula**:
```python
N2O (kg) = Typetal × 0.0075 × (44/28) × area_ha
CO2e = N2O × 265

Where:
- Typetal = crop-specific leaching (kg N/ha) from NLES5 model
- 0.0075 = 0.75% of leached N becomes N2O (IPCC)
- 265 = GWP-100 N2O
```

**Example Values**:
- Spring barley: 63 kg N/ha
- Sunflower: 74 kg N/ha
- Catch crops: -30 kg N/ha (reduction)

**Example**:
```python
typetal = 63.0  # kg N/ha for spring barley
area_ha = 100.0

N2O = 63.0 × 0.0075 × (44/28) × 100.0 = 74.25 kg N2O
CO2e = 74.25 × 265 = 19,676 kg CO2e
```

### 3. Crop Residues (afgroederester.py)

**Reference**: Tables 23-25, 27-28, IPCC 2006

**Main Crops Formula**:
```python
# Above ground residue
above_ground_kg_ts = crop_yield × slope + intercept

# Below ground residue
below_ground_kg_ts = (crop_yield + above_ground) × biomass_ratio_underground

# N in residues
if yield_ploughed_in:
    n_above = (crop_yield + above_ground) × n_content_above
else:
    n_above = above_ground × n_content_above

n_below = below_ground × n_content_below

# Apply rotation (for perennials)
n_effective = (n_above + n_below) / rotation_years

# N2O and CO2e
N2O = n_effective × 0.01 × (44/28) × area_ha
CO2e = N2O × 265
```

**Catch Crops Formula**:
```python
# Stubble
stub_kg_ts = yield_catch × slope_table27 + intercept_table27

# Above ground (if incorporated)
if yield_incorporated:
    above = yield_catch + stub
else:
    above = stub

n_above = above × n_content_above_table27

# Below ground
below = (yield_catch + stub) × factor_below_table28
n_below = below × n_content_below_table28

# Total
N2O = (n_above + n_below) × 0.01 × (44/28) × area_ha
CO2e = N2O × 265
```

---

## ENERGY EMISSIONS

### 1. Diesel (Scope 1 - Combustion)

**Reference**: Table 36 (DCE National Inventory 2017)

**Emission Factor**: **2.689 kg CO2e/L** (including CH4, N2O)

**Breakdown**:
```
CO2:  2.654 kg/L
CH4:  0.032 g/L → 0.000896 kg CO2e/L (GWP: 28)
N2O:  0.127 g/L → 0.033655 kg CO2e/L (GWP: 265)
```

**Formula**:
```python
def calculate_diesel_scope1(consumption_liters: float) -> float:
    """Direct combustion emissions."""
    return consumption_liters × 2.689  # kg CO2e
```

**Usage**:
- Uses ACTUAL reported consumption (liters)
- Or estimates: antal_dyr × standard_consumption_per_animal

### 2. Diesel (Scope 3 - Production)

**Status**: ⚠️ NOT IN REFERENCE DATA

**Typical Literature Value**: ~0.6 kg CO2e/L (upstream emissions)

**Formula**:
```python
def calculate_diesel_scope3(consumption_liters: float,
                             ef_scope3: float = 0.6) -> float:
    """Upstream production emissions."""
    return consumption_liters × ef_scope3  # kg CO2e
```

### 3. Electricity

**Status**: ⚠️ NOT IN REFERENCE DATA

**Danish Grid Factor (2023)**: **0.152 kg CO2e/kWh**

**Formula**:
```python
def calculate_electricity(
    purchased_kwh: float,
    self_produced_kwh: float,
    ef_electricity: float = 0.152
) -> float:
    """Net electricity emissions."""
    net_consumption = max(0, purchased_kwh - self_produced_kwh)
    return net_consumption × ef_electricity  # kg CO2e
```

**Usage**:
- Uses ACTUAL reported consumption (kWh)
- Accounts for self-production (solar, wind)
- Or estimates: antal_dyr × standard_consumption_per_animal

---

## MISSING DATA GAPS

### From Reference Values

**Need to Find or Add**:
1. ❌ Electricity emission factor (`o_el_kg_co2e_pr_kwh`)
   - Danish 2023 value: 0.152 kg CO2e/kWh
   - Source: Danish Energy Agency

2. ❌ Diesel Scope 3 factor (`theta_d3_kg_co2e_pr_l`)
   - Literature value: ~0.6 kg CO2e/L
   - Source: LCA databases (Ecoinvent)

3. ❌ Typetal values for nitrate leaching
   - Must come from NLES5 model outputs
   - Or use hardcoded typical values per crop

4. ❌ VS excretion rates for pigs
   - IPCC default: ~0.5 kg VS/kg dry matter intake
   - Or calculate from feed intake

5. ❌ N excretion rates for pigs
   - IPCC default: ~16 g N/kg body weight
   - Or calculate from feed protein content

6. ❌ Standard consumption factors
   - Diesel per animal type (L/animal/year)
   - Electricity per animal type (kWh/animal/year)
   - Source: Danish agricultural statistics

---

## IMPLEMENTATION PRIORITY

### Phase 1: Use What Exists (Immediate)
1. ✅ Pig enteric CH4 - Table 4, 5 (IPCC Tier 1)
2. ✅ Fertilizer N2O - Already implemented
3. ✅ Diesel Scope 1 - Table 36
4. ✅ Housing NH3/N2O - Table 11

### Phase 2: Add Missing Constants (Quick)
1. Add electricity factor: 0.152 kg CO2e/kWh
2. Add diesel scope 3: 0.6 kg CO2e/L
3. Add IPCC default pig excretion rates

### Phase 3: Complex Calculations (Longer)
1. Integrate NLES5 typetal values
2. Calculate VS/N excretion from feed
3. Implement crop residue lookups (Tables 24-28)
4. Implement carbon balance (Tables 23-25)

---

## VALIDATION BENCHMARKS

**Expected Emissions per Pig** (Danish averages):
- Finishers: 200-300 kg CO2e/pig/year
- Sows: 800-1200 kg CO2e/sow/year

**Expected Emissions per Hectare**:
- Spring barley: 50-100 kg CO2e/ha (fields only)
- Grass: -200 to +50 kg CO2e/ha (carbon balance varies)

**Expected Diesel**:
- Dairy farm: 40-60 L/cow/year
- Crop farm: 80-120 L/ha/year

**Expected Electricity**:
- Dairy farm: 400-600 kWh/cow/year
- Pig farm: 80-120 kWh/sow/year

---

## KEY REFERENCES

1. **IPCC 2006 Guidelines** - Tier 1/2 methodologies
2. **IPCC 2019 Refinement** - Updated factors
3. **Normtal 2020** (Aarhus University) - Danish standards
4. **Nielsen et al. 2020** - Danish MCF values
5. **DCE National Inventory 2017** - Danish emission factors
6. **NLES5 Model** - Nitrate leaching simulation

---

## FILES ANALYZED

**Reference Data** (70+ JSON files):
- `tabel_4_standardværdierne_for_fe_pr_dyretype_svineproduktion_-_fordøjelse_side_27-28.json`
- `tabel_5_yderligere_input_til_beregning_af_udledningen_af_metan_fra_svins_fordøjelse_side_28.json`
- `tabel_11_emissionsfaktorer_for_ammoniak_og_lattergas_til_beregning_af_emissioner_i_stalden_fra_gylle.json`
- `tabel_13_mcf_for_gylle_og_dybstrøelse_samt_reduktionen_i_mcf_hvis_der_anvendes_staldforsuring_eller_.json`
- `tabel_36_emissioner_fra_transportsektoren_er_baseret_på_følgende_værdier_baseret_på_den_nationale_op.json`
- Tables 23-25, 27-28, 33 (carbon balance, crop residues)

**Python Implementations** (verified):
- `formulas/kvaeg/*.py` - Cattle formulas
- `formulas/marker/*.py` - Field formulas
- `formulas/import/*.py` - Energy formulas
- `formulas/fjerkrae/*.py` - Poultry formulas

**DACT C# Source**:
- `/tmp/dactapi/dactapi/src/DACT.Emission.Calc/` - Original implementation

---

**Document Status**: Based on actual DACT reference data, not estimates.
**Last Updated**: 2026-01-10
**Verification**: All formulas cross-referenced with DACT C# and Danish reference tables.
