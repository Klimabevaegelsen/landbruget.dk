# Constants Usage Examples

This file provides practical examples of how to use the centralized constants in the climate tool.

## Example 1: Basic GHG Conversions

```python
from utils.conversions import ch4_to_co2e, n2o_to_co2e, nh3_to_indirect_co2e

# Convert methane emissions to CO2 equivalent
ch4_emissions_kg = 150
co2e_from_ch4 = ch4_to_co2e(ch4_emissions_kg)
# Result: 150 * 28 = 4,200 kg CO2e

# Convert nitrous oxide emissions to CO2 equivalent
n2o_emissions_kg = 5
co2e_from_n2o = n2o_to_co2e(n2o_emissions_kg)
# Result: 5 * 265 = 1,325 kg CO2e

# Convert ammonia to indirect N2O and CO2e
nh3_n_emissions_kg = 20
co2e_indirect = nh3_to_indirect_co2e(nh3_n_emissions_kg)
# Result: 20 * 0.01 * 1.5714 * 265 = 83.3 kg CO2e
```

## Example 2: Manure Storage CH4 Calculation

```python
from constants import get_mcf, get_b0_factor

# Get factors for dairy cow slurry storage
mcf_data = get_mcf('gylle')
mcf = mcf_data['value']  # 12.4%

b0_data = get_b0_factor('malkekøer')
b0 = b0_data['value']  # 0.24 m³ CH4/kg VS

# Calculate CH4 emissions from manure storage
volatile_solids_kg = 1000  # kg VS produced
storage_days = 182.5

# CH4 production (simplified)
ch4_m3 = volatile_solids_kg * b0 * (mcf / 100)
ch4_kg = ch4_m3 * 0.67  # Convert m³ to kg (density of CH4)

# Convert to CO2e
from utils.conversions import ch4_to_co2e
co2e = ch4_to_co2e(ch4_kg)

print(f"CH4 emissions: {ch4_kg:.2f} kg")
print(f"CO2e emissions: {co2e:.2f} kg")
```

## Example 3: NH3 Emissions from Field Application

```python
from constants import get_nh3_emission_factor

# Get NH3 emission factor for cattle slurry application
ef_data = get_nh3_emission_factor('field_application', 'kvæggylle')
nh3_ef = ef_data['value']  # 0.132 kg NH3-N per kg TAN
tan_percent = ef_data['tan_percent']  # 60.0%

# Calculate NH3 emissions
total_n_kg = 500  # kg total N in slurry
tan_kg = total_n_kg * (tan_percent / 100)
nh3_n_kg = tan_kg * nh3_ef

print(f"TAN: {tan_kg:.2f} kg")
print(f"NH3-N emissions: {nh3_n_kg:.2f} kg")

# Calculate indirect N2O from NH3 volatilization
from utils.conversions import nh3_to_indirect_co2e
indirect_co2e = nh3_to_indirect_co2e(nh3_n_kg)
print(f"Indirect N2O as CO2e: {indirect_co2e:.2f} kg")
```

## Example 4: Direct N2O from Housing

```python
from constants import get_n2o_emission_factor

# Get N2O emission factor for slurry in barn
ef_data = get_n2o_emission_factor('housing_emissions', 'gylle')
n2o_ef = ef_data['value']  # 0.2% of N

# Calculate direct N2O from housing
total_n_excreted_kg = 300  # kg N excreted in barn
n2o_n_kg = total_n_excreted_kg * (n2o_ef / 100)

# Convert N2O-N to N2O
from utils.conversions import n_to_n2o, n2o_to_co2e
n2o_kg = n_to_n2o(n2o_n_kg)

# Convert to CO2e
co2e = n2o_to_co2e(n2o_kg)

print(f"N2O-N: {n2o_n_kg:.2f} kg")
print(f"N2O: {n2o_kg:.2f} kg")
print(f"CO2e: {co2e:.2f} kg")
```

## Example 5: NH3 from Grazing

```python
from constants import get_nh3_emission_factor

# Get NH3 emission factor for cattle grazing
ef_data = get_nh3_emission_factor('grazing', 'kvæg')
nh3_ef = ef_data['value']  # 0.14 kg NH3-N per kg TAN
tan_percent = ef_data['tan_percent']  # 60.0%

# Calculate grazing NH3 emissions
n_excreted_grazing_kg = 200  # kg N excreted during grazing
tan_kg = n_excreted_grazing_kg * (tan_percent / 100)
nh3_n_kg = tan_kg * nh3_ef

print(f"NH3-N from grazing: {nh3_n_kg:.2f} kg")
```

## Example 6: Crop Residue N Content

```python
from constants import get_crop_residue_factors

# Get wheat residue factors
wheat_dm = get_crop_residue_factors('wheat', 'ipcc_2006_dry_matter')
wheat_n = get_crop_residue_factors('wheat', 'nitrogen_content')

# Calculate residue dry matter from yield
wheat_yield_fresh_kg = 8000  # kg fresh weight
dry_content = wheat_dm['dry_content']  # 0.89
slope = wheat_dm['slope']  # 1.51
intercept = wheat_dm['intercept']  # 0.52

residue_dry_kg = (wheat_yield_fresh_kg * dry_content * slope) + intercept

# Calculate N in aboveground residue
n_content_ag = wheat_n['wheat_aboveground']['value']  # 0.006 kg N per kg DM
n_in_residue_kg = residue_dry_kg * n_content_ag

print(f"Residue dry matter: {residue_dry_kg:.2f} kg")
print(f"N in residue: {n_in_residue_kg:.2f} kg")
```

## Example 7: Complete Farm Calculation

```python
from constants import (
    get_mcf,
    get_b0_factor,
    get_nh3_emission_factor,
    get_n2o_emission_factor
)
from utils.conversions import ch4_to_co2e, n2o_to_co2e, nh3_to_indirect_co2e

# Farm parameters
n_dairy_cows = 100
n_excreted_per_cow_kg = 100  # kg N/cow/year
vs_per_cow_kg = 1500  # kg VS/cow/year

# 1. Housing CH4 emissions
b0 = get_b0_factor('malkekøer')['value']
mcf = get_mcf('gylle')['value']
ch4_m3 = n_dairy_cows * vs_per_cow_kg * b0 * (mcf / 100)
ch4_kg = ch4_m3 * 0.67
housing_ch4_co2e = ch4_to_co2e(ch4_kg)

# 2. Housing N2O emissions
n2o_ef_housing = get_n2o_emission_factor('housing_emissions', 'gylle')['value']
total_n_housing = n_dairy_cows * n_excreted_per_cow_kg
n2o_n_housing = total_n_housing * (n2o_ef_housing / 100)
from utils.conversions import n_to_n2o
n2o_housing_kg = n_to_n2o(n2o_n_housing)
housing_n2o_co2e = n2o_to_co2e(n2o_housing_kg)

# 3. Housing NH3 emissions (example: tie stall with slatted floor)
nh3_ef_housing = get_nh3_emission_factor('housing_emissions', 'bindestald_riste')['value']
tan_housing = total_n_housing * 0.6  # 60% is TAN
nh3_n_housing = tan_housing * (nh3_ef_housing / 100)

# 4. Field application emissions
nh3_ef_field = get_nh3_emission_factor('field_application', 'kvæggylle')['value']
n_to_field = total_n_housing - (nh3_n_housing + n2o_n_housing)
tan_field = n_to_field * 0.6
nh3_n_field = tan_field * nh3_ef_field
field_indirect_co2e = nh3_to_indirect_co2e(nh3_n_field + nh3_n_housing)

# Total farm emissions
total_co2e = housing_ch4_co2e + housing_n2o_co2e + field_indirect_co2e

print(f"\\nFarm GHG Emissions Summary ({n_dairy_cows} dairy cows)")
print("=" * 50)
print(f"Housing CH4:          {housing_ch4_co2e:>10,.0f} kg CO2e")
print(f"Housing N2O:          {housing_n2o_co2e:>10,.0f} kg CO2e")
print(f"Indirect N2O (NH3):   {field_indirect_co2e:>10,.0f} kg CO2e")
print("-" * 50)
print(f"TOTAL:                {total_co2e:>10,.0f} kg CO2e")
print(f"Per cow:              {total_co2e/n_dairy_cows:>10,.0f} kg CO2e")
```

## Example 8: Comparing Storage Systems

```python
from constants import get_mcf
from utils.conversions import ch4_to_co2e

systems = ['gylle', 'dybstrøelse', 'afgræsning']
vs_kg = 1000  # Same amount of volatile solids
b0 = 0.24  # Dairy cows

print("\\nCH4 Emissions Comparison by Storage System")
print("=" * 60)
print(f"{'System':<20} {'MCF (%)':<10} {'CH4 (kg)':<12} {'CO2e (kg)':<12}")
print("-" * 60)

for system in systems:
    mcf_data = get_mcf(system)
    mcf = mcf_data['value']

    ch4_m3 = vs_kg * b0 * (mcf / 100)
    ch4_kg = ch4_m3 * 0.67
    co2e = ch4_to_co2e(ch4_kg)

    system_name = mcf_data['description'].split(' (')[0]
    print(f"{system_name:<20} {mcf:<10} {ch4_kg:<12.2f} {co2e:<12.0f}")

# Show reduction potential
print("\\nReduction Measures:")
staldforsuring = get_mcf('staldforsuring_reduction')
biogas = get_mcf('biogas_reduction')
print(f"Barn acidification:   {staldforsuring['value']}% MCF reduction")
print(f"Biogas:               {biogas['value']}% MCF reduction")
```

## Notes

- Always check the `unit` field in returned data to ensure correct calculations
- References for each factor are included in the JSON data
- For detailed animal-specific or crop-specific factors, refer to the full reference_values tables
- Use the validation script to ensure data integrity: `python constants/validate_constants.py`
