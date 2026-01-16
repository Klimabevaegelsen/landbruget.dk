# Climate Tool Constants

This directory contains centralized constants and emission factors used throughout the climate tool calculations.

## Files

### `gwp_factors.json`
Global Warming Potential (GWP) factors and molecular weight conversion factors.

**Source**: IPCC Fifth Assessment Report (AR5)

**Contents**:
- `gwp_100`: GWP-100 values for CO2, CH4, and N2O
  - CO2: 1 (baseline)
  - CH4: 28 (kg CO2e per kg CH4)
  - N2O: 265 (kg CO2e per kg N2O)

- `molecular_weights`: Conversion factors between elements and compounds
  - N2O_N_factor: 1.5714 (44/28, converts N2O-N to N2O)
  - NOx_N_factor: 3.2857 (46/14, converts NOx-N to NOx)
  - CO2_C_factor: 3.6667 (44/12, converts C to CO2)

- `indirect_n2o_factors`: IPCC default emission factors for indirect N2O
  - atmospheric_deposition: 0.01 (kg N2O-N per kg NH3-N)
  - leaching_runoff: 0.0075 (kg N2O-N per kg N leached)

### `emission_factors.json`
Danish-specific emission factors extracted from the Climate Tool reference tables.

**Source**: Klimaværktøj til landbruget (Danish Climate Tool for Agriculture)

**Contents**:

1. **Manure Storage** (`manure_storage`)
   - MCF (Methane Conversion Factors) for different storage systems
   - B0 factors (Maximum CH4 producing capacity) for animal types

2. **Housing Emissions** (`housing_emissions`)
   - N2O emission factors for different housing systems
   - NH3 emission factors by barn type and manure system

3. **Field Application** (`field_application`)
   - NH3 emission factors for different manure and fertilizer types
   - N2O emission factors (direct and indirect)

4. **Grazing** (`grazing`)
   - NH3 emission factors by animal type
   - N2O emission factors for grazing systems

5. **Crop Residues** (`crop_residues`)
   - IPCC 2006 dry matter content and residue calculations
   - Nitrogen content in above- and belowground residues

## Usage

### In Python Code

```python
import json
from pathlib import Path

# Load GWP factors
CONSTANTS_DIR = Path(__file__).parent / "constants"
with open(CONSTANTS_DIR / "gwp_factors.json") as f:
    gwp_factors = json.load(f)

# Access values
ch4_gwp = gwp_factors['gwp_100']['CH4']  # 28
n2o_n_factor = gwp_factors['molecular_weights']['N2O_N_factor']  # 1.5714

# Load emission factors
with open(CONSTANTS_DIR / "emission_factors.json") as f:
    emission_factors = json.load(f)

# Access values
mcf_slurry = emission_factors['manure_storage']['mcf']['gylle']['value']  # 12.4
nh3_cattle_grazing = emission_factors['grazing']['nh3']['kvæg']['value']  # 0.14
```

### Using the Conversions Module

The `utils/conversions.py` module automatically loads these constants:

```python
from utils.conversions import (
    ch4_to_co2e,
    n2o_to_co2e,
    nh3_to_indirect_co2e,
    CH4_GWP,
    N2O_GWP,
    N2O_N_FACTOR
)

# Convert CH4 to CO2e
ch4_emissions_kg = 100
co2e = ch4_to_co2e(ch4_emissions_kg)  # 100 * 28 = 2800 kg CO2e

# Convert N2O to CO2e
n2o_emissions_kg = 10
co2e = n2o_to_co2e(n2o_emissions_kg)  # 10 * 265 = 2650 kg CO2e

# Convert NH3-N to indirect N2O and CO2e
nh3_n_emissions_kg = 50
co2e = nh3_to_indirect_co2e(nh3_n_emissions_kg)
```

## References

### GWP Factors
IPCC, 2014: Climate Change 2014: Synthesis Report. Contribution of Working Groups I, II and III to the Fifth Assessment Report of the Intergovernmental Panel on Climate Change [Core Writing Team, R.K. Pachauri and L.A. Meyer (eds.)]. IPCC, Geneva, Switzerland, 151 pp.

### Emission Factors
Detailed references for each emission factor are included in the JSON files. Main sources:
- IPCC 2006: IPCC Guidelines for National Greenhouse Gas Inventories
- Nielsen et al. 2020: Danish-specific research on manure management
- Normtal 2020: Danish agricultural norms
- Olesen et al. 2018: Barn acidification research

## Maintenance

### Adding New Constants

1. Add the constant to the appropriate JSON file
2. Include metadata (description, unit, reference)
3. Update this README if adding new categories
4. Run tests to ensure backward compatibility

### Updating Values

When updating emission factors:
1. Document the reason for change (new research, updated guidelines)
2. Note the previous value in git commit message
3. Update the reference in the JSON file
4. Verify all calculations that depend on the changed value

## Relationship to reference_values/

The `reference_values/` directory contains the complete, detailed reference tables from the Danish Climate Tool. The constants in this directory (`constants/`) are:

- **Extracted**: Most commonly used values from those detailed tables
- **Simplified**: Organized for programmatic access
- **Consolidated**: Combined related factors for easier use

For complex lookups (e.g., specific crop types, detailed animal categories), always use the full reference tables in `reference_values/`. For standard calculations (GWP conversions, common emission factors), use these centralized constants.
