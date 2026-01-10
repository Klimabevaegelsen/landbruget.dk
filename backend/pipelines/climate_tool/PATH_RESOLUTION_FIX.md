# Path Resolution Fix for Formula Files

## Problem

All formula files that load JSON reference data had incorrect path resolution. They were using `.parent.parent` to navigate from their location to the `reference_values` directory, but this was resolving to the wrong location.

### Broken Path Resolution

```python
base_path = Path(__file__).resolve().parent.parent / "reference_values"
```

For a file at `climate_tool/formulas/marker/file.py`:
- `.parent` → `climate_tool/formulas/marker`
- `.parent.parent` → `climate_tool/formulas`
- Result: `climate_tool/formulas/reference_values` ❌ (doesn't exist)

### Fixed Path Resolution

```python
base_path = Path(__file__).resolve().parent.parent.parent / "reference_values"
```

For a file at `climate_tool/formulas/marker/file.py`:
- `.parent` → `climate_tool/formulas/marker`
- `.parent.parent` → `climate_tool/formulas`
- `.parent.parent.parent` → `climate_tool`
- Result: `climate_tool/reference_values` ✅ (correct location)

## Files Fixed

All occurrences of the path resolution pattern were updated in these files:

1. **backend/pipelines/climate_tool/formulas/marker/goedning_og_nitrifikationshaemmer.py** (line 18)
2. **backend/pipelines/climate_tool/formulas/marker/afgroederester.py** (line 14)
3. **backend/pipelines/climate_tool/formulas/marker/organogene_jorde.py** (line 12)
4. **backend/pipelines/climate_tool/formulas/fjerkrae/stald.py** (line 8)
5. **backend/pipelines/climate_tool/formulas/fjerkrae/lager.py** (line 8)

## Verification

### Test Results

Created and ran test scripts to verify the fix:

1. **test_path_resolution.py** - Tests that all formula modules can be imported and JSON files loaded
   - Result: ✅ All 5 modules loaded successfully

2. **verify_paths.py** - Verifies the exact path resolution for each file
   - Result: ✅ All paths now resolve to the correct `climate_tool/reference_values` directory

3. **Functional tests** - Ran the main functions in formula files
   - Result: ✅ All formulas execute correctly and produce expected outputs

### JSON Files Verified

All required reference JSON files are present and accessible:
- ✅ tabel_19_ammoniak-emissionerne_fra_udbringning_af_organisk_gødning_side_75-76.json
- ✅ tabel_22_nh3_emissionsfaktorer_for_forskellige_typer_handelsgødning_2011-2017_kg_nh3-n_pr_kg_n_side_.json
- ✅ tabel_31_emission_af_co2_fra_nedbrydning_af_organisk_stof_på_organogen_jord_ton_co2_pr_ha_side_96.json
- ✅ tabel_32_effekter_af_udtagning_af_organogen_jord_olesen_et_al_2018_dca_rapport_nr_130_side_97.json

## Impact

- **Before**: Formula files would fail to load JSON reference data, causing runtime errors
- **After**: All formula files correctly load reference data and execute successfully

## Date

Fixed: 2026-01-10
