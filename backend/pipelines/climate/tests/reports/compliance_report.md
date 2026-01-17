# Compliance Report

Generated: 2026-01-11T18:45:16.164672
Python Implementation: climate v1.0
Reference: Reference emission calculation implementation
GWP Version: IPCC AR6 (2021)

## Executive Summary

| Metric | Value |
|--------|-------|
| Total Tests | 19 |
| Passed | 19 |
| Failed | 0 |
| **Compliance Rate** | **100.0%** |

## Intentional Deviations from AR4 Reference

These are NOT bugs - they represent intentional updates to use more recent science:


### DEV-001: N2O GWP: Using AR6 (273) instead of AR4 (298)

- **Impact**: ~8% lower N2O-related CO2e emissions
- **Rationale**: IPCC AR6 (2021) is most recent science
- **Affected Formulas**: nitrate_leaching, crop_residue, fertilizer_application

### DEV-002: CH4 GWP: Using AR6 biogenic (27) instead of AR4 (25)

- **Impact**: ~8% higher CH4-related CO2e emissions
- **Rationale**: IPCC AR6 (2021) distinguishes biogenic vs fossil CH4
- **Affected Formulas**: cattle_digestion, pig_digestion, manure_storage


## Formula Compliance Status

### Field Emissions
- ✅ Nitrate leaching: Formula logic matches C# exactly
- ✅ Crop residues: Above/below-ground calculations match C#
- ✅ Liming: Molecular weight conversions match C#
- ✅ Carbon balance: Humification coefficients match reference

### Cattle Emissions
- ✅ Heavy breed digestion: Formula coefficients match C# (1.230, -0.145, 0.012, 0.304)
- ✅ Jersey breed digestion: Formula coefficients match C# (1.230, -0.145, 0.012, 0.207)
- ✅ Manure storage: MCF values match reference (12.4% slurry, 17.0% deep litter)

### Pig Emissions
- ✅ Enteric fermentation: IPCC Tier 1 formula with correct Ym factor (0.006)
- ✅ Feed emissions: Reference FE values per animal type
- ✅ Manure emissions: MCF and N2O factors match reference

## Recommendations

1. **Continue using AR6 GWP values** - These represent the most current science
2. **Document deviation in user-facing reports** - Explain why emissions may differ from older tools using AR4
3. **Monitor for AR7** - IPCC's next assessment (expected 2027-2028)

## Test Execution

To re-run these tests:

```bash
cd backend/pipelines/climate
pytest tests/compliance/ -v --tb=short -m compliance
```

To update this report:

```bash
python tests/reports/compliance_report_generator.py
```
