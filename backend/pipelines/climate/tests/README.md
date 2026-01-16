# Compliance Verification & QA Framework

This test suite verifies that the Python climate tool implementation matches reference emission calculation methodologies.

## Quick Start

### Install Dependencies

```bash
pip install pytest pytest-json-report
```

### Run Compliance Tests

```bash
# Run all compliance tests
pytest tests/compliance/ -v --tb=short -m compliance

# Run only field emission tests
pytest tests/compliance/test_field.py -v

# Run only cattle emission tests
pytest tests/compliance/test_cattle.py -v

# Run critical tests only
pytest tests/compliance/ -v -m critical
```

### Generate Compliance Report

```bash
python tests/reports/compliance_report_generator.py
```

This generates:
- `tests/reports/compliance_report.md` - Human-readable report
- `tests/reports/compliance_report.json` - Machine-readable data

## What's Being Tested

### Formula Parity
- **Nitrate leaching**: `FormulaNitrateLeaching.cs` vs `nitratudvaskning.py`
- **Crop residues**: `FormulaCropResidue.cs` vs `afgroederester.py`
- **Cattle digestion**: `FormulaDigestionDairyCows.cs` vs `enterisk_metan.py`
- **Liming**: `FormulaLimeApplied.cs` vs `kalkning.py`
- **Carbon balance**: `FormulaCarbonBalanceByCrop.cs` vs `kulstofbalance.py`

### Intentional Deviations

The Python implementation intentionally uses **IPCC AR6 (2021)** GWP values instead of AR4:

| Gas | AR4 | AR6 (Python) | Difference |
|-----|-----|--------------|------------|
| N2O | 298 | 273 | ~8% lower |
| CH4 (biogenic) | 25 | 27 | ~8% higher |

**Rationale**: AR6 represents the most recent climate science (2021). These deviations are documented and flagged as intentional, not bugs.

## Test Structure

```
tests/
├── conftest.py                    # Pytest fixtures (GWP constants, test cases)
├── reference/
│   ├── reference_values.json      # Constants (AR4, AR5, AR6 GWP values)
│   └── test_cases.json            # Input/output test vectors
├── compliance/
│   ├── test_field.py              # Field emission formula tests
│   └── test_cattle.py             # Cattle emission formula tests
└── reports/
    └── compliance_report_generator.py  # Report generation script
```

## Key Test Cases

### Nitrate Leaching (NL001)
- **Input**: 63 kg N/ha typetal, 100 ha area
- **Expected N2O**: 74.25 kg
- **Expected CO2e (AR6)**: 20,270 kg (273 GWP)
- **Expected CO2e (AR4)**: 22,127 kg (298 GWP)
- **Deviation**: ~8%

### Heavy Breed Dairy Cow (CD001)
- **Input**: 100 animals, 24 kg DM/day, 20 g FA/kg, 350 g NDF/kg
- **Expected CH4**: 161.42 kg/animal/year
- **Expected CO2e (AR6)**: 435,834 kg (27 GWP)
- **Expected CO2e (AR4)**: 403,550 kg (25 GWP)
- **Deviation**: ~8%

## Understanding Test Results

### ✅ PASS: Formula Logic Match
Test verifies that mathematical operations match C# exactly:
- Same coefficients (e.g., 0.0075, 44/28, 1.230)
- Same formula structure
- Same intermediate calculations

### ⚠️ INTENTIONAL DEVIATION: GWP Difference
Test documents expected ~8% difference due to AR6 vs AR4 GWP values.
This is NOT a bug - it's a scientific update.

### ❌ FAIL: Actual Bug
Test fails because formula logic doesn't match C#.
Requires code fix.

## Adding New Tests

1. **Add test case to `test_cases.json`**:
```json
{
  "test_id": "NL004",
  "description": "Your test description",
  "inputs": {...},
  "expected_outputs": {
    "n2o_kg": ...,
    "co2e_kg_gwp_ar4": ...,
    "co2e_kg_gwp_ar6": ...
  },
  "tolerance_pct": 0.01
}
```

2. **Create test method**:
```python
@pytest.mark.compliance
@pytest.mark.field
def test_your_formula(self, test_cases):
    test_case = test_cases["your_category"][0]
    # ... test implementation
```

## Troubleshooting

### Test Fails: "N2O calculation mismatch"
- Check formula coefficients (0.0075, 44/28)
- Verify input units match test case
- Check for floating-point rounding differences

### Test Fails: "CO2e mismatch"
- Verify GWP constant is correct (273 for N2O, 27 for biogenic CH4)
- Check if test is using AR6 or AR4 expected value
- Ensure GWP is applied after N2O/CH4 calculation

### Import Errors
- Ensure you're in `backend/pipelines/climate` directory
- Check `sys.path` modifications in test files
- Verify formula modules are importable

## CI/CD Integration

### GitHub Actions (Future)
```yaml
- name: Run Compliance Tests
  run: |
    pytest tests/compliance/ -v --tb=short
    python tests/reports/compliance_report_generator.py

- name: Check Compliance Threshold
  run: |
    python -c "
    import json
    with open('tests/reports/compliance_report.json') as f:
        report = json.load(f)
    compliance = report['summary']['compliance_percentage']
    if compliance < 90:
        raise SystemExit(f'Compliance {compliance}% below 90% threshold')
    "
```

## References

- IPCC AR6 WG1 Chapter 7: Global Warming Potentials
- IPCC 2006 Guidelines: Tier 1/2 Methodologies
- Danish Climate Tool Documentation: "Klimaværktøj til landbruget"

## Support

For questions or issues:
1. Check test output for specific failure details
2. Review `tests/reference/reference_values.json` for constants
3. Review plan at `.claude/plans/lexical-hatching-mochi.md`
