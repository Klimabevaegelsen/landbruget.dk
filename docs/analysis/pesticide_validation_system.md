# Pesticide Disaggregation Validation System

## Overview

The pesticide disaggregation pipeline now includes comprehensive validation checks to ensure data integrity and track pesticide amounts at each step of the processing pipeline. This system addresses the critical need to validate that pesticide quantities are correctly preserved during the disaggregation process, even though not all applications can be successfully disaggregated.

## Why Validation is Critical

The pesticide disaggregation process converts company-level pesticide applications into field-level allocations. This involves:

1. **Complex matching algorithms** that match pesticide applications to field boundaries
2. **Proportional distribution** that splits applications across multiple fields
3. **Multiple strategies** with different success rates and edge case handling
4. **Data losses** where some applications cannot be disaggregated due to missing CVR matches, area mismatches, etc.

Without validation, we cannot:
- Verify that proportional allocations are mathematically correct
- Track how much of the original data is successfully processed
- Detect data corruption or processing errors
- Provide confidence in the disaggregated results for environmental analysis

## Validation Components

### 1. Original Data Capture
**Method**: `_validate_original_pesticide_totals()`

Captures baseline metrics before any processing:
- Total records in original dataset
- Total dosage quantities (all pesticide amounts)
- Total acreage covered by applications
- Processable records (excluding "no-pesticides" applications)
- Excluded records and their amounts

**Output Example**:
```
📊 VALIDATION: Original pesticide totals captured
   📈 Total records: 125,847
   📈 Total dosage: 45,892,341.25 units
   📈 Total acreage: 2,847,392.50 ha
   ✅ Processable records (excluding no-pesticides): 118,293
   ✅ Processable dosage: 43,127,829.75 units
   ✅ Processable acreage: 2,698,451.25 ha
   🚫 Excluded (no-pesticides): 7,554 records, 2,764,511.50 dosage units
```

### 2. Strategy-Level Validation
**Method**: `_validate_strategy_results(strategy_name, processed_count)`

Validates results after each disaggregation strategy:
- Counts records processed by the strategy
- Tracks dosage and acreage amounts handled
- Validates against expected processing counts
- Maintains cumulative progress tracking

**Called after each strategy**:
- Ethical Best-Match Strategy
- Main Area Matching Strategy  
- Non-Organic Matching Strategy
- Partial Field Coverage Strategy

**Output Example**:
```
📊 VALIDATION: Main Area Match completed
   ✅ Strategy processed: 89,432 records from 23,847 original applications
   ✅ Strategy dosage: 31,289,471.25 units
   ✅ Strategy acreage: 1,847,293.75 ha
   📈 Cumulative progress: 89,432 records, 31,289,471.25 dosage units
```

### 3. Proportional Allocation Integrity
**Method**: `_validate_proportional_allocation_integrity()`

Ensures that field-level allocations sum back to original application amounts:
- Checks that each original pesticide application's fields sum to the correct total
- Calculates allocation accuracy percentages
- Identifies applications with significant discrepancies
- Reports on allocation quality metrics

**Output Example**:
```
🔍 VALIDATION: Proportional allocation integrity check
   📊 Total applications checked: 23,847
   💊 DOSAGE ALLOCATION:
     Perfect matches (≤0.01% diff): 23,201 (97.3%)
     Minor differences (0.01-1%): 623
     Major differences (>1%): 23
     Average difference: 0.007%
     Maximum difference: 2.3%
   📏 AREA ALLOCATION:
     Perfect matches (≤0.01% diff): 23,455 (98.4%)
     Minor differences (0.01-1%): 392
     Major differences (>1%): 0
     Average difference: 0.003%
     Maximum difference: 0.8%
```

### 4. Final Integrity Check
**Method**: `_validate_final_disaggregation_integrity()`

Comprehensive validation of the complete disaggregation results:
- Calculates overall coverage percentages
- Validates total amount conservation
- Reports strategy-by-strategy breakdown
- Identifies remaining unprocessed applications
- Checks for data discrepancies

**Output Example**:
```
🎯 VALIDATION: Final Disaggregation Results
============================================================
📊 INPUT DATA:
   Total original records: 125,847
   Processable records: 118,293
   Processable dosage: 43,127,829.75 units
   Processable acreage: 2,698,451.25 ha
📈 DISAGGREGATION RESULTS:
   Successfully disaggregated: 95,847 original applications
   Total disaggregated records: 287,432 (multi-field expansions)
   Disaggregated dosage: 39,847,291.50 units
   Disaggregated acreage: 2,398,274.25 ha
📉 REMAINING UNPROCESSED:
   Pending records: 22,446
   Pending dosage: 3,280,538.25 units
   Pending acreage: 300,177.00 ha
🎯 COVERAGE ANALYSIS:
   Record coverage: 81.1%
   Dosage coverage: 92.4%
   Acreage coverage: 88.9%
📋 STRATEGY BREAKDOWN:
   Ethical Best-Match: 8,432 applications → 18,947 records
   Main Area Match: 23,847 applications → 89,432 records
   Non-Organic Match: 3,294 applications → 12,847 records
   Partial Field Coverage: 9,274 applications → 21,306 records
✅ Perfect dosage integrity - all amounts accounted for
```

## Implementation Details

### Integration Points

The validation system is integrated at key points in the processing pipeline:

1. **After database setup**: Capture original totals
2. **After each strategy**: Validate strategy results
3. **Before saving results**: Run final integrity checks
4. **Year reset**: Clear validation data between years

### Data Structure

```python
_validation_data = {
    "original_total_dosage": 0.0,
    "original_total_acreage": 0.0, 
    "original_record_count": 0,
    "processable_record_count": 0,
    "processable_total_dosage": 0.0,
    "processable_total_acreage": 0.0,
    "strategy_totals": {
        "strategy_name": {
            "record_count": 0,
            "dosage": 0.0,
            "acreage": 0.0,
            "original_records": 0
        }
    },
    "final_total_dosage": 0.0,
    "final_total_acreage": 0.0,
    "final_record_count": 0
}
```

### Error Handling

- Validation failures are logged as warnings, not fatal errors
- Default values are used if validation queries fail
- Processing continues even if validation encounters issues
- Comprehensive error logging for debugging

## Benefits

### 1. Data Integrity Assurance
- **Conservation Verification**: Ensures total pesticide amounts are conserved (processed + pending = original)
- **Allocation Accuracy**: Verifies that field-level distributions sum correctly to original applications
- **No Double-Counting**: Prevents applications from being processed multiple times

### 2. Transparency & Auditability  
- **Coverage Reporting**: Clear visibility into what percentage of data is successfully disaggregated
- **Strategy Effectiveness**: Shows which strategies handle which types of applications
- **Loss Analysis**: Identifies what cannot be processed and why

### 3. Quality Assurance
- **Error Detection**: Identifies proportional allocation discrepancies
- **Data Quality Issues**: Flags potential problems with source data
- **Performance Monitoring**: Tracks disaggregation success rates over time

### 4. Regulatory Compliance
- **Audit Trail**: Provides detailed logging for compliance reviews
- **Methodology Verification**: Documents that disaggregation follows established protocols
- **Result Confidence**: Quantifies the reliability of disaggregated results

## Usage

The validation system runs automatically as part of the pesticide disaggregation pipeline. No additional configuration is required.

### Interpreting Results

**Coverage Percentages**:
- **Record Coverage**: % of original applications successfully disaggregated
- **Dosage Coverage**: % of total pesticide amounts successfully allocated
- **Acreage Coverage**: % of application acreage successfully distributed

**Quality Metrics**:
- **Perfect Matches**: Applications with ≤0.01% allocation discrepancy
- **Minor Differences**: Applications with 0.01-1% allocation discrepancy  
- **Major Differences**: Applications with >1% allocation discrepancy (investigate these)

**Expected Performance**:
- Record coverage: 80-90% (depends on CVR match rates)
- Dosage coverage: 85-95% (larger applications more likely to match)
- Perfect allocation matches: >95% (indicates good algorithm performance)

### Troubleshooting

**Low Coverage Rates**:
- Check CVR number quality in source data
- Verify field boundary data completeness
- Review area tolerance settings (default 2%)

**High Allocation Discrepancies**:
- Investigate field area calculation methods
- Check for floating-point precision issues
- Review proportional distribution algorithms

**Data Conservation Issues**:
- Verify no applications are double-processed
- Check for missing records in pending table
- Review strategy processing logic

## Testing

Use the test script to verify validation functionality:

```bash
python scripts/testing/pesticide_validation_test.py
```

This demonstrates the validation system components and ensures all methods are working correctly.