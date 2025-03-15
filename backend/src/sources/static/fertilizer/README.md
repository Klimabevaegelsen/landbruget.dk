# Agricultural Nitrogen Field-Level Analysis

## Purpose
Distributes farm-level nitrogen quotas and consumption patterns to individual fields while maintaining compliance with agricultural regulations. Translates farm-level fertilizer data to field-level estimates based on each farm's quota adjustments and consumption patterns.

## Input Data Structure

### Farm-Level File
Farm-wide nitrogen data with columns:
- CVR: Unique farm identifier
- F_504_1: Base nitrogen quota
- F_505_1: Additional nitrogen quota
- F_512: Corrected quota (when regulatory adjustments apply)
- F_901: Total nitrogen consumption
- F_610: Animal fertilizer usage
- F_706_1: Mineral fertilizer usage
- F_193: Other organic fertilizer usage

Note: Negative values in fertilizer columns represent transfers or sales to other farms.

### Field-Level File
Individual field data:
- CVR: Farm identifier
- Marknummer: Field ID number
- Areal: Total field area
- Harmoni Areal: Area eligible for fertilizer application
- N Kvote Mark: Field's base nitrogen quota

## Analysis Steps

### 1. Data Preparation

#### Farm-Level Data (df1)
- Convert all quota and fertilizer columns to numeric values
- Calculate original quota (F_504_1 + F_505_1)
- Identify final quota (use F_512 if available, otherwise use original)
- Calculate quota adjustment factor = final quota / original quota
- Calculate fertilizer type shares from total consumption
- Handle negative values:
  * Negative fertilizer values reflect transfers/sales
  * These affect farm's total available nitrogen

#### Field-Level Data (df2)
- Remove header rows
- Convert numeric columns: CVR, Areal, Harmoni Areal, N Kvote Mark
- Verify field areas and quotas

### 2. Farm-Level Calculations

#### Quota Processing
1. Original quota = F_504_1 + F_505_1
2. Final quota = F_512 (if available) or original quota
3. Adjustment factor = final quota / original quota
   * Default to 1 if original quota is zero

#### Consumption Analysis
1. Farm performance = total consumption / final quota
2. Fertilizer distribution:
   * Animal share = F_610 / total consumption
   * Mineral share = F_706_1 / total consumption
   * Organic share = F_193 / total consumption

### 3. Field-Level Distribution

For each field:

1. Harmonized Area Check
   * If zero: field ineligible for fertilizer application
   * If non-zero: proceed with calculations

2. Quota Adjustment
   * Adjusted quota = N Kvote Mark × farm's adjustment factor

3. Consumption Calculation
   * Expected consumption = adjusted quota × farm performance
   * Consumption percentage = (consumption / adjusted quota) × 100

4. Fertilizer Type Distribution
   * Apply farm's fertilizer shares to field consumption
   * Maintains farm's fertilizer mix at field level

### 4. Validation Checks

#### Farm Level
- Complete quota information (original or corrected)
- Fertilizer shares sum to 100%
- Transfer/sale amounts (negative values) verification
- Total consumption versus quota alignment

#### Field Level
- Harmonized area ≤ total area
- Field quota presence
- Consumption percentage reasonability
- CVR matches in both datasets

### 5. Output Documentation
Each field record includes:
- Original field data
- Adjusted quotas
- Calculated consumption
- Fertilizer type breakdowns
- Special cases:
  * Zero harmonized area
  * Missing quota data
  * High consumption percentages

## Key Assumptions
1. Farm-level patterns apply uniformly to fields
2. Field consumption follows farm performance ratio
3. Negative values represent legitimate transfers/sales
4. Zero harmonized area means no fertilizer allocation

## Limitations
- Cannot account for field-specific practices
- Assumes uniform distribution within farms
- Based on reported data
- No consideration of soil types or crop requirements

## Notes
- Negative fertilizer values are legitimate when representing transfers
- Field-level estimates maintain farm totals
- Analysis focuses on nitrogen distribution patterns
- Results should be viewed as estimates based on farm patterns

## Exploration code:
https://colab.research.google.com/drive/1AghsRm8Dsfk3kVR-tUe2BUS3E9v7KY2I?usp=sharing
