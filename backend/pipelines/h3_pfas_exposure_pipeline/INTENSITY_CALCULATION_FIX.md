# H3 PFAS Intensity Calculation Fix

## Problem Identified

The intensity calculations for H3 and kommune data had a fundamental issue: they were using incorrect denominators for the per-hectare calculations.

### The Issue

**Correct Approach (what the pipeline calculates):**
- Intensity = Pesticide amount / Field area within H3 cell
- Uses `total_intersection_area_ha` (actual agricultural field area within the H3 cell)

**Incorrect Approach (what the database views calculated):**
- Intensity = Pesticide amount / Total H3 cell area  
- Used `agricultural_area_ha` which wasn't properly populated from the pipeline results

### Why This Matters

The difference is significant:
- **H3 cell area**: Total area of the hexagon (includes non-agricultural land like roads, buildings, water)
- **Field area within H3 cell**: Only the actual agricultural fields that received pesticide

Using the total H3 cell area would systematically **underestimate** pesticide intensity because the denominator would be larger than the actual treated area.

## Root Cause

The pipeline correctly calculated intensity using field area (`total_intersection_area_ha`), but when saving results to the database:

1. **H3 results**: The `agricultural_area_ha` field was not being populated from `total_intersection_area_ha`
2. **Database views**: Used `agricultural_area_ha` for intensity calculations, which was empty or incorrect
3. **Frontend**: Relied on database views for intensity calculations, inheriting the wrong values

## Solution Applied

### 1. Fixed Result Saver (`result_saver.py`)

Added proper mapping from pipeline field area to database schema:

```sql
-- Map field area for database compatibility
CAST(total_intersection_area_ha AS DOUBLE) as agricultural_area_ha,
```

This ensures that:
- `agricultural_area_ha` = actual field area within H3 cell
- Database views calculate intensity correctly
- Frontend receives correct intensity values

### 2. Updated PMTiles Generator

Added the `agricultural_area_ha` field to PMTiles output for consistency:

```sql
'agricultural_area_ha', ROUND(COALESCE(total_intersection_area_ha, 0), 3),
```

### 3. Verified Kommune Calculations

Confirmed that kommune-level calculations already use the correct approach:
- Uses `SUM(intersection_area_ha)` (field area within kommune)
- Not affected by this issue

## Verification

After this fix:

1. **Pipeline calculations**: ✅ Already correct (using field area)
2. **Database storage**: ✅ Now correctly maps field area to `agricultural_area_ha`
3. **Database views**: ✅ Now calculate intensity using correct field area
4. **Frontend display**: ✅ Will show correct intensity values

## Technical Details

### Before Fix
```sql
-- Database view (INCORRECT)
CASE 
    WHEN agricultural_area_ha > 0 THEN total_pfas_grams / agricultural_area_ha 
    ELSE 0 
END AS pfas_intensity
-- agricultural_area_ha was empty or wrong
```

### After Fix
```sql
-- Pipeline result saver (FIXED)
CAST(total_intersection_area_ha AS DOUBLE) as agricultural_area_ha,

-- Database view (NOW CORRECT)
CASE 
    WHEN agricultural_area_ha > 0 THEN total_pfas_grams / agricultural_area_ha 
    ELSE 0 
END AS pfas_intensity
-- agricultural_area_ha now contains actual field area
```

## Impact

This fix ensures that:
- Pesticide intensity values are calculated correctly
- Environmental risk assessments use accurate data
- Spatial analysis reflects true pesticide concentration per agricultural hectare
- Comparisons between different regions/time periods are valid

The intensity calculations now properly represent **pesticide amount per hectare of actual agricultural land** rather than per hectare of total geographic area. 