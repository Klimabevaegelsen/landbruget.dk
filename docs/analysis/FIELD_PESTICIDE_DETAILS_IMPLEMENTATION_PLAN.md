# Field Pesticide Details Implementation Plan

## 🎯 Overview

This document outlines the implementation plan for adding detailed pesticide product information to field analysis in landbruget.dk. Currently, field analysis shows aggregate pesticide counts (e.g., "9 pesticider") but lacks the specific product details that users need.

## 🔍 Current State Analysis

### ✅ What Works Currently

1. **Frontend Components**: Ready to display detailed pesticide information

   - `FieldDetailsPanel.tsx` and `field-details-content.tsx` have complete logic for parsing and displaying pesticide details
   - Support for multiple dosage units (kg, liters, grams, ml, tablets)
   - Categorized display with color coding for different units
   - Proper formatting and sorting by dosage

2. **Data Pipeline Infrastructure**: Solid foundation exists

   - Pesticide disaggregation pipeline provides field-level pesticide applications (92% coverage)
   - BMD data integration provides PFAS/Diquat/Glyphosate classifications
   - PMTiles generation pipeline is configurable and extensible

3. **Data Quality**: High-quality disaggregated data available
   - 2.5M+ field-level pesticide applications from disaggregation pipeline
   - Proper dosage unit handling and sanitization
   - Field UUID mapping for precise field identification

### ❌ Current Gaps

1. **Missing Field-Level Aggregation**: The PMTiles pipeline doesn't aggregate pesticide products by dosage unit at the field level
2. **No Detailed Product Strings**: The `pesticides_*_detail` fields expected by the frontend are not populated
3. **Limited Categorization**: No separation of products by PFAS/Diquat/Glyphosate/Other categories

## 📊 Data Architecture Analysis

### Current Data Flow

```
Company Pesticide Applications (4.8M records)
    ↓ [Disaggregation Pipeline - 92% coverage]
Disaggregated Field Applications (2.5M+ records)
    ↓ [BMD Join - PFAS/Diquat/Glyphosate classification]
Enhanced Field Applications (with classifications)
    ↓ [PMTiles Generation - Basic aggregation only]
Field Analysis PMTiles (aggregate counts only)
```

### Proposed Enhanced Data Flow

```
Company Pesticide Applications (4.8M records)
    ↓ [Disaggregation Pipeline - 92% coverage]
Disaggregated Field Applications (2.5M+ records)
    ↓ [BMD Join - PFAS/Diquat/Glyphosate classification]
Enhanced Field Applications (with classifications)
    ↓ [NEW: Field-Level Product Aggregation]
Field Pesticide Product Details (by field_uuid + dosage unit + category)
    ↓ [PMTiles Generation - Enhanced with product details]
Field Analysis PMTiles (with detailed product information)
```

## 🏗️ Proposed Solution Architecture

### 1. Data Structure Design

Based on your suggestion and the frontend requirements, implement a categorized array structure:

```json
{
  "field_uuid": "abc-123",
  "pesticide_categories": {
    "pfas": [
      { "name": "Product A", "dosage": 2.5, "unit": "kg" },
      { "name": "Product B", "dosage": 1.2, "unit": "L" }
    ],
    "diquat": [{ "name": "Product C", "dosage": 0.8, "unit": "kg" }],
    "glyphosate": [{ "name": "Product D", "dosage": 3.0, "unit": "L" }],
    "other": [
      { "name": "Product E", "dosage": 1.5, "unit": "kg" },
      { "name": "Product F", "dosage": 500, "unit": "g" }
    ]
  }
}
```

### 2. PMTiles Field Structure

For PMTiles optimization (size constraints), use concatenated strings that the frontend can parse:

```json
{
  "field_uuid": "abc-123",
  "total_pesticide_applications": 9,

  // PFAS products: "ProductName:dosage:unit;ProductName2:dosage:unit"
  "pfas_products_detail": "Product A:2.5:kg;Product B:1.2:L",
  "pfas_applications": 2,

  // Diquat products
  "diquat_products_detail": "Product C:0.8:kg",
  "diquat_applications": 1,

  // Glyphosate products
  "glyphosate_products_detail": "Product D:3.0:L",
  "glyphosate_applications": 1,

  // Other products
  "other_products_detail": "Product E:1.5:kg;Product F:500:g",
  "other_applications": 5,

  // Legacy fields for backward compatibility
  "pesticides_kg_detail": "Product A:2.5;Product C:0.8;Product E:1.5",
  "pesticides_liters_detail": "Product B:1.2;Product D:3.0",
  "pesticides_grams_detail": "Product F:500"
}
```

## 🔧 Implementation Plan

### Phase 1: Enhanced Field-Level Aggregation (Backend)

#### 1.1 Extend PMTiles Data Loader

**File**: `backend/pipelines/unified_pipeline/src/unified_pipeline/gold/pmtiles_generator/data_loader.py`

**Changes**:

- Enhance `_add_pesticide_summary()` method to create detailed product aggregations
- Add new aggregation logic that groups by `field_uuid` and creates product detail strings

```sql
-- New aggregation query structure
CREATE OR REPLACE TABLE temp_pesticide_detailed_summary AS
SELECT
    field_uuid,

    -- PFAS products
    STRING_AGG(
        CASE WHEN contains_pfas = true
        THEN PesticideName || ':' || DosageQuantity || ':' || DosageUnit
        END, ';'
    ) as pfas_products_detail,
    COUNT(CASE WHEN contains_pfas = true THEN 1 END) as pfas_applications,

    -- Diquat products
    STRING_AGG(
        CASE WHEN contains_diquat = true
        THEN PesticideName || ':' || DosageQuantity || ':' || DosageUnit
        END, ';'
    ) as diquat_products_detail,
    COUNT(CASE WHEN contains_diquat = true THEN 1 END) as diquat_applications,

    -- Glyphosate products
    STRING_AGG(
        CASE WHEN contains_glyphosate = true
        THEN PesticideName || ':' || DosageQuantity || ':' || DosageUnit
        END, ';'
    ) as glyphosate_products_detail,
    COUNT(CASE WHEN contains_glyphosate = true THEN 1 END) as glyphosate_applications,

    -- Other products (not PFAS/Diquat/Glyphosate)
    STRING_AGG(
        CASE WHEN COALESCE(contains_pfas, false) = false
                  AND COALESCE(contains_diquat, false) = false
                  AND COALESCE(contains_glyphosate, false) = false
        THEN PesticideName || ':' || DosageQuantity || ':' || DosageUnit
        END, ';'
    ) as other_products_detail,
    COUNT(CASE WHEN COALESCE(contains_pfas, false) = false
                    AND COALESCE(contains_diquat, false) = false
                    AND COALESCE(contains_glyphosate, false) = false THEN 1 END) as other_applications,

    -- Legacy unit-based aggregations for backward compatibility
    STRING_AGG(
        CASE WHEN DosageUnit IN ('2', 'kg')
        THEN PesticideName || ':' || DosageQuantity
        END, ';'
    ) as pesticides_kg_detail,

    STRING_AGG(
        CASE WHEN DosageUnit IN ('4', 'L', 'liter')
        THEN PesticideName || ':' || DosageQuantity
        END, ';'
    ) as pesticides_liters_detail,

    -- ... other unit aggregations

FROM {pesticide_table_with_bmd}
GROUP BY field_uuid
```

#### 1.2 Update Field Analysis Query

**File**: `backend/pipelines/unified_pipeline/src/unified_pipeline/gold/pmtiles_generator/field_analysis_generator.py`

**Changes**:

- Add new pesticide detail fields to the field analysis query
- Ensure BMD-enhanced pesticide data is available in the pipeline

```python
# Add to pesticide_fields list
pesticide_fields = [
    "pesticide_applications as total_pesticide_applications",
    "pesticides_used",

    # New categorized product details
    "pfas_products_detail",
    "pfas_applications",
    "diquat_products_detail",
    "diquat_applications",
    "glyphosate_products_detail",
    "glyphosate_applications",
    "other_products_detail",
    "other_applications",

    # Legacy unit-based details
    "pesticides_kg_detail",
    "pesticides_liters_detail",
    "pesticides_grams_detail",
    "pesticides_ml_detail",
    "pesticides_tablets_detail",

    # Proximity data
    "residential_buildings_formatted as residential_buildings_proximity",
    "educational_facilities_formatted as educational_facilities_proximity",
    "water_distance_formatted as water_distance_proximity",
    "avg_match_confidence",
]
```

### Phase 2: Frontend Enhancement

#### 2.1 Update Type Definitions

**File**: `frontend/src/components/field-analysis/types.ts`

**Changes**:

- Add new fields to `FieldAnalysisData` interface

```typescript
export interface FieldAnalysisData {
  // ... existing fields ...

  // New categorized pesticide details
  pfas_products_detail?: string;
  pfas_applications?: number;
  diquat_products_detail?: string;
  diquat_applications?: number;
  glyphosate_products_detail?: string;
  glyphosate_applications?: number;
  other_products_detail?: string;
  other_applications?: number;

  // ... existing detail fields remain for backward compatibility ...
}
```

#### 2.2 Enhanced Parsing Functions

**Files**:

- `frontend/src/components/field-analysis/FieldDetailsPanel.tsx`
- `frontend/src/app/markanalyse/components/shared/field-details-content.tsx`

**Changes**:

- Add new parsing function for enhanced product details (with unit information)
- Update display logic to show categorized products

```typescript
// Enhanced parsing function with unit support
const parsePesticideDetailWithUnit = (
  detailString: string | undefined
): Array<{ name: string; dosage: number; unit: string }> => {
  if (!detailString || detailString.trim() === "") return [];

  try {
    return detailString
      .split(";")
      .map((item) => item.trim())
      .filter((item) => item.length > 0)
      .map((item) => {
        const [name, dosageStr, unit] = item.split(":");
        return {
          name: name?.trim() || "Ukendt produkt",
          dosage: parseFloat(dosageStr?.trim() || "0"),
          unit: unit?.trim() || "ukendt",
        };
      })
      .filter((item) => item.dosage > 0)
      .sort((a, b) => b.dosage - a.dosage);
  } catch (e) {
    console.warn("Error parsing enhanced pesticide detail:", detailString, e);
    return [];
  }
};
```

#### 2.3 Categorized Display Component

**New Component**: Enhanced pesticide details display with categories

```tsx
// Categorized Pesticide Products Display
{
  (field.pfas_products_detail ||
    field.diquat_products_detail ||
    field.glyphosate_products_detail ||
    field.other_products_detail) && (
    <div className="mb-3">
      <h4 className="text-foreground mb-2 text-sm font-medium lg:text-base">
        Anvendte pesticider (kategoriseret)
      </h4>

      {/* PFAS Products */}
      {field.pfas_products_detail && (
        <div className="mb-2">
          <div className="text-xs text-orange-600 font-medium mb-1">
            🚨 PFAS-holdige produkter ({field.pfas_applications})
          </div>
          {parsePesticideDetailWithUnit(field.pfas_products_detail).map(
            (product, index) => (
              <div
                key={`pfas-${index}`}
                className="bg-orange-50 border-l-4 border-orange-400 rounded p-2 mb-1"
              >
                <span className="font-medium text-orange-800">
                  {product.name}
                </span>
                <span className="text-orange-600 ml-2">
                  {formatNumber(product.dosage, 2)} {product.unit}
                </span>
              </div>
            )
          )}
        </div>
      )}

      {/* Diquat Products */}
      {field.diquat_products_detail && (
        <div className="mb-2">
          <div className="text-xs text-red-600 font-medium mb-1">
            ⚠️ Diquat-holdige produkter ({field.diquat_applications})
          </div>
          {parsePesticideDetailWithUnit(field.diquat_products_detail).map(
            (product, index) => (
              <div
                key={`diquat-${index}`}
                className="bg-red-50 border-l-4 border-red-400 rounded p-2 mb-1"
              >
                <span className="font-medium text-red-800">{product.name}</span>
                <span className="text-red-600 ml-2">
                  {formatNumber(product.dosage, 2)} {product.unit}
                </span>
              </div>
            )
          )}
        </div>
      )}

      {/* Glyphosate Products */}
      {field.glyphosate_products_detail && (
        <div className="mb-2">
          <div className="text-xs text-yellow-600 font-medium mb-1">
            🌾 Glyphosat-holdige produkter ({field.glyphosate_applications})
          </div>
          {parsePesticideDetailWithUnit(field.glyphosate_products_detail).map(
            (product, index) => (
              <div
                key={`glyphosate-${index}`}
                className="bg-yellow-50 border-l-4 border-yellow-400 rounded p-2 mb-1"
              >
                <span className="font-medium text-yellow-800">
                  {product.name}
                </span>
                <span className="text-yellow-600 ml-2">
                  {formatNumber(product.dosage, 2)} {product.unit}
                </span>
              </div>
            )
          )}
        </div>
      )}

      {/* Other Products */}
      {field.other_products_detail && (
        <div className="mb-2">
          <div className="text-xs text-gray-600 font-medium mb-1">
            🧪 Øvrige produkter ({field.other_applications})
          </div>
          {parsePesticideDetailWithUnit(field.other_products_detail).map(
            (product, index) => (
              <div
                key={`other-${index}`}
                className="bg-gray-50 border-l-4 border-gray-400 rounded p-2 mb-1"
              >
                <span className="font-medium text-gray-800">
                  {product.name}
                </span>
                <span className="text-gray-600 ml-2">
                  {formatNumber(product.dosage, 2)} {product.unit}
                </span>
              </div>
            )
          )}
        </div>
      )}
    </div>
  );
}
```

### Phase 3: Data Pipeline Integration

#### 3.1 BMD Data Integration

**Requirement**: Ensure BMD (pesticide product database) data is available during PMTiles generation

**Implementation**:

- Modify pesticide proximity data loading to include BMD joins
- Ensure PFAS/Diquat/Glyphosate classifications are available at field level

#### 3.2 Pipeline Configuration

**File**: `backend/pipelines/unified_pipeline/src/unified_pipeline/gold/pmtiles_generator/config.py`

**Changes**:

- Add configuration for detailed pesticide product aggregation
- Add size limits for product detail strings (PMTiles optimization)

```python
class PMTilesGeneratorConfig(BaseJobConfig):
    # ... existing config ...

    # Pesticide detail settings
    include_pesticide_product_details: bool = Field(
        default=True, description="Include detailed pesticide product information"
    )
    max_pesticide_detail_length: int = Field(
        default=2000, description="Maximum length for pesticide detail strings (PMTiles optimization)"
    )
    enable_pesticide_categorization: bool = Field(
        default=True, description="Enable PFAS/Diquat/Glyphosate categorization"
    )
```

## 📈 Performance Considerations

### PMTiles Size Impact

- **Current field_analysis_2023.pmtiles**: ~647MB
- **Estimated increase**: 15-25% (additional ~100-160MB)
- **Mitigation**: String length limits, selective field inclusion

### Query Performance

- Field-level aggregation will add ~2-5 minutes to PMTiles generation
- Memory usage increase: ~10-20% during processing
- **Optimization**: Use efficient DuckDB aggregation functions

### Frontend Performance

- Parsing overhead: Minimal (cached parsing results)
- Render performance: Improved with categorized display
- **Optimization**: Lazy loading of detailed product information

## 🚨 Critical Issues Found & Fixes Required

### Issue 1: Variable Scope Bug in PMTiles Pipeline

**Problem**: `field_pmtiles` variable is undefined when no years are processed, causing pipeline failure.

**Root Cause**: In `main.py` lines 145-148, `field_pmtiles` is only defined inside the `if years_to_process:` block, but used unconditionally in line 166.

**Error Message**: `"cannot access local variable 'field_pmtiles' where it is not associated with a value"`

**Fix Required**:

```python
# In main.py around line 144
field_pmtiles = {}  # Initialize empty dict

# Generate field analysis PMTiles for each year
if years_to_process:
    logger.info("Generating field analysis PMTiles")
    field_pmtiles = await self.field_generator.generate_multiple_years(years_to_process)
    results["field_analysis_pmtiles"] = field_pmtiles
else:
    logger.info("No years to process for field analysis")
    results["field_analysis_pmtiles"] = {}
```

### Issue 2: Buildings Table Reference Error

**Problem**: Buildings generator references non-existent `buildings_utm` table.

**Root Cause**: Error suggests the buildings generator is looking for `buildings_utm` but the actual table is `buildings_for_proximity`.

**Error Message**: `"Table with name buildings_utm does not exist! Did you mean 'buildings_for_proximity'?"`

**Investigation Needed**: Check if there's a dynamic table name generation issue or legacy reference.

### Issue 3: Missing PMTILES_WARMUP_TOKEN

**Problem**: Cache warmup is skipped due to missing token.

**Impact**: PMTiles files are not pre-warmed in CDN cache, causing slower initial load times.

**Fix Required**: Configure `PMTILES_WARMUP_TOKEN` in GitHub secrets.

## 🧪 Testing Strategy

### 1. Data Quality Tests

- Verify all pesticide products are correctly categorized
- Ensure dosage units are properly handled
- Test string length limits don't truncate important data
- **NEW**: Test variable initialization edge cases

### 2. Frontend Tests

- Test parsing functions with various data formats
- Verify categorized display renders correctly
- Test with missing/partial data scenarios

### 3. Performance Tests

- Measure PMTiles generation time increase
- Test PMTiles file size impact
- Verify frontend rendering performance

### 4. Pipeline Reliability Tests

- **NEW**: Test pipeline with empty year ranges
- **NEW**: Test buildings generator with missing data sources
- **NEW**: Verify error handling doesn't break variable scope

## 🚀 Deployment Plan

### Phase 0: Critical Bug Fixes (Immediate - 1-2 days)

1. **Fix variable scope bug in main.py**
   - Initialize `field_pmtiles = {}` before conditional block
   - Add proper error handling for empty year ranges
2. **Investigate and fix buildings_utm table issue**
   - Check for dynamic table naming problems
   - Ensure consistent table references in buildings generator
3. **Configure missing GitHub secrets**
   - Set up `PMTILES_WARMUP_TOKEN` for cache warming
   - Verify all R2 credentials are properly configured

### Phase 1: Backend Implementation (Week 1)

1. Implement enhanced pesticide aggregation logic
2. Update PMTiles generator to include new fields
3. Test with sample data and validate output

### Phase 2: Frontend Enhancement (Week 2)

1. Update type definitions and parsing functions
2. Implement categorized display components
3. Test with sample PMTiles data

### Phase 3: Integration & Testing (Week 3)

1. Full pipeline testing with real data
2. Performance optimization
3. User acceptance testing

### Phase 4: Production Deployment (Week 4)

1. Generate new PMTiles with detailed pesticide information
2. Deploy frontend changes
3. Monitor performance and user feedback

## 🔄 Backward Compatibility

- Maintain existing `pesticides_used` field for basic product list
- Keep legacy unit-based detail fields (`pesticides_kg_detail`, etc.)
- Graceful degradation when detailed data is not available
- Progressive enhancement approach for new categorized display

## 📊 Success Metrics

### Technical Metrics

- ✅ PMTiles generation completes successfully with <25% size increase
- ✅ Frontend renders detailed pesticide information correctly
- ✅ All existing functionality remains intact

### User Experience Metrics

- ✅ Users can see specific pesticide products used on each field
- ✅ PFAS/Diquat/Glyphosate products are clearly identified
- ✅ Dosage information is displayed with proper units
- ✅ Product information loads quickly and renders smoothly

## 🛠️ Alternative Approaches Considered

### 1. JSON Arrays in PMTiles

**Pros**: More structured data, easier parsing
**Cons**: Larger file sizes, PMTiles compatibility concerns
**Decision**: Use string concatenation for PMTiles optimization

### 2. Separate API Endpoint for Product Details

**Pros**: Smaller PMTiles, more flexible data structure
**Cons**: Additional API calls, latency, complexity
**Decision**: Include in PMTiles for offline capability and performance

### 3. Unit-Based Categorization Only

**Pros**: Simpler implementation, backward compatible
**Cons**: Doesn't address PFAS/Diquat/Glyphosate identification need
**Decision**: Implement both unit-based and substance-based categorization

## 📝 Conclusion

This implementation plan provides a comprehensive approach to adding detailed pesticide product information to field analysis while maintaining performance, backward compatibility, and user experience. The categorized approach (PFAS/Diquat/Glyphosate/Other) addresses the specific regulatory and environmental concerns while providing the detailed product information users need.

The phased implementation approach allows for thorough testing and gradual rollout, minimizing risk while delivering significant value to users analyzing pesticide usage patterns in Danish agriculture.
