# Buildings PMTiles Investigation

## Problem Statement

Buildings are not visible on the frontend map despite successful PMTiles generation. The buildings layer (`buildings_proximity.pmtiles`) appears to be generated but buildings do not render on the map interface.

## Investigation History

### Previous Error Patterns

1. **"Query returned no results" Error**: Initial issue where spatial join between buildings and agricultural fields returned zero intersections
2. **Coordinate System Mismatch**: Identified that BBR buildings use (lon, lat) while FVM marker data uses (lat, lon)
3. **Table Reference Error**: `buildings_utm` table not found - suggests stale table references
4. **Empty PMTiles Files**: Generated PMTiles had no data due to failed spatial joins

### Applied Fixes

1. **Coordinate Flipping**: Applied `ST_FlipCoordinates` to BBR buildings before spatial join
2. **Table Name Updates**: Changed references from `buildings_utm` to `buildings_for_proximity`
3. **Spatial Join Optimization**: Implemented chunked processing for memory safety
4. **Cache Management**: Manual cache invalidation and warmup

## Current Investigation Plan

### Phase 1: Data Pipeline Verification

#### 1.1 Verify Raw Data Availability
- [ ] Check if BBR buildings data exists in GCS bucket
- [ ] Verify FVM marker data (agricultural fields) availability
- [ ] Confirm data timestamps and freshness
- [ ] Validate data schema and geometry columns

#### 1.2 Test Data Loading
- [ ] Create local test script to load BBR buildings data
- [ ] Verify coordinate ranges and CRS of raw data
- [ ] Test agricultural fields data loading
- [ ] Compare coordinate systems between datasets

#### 1.3 Spatial Join Testing
- [ ] Create isolated test for spatial intersection
- [ ] Test with small data samples
- [ ] Verify ST_FlipCoordinates behavior
- [ ] Test different buffer sizes and intersection methods

### Phase 2: PMTiles Generation Analysis

#### 2.1 Generation Process Verification
- [ ] Monitor GitHub Actions logs for latest run
- [ ] Check for any error messages or warnings
- [ ] Verify file sizes of generated PMTiles
- [ ] Confirm successful upload to R2 storage

#### 2.2 PMTiles File Inspection
- [ ] Download and inspect generated `buildings_proximity.pmtiles`
- [ ] Use PMTiles CLI tools to examine metadata
- [ ] Check coordinate bounds and zoom levels
- [ ] Verify feature count and geometry types

#### 2.3 File Naming and Path Verification
- [ ] Confirm PMTiles filename matches frontend expectations
- [ ] Verify R2 upload path and accessibility
- [ ] Check for any timestamp suffixes or naming conflicts

### Phase 3: Frontend Integration Testing

#### 3.1 URL and Loading Verification
- [ ] Test PMTiles URL accessibility from browser
- [ ] Check browser network tab for loading attempts
- [ ] Verify CORS headers and response codes
- [ ] Test PMTiles proxy functionality

#### 3.2 Map Layer Configuration
- [ ] Review frontend layer configuration for buildings
- [ ] Check if buildings layer is enabled by default
- [ ] Verify styling and visibility settings
- [ ] Test layer toggle functionality

#### 3.3 Coordinate System Frontend
- [ ] Verify map projection settings
- [ ] Check if frontend expects specific coordinate order
- [ ] Test with known working PMTiles for comparison
- [ ] Validate zoom level visibility ranges

### Phase 4: Systematic Debugging

#### 4.1 Create Debug Scripts
- [ ] Local spatial join test script
- [ ] PMTiles generation test (small dataset)
- [ ] Coordinate system validation script
- [ ] Frontend URL accessibility test

#### 4.2 Comparative Analysis
- [ ] Compare with working PMTiles (fields, BNBO)
- [ ] Check differences in generation process
- [ ] Analyze successful vs failed layer patterns
- [ ] Review coordinate bounds of working layers

#### 4.3 End-to-End Testing
- [ ] Full pipeline test with minimal dataset
- [ ] Step-by-step verification of each stage
- [ ] Manual PMTiles creation and upload test
- [ ] Frontend integration with test PMTiles

## Key Questions to Answer

### Data Questions
1. **Are BBR buildings and FVM fields using the same coordinate reference system?**
2. **Do the spatial joins actually find intersections with the current coordinate fixes?**
3. **Is the buildings data being filtered correctly (within 100m of fields)?**
4. **Are there any data quality issues (null geometries, invalid coordinates)?**

### Technical Questions
1. **Is the PMTiles file being generated with actual feature data?**
2. **Are the coordinate bounds of the PMTiles file correct for Denmark?**
3. **Is the frontend correctly requesting and loading the buildings PMTiles?**
4. **Are there any caching issues preventing updated PMTiles from loading?**

### Integration Questions
1. **Does the buildings layer configuration match other working layers?**
2. **Are there any zoom level restrictions preventing buildings from showing?**
3. **Is the styling configuration correct for building features?**
4. **Are there any layer ordering or visibility conflicts?**

## Debug Scripts to Create

### 1. Coordinate System Validator
```python
# scripts/testing/validate_coordinate_systems.py
# - Load BBR buildings sample
# - Load FVM fields sample  
# - Compare coordinate ranges
# - Test ST_FlipCoordinates effect
# - Verify spatial intersection results
```

### 2. PMTiles Inspector
```python
# scripts/testing/inspect_pmtiles.py
# - Download buildings_proximity.pmtiles
# - Extract metadata and bounds
# - Count features and verify geometry
# - Compare with working PMTiles structure
```

### 3. End-to-End Test
```python
# scripts/testing/buildings_e2e_test.py
# - Small dataset spatial join test
# - Generate test PMTiles file
# - Upload to test location
# - Verify frontend accessibility
```

### 4. Frontend Integration Test
```javascript
// Test PMTiles URL loading
// Verify layer configuration
// Check coordinate projection
// Test feature rendering
```

## Expected Outcomes

### Success Criteria
- [ ] Buildings PMTiles file contains actual building features
- [ ] Coordinate bounds are within Denmark's geographic area
- [ ] Frontend successfully loads and renders buildings
- [ ] Buildings appear at appropriate zoom levels
- [ ] Spatial relationship with agricultural fields is correct

### Failure Investigation Points
- **Empty PMTiles**: Data pipeline issue (spatial join, filtering)
- **Wrong Coordinates**: Coordinate system transformation problem
- **File Not Found**: Upload, naming, or path configuration issue
- **Frontend Not Loading**: URL, CORS, or caching problem
- **Not Visible**: Styling, zoom levels, or layer configuration issue

## Next Steps

1. **Immediate**: Create and run coordinate system validation script
2. **Short-term**: Inspect generated PMTiles file for content and bounds
3. **Medium-term**: End-to-end test with minimal dataset
4. **Long-term**: Comprehensive frontend integration testing

## Success Metrics

- Buildings visible on map at zoom level 12+
- Proper spatial relationship with agricultural fields
- Performance comparable to other PMTiles layers
- No console errors or loading issues

---

*Investigation started: 2025-09-24*  
*Status: In Progress*  
*Priority: High*




