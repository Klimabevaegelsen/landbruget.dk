# CVR P-Number Address Discovery

## Executive Summary

**Major Discovery**: CVR P-number (production unit) addresses are available via direct API queries to the `/produktionsenhed/_search` endpoint, representing a significant opportunity to enhance building-to-company matching coverage.

**Impact**: This could potentially add thousands of additional address points for CVR companies, significantly improving our CVR→BBR building matching success rates.

## Background

During investigation of CVR data for building matching, we initially concluded that P-number addresses were not available. However, analysis of the [CVR API mapping schema](http://distribution.virk.dk/cvr-permanent/_mapping) revealed that P-numbers are separate entities with their own rich data structures, including full address information.

## Technical Discovery

### Initial Incorrect Approach
- **Endpoint**: `/cvr-permanent/virksomhed/_search` (company endpoint)
- **Method**: Querying companies and examining nested `penheder` field
- **Result**: P-numbers contained only basic metadata (`pNummer`, `periode`, `sidstOpdateret`)
- **Conclusion**: ❌ No P-number addresses available

### Correct Approach
- **Endpoint**: `/cvr-permanent/produktionsenhed/_search` (production unit endpoint)  
- **Method**: Directly querying P-numbers as independent entities
- **Result**: ✅ Full address structures with complete location data
- **Conclusion**: ✅ P-number addresses are abundantly available

## Verification Results

### Test Cases
Direct API calls to production unit endpoint confirmed address availability:

**P-Number 1000000175** (SIMON PEDERSEN):
- **Addresses**: 5 addresses found
- **Location**: Tørslevvej 60, 4050 Skibby
- **Status**: 1 current, 4 historical addresses

**P-Number 1021354151** (FMP Sofiendal ApS):
- **Addresses**: 3 addresses found  
- **Location**: Koldinghus Alle 1, 4690 Haslev
- **Status**: 1 current, 2 historical addresses

### Available Data Fields
P-numbers contain rich data structures including:
- `beliggenhedsadresse` - Full location addresses
- `postadresse` - Postal addresses
- `navne` - Production unit names
- `hovedbranche` - Main industry classification
- `telefonNummer` - Phone numbers
- `elektroniskPost` - Email addresses
- `aarsbeskaeftigelse` - Employment data
- `attributter` - Additional attributes

## Address Structure

P-number addresses follow the same structure as company addresses:

```json
{
  "beliggenhedsadresse": [
    {
      "adresseId": "text",
      "vejnavn": "Tørslevvej",
      "husnummerFra": 60,
      "postnummer": 4050,
      "postdistrikt": "Skibby",
      "kommune": {
        "kommuneKode": 123,
        "kommuneNavn": "Municipality Name"
      },
      "periode": {
        "gyldigFra": "date",
        "gyldigTil": "date_or_null"
      }
    }
  ]
}
```

## Implementation Opportunities

### 1. CVR Pipeline Enhancement
**Location**: `backend/pipelines/unified_pipeline/src/unified_pipeline/util/cvr_api_client.py`

**Current State**: Only queries company endpoint (`/virksomhed/_search`)

**Enhancement**: Add P-number fetching capability:
- Query `/produktionsenhed/_search` endpoint
- Extract P-number addresses using existing address parsing logic
- Link P-numbers to parent companies via CVR number relationship
- Store P-number addresses in separate table or extend existing addresses table

### 2. Building Matching Enhancement
**Location**: Building-to-CVR matching scripts

**Current Coverage**: Only main company addresses from `cvr_addresses` table

**Enhanced Coverage**: Include P-number addresses:
- Additional address points for existing CVR companies
- Potentially thousands of new address-building matches
- Improved spatial coverage for companies with multiple locations

### 3. Data Architecture Considerations

**P-Number to CVR Relationship**:
- P-numbers are linked to companies via `virksomhedsrelation` field
- Need to establish proper foreign key relationships
- Consider separate `cvr_pnumber_addresses` table vs extending `cvr_addresses`

**Address Deduplication**:
- Some P-number addresses may duplicate main company addresses
- Implement deduplication logic during matching
- Prioritize current addresses over historical ones

## Estimated Impact

### Potential Coverage Increase
- **Current CVR addresses**: ~50,000 companies with main addresses
- **Estimated P-number addresses**: Unknown, but potentially 10,000s additional addresses
- **Building matching improvement**: Could increase CVR→BBR match rates significantly

### Use Cases
- **Multi-location companies**: Farms, retail chains, manufacturing with multiple sites
- **Agricultural operations**: Production facilities separate from administrative offices
- **Industrial companies**: Factories, warehouses, distribution centers

## Implementation Plan

### Phase 1: Data Collection
1. **Enhance CVR API client** to fetch P-number data
2. **Create P-number address extraction** logic
3. **Establish parent company relationships**
4. **Test with sample companies**

### Phase 2: Integration
1. **Modify CVR enrichment pipeline** to include P-numbers
2. **Create P-number address tables** in data architecture
3. **Update building matching scripts** to include P-number addresses
4. **Validate address quality and coverage**

### Phase 3: Analysis
1. **Measure coverage improvement** in building matching
2. **Analyze P-number address quality** vs main company addresses
3. **Document best practices** for P-number utilization
4. **Optimize matching algorithms** for multiple address sources

## Technical Requirements

### API Access
- **Endpoint**: `http://distribution.virk.dk/cvr-permanent/produktionsenhed/_search`
- **Authentication**: Same CVR credentials as company endpoint
- **Query Structure**: Elasticsearch-style queries by P-number or parent CVR
- **Rate Limiting**: Same constraints as company API

### Data Processing
- **Address Parsing**: Reuse existing Danish address parser
- **Geocoding**: Apply same DAWA API enrichment as company addresses  
- **Validation**: Implement P-number specific validation rules
- **Storage**: Design schema for P-number addresses and relationships

## Key Learnings

1. **API Endpoint Discovery**: CVR system has separate endpoints for different entity types
2. **Schema Documentation**: The mapping schema at `/cvr-permanent/_mapping` is crucial for understanding data structure
3. **Entity Relationships**: P-numbers are independent entities, not just nested company data
4. **Data Richness**: P-numbers contain as much detail as main companies, including full address histories

## Next Steps

1. **Implement P-number fetching** in CVR API client
2. **Design data schema** for P-number addresses
3. **Create test implementation** with sample companies
4. **Measure impact** on building matching coverage
5. **Document integration process** for production deployment

## References

- [CVR API Mapping Schema](http://distribution.virk.dk/cvr-permanent/_mapping)
- CVR API Documentation (distribution.virk.dk)
- Existing CVR API Client: `backend/pipelines/unified_pipeline/src/unified_pipeline/util/cvr_api_client.py`
- Building Matching Strategy: `docs/analysis/building_cvr_chr_matching_strategy.md`

---

**Document Created**: 2025-08-10  
**Discovery Verified**: Direct API testing confirmed P-number address availability  
**Status**: Ready for implementation planning
