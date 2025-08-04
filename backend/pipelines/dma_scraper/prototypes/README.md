# DMA Scraper Prototypes 🧪

This directory contains prototype analysis tools that work with data scraped by the DMA pipeline. These are experimental scripts that are **NOT integrated into the production pipeline**.

## Current Prototypes

### 🧪 DMA Environmental Permits Analysis
**File**: `dma_environmental_permits_analysis_prototype.py`  
**Status**: Prototype - Manual execution required  
**Purpose**: Extract structured data from environmental permit PDFs using AI

**Description**:
Advanced multimodal AI analysis of Danish environmental permits (afgørelser) using Google's Gemini 2.5 Flash. Automatically identifies environmental permits, extracts operational data, and intelligently groups facilities.

**Key Features**:
- ✅ **Cost-optimized**: Only sends first 2 pages for permit classification
- ✅ **Parallel processing**: 5-10x faster than sequential processing
- ✅ **Smart address grouping**: AI-powered facility consolidation
- ✅ **Temporal logic**: Handles permit renewals and updates correctly
- ✅ **Multi-folder discovery**: Comprehensive document finding

**Requirements**:
```bash
pip install PyPDF2
# Google Cloud ADC must be configured
gcloud auth application-default login
```

**Usage**:
```bash
cd backend/pipelines/dma_scraper/prototypes
python dma_environmental_permits_analysis_prototype.py
```

**Output**: 
- `data/dma_environmental_permits_analysis.json` - Structured analysis results
- Console logs with detailed processing information

**Data Extracted**:
- Energy consumption (electricity, oil, gas, heat)
- Animal production (cattle, pigs, poultry by type)
- Ammonia emissions (NH3 kg/year)
- Biogas manure delivery agreements
- Transport activity estimates
- Facility addresses and permit details

## Integration Status

⚠️ **These are prototypes** - they are:
- ❌ **Not integrated** into the production pipeline
- ❌ **Not scheduled** or automated
- ❌ **Manual execution** required
- ❌ **No production monitoring**

## Production Integration TODO

For production integration, each prototype needs:
- [ ] Configuration management integration
- [ ] Pipeline scheduling and triggers
- [ ] Batch processing for all relevant companies
- [ ] Data validation and quality checks
- [ ] Output schema documentation
- [ ] Monitoring, logging, and alerting
- [ ] Error handling and recovery
- [ ] Performance optimization
- [ ] Unit and integration tests

## Development Guidelines

When adding new prototypes:

1. **Clear naming**: Use `*_prototype.py` suffix
2. **Documentation**: Add comprehensive docstrings with prototype status
3. **README updates**: Document the prototype in this README
4. **Requirements**: List all dependencies clearly
5. **Usage examples**: Provide clear usage instructions
6. **Integration status**: Mark clearly as prototype/experimental

## Architecture Notes

These prototypes work with data from the main DMA scraper pipeline but operate independently. They:
- Access scraped PDFs from GCS buckets
- Use the same CVR identification as the main pipeline
- Generate independent analysis outputs
- Do not modify or interfere with production data flows

## Contact

For questions about these prototypes or integration planning, consult the main DMA pipeline documentation and development team.