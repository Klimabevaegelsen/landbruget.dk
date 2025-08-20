# Financial Pipeline Fix Summary

## Problem Identified

The CVR enrichment financial pipeline was **parsing XBRL data correctly** but **discarding the comprehensive financial metrics** due to memory optimization concerns. The pipeline was only saving document metadata instead of the actual parsed financial statements.

## Root Cause

In `backend/pipelines/unified_pipeline/src/unified_pipeline/gold/cvr_enrichment/financial_documents.py` line 760:

```sql
-- json_data as financial_data_json,  -- Removed to prevent memory bloat
```

The parsed XBRL financial data was being intentionally discarded to avoid memory issues.

## Solution Implemented

### 1. Enhanced Financial Documents Pipeline

**File**: `backend/pipelines/unified_pipeline/src/unified_pipeline/gold/cvr_enrichment/financial_documents.py`

**Changes**:
- Creates **TWO tables** instead of one:
  - `cvr_financial_documents` - Document metadata (for compatibility)
  - `cvr_financial_statements` - **NEW!** Comprehensive parsed financial data
- Extracts comprehensive financial metrics from parsed XBRL data
- Maps both English and Danish XBRL field names
- Calculates financial ratios (equity_ratio, return_on_assets, profit_per_employee)
- Uses smaller batch sizes (25 vs 50) for memory efficiency
- Saves both tables to separate GCS locations

### 2. Comprehensive Financial Data Schema

The new `cvr_financial_statements` table includes:

**Company Identification**:
- `company_uuid` - UUID v5 for consistent company linking
- `cvr_number` - Danish company registration number

**Document Metadata**:
- `publication_type`, `publication_time`, `case_number`
- `reporting_period_start`, `reporting_period_end`
- `document_count`, `xml_size_bytes`, `download_success`

**Income Statement Data**:
- `net_profit_loss` - Net profit/loss (multiple XBRL field mappings)
- `gross_profit_loss` - Gross profit/loss
- `operating_profit_loss` - Operating profit/loss
- `profit_loss_before_tax` - Profit before tax
- `employee_benefits_expense` - Employee costs
- `depreciation_expense` - Depreciation
- `tax_expense` - Tax expense
- `other_finance_income`, `other_finance_expenses`

**Balance Sheet Data**:
- `total_assets` - Total assets (AktiverIAlt, Assets)
- `total_equity` - Total equity (EgenkapitalIAlt, Equity)  
- `current_assets` - Current assets (OmsætningsAktiver)
- `noncurrent_assets` - Non-current assets (AnlægsAktiver)
- `cash_and_cash_equivalents` - Cash (LikvideBehold, Cash)
- `liabilities_other_than_provisions` - Total liabilities
- `shortterm_liabilities_other_than_provisions` - Short-term liabilities
- `longterm_liabilities_other_than_provisions` - Long-term liabilities
- `provisions` - Provisions
- `property_plant_equipment` - Fixed assets (MaterialeAktiver)
- `contributed_capital` - Share capital

**Employee & Ratio Data**:
- `average_number_of_employees` - Employee count (GennemsnitligtAntalMedarbejdere)
- `equity_ratio` - Calculated: equity/assets
- `profit_per_employee` - Calculated: profit/employees
- `return_on_assets` - Calculated: profit/assets

### 3. XBRL Field Mapping

The solution maps both English and Danish XBRL field names:

```sql
-- Example: Net profit mapping
COALESCE(
    TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.net_profit_loss') AS DOUBLE),
    TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.ResultatEfterSkat') AS DOUBLE),
    TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.ProfitLoss') AS DOUBLE)
) as net_profit_loss
```

### 4. Updated Migration Script

**File**: `scripts/analysis/migrate_yearly_financials.py`

**Enhancements**:
- Downloads both document metadata AND comprehensive financial statements
- Analyzes both datasets to determine migration strategy
- Prioritizes comprehensive financial data when available
- Falls back to document metadata if comprehensive data unavailable
- Provides detailed analysis of financial data coverage

## Data Flow

### Before Fix:
1. ✅ Fetch financial documents from CVR API
2. ✅ Download and parse XBRL XML
3. ✅ Extract financial metrics 
4. ❌ **DISCARD parsed financial data**
5. ❌ Save only document metadata

### After Fix:
1. ✅ Fetch financial documents from CVR API
2. ✅ Download and parse XBRL XML
3. ✅ Extract financial metrics
4. ✅ **SAVE comprehensive financial data**
5. ✅ Save document metadata (for compatibility)

## GCS Output Locations

**Document Metadata** (existing):
- `gs://landbrugsdata-raw-data/gold/cvr_enrichment_financial/{timestamp}/financial_documents.parquet`

**Comprehensive Financial Statements** (NEW!):
- `gs://landbrugsdata-raw-data/gold/cvr_enrichment_financial_statements/{timestamp}/financial_statements.parquet`

## Migration Strategy

1. **Phase 1**: Run the fixed financial pipeline to generate comprehensive financial data
2. **Phase 2**: Run the migration script to populate Supabase yearly_financials table
3. **Phase 3**: Update API and frontend to use the rich financial data

## Expected Data Volume

Based on the log data:
- **3,492 companies** with financial documents
- **100% have parsed financial metrics** (`has_financial_metrics: true`)
- **1.2+ GB of XBRL data** processed
- **Expected output**: 3,492 comprehensive financial records with full income statement and balance sheet data

## Benefits

1. **Rich Financial Data**: Full income statements and balance sheets instead of just metadata
2. **Multi-language Support**: Maps both English and Danish XBRL field names
3. **Calculated Ratios**: Automatic calculation of key financial ratios
4. **Memory Efficient**: Uses smaller batches and proper cleanup
5. **Backward Compatible**: Maintains existing document metadata table
6. **Future Ready**: Schema matches analysis document recommendations

## Next Steps

1. **Deploy the fix**: Run the updated financial documents pipeline
2. **Verify data quality**: Check the comprehensive financial statements output
3. **Run migration**: Execute the yearly_financials migration to Supabase
4. **Update API**: Modify API endpoints to use comprehensive financial data
5. **Update frontend**: Enhance dashboards with rich financial metrics

The financial pipeline is now fixed and will generate the comprehensive financial data that was being discarded before!
