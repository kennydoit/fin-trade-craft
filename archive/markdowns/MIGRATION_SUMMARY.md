# Database Migration Summary

## Overview
This document summarizes the successful migration of reference tables from the `extracted` schema to the `source` schema to improve architectural consistency.

## Completed Migrations

### 1. Listing Status Migration ✅
- **Date**: Today
- **Source**: `extracted.listing_status`
- **Destination**: `source.listing_status`
- **Records Migrated**: 20,757
- **Status**: Complete
- **Script**: `archive/migrate_listing_status.py`

### 2. Company Overview Migration ✅
- **Date**: Today
- **Source**: `extracted.overview`
- **Destination**: `source.company_overview`
- **Records Migrated**: 14,285 out of 17,548 (3,263 had invalid symbol_ids)
- **Status**: Complete
- **Script**: `archive/migrate_company_overview.py`

## Schema Improvements

### Listing Status Table
- Added proper foreign key constraints
- Standardized data types (TEXT, TIMESTAMP, UUID)
- Created backward compatibility view at `extracted.listing_status`
- Updated ETL utils to reference `source.listing_status`

### Company Overview Table
- Renamed from `overview` to `company_overview` for clarity
- Added proper foreign key constraint to `source.listing_status`
- Implemented deduplication logic (most recent record per symbol)
- Created backward compatibility view at `extracted.overview`
- Added content hashing for data integrity

## Related Fixes

### 1. Numeric Overflow Resolution ✅
- **Issue**: DECIMAL precision too small for stock prices
- **Solution**: Increased from DECIMAL(15,4) to DECIMAL(28,8)
- **Files Updated**: 
  - `extract_time_series_daily_adjusted.py`
  - `db/schema/source_schema.sql`

### 2. ETF Inclusion ✅
- **Issue**: ETFs excluded from time series processing
- **Solution**: Updated WatermarkManager to include both Stocks and ETFs by default
- **File Updated**: `utils/incremental_etl.py`

## Backward Compatibility

All migrations maintain backward compatibility through views in the `extracted` schema:
- `extracted.listing_status` → points to `source.listing_status`
- `extracted.overview` → points to `source.company_overview`

## Testing Results

### Balance Sheet Transformer ✅
- Successfully processed 314,360 records
- Created 98 feature columns
- Confirmed backward compatibility view works correctly

### Time Series Extraction ✅
- Processed 5 symbols with 10,603 records
- Included both Stocks (1,130) and ETFs (2,904)
- No numeric overflow issues

## Architecture Benefits

1. **Consistency**: All master reference data now lives in `source` schema
2. **Maintainability**: Clear separation between raw extracted data and curated source data
3. **Data Integrity**: Proper foreign key constraints enforce referential integrity
4. **Backward Compatibility**: Existing code continues to work through views

## Cleanup Recommendations

After sufficient testing period:
1. Consider dropping `extracted.overview_backup`
2. Consider dropping `extracted.listing_status_backup`
3. Update extractor code to reference `source` tables directly instead of views

## Files Modified

- `utils/incremental_etl.py` - Updated to reference source schema
- `db/schema/source_schema.sql` - Updated DECIMAL precision
- `extract_time_series_daily_adjusted.py` - Fixed numeric overflow
- Created migration scripts (now in archive/)
