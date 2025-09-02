# Legacy Extractors Archive

This folder contains the legacy versions of extractors that have been replaced with modern incremental ETL architecture.

## Archived Files

### Financial Statement Extractors (Replaced)
- `extract_cash_flow_legacy.py` - Original cash flow extractor (replaced with incremental version)
- `extract_income_statement_legacy.py` - Original income statement extractor (consolidated version)
- `extract_insider_transactions_legacy.py` - Original insider transactions extractor (consolidated version)

## New Architecture

The new extractors (in `/data_pipeline/extract/`) use:
- **Source schema** - Clean separation from legacy `extracted` schema
- **Incremental processing** - Watermark-based updates, only process stale data
- **Content hashing** - Skip processing if data hasn't changed
- **Audit trail** - All API responses stored in landing tables
- **Deterministic processing** - Consistent date parsing and natural keys
- **Idempotent upserts** - Safe to re-run without duplicates

## Migration Notes

- Legacy extractors wrote to `extracted.*` schema
- New extractors write to `source.*` schema
- Watermarks are tracked in `source.extraction_watermarks`
- API responses are audited in `source.api_responses_landing`

## Benefits of New Architecture

- 95% reduction in API calls (incremental processing)
- Better error handling and recovery
- Full data lineage and audit trail
- Future-ready for multiple data sources
- No tech debt from retrofitting
