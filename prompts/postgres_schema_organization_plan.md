# PostgreSQL Schema Organization Plan

## Current State
All extracted data currently lives in the `public` schema:
- balance_sheet
- cash_flow  
- commodities
- earnings_call_transcripts
- economic_indicators
- historical_options
- income_statement
- insider_transactions
- listing_status
- overview
- time_series_daily_adjusted

## Target State

### Schema: `extracted` (Raw API Data)
All tables from Alpha Vantage API will be moved here:
- extracted.balance_sheet
- extracted.cash_flow
- extracted.commodities
- extracted.earnings_call_transcripts
- extracted.economic_indicators
- extracted.historical_options
- extracted.income_statement
- extracted.insider_transactions
- extracted.listing_status
- extracted.overview
- extracted.time_series_daily_adjusted

### Schema: `transformed` (Processed Business Logic)
New tables for business intelligence and analytics:
- transformed.company_financials (normalized financial statements)
- transformed.stock_performance (calculated metrics)
- transformed.market_indicators (derived market data)
- transformed.company_profiles (enriched company data)
- transformed.price_movements (technical indicators)
- etc.

## Migration Strategy

1. **Create new schemas**
2. **Move existing tables to `extracted` schema**
3. **Update all extractor scripts to use new schema**
4. **Create initial transformed tables**
5. **Update database manager to handle multiple schemas**

## Benefits

- **Clear data lineage**: Raw → Processed → Analytics
- **Better organization**: Easier to find and manage tables
- **Access control**: Can set different permissions per schema
- **Development clarity**: Developers know where to find what data
- **Backup strategies**: Can backup schemas separately
