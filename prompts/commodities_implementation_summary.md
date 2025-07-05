# Commodities Data Extraction

## Overview

The `extract_commodities.py` module provides robust extraction and loading of commodities data from the Alpha Vantage API into the database. It follows the same patterns and best practices as the other ETL extractors in the pipeline.

## Supported Commodities

### Daily Data (Energy)
- **WTI**: Crude Oil WTI - Daily prices since 1986
- **BRENT**: Crude Oil Brent - Daily prices since 1987  
- **NATURAL_GAS**: Natural Gas - Daily prices since 1997

### Monthly Data (Metals, Agriculture, Indices)
- **COPPER**: Copper prices - Monthly since 1980
- **ALUMINUM**: Aluminum prices - Monthly since 1980
- **WHEAT**: Wheat prices - Monthly since 1980
- **CORN**: Corn prices - Monthly since 1980
- **COTTON**: Cotton prices - Monthly since 1980
- **SUGAR**: Sugar prices - Monthly since 1980
- **COFFEE**: Coffee prices - Monthly since 1980
- **ALL_COMMODITIES**: Global Commodities Index - Monthly since 1992

## Database Schema

The commodities data is stored in the `commodities` table with the following structure:

```sql
CREATE TABLE commodities (
    commodity_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    commodity_name        TEXT NOT NULL,        -- Display name (e.g., "Crude Oil WTI")
    function_name         TEXT NOT NULL,        -- API function (e.g., "WTI")
    date                  DATE,                 -- Price date (NULL for error records)
    interval              TEXT NOT NULL,        -- "daily" or "monthly"
    unit                  TEXT,                 -- Price unit (e.g., "dollars per barrel")
    value                 REAL,                 -- Price value
    name                  TEXT,                 -- Full name from API
    api_response_status   TEXT,                 -- "data", "empty", "error", "pass"
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(commodity_name, date, interval)
);
```

## Key Features

### Incremental ETL
- Avoids duplicate extractions by checking existing data
- Only downloads new data points not already in database
- Handles both initial data loads and updates

### Error Handling
- Records API errors, empty responses, and rate limits
- Continues processing other commodities if one fails
- Provides detailed status tracking

### Rate Limiting
- Respects Alpha Vantage API limits (75 requests/minute)
- 0.8 second delay between requests
- Batch processing with longer pauses

### Update Strategies
- **Incremental**: Only extract commodities not yet in database
- **Force Update**: Re-extract all data regardless of existing records
- **Latest Periods**: Update only commodities older than threshold
- **Smart Updates**: Different thresholds for daily vs monthly commodities

## Usage Examples

### Basic Extraction
```python
from data_pipeline.extract.extract_commodities import CommoditiesExtractor

extractor = CommoditiesExtractor()

# Extract all commodities (initial load)
extractor.run_etl_batch(force_update=False)

# Extract specific commodities
energy_commodities = ['WTI', 'BRENT', 'NATURAL_GAS']
extractor.run_etl_batch(energy_commodities, batch_size=2)
```

### Production Scheduling
```python
# Daily updates (energy commodities)
extractor.run_etl_latest_periods(days_threshold=1, batch_size=2)

# Monthly updates (other commodities)  
extractor.run_etl_latest_periods(days_threshold=30, batch_size=3)

# Force refresh priority commodities
priority = ['WTI', 'BRENT']
extractor.run_etl_update(priority, batch_size=2)
```

### Database Summary
```python
# Get overview of data in database
extractor.get_database_summary()
```

## Files

- **`extract_commodities.py`**: Main extractor class
- **`update_commodities_examples.py`**: Example scripts for different use cases
- **`stock_db_schema.sql`**: Database schema (updated with commodities table)

## Data Volume

Current dataset contains:
- **31,893 total records** across 11 commodities
- **Daily data**: ~27,677 records (WTI: 10,303, Brent: 9,944, Natural Gas: 7,430)
- **Monthly data**: ~4,216 records (545 each for most commodities, 401 for global index)
- **Historical coverage**: 1980-2025 (depending on commodity)

## Best Practices

1. **Initial Setup**: Start with energy commodities (smaller, daily updates)
2. **Scheduling**: Daily updates for energy, monthly for others
3. **Rate Limiting**: Respect API limits with appropriate batch sizes
4. **Monitoring**: Check status summaries and database integrity
5. **Error Recovery**: Re-run failed extractions individually

## Integration

The commodities extractor follows the same patterns as other extractors:
- Uses `DatabaseManager` for connection management
- Implements proper error handling and status tracking
- Provides consistent logging and progress reporting
- Maintains referential integrity and data quality standards
