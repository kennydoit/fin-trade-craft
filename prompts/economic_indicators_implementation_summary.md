# Economic Indicators Data Extraction

## Overview

The `extract_economic_indicators.py` module provides robust extraction and loading of economic indicators data from the Alpha Vantage API into the database. It follows the same patterns and best practices as the other ETL extractors in the pipeline.

## Supported Economic Indicators

### Daily Data (Interest Rates)
- **FEDERAL_FUNDS_RATE**: Federal Funds Rate - Daily rates since 1954
- **TREASURY_YIELD_3MONTH**: 3-Month Treasury Yield - Daily rates since 1981
- **TREASURY_YIELD_2YEAR**: 2-Year Treasury Yield - Daily rates since 1976
- **TREASURY_YIELD_5YEAR**: 5-Year Treasury Yield - Daily rates since 1962
- **TREASURY_YIELD_7YEAR**: 7-Year Treasury Yield - Daily rates since 1969
- **TREASURY_YIELD_10YEAR**: 10-Year Treasury Yield - Daily rates since 1962
- **TREASURY_YIELD_30YEAR**: 30-Year Treasury Yield - Daily rates since 1977

### Monthly Data (Economic Activity)
- **CPI**: Consumer Price Index - Monthly since 1913
- **INFLATION**: Inflation Rate - Monthly since 1960
- **UNEMPLOYMENT**: Unemployment Rate - Monthly since 1948
- **NONFARM_PAYROLL**: Total Nonfarm Payroll - Monthly since 1939
- **RETAIL_SALES**: Retail Sales - Monthly since 1992
- **DURABLES**: Durable Goods Orders - Monthly since 1992

### Quarterly Data (Economic Growth)
- **REAL_GDP**: Real Gross Domestic Product - Quarterly since 2002
- **REAL_GDP_PER_CAPITA**: Real GDP Per Capita - Quarterly since 2002

## Database Schema

The economic indicators data is stored in the `economic_indicators` table with the following structure:

```sql
CREATE TABLE economic_indicators (
    economic_indicator_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    economic_indicator_name        TEXT NOT NULL,        -- Display name (e.g., "Federal Funds Rate")
    function_name         TEXT NOT NULL,        -- API function (e.g., "FEDERAL_FUNDS_RATE")
    maturity              TEXT,                 -- For Treasury yields (3month, 2year, etc.)
    date                  DATE,                 -- Indicator date (NULL for error records)
    interval              TEXT NOT NULL,        -- "daily", "monthly", or "quarterly"
    unit                  TEXT,                 -- Value unit (e.g., "percent")
    value                 REAL,                 -- Indicator value
    name                  TEXT,                 -- Full name from API
    api_response_status   TEXT,                 -- "data", "empty", "error", "pass"
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(economic_indicator_name, function_name, maturity, date, interval)
);
```

## Key Features

### Incremental ETL
- Avoids duplicate extractions by checking existing data
- Only downloads new data points not already in database
- Handles both initial data loads and updates

### Treasury Yield Handling
- Special handling for Treasury yields with different maturities
- Uses maturity parameter to distinguish between yield curves
- Unique constraints include maturity to prevent conflicts

### Error Handling
- Records API errors, empty responses, and rate limits
- Continues processing other indicators if one fails
- Provides detailed status tracking

### Rate Limiting
- Respects Alpha Vantage API limits (75 requests/minute)
- 0.8 second delay between requests
- Batch processing with longer pauses

### Update Strategies
- **Incremental**: Only extract indicators not yet in database
- **Force Update**: Re-extract all data regardless of existing records
- **Latest Periods**: Update only indicators older than threshold
- **Smart Updates**: Different thresholds for daily/monthly/quarterly indicators

## Usage Examples

### Basic Extraction
```python
from data_pipeline.extract.extract_economic_indicators import EconomicIndicatorsExtractor

extractor = EconomicIndicatorsExtractor()

# Extract all indicators (initial load)
extractor.run_etl_batch(force_update=False)

# Extract specific indicators
key_indicators = ['FEDERAL_FUNDS_RATE', 'TREASURY_YIELD_10YEAR', 'UNEMPLOYMENT']
extractor.run_etl_batch(key_indicators, batch_size=2)
```

### Production Scheduling
```python
# Daily updates (interest rates)
extractor.run_etl_latest_periods(days_threshold=1, batch_size=3)

# Monthly updates (economic activity indicators)  
extractor.run_etl_latest_periods(days_threshold=30, batch_size=3)

# Quarterly updates (GDP indicators)
extractor.run_etl_latest_periods(days_threshold=90, batch_size=2)

# Force refresh priority indicators
priority = ['FEDERAL_FUNDS_RATE', 'TREASURY_YIELD_10YEAR']
extractor.run_etl_update(priority, batch_size=2)
```

### Database Summary
```python
# Get overview of data in database
extractor.get_database_summary()
```

## Files

- **`extract_economic_indicators.py`**: Main extractor class
- **`copilot_test_scripts/update_economic_indicators_examples.py`**: Example scripts for different use cases
- **`stock_db_schema.sql`**: Database schema (updated with economic_indicators table)

## Data Volume

Current dataset contains:
- **114,847+ total records** across 16 economic indicators
- **Daily data**: ~100,000+ records (Treasury yields, Fed funds rate)
- **Monthly data**: ~14,000+ records (CPI, unemployment, payrolls, etc.)
- **Quarterly data**: ~500+ records (GDP metrics)
- **Historical coverage**: 1913-2025 (depending on indicator)

## Data Categories

### Interest Rates & Monetary Policy
- Federal Funds Rate (daily)
- Complete Treasury yield curve (3M, 2Y, 5Y, 7Y, 10Y, 30Y)
- Critical for financial modeling and risk management

### Economic Growth
- Real GDP and GDP per capita (quarterly)
- Core measures of economic performance

### Labor Market
- Unemployment rate and nonfarm payrolls (monthly)
- Key employment indicators

### Inflation & Prices
- Consumer Price Index and inflation rate (monthly)
- Essential for monetary policy analysis

### Economic Activity
- Retail sales and durable goods orders (monthly)
- Indicators of consumer and business spending

## Best Practices

1. **Initial Setup**: Start with key indicators (Fed funds, 10Y Treasury, unemployment)
2. **Scheduling**: 
   - Daily updates for interest rates
   - Monthly for economic activity indicators
   - Quarterly for GDP metrics
3. **Rate Limiting**: Respect API limits with appropriate batch sizes
4. **Monitoring**: Check status summaries and data integrity
5. **Error Recovery**: Re-run failed extractions individually

## Integration

The economic indicators extractor follows the same patterns as other extractors:
- Uses `DatabaseManager` for connection management
- Implements proper error handling and status tracking
- Provides consistent logging and progress reporting
- Maintains referential integrity and data quality standards

## Use Cases

This data is essential for:
- **Financial Analysis**: Interest rate modeling, yield curve analysis
- **Economic Research**: GDP analysis, inflation studies, labor market research
- **Risk Management**: Interest rate risk, economic scenario analysis
- **Portfolio Management**: Asset allocation, duration management
- **Forecasting**: Economic predictions, policy impact analysis
