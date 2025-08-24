# Purpose
Create a configuration file that will feed into a feature engineering pipeline. Populate
 the target file with the below parameters and data switches:
 

# Target File
features\config.yaml

# Parameters
- universe: this can either be universe_id or universe_name
  if the format is uuid, then it is universe_id.
  otherwise it is universe_name. Must be validated against 
  fin_trade_craft.transformed.symbol_universes
- Collection Dates
  - start_date: '2020-01-01'
  - end_date: '2025-07-31'

# Data Switches:
  - Fundamentals
    - balance_sheet
    - cash_flow
    - income_statement
    - earnings_call_transcripts
    - insider_transactions
  - FRED
    - commodities
    - economic_indicators
  - OHLCV
    - time_series_daily_adjusted  