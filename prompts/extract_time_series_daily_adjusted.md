
# Purpose 
The program extract_time_series_daily_adjusted.py currently uses an exchange_filter to identify stocks to extract
from the Alpha Vantage API. There is an assumption that the only asset type to be pulled are 'Stocks'. The purpose of 
this task is to modify extract_time_series_daily_adjusted.py so that different asset types can also be extracted. 

# Suggested modification options
I suggest one of the following:
- option A: Remove asset_type from Base Query (see line 46) add an additional filter called asset_type_filter that 
            also subsets to asset_type
- option B: rename exchange filter to exchange_asset_filter and combine into a single filter with 'or' logic

# Target Program
data_pipeline\extract\extract_time_series_daily_adjusted.py

# Additional asset types
I specifically want to be able to extract asset_type = 'ETF' in addition to the current 'Stocks'