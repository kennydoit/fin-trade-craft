# Situation
The commodities data extracted from Alpha Vantage API comes in 2 different 
time intervals: daly, monthly

# Needs
For now, I need all data to be daily.

# Task
Update the commodities extraction script to 
1. maintain its current extraction
2. add a step that transforms all data to daily using the following logic:
   - daily: daily data do not require tranformation since they are already daily
   - monthly: forward fill monthly data to the current date
   - quarterly: forward fill quarterly data to the current date

# Guidelines
- Only modify this program: data_pipeline\extract\extract_commodities.py
- Add an additional table to the extracted schema called commodities_daily which contains 
  all of the forward filled data. Add an additional column called updated_interval with values 'daily'

# Reference Program
If needed, refer to extract_economic_indcators.py for pattern and logic