# Situation
The economic indicators data extracted from Alpha Vantage API comes in 3 different 
time intervals: daly, monthly, quarterly.

# Needs
For now, I need all data to be daily.

# Task
Update the economic indicators extraction script to 
1. maintain its current extraction
2. add a step that transforms all data to daily using the following logic:
   - daily: daily data do not require tranformation since they are already daily
   - monthly: forward fill monthly data to the current date
   - quarterly: forward fill quarterly data to the current date

# Guidelines
- Only modify this program: data_pipeline\extract\extract_economic_indicators.py
- Add an additional table to the schema transformed called economic_indicators_daily which contains 
  all of the forward filled data. Add an additional column called updated_interval with values 'daily'