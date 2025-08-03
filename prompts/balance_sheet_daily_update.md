# Situation
The balance sheet data extracted from Alpha Vantage API comes in 2 different 
report types: quarterly, annual

# Needs
For now, I need all data to be daily.

# Task
Update the balance sheet extraction script to 
1. maintain its current extraction
2. add a step that transforms all data to daily using the following logic:
   - quarterly: forward fill quarterly data to the current date
   - annual: do not include annual data in this table; we will only be forward filling quarterly

# Guidelines
- Only modify this program: data_pipeline\extract\extract_balance_sheet.py
- Add an additional table to the extracted schema called balance_sheet_daily which contains 
  all of the forward filled data. 
- the column report_type contains the interval and has values 'quarterly' and 'annual'. We only
  care about 'quarterly'.

# Reference Program
If needed, refer to extract_economic_indcators.py for pattern and logic