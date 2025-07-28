# Outline for building table company_master

- This outline is intended to be used to create the program data_pipeline/transform/transform_company_master.py

- This will be the first table added to the transformed table schema

## Step 1 Join Master Tables
- Input tables:
  1. fin_trade_craft.extract.overview
  2. fin_trade_craft.listing_status

- Output table: Python data frame named overview_listing_status 

- Operations:
  Join Input table 1 (overview) and table 2 (listing status) by exchange and symbol. 

## Step 2 Create Table Flags
- Input tables:
  1. fin_trade_craft.extract.cash_flow
  2. fin_trade_craft.extract.income_statement
  3. fin_trade_craft.extract.insider_transactions
  4. fin_trade_craft.extract.balance_sheet
  5. fin_trade_craft.extract.earnings_call_transcripts
  6. fin_trade_craft.extract.time_series_daily_adjusted

- Operations:
  For each of the 6 input tables create an aggregate table containing the following columns:
  1. symbol, cash_flow_count
  2. symbol, income_statement_count
  3. symbol, insider_transactions_count
  4. symbol, balance_sheet_count
  5. symbol, earnings_call_transcripts_count
  6. symbol, time_series_daily_adjusted_count

 ## Step 3 Output Table
 - Input tables:
   1. The data frame overview_listing_status
   2. 6 data frames from step 2

- Operations:
  Join all 6 summary data frames from step 2 onto the table from step 1

- Output Table:
  fin_trade_craft.transformed.company_master

  Schema shoudl be as follows:
  All columns from listing status, 
  All columns from overview,
  6 indicators from step 2:
    cash_flow_count
    income_statement_count
    insider_transactions_count
    balance_sheet_count
    earnings_call_transcripts_count
    time_series_daily_adjusted_count
