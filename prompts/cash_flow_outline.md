
# This markdown file is a program outline for use by Github Copilot. Do not remove this file
- Program outline for /data_pipeline/extract/extract_cash_flow.py
- Purpose: extract_cash_flow.py will perform the following:
  1. Initialize db schema in /db/schema/stock_db_schema.sql. See line 152 of stock_db_schema.sql for outline of schema. You are to complete it using the Sample API Response below starting on line 18 of this file.
  2. Follow pattern of extract_income_statement.py
  3. Get annual and quarterly cash flow for each stock and store in db table
  3. Run stocks incrementally just like extract_income_statement.py
  4. Just like income_statement, cash_flow has an annualReports and quarterlyReports

# References:
1. Program to modify: /data_pipeline/extract/extract_cash_flow.py
2. Database table to create: cash_flow
3. API Function: CASH_FLOW
3. Schema: C:\Users\Kenrm\repositories\fin-trade-craft\db\schema\stock_db_schema.sql
   Note: See line 152 for cash_flow table definition. Use Sample API Response to create schema

# Sample API Response
- Use the following API response to build the schema for table cash_flow:

    "symbol": "IBM",
    "annualReports": [
        {
            "fiscalDateEnding": "2024-12-31",
            "reportedCurrency": "USD",
            "operatingCashflow": "13445000000",
            "paymentsForOperatingActivities": "None",
            "proceedsFromOperatingActivities": "None",
            "changeInOperatingLiabilities": "None",
            "changeInOperatingAssets": "None",
            "depreciationDepletionAndAmortization": "4667000000",
            "capitalExpenditures": "1685000000",
            "changeInReceivables": "None",
            "changeInInventory": "-166000000",
            "profitLoss": "None",
            "cashflowFromInvestment": "None",
            "cashflowFromFinancing": "-7079000000",
            "proceedsFromRepaymentsOfShortTermDebt": "None",
            "paymentsForRepurchaseOfCommonStock": "None",
            "paymentsForRepurchaseOfEquity": "None",
            "paymentsForRepurchaseOfPreferredStock": "None",
            "dividendPayout": "6147000000",
            "dividendPayoutCommonStock": "6147000000",
            "dividendPayoutPreferredStock": "None",
            "proceedsFromIssuanceOfCommonStock": "None",
            "proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet": "None",
            "proceedsFromIssuanceOfPreferredStock": "None",
            "proceedsFromRepurchaseOfEquity": "-651000000",
            "proceedsFromSaleOfTreasuryStock": "None",
            "changeInCashAndCashEquivalents": "None",
            "changeInExchangeRate": "None",
            "netIncome": "6023000000"
        },