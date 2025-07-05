
# This markdown file is a program outline for use by Github Copilot. Do not remove this file
- Program outline for /data_pipeline/extract/extract_balance_sheet.py
- Purpose: extract_balance_sheet.py will perform the following:
  1. Initialize db schema for /db/schema/stock_db_schema.sql
  2. Follow pattern of extract_income_statement.py
  3. Get annual and quarterly balance sheets for each stock and store in db table
  3. Run stocks incrementally just like extract_income_statement.py
  4. Just like income_statement, balance_sheet has an annualReports and quarterlyReports

# References:
1. Program to modify: /data_pipeline/extract/extract_balance_sheet.py
2. Database table to create: balance_sheet
3. Schema: C:\Users\Kenrm\repositories\fin-trade-craft\db\schema\stock_db_schema.sql
   Note: See line 101 for balance_sheet table definition. Use Sample API Response to create schema

# Sample API Response
- Use the following API response to build the schema for table balance_sheet:

    "symbol": "IBM",
    "annualReports": [
        {
            "fiscalDateEnding": "2024-12-31",
            "reportedCurrency": "USD",
            "totalAssets": "137175000000",
            "totalCurrentAssets": "34482000000",
            "cashAndCashEquivalentsAtCarryingValue": "13947000000",
            "cashAndShortTermInvestments": "13947000000",
            "inventory": "1289000000",
            "currentNetReceivables": "14010000000",
            "totalNonCurrentAssets": "102694000000",
            "propertyPlantEquipment": "None",
            "accumulatedDepreciationAmortizationPPE": "None",
            "intangibleAssets": "10660000000",
            "intangibleAssetsExcludingGoodwill": "10660000000",
            "goodwill": "60706000000",
            "investments": "None",
            "longTermInvestments": "None",
            "shortTermInvestments": "644000000",
            "otherCurrentAssets": "4592000000",
            "otherNonCurrentAssets": "None",
            "totalLiabilities": "109782000000",
            "totalCurrentLiabilities": "33142000000",
            "currentAccountsPayable": "4032000000",
            "deferredRevenue": "None",
            "currentDebt": "None",
            "shortTermDebt": "5857000000",
            "totalNonCurrentLiabilities": "76640000000",
            "capitalLeaseObligations": "3423000000",
            "longTermDebt": "49884000000",
            "currentLongTermDebt": "5089000000",
            "longTermDebtNoncurrent": "None",
            "shortLongTermDebtTotal": "58396000000",
            "otherCurrentLiabilities": "7313000000",
            "otherNonCurrentLiabilities": "981000000",
            "totalShareholderEquity": "27307000000",
            "treasuryStock": "None",
            "retainedEarnings": "151163000000",
            "commonStock": "61380000000",
            "commonStockSharesOutstanding": "937200000"
        },