# Outline for extracting insider_transactions from Alpha Vantage API

- DB Schema: Use the below Sample API Response to create a schema in db/schema/postgres_stock_db_schema.sql starting
- Program to place code: data_pipeline/extract/extract_insider_transactions.py
- Reference program for design pattern: data_pipeline\extract\extract_time_series_daily_adjusted.py

# API Parameters
Required: function
The function of your choice. In this case, function=INSIDER_TRANSACTIONS

Required: symbol
The symbol of the ticker of your choice. For example: symbol=IBM.

Example (click for JSON output)
https://www.alphavantage.co/query?function=INSIDER_TRANSACTIONS&symbol=IBM&apikey=demo

# Sample API Response:
```
    "data": [
        {
            "transaction_date": "2025-07-01",
            "ticker": "IBM",
            "executive": "ROBINSON, ANNE",
            "executive_title": "Senior Vice President",
            "security_type": "Rst. Stock Unit",
            "acquisition_or_disposal": "D",
            "shares": "1412.0",
            "share_price": "0.0"
        },
        {
            "transaction_date": "2025-07-01",
            "ticker": "IBM",
            "executive": "ROBINSON, ANNE",
            "executive_title": "Senior Vice President",
            "security_type": "Common Stock",
            "acquisition_or_disposal": "A",
            "shares": "8825.0",
            "share_price": "0.0"
        },
        {
            "transaction_date": "2025-07-01",
            "ticker": "IBM",
            "executive": "ROBINSON, ANNE",
            "executive_title": "Senior Vice President",
            "security_type": "Common Stock",
            "acquisition_or_disposal": "D",
            "shares": "4881.0",
            "share_price": "292.595"
        },
```        