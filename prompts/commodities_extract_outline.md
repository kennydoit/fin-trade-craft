# Outline for Extracting Commodities Data from Alpha Vantage API


# The below outlines the extraction of commodities data from the Alpha Vantage API, including the functions, 
# intervals, and data types for each commodity.

# This is intended to be used as a reference for implementing the extraction logic in the data pipeline.

# Outline for Extracting Commodities Data from Alpha Vantage API

- Reference: schema is partially defined in db/schema/stock_db_schema.sql starting on line 193.
- Program to place code: data_pipeline/extract/extract_commodities.py

Crude Oil (WTI)
- function: WTI
- interval: daily
- datatype: json

Crude Oil (Brent)
- function: BRENT
- interval: daily
- datatype: json

Natural Gas
- function: NATURAL_GAS
- interval: daily
- datatype: json

Copper
- function: COPPER
- interval: monthly
- datatype: json

Aluminum
- function: ALUMINUM
- interval: monthly
- datatype: json

Wheat
- function: WHEAT
- interval: monthly
- datatype: json

Corn
- function: CORN
- interval: monthly
- datatype: json

Cotton
- function: COTTON
- interval: monthly
- datatype: json

Sugar
- function: SUGAR
- interval: monthly
- datatype: json

Coffee
- function: COFFEE
- interval: monthly
- datatype: json

Global Commodities Index
- function: ALL_COMMODITIES
- interval: monthly
- datatype: json


# Below is a snippet of the API result for crude oil:
    "name": "Crude Oil Prices WTI",
    "interval": "monthly",
    "unit": "dollars per barrel",
    "data": [
        {
            "date": "2025-06-01",
            "value": "68.17"
        },
        {
            "date": "2025-05-01",
            "value": "62.17"
        },
        {
            "date": "2025-04-01",
            "value": "63.54"
        },
        {