# Outline for Extracting economic indicators Data from Alpha Vantage API


# The below outlines the extraction of economic indicators data from the Alpha Vantage API, including the functions, 
# intervals, and data types for each indicator.

# This is intended to be used as a reference for implementing the extraction logic in the data pipeline.

# Outline for Extracting economic indicators Data from Alpha Vantage API

- Reference: schema is partially defined in db/schema/stock_db_schema.sql starting on line 210.
- Program to place code: data_pipeline/extract/extract_economic_indicators.py

Real GDP
- function: REAL_GDP
- interval: quarterly
- datatype: json

REAL_GDP_PER_CAPITA
- function: REAL_GDP_PER_CAPITA
- interval: quarterly
- datatype: json

TREASURY_YIELD_10YEAR
- function: TREASURY_YIELD
- interval: daily
- maturity: 10year
- datatype: json

TREASURY_YIELD_3MONTH
- function: TREASURY_YIELD
- interval: daily
- maturity: 3month
- datatype: json

TREASURY_YIELD_2YEAR
- function: TREASURY_YIELD
- interval: daily
- maturity: 2year
- datatype: json

TREASURY_YIELD_5YEAR
- function: TREASURY_YIELD
- interval: daily
- maturity: 5year
- datatype: json

TREASURY_YIELD_7YEAR
- function: TREASURY_YIELD
- interval: daily
- maturity: 7year
- datatype: json

TREASURY_YIELD_30YEAR
- function: TREASURY_YIELD
- interval: daily
- maturity: 30year
- datatype: json

FEDERAL_FUNDS_RATE
- function: FEDERAL_FUNDS_RATE
- interval: daily
- datatype: json

CPI
- function: CPI
- interval: monthly
- datatype: json

INFLATION
- function: INFLATION
- interval: monthly
- datatype: json

RETAIL_SALES
- function: RETAIL_SALES
- interval: monthly
- datatype: json

DURABLES
- function: DURABLES
- interval: monthly
- datatype: json

UNEMPLOYMENT
- function: UNEMPLOYMENT
- interval: monthly
- datatype: json

NONFARM_PAYROLL
- function: NONFARM_PAYROLL
- interval: monthly
- datatype: json




# Below is a snippet of the API result for NONFAR_PAYROLL:
    "name": "Total Nonfarm Payroll",
    "interval": "monthly",
    "unit": "thousands of persons",
    "data": [
        {
            "date": "2025-06-01",
            "value": "160475"
        },
        {
            "date": "2025-05-01",
            "value": "159958"
        },
        {
            "date": "2025-04-01",
            "value": "159227"

