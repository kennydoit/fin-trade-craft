#!/usr/bin/env python3
"""
Quick script to check the data types in our company_master DataFrame
to understand why we're getting integer type errors
"""

import pandas as pd
from db.postgres_database_manager import PostgresDatabaseManager

def main():
    db_manager = PostgresDatabaseManager()
    
    with db_manager as db:
        print("🔍 Checking overview table structure...")
        overview_query = """
        SELECT overview_id, symbol_id, symbol, assettype, name, description, cik, exchange, 
               currency, country, sector, industry, address, officialsite, fiscalyearend, 
               status, created_at, updated_at 
        FROM extracted.overview LIMIT 5
        """
        overview_data = db.fetch_query(overview_query)
        overview_df = pd.DataFrame(overview_data, columns=[
            'overview_id', 'symbol_id', 'symbol', 'assettype', 'name', 'description', 
            'cik', 'exchange', 'currency', 'country', 'sector', 'industry', 'address', 
            'officialsite', 'fiscalyearend', 'status', 'created_at', 'updated_at'
        ])
        
        print(f"Overview columns: {overview_df.columns.tolist()}")
        print(f"Overview dtypes:")
        for col in overview_df.columns:
            print(f"  {col}: {overview_df[col].dtype}")
        
        print("\n🔍 Checking listing_status table structure...")
        listing_query = """
        SELECT symbol_id, symbol, name, exchange, asset_type, ipo_date, 
               delisting_date, status, created_at, updated_at 
        FROM extracted.listing_status LIMIT 5
        """
        listing_data = db.fetch_query(listing_query)
        listing_df = pd.DataFrame(listing_data, columns=[
            'symbol_id', 'symbol', 'name', 'exchange', 'asset_type', 'ipo_date', 
            'delisting_date', 'status', 'created_at', 'updated_at'
        ])
        
        print(f"Listing columns: {listing_df.columns.tolist()}")
        print(f"Listing dtypes:")
        for col in listing_df.columns:
            print(f"  {col}: {listing_df[col].dtype}")
        
        print("\n🔍 Sample data from overview (first few columns):")
        print(overview_df[['symbol', 'name', 'exchange', 'currency', 'country']].head())
        
        print("\n🔍 Sample data from listing_status:")
        print(listing_df[['symbol', 'name', 'exchange', 'asset_type']].head())
        
        print("\n🔍 Checking for problematic data patterns...")
        # Check for 'USA' in currency column
        usa_currency = overview_df[overview_df['currency'] == 'USA']
        if len(usa_currency) > 0:
            print(f"  ⚠️  Found {len(usa_currency)} rows with 'USA' in currency column")
            print(f"    Sample: {usa_currency[['symbol', 'currency', 'country']].head()}")
        
        # Check for 'USA' in country column
        usa_country = overview_df[overview_df['country'] == 'USA']
        if len(usa_country) > 0:
            print(f"  ✅ Found {len(usa_country)} rows with 'USA' in country column (this is correct)")

if __name__ == "__main__":
    main()
