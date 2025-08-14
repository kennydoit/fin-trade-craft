#!/usr/bin/env python3
"""
Verify ETF time series data was loaded
"""
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

def verify_etf_data():
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        database=os.getenv('POSTGRES_DB', 'fin_trade_craft'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD')
    )
    cursor = conn.cursor()

    print("🧪 Verifying ETF Time Series Data")
    print("=" * 40)

    # Check the specific ETFs we just processed
    etf_symbols = ['AADR', 'AAPB', 'AAPD', 'AAPU', 'AAVM']

    cursor.execute("""
        SELECT symbol, COUNT(*) 
        FROM extracted.time_series_daily_adjusted 
        WHERE symbol = ANY(%s)
        GROUP BY symbol 
        ORDER BY symbol
    """, (etf_symbols,))

    records = cursor.fetchall()

    print("ETF Time Series Data Loaded:")
    total_records = 0
    for symbol, count in records:
        print(f"  {symbol}: {count:,} records")
        total_records += count

    print(f"\n✅ Total ETF records: {total_records:,}")

    # Check if these are classified as ETFs in the listing_status
    cursor.execute("""
        SELECT symbol, asset_type, exchange 
        FROM extracted.listing_status 
        WHERE symbol = ANY(%s)
        ORDER BY symbol
    """, (etf_symbols,))

    etf_info = cursor.fetchall()
    print("\n📊 ETF Classification:")
    for symbol, asset_type, exchange in etf_info:
        print(f"  {symbol}: {asset_type} on {exchange}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    verify_etf_data()
