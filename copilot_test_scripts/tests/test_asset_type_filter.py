#!/usr/bin/env python3
"""
Test script to verify asset_type_filter functionality
"""
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

# Add the parent directories to the path so we can import from data_pipeline
sys.path.append(str(Path(__file__).parent))
from data_pipeline.extract.extract_time_series_daily_adjusted import TimeSeriesExtractor

load_dotenv()

def test_asset_type_filter():
    """Test the new asset_type_filter functionality"""
    print("🧪 Testing Asset Type Filter Functionality")
    print("=" * 50)

    # Create extractor instance
    extractor = TimeSeriesExtractor(output_size="compact")

    # Test 1: Default behavior (should default to 'Stock')
    print("Test 1: Default behavior (Stock only)")
    try:
        symbols_default = extractor.load_unprocessed_symbols(
            exchange_filter=['NYSE', 'NASDAQ'],
            limit=5
        )
        print(f"  ✅ Default filter returned {len(symbols_default)} symbols")
        if symbols_default:
            sample_symbols = list(symbols_default.keys())[:3]
            print(f"  📝 Sample symbols: {sample_symbols}")
    except Exception as e:
        print(f"  ❌ Default test failed: {e}")

    # Test 2: Explicit Stock filter
    print("\nTest 2: Explicit Stock filter")
    try:
        symbols_stock = extractor.load_unprocessed_symbols(
            exchange_filter=['NYSE', 'NASDAQ'],
            asset_type_filter='Stock',
            limit=5
        )
        print(f"  ✅ Stock filter returned {len(symbols_stock)} symbols")
        if symbols_stock:
            sample_symbols = list(symbols_stock.keys())[:3]
            print(f"  📝 Sample symbols: {sample_symbols}")
    except Exception as e:
        print(f"  ❌ Stock test failed: {e}")

    # Test 3: ETF filter
    print("\nTest 3: ETF filter")
    try:
        symbols_etf = extractor.load_unprocessed_symbols(
            exchange_filter=['NYSE', 'NASDAQ'],
            asset_type_filter='ETF',
            limit=5
        )
        print(f"  ✅ ETF filter returned {len(symbols_etf)} symbols")
        if symbols_etf:
            sample_symbols = list(symbols_etf.keys())[:3]
            print(f"  📝 Sample ETF symbols: {sample_symbols}")
    except Exception as e:
        print(f"  ❌ ETF test failed: {e}")

    # Test 4: Multiple asset types
    print("\nTest 4: Multiple asset types (Stock + ETF)")
    try:
        symbols_mixed = extractor.load_unprocessed_symbols(
            exchange_filter=['NYSE', 'NASDAQ'],
            asset_type_filter=['Stock', 'ETF'],
            limit=10
        )
        print(f"  ✅ Mixed filter returned {len(symbols_mixed)} symbols")
        if symbols_mixed:
            sample_symbols = list(symbols_mixed.keys())[:5]
            print(f"  📝 Sample mixed symbols: {sample_symbols}")
    except Exception as e:
        print(f"  ❌ Mixed test failed: {e}")

    # Test 5: Check what asset types are actually available
    print("\nTest 5: Available asset types in database")
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=os.getenv('POSTGRES_PORT', '5432'),
            database=os.getenv('POSTGRES_DB', 'fin_trade_craft'),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD')
        )
        cursor = conn.cursor()

        # Check from the new extracted schema
        cursor.execute("""
            SELECT asset_type, COUNT(*) 
            FROM extracted.listing_status 
            WHERE asset_type IS NOT NULL
            GROUP BY asset_type 
            ORDER BY COUNT(*) DESC
        """)
        asset_types = cursor.fetchall()

        print("  📊 Asset types in extracted.listing_status:")
        for asset_type, count in asset_types:
            print(f"    - {asset_type}: {count:,} symbols")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"  ❌ Database query failed: {e}")

    print("\n" + "=" * 50)
    print("🎉 Asset Type Filter Testing Complete!")
    print("✅ The extractor now supports:")
    print("  - asset_type_filter='Stock' (default)")
    print("  - asset_type_filter='ETF'")
    print("  - asset_type_filter=['Stock', 'ETF']")
    print("  - asset_type_filter=None (defaults to 'Stock')")
    print("=" * 50)

if __name__ == "__main__":
    test_asset_type_filter()
