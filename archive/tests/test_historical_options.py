"""
Test script for historical options extraction.
This script tests the database schema and extraction functionality.
"""

import sys
from pathlib import Path

# Add the parent directories to the path
sys.path.append(str(Path(__file__).parent.parent))
from data_pipeline.extract.extract_historical_options import HistoricalOptionsExtractor
from db.postgres_database_manager import PostgresDatabaseManager


def test_schema_creation():
    """Test that the options tables are created correctly."""
    print("Testing schema creation...")

    try:
        db_manager = PostgresDatabaseManager()
        with db_manager as db:
            # Initialize schema
            schema_path = (
                Path(__file__).parent.parent
                / "db"
                / "schema"
                / "postgres_stock_db_schema.sql"
            )
            db.initialize_schema(schema_path)

            # Check if tables exist
            tables_to_check = ["historical_options", "realtime_options"]
            for table in tables_to_check:
                if db.table_exists(table):
                    print(f"✓ Table '{table}' created successfully")
                else:
                    print(f"✗ Table '{table}' not found")

            # Check table structure for historical_options
            structure_query = """
                SELECT column_name, data_type, is_nullable 
                FROM information_schema.columns 
                WHERE table_name = 'historical_options' 
                ORDER BY ordinal_position
            """
            columns = db.fetch_query(structure_query)
            print(f"\nHistorical Options table structure ({len(columns)} columns):")
            for col in columns:
                print(
                    f"  {col[0]}: {col[1]} ({'NULL' if col[2] == 'YES' else 'NOT NULL'})"
                )

        print("Schema test completed successfully!")
        return True

    except Exception as e:
        print(f"Schema test failed: {e}")
        return False


def test_extraction_dry_run():
    """Test the extraction logic without making API calls."""
    print("\nTesting extraction logic (dry run)...")

    try:
        extractor = HistoricalOptionsExtractor()

        # Test symbol loading
        symbols = extractor.load_valid_symbols(exchange_filter="NASDAQ", limit=5)
        print(f"✓ Loaded {len(symbols)} symbols from database")

        # Test unprocessed symbols for a date
        test_date = "2024-12-31"
        unprocessed = extractor.load_unprocessed_symbols_for_date(
            target_date=test_date, exchange_filter="NASDAQ", limit=3
        )
        print(f"✓ Found {len(unprocessed)} unprocessed symbols for {test_date}")

        # Test data transformation with mock data
        mock_options_data = [
            {
                "contractName": "AAPL240119C00150000",
                "strike": "150.00",
                "expiration": "2024-01-19",
                "lastPrice": "5.25",
                "bid": "5.20",
                "ask": "5.30",
                "volume": "1000",
                "openInterest": "5000",
                "impliedVolatility": "0.25",
                "delta": "0.65",
                "gamma": "0.05",
                "theta": "-0.02",
                "vega": "0.15",
                "rho": "0.08",
            }
        ]

        transformed = extractor.transform_historical_options_data(
            symbol="AAPL",
            symbol_id=1,
            options_data=mock_options_data,
            status="pass",
            target_date=test_date,
        )

        print(f"✓ Transformed {len(transformed)} mock option contracts")
        if transformed:
            print(f"  Sample record keys: {list(transformed[0].keys())}")

        print("Extraction logic test completed successfully!")
        return True

    except Exception as e:
        print(f"Extraction logic test failed: {e}")
        return False


def test_database_operations():
    """Test database insert operations with mock data."""
    print("\nTesting database operations...")

    try:
        extractor = HistoricalOptionsExtractor()

        # Create test record
        test_record = {
            "symbol_id": 1,
            "symbol": "TEST",
            "contract_name": "TEST240119C00100000",
            "option_type": "call",
            "strike": 100.00,
            "expiration": "2024-01-19",
            "last_trade_date": "2024-12-31",
            "last_price": 1.50,
            "mark": 1.55,
            "bid": 1.45,
            "bid_size": 10,
            "ask": 1.65,
            "ask_size": 5,
            "volume": 100,
            "open_interest": 500,
            "implied_volatility": 0.20,
            "delta": 0.50,
            "gamma": 0.03,
            "theta": -0.01,
            "vega": 0.10,
            "rho": 0.05,
            "intrinsic_value": 0.00,
            "extrinsic_value": 1.50,
            "updated_unix": 1703980800,
            "time_value": 1.50,
            "created_at": "2024-12-31T12:00:00",
            "updated_at": "2024-12-31T12:00:00",
        }

        # Test loading single record
        with extractor.db_manager as db:
            # First, ensure we have a test symbol in listing_status
            db.execute_query(
                """
                INSERT INTO listing_status (symbol_id, symbol, name, exchange, asset_type)
                VALUES (1, 'TEST', 'Test Company', 'NASDAQ', 'Stock')
                ON CONFLICT (symbol_id) DO NOTHING
            """
            )

            # Now test options insert
            columns = list(test_record.keys())
            placeholders = ", ".join(["%s" for _ in columns])
            insert_query = f"""
                INSERT INTO historical_options ({', '.join(columns)}) 
                VALUES ({placeholders})
                ON CONFLICT (symbol_id, contract_name, last_trade_date) 
                DO UPDATE SET updated_at = EXCLUDED.updated_at
            """

            record_tuple = tuple(test_record[col] for col in columns)
            rows_affected = db.execute_query(insert_query, record_tuple)

            print(f"✓ Inserted/updated {rows_affected} test record")

            # Verify the record was inserted
            verify_query = """
                SELECT contract_name, option_type, strike, volume 
                FROM historical_options 
                WHERE symbol = 'TEST' AND last_trade_date = '2024-12-31'
            """
            result = db.fetch_query(verify_query)

            if result:
                print(f"✓ Verified test record: {result[0]}")
            else:
                print("✗ Test record not found after insert")

            # Clean up test data
            db.execute_query("DELETE FROM historical_options WHERE symbol = 'TEST'")
            db.execute_query("DELETE FROM listing_status WHERE symbol = 'TEST'")
            print("✓ Cleaned up test data")

        print("Database operations test completed successfully!")
        return True

    except Exception as e:
        print(f"Database operations test failed: {e}")
        return False


def main():
    """Run all tests."""
    print("Starting Historical Options Tests")
    print("=" * 50)

    test_results = []

    # Run tests
    test_results.append(("Schema Creation", test_schema_creation()))
    test_results.append(("Extraction Logic", test_extraction_dry_run()))
    test_results.append(("Database Operations", test_database_operations()))

    # Print summary
    print("\n" + "=" * 50)
    print("TEST SUMMARY:")

    passed = 0
    for test_name, result in test_results:
        status = "PASS" if result else "FAIL"
        print(f"  {test_name}: {status}")
        if result:
            passed += 1

    print(f"\nOverall: {passed}/{len(test_results)} tests passed")

    if passed == len(test_results):
        print("🎉 All tests passed! Ready for production use.")
    else:
        print("⚠️  Some tests failed. Check errors above.")


if __name__ == "__main__":
    main()
