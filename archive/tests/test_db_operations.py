#!/usr/bin/env python3
"""
Test script to debug the database connection issue.
"""

import sys
from pathlib import Path

# Add the parent directory to the path
sys.path.append(str(Path(__file__).parent))


def test_db_operations():
    """Test basic database operations."""
    try:
        from db.postgres_database_manager import PostgresDatabaseManager

        print("Testing database operations...")

        # Test 1: Basic connection
        print("Test 1: Basic connection")
        with PostgresDatabaseManager() as db:
            print("✓ Connection successful")

            # Test if table exists
            exists = db.table_exists("historical_options")
            print(f"✓ historical_options table exists: {exists}")

        print("✓ Connection closed successfully")

        # Test 2: Insert some test data
        print("\nTest 2: Insert test data")
        test_record = {
            "symbol_id": 1,
            "symbol": "TEST",
            "contract_name": "TEST241220C00100000",
            "option_type": "call",
            "strike": 100.0,
            "expiration": "2024-12-20",
            "last_trade_date": "2024-12-20",
            "last_price": 5.0,
            "mark": 5.5,
            "bid": 4.8,
            "bid_size": 10,
            "ask": 5.2,
            "ask_size": 15,
            "volume": 100,
            "open_interest": 500,
            "implied_volatility": 0.25,
            "delta": 0.5,
            "gamma": 0.1,
            "theta": -0.02,
            "vega": 0.15,
            "rho": 0.05,
            "intrinsic_value": None,
            "extrinsic_value": None,
            "updated_unix": None,
            "time_value": None,
            "created_at": "2025-01-06T10:00:00",
            "updated_at": "2025-01-06T10:00:00",
        }

        with PostgresDatabaseManager() as db:
            # Check if symbol_id 1 exists in listing_status
            symbol_exists = db.fetch_query(
                "SELECT COUNT(*) FROM listing_status WHERE symbol_id = %s", [1]
            )
            if symbol_exists[0][0] == 0:
                print("Inserting test symbol into listing_status...")
                db.execute_query(
                    """
                    INSERT INTO listing_status (symbol_id, symbol, name, exchange, asset_type) 
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (symbol_id) DO NOTHING
                """,
                    [1, "TEST", "Test Company", "TEST", "Stock"],
                )

            # Prepare insert query
            columns = list(test_record.keys())
            placeholders = ", ".join(["%s" for _ in columns])
            insert_query = f"""
                INSERT INTO historical_options ({', '.join(columns)}) 
                VALUES ({placeholders})
                ON CONFLICT (symbol_id, contract_name, last_trade_date) 
                DO UPDATE SET
                    last_price = EXCLUDED.last_price,
                    updated_at = EXCLUDED.updated_at
            """

            # Execute insert
            record_tuple = tuple(test_record[col] for col in columns)
            rows_affected = db.execute_query(insert_query, record_tuple)
            print(f"✓ Inserted test record, rows affected: {rows_affected}")

            # Verify insert
            result = db.fetch_query(
                "SELECT COUNT(*) FROM historical_options WHERE symbol = %s", ["TEST"]
            )
            print(f"✓ Test records in database: {result[0][0]}")

        print("✓ Database operations test completed successfully!")
        return True

    except Exception as e:
        print(f"✗ Database operations test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("Testing Database Operations")
    print("=" * 50)

    success = test_db_operations()

    print("\n" + "=" * 50)
    if success:
        print("✓ Test completed successfully!")
    else:
        print("✗ Test failed!")
