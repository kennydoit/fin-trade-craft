"""
Test the PostgreSQL DatabaseManager functionality.
"""

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from datetime import date

from db.postgres_database_manager import PostgresDatabaseManager


def test_postgres_database_manager():
    """Test PostgreSQL DatabaseManager functionality."""

    print("Testing PostgreSQL DatabaseManager...")

    try:
        # Test database connection
        with PostgresDatabaseManager() as db:
            print("✅ Database connection successful")

            # Test table existence
            tables_to_check = [
                "listing_status",
                "overview",
                "time_series_daily_adjusted",
                "income_statement",
                "balance_sheet",
                "cash_flow",
                "commodities",
                "economic_indicators",
            ]

            for table in tables_to_check:
                exists = db.table_exists(table)
                print(f"  Table '{table}' exists: {exists}")
                if not exists:
                    print(f"❌ Table '{table}' does not exist!")
                    return False

            print("✅ All expected tables exist")

            # Test get_table_info
            listing_info = db.get_table_info("listing_status")
            print(
                f"✅ Retrieved table info for 'listing_status': {len(listing_info)} columns"
            )

            # Test symbol insertion and retrieval
            test_symbol = "TEST"
            symbol_id = db.get_symbol_id(test_symbol)
            print(
                f"✅ Created/retrieved symbol_id {symbol_id} for symbol {test_symbol}"
            )

            # Test the same symbol again (should return existing ID)
            symbol_id_2 = db.get_symbol_id(test_symbol)
            if symbol_id == symbol_id_2:
                print(f"✅ Symbol ID retrieval consistent: {symbol_id}")
            else:
                print(f"❌ Symbol ID inconsistent: {symbol_id} vs {symbol_id_2}")
                return False

            # Test upsert functionality
            test_data = {
                "symbol_id": symbol_id,
                "symbol": test_symbol,
                "date": date.today(),
                "open": 100.50,
                "high": 105.25,
                "low": 99.75,
                "close": 103.00,
                "adjusted_close": 103.00,
                "volume": 1000000,
                "dividend_amount": 0.0,
                "split_coefficient": 1.0,
            }

            rows_affected = db.upsert_data(
                "time_series_daily_adjusted", test_data, ["symbol_id", "date"]
            )
            print(f"✅ Upserted time series data: {rows_affected} rows affected")

            # Test the same data again (should update)
            test_data["close"] = 104.00
            rows_affected_2 = db.upsert_data(
                "time_series_daily_adjusted", test_data, ["symbol_id", "date"]
            )
            print(f"✅ Updated time series data: {rows_affected_2} rows affected")

            # Test fetch_query
            results = db.fetch_query(
                "SELECT symbol, close FROM time_series_daily_adjusted WHERE symbol_id = %s",
                (symbol_id,),
            )
            print(f"✅ Fetched data: {results}")

            # Test fetch_dataframe
            df = db.fetch_dataframe(
                "SELECT * FROM time_series_daily_adjusted WHERE symbol_id = %s",
                (symbol_id,),
            )
            print(f"✅ Fetched DataFrame: {df.shape[0]} rows, {df.shape[1]} columns")

            # Test execute_many
            batch_data = [
                (
                    symbol_id,
                    test_symbol,
                    "2024-01-01",
                    95.0,
                    98.0,
                    94.0,
                    97.0,
                    97.0,
                    500000,
                    0.0,
                    1.0,
                ),
                (
                    symbol_id,
                    test_symbol,
                    "2024-01-02",
                    97.0,
                    99.0,
                    96.0,
                    98.5,
                    98.5,
                    600000,
                    0.0,
                    1.0,
                ),
                (
                    symbol_id,
                    test_symbol,
                    "2024-01-03",
                    98.5,
                    101.0,
                    98.0,
                    100.0,
                    100.0,
                    700000,
                    0.0,
                    1.0,
                ),
            ]

            insert_query = """
                INSERT INTO time_series_daily_adjusted 
                (symbol_id, symbol, date, open, high, low, close, adjusted_close, volume, dividend_amount, split_coefficient)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol_id, date) DO NOTHING
            """

            rows_inserted = db.execute_many(insert_query, batch_data)
            print(f"✅ Batch insert: processed {len(batch_data)} records")

            # Final verification
            total_records = db.fetch_query(
                "SELECT COUNT(*) FROM time_series_daily_adjusted WHERE symbol_id = %s",
                (symbol_id,),
            )[0][0]
            print(f"✅ Total records for test symbol: {total_records}")

            print("\n🎉 All PostgreSQL DatabaseManager tests passed!")
            return True

    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        return False


if __name__ == "__main__":
    success = test_postgres_database_manager()
    if not success:
        sys.exit(1)
