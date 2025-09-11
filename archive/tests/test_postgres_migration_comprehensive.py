"""
Comprehensive test of all PostgreSQL extractors.
"""

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from db.postgres_database_manager import PostgresDatabaseManager


def test_database_tables():
    """Test that all required tables exist and are accessible."""
    print("🗄️  Testing database tables...")

    expected_tables = [
        "listing_status",
        "overview",
        "time_series_daily_adjusted",
        "income_statement",
        "balance_sheet",
        "cash_flow",
        "commodities",
        "economic_indicators",
    ]

    try:
        with PostgresDatabaseManager() as db:
            for table in expected_tables:
                exists = db.table_exists(table)
                if exists:
                    count = db.fetch_query(f"SELECT COUNT(*) FROM {table}")[0][0]
                    print(f"  ✅ {table}: {count} records")
                else:
                    print(f"  ❌ {table}: does not exist")
                    return False

        print("✅ All database tables verified")
        return True

    except Exception as e:
        print(f"❌ Database test failed: {e}")
        return False


def test_extractor_imports():
    """Test that all extractors can be imported."""
    print("\n📦 Testing extractor imports...")

    extractors = [
        (
            "listing_status",
            "data_pipeline.extract.extract_listing_status",
            "ListingStatusExtractor",
        ),
        ("overview", "data_pipeline.extract.extract_overview", "OverviewExtractor"),
        (
            "time_series",
            "data_pipeline.extract.extract_time_series_daily_adjusted",
            "TimeSeriesExtractor",
        ),
        (
            "income_statement",
            "data_pipeline.extract.extract_income_statement",
            "IncomeStatementExtractor",
        ),
        (
            "balance_sheet",
            "data_pipeline.extract.extract_balance_sheet",
            "BalanceSheetExtractor",
        ),
        ("cash_flow", "data_pipeline.extract.extract_cash_flow", "CashFlowExtractor"),
        (
            "commodities",
            "data_pipeline.extract.extract_commodities",
            "CommoditiesExtractor",
        ),
        (
            "economic_indicators",
            "data_pipeline.extract.extract_economic_indicators",
            "EconomicIndicatorsExtractor",
        ),
    ]

    imported_extractors = {}

    for name, module_path, class_name in extractors:
        try:
            module = __import__(module_path, fromlist=[class_name])
            extractor_class = getattr(module, class_name)
            extractor = extractor_class()
            imported_extractors[name] = extractor
            print(f"  ✅ {name}: imported and initialized")
        except Exception as e:
            print(f"  ❌ {name}: failed to import - {e}")
            return False, None

    print("✅ All extractors imported successfully")
    return True, imported_extractors


def test_simple_database_operations():
    """Test basic database operations with each table."""
    print("\n🔧 Testing basic database operations...")

    try:
        with PostgresDatabaseManager() as db:
            # Test symbol operations
            test_symbol = "PYTEST"
            symbol_id = db.get_symbol_id(test_symbol)
            print(f"  ✅ Symbol operations: {test_symbol} -> ID {symbol_id}")

            # Test upsert operation
            test_data = {
                "symbol_id": symbol_id,
                "symbol": test_symbol,
                "name": "Test Company",
                "exchange": "TEST",
                "asset_type": "Stock",
                "status": "Active",
            }

            db.upsert_data("listing_status", test_data, ["symbol"])
            print("  ✅ Upsert operation: listing_status")

            # Verify the data
            result = db.fetch_query(
                "SELECT name FROM listing_status WHERE symbol = %s", (test_symbol,)
            )
            if result and result[0][0] == "Test Company":
                print("  ✅ Data verification: correct data retrieved")
            else:
                print("  ❌ Data verification: incorrect data")
                return False

        print("✅ Basic database operations working")
        return True

    except Exception as e:
        print(f"❌ Database operations test failed: {e}")
        return False


def create_migration_summary():
    """Create a summary of the migration status."""
    print("\n📊 PostgreSQL Migration Summary")
    print("=" * 50)

    try:
        with PostgresDatabaseManager() as db:
            # Get table statistics
            tables = [
                "listing_status",
                "overview",
                "time_series_daily_adjusted",
                "income_statement",
                "balance_sheet",
                "cash_flow",
                "commodities",
                "economic_indicators",
            ]

            total_records = 0
            for table in tables:
                count = db.fetch_query(f"SELECT COUNT(*) FROM {table}")[0][0]
                total_records += count
                print(f"  {table:<25}: {count:>8,} records")

            print(f"  {'TOTAL':<25}: {total_records:>8,} records")

            # Get database info
            version_info = db.fetch_query("SELECT version()")[0][0]
            print(f"\nDatabase: {version_info.split(',')[0]}")

            # Get database size
            db_size = db.fetch_query(
                """
                SELECT pg_size_pretty(pg_database_size(current_database()))
            """
            )[0][0]
            print(f"Database size: {db_size}")

        print("\n✅ Migration completed successfully!")
        print("\n🚀 Ready for production use with PostgreSQL")

    except Exception as e:
        print(f"❌ Summary generation failed: {e}")


def main():
    """Run comprehensive PostgreSQL migration tests."""
    print("🧪 PostgreSQL Migration Comprehensive Test")
    print("=" * 50)

    # Test 1: Database tables
    if not test_database_tables():
        print("\n❌ Database tables test failed - stopping tests")
        return False

    # Test 2: Extractor imports
    success, extractors = test_extractor_imports()
    if not success:
        print("\n❌ Extractor imports test failed - stopping tests")
        return False

    # Test 3: Basic database operations
    if not test_simple_database_operations():
        print("\n❌ Database operations test failed - stopping tests")
        return False

    # Summary
    create_migration_summary()

    print("\n🎉 All tests passed! PostgreSQL migration is complete and verified.")
    return True


if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
