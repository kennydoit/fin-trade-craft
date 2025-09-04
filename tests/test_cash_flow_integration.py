"""
Integration tests for cash flow extractor with real database connections.
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from data_pipeline.extract.extract_cash_flow import CashFlowExtractor
from db.postgres_database_manager import PostgresDatabaseManager


def test_database_connectivity():
    """Test database connection and basic queries."""
    print("🧪 Testing database connectivity...")
    
    try:
        with PostgresDatabaseManager() as db:
            # Test basic connectivity
            result = db.fetch_query("SELECT NOW()")
            assert result is not None
            print(f"✅ Database connected successfully at {result[0][0]}")
            
            # Test source schema exists
            schema_check = db.fetch_query("""
                SELECT schema_name FROM information_schema.schemata 
                WHERE schema_name = 'source'
            """)
            assert len(schema_check) > 0
            print("✅ Source schema exists")
            
            # Test cash_flow table exists
            table_check = db.fetch_query("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'source' AND table_name = 'cash_flow'
            """)
            assert len(table_check) > 0
            print("✅ Cash flow table exists")
            
    except Exception as e:
        print(f"❌ Database connectivity test failed: {e}")
        raise


def test_extractor_initialization():
    """Test extractor can be initialized properly."""
    print("🧪 Testing extractor initialization...")
    
    try:
        extractor = CashFlowExtractor()
        assert extractor.api_key is not None
        assert extractor.table_name == "cash_flow"
        assert extractor.schema_name == "source"
        print("✅ Extractor initialized successfully")
        
    except Exception as e:
        print(f"❌ Extractor initialization failed: {e}")
        raise


def test_schema_initialization():
    """Test schema initialization works."""
    print("🧪 Testing schema initialization...")
    
    try:
        extractor = CashFlowExtractor()
        
        with extractor._get_db_manager() as db:
            extractor._ensure_schema_exists(db)
            
            # Verify tables exist after initialization
            tables_check = db.fetch_query("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'source' 
                AND table_name IN ('cash_flow', 'api_responses_landing', 'extraction_watermarks')
                ORDER BY table_name
            """)
            
            table_names = [row[0] for row in tables_check]
            expected_tables = ['api_responses_landing', 'cash_flow', 'extraction_watermarks']
            
            for table in expected_tables:
                assert table in table_names, f"Missing table: {table}"
            
            print("✅ Schema initialization successful")
            
    except Exception as e:
        print(f"❌ Schema initialization test failed: {e}")
        raise


def test_watermark_system():
    """Test watermark system integration."""
    print("🧪 Testing watermark system...")
    
    try:
        extractor = CashFlowExtractor()
        
        with extractor._get_db_manager() as db:
            watermark_mgr = extractor._initialize_watermark_manager(db)
            
            # Test getting symbols needing processing
            symbols = watermark_mgr.get_symbols_needing_processing(
                "cash_flow", 
                staleness_hours=24, 
                limit=5
            )
            
            print(f"✅ Found {len(symbols)} symbols needing processing")
            
            if symbols:
                symbol_example = symbols[0]
                required_keys = ['symbol_id', 'symbol']
                for key in required_keys:
                    assert key in symbol_example, f"Missing key in symbol data: {key}"
                print(f"✅ Symbol data structure valid: {symbol_example}")
            
    except Exception as e:
        print(f"❌ Watermark system test failed: {e}")
        raise


def test_table_structure():
    """Test cash flow table has expected structure."""
    print("🧪 Testing table structure...")
    
    try:
        with PostgresDatabaseManager() as db:
            # Get column information
            columns_info = db.fetch_query("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_schema = 'source' AND table_name = 'cash_flow'
                ORDER BY ordinal_position
            """)
            
            columns = {row[0]: {'type': row[1], 'nullable': row[2]} for row in columns_info}
            
            # Check required columns exist
            required_columns = [
                'cash_flow_id', 'symbol_id', 'symbol', 'fiscal_date_ending',
                'report_type', 'operating_cashflow', 'capital_expenditures',
                'cashflow_from_investment', 'cashflow_from_financing',
                'content_hash', 'source_run_id', 'created_at', 'updated_at'
            ]
            
            for column in required_columns:
                assert column in columns, f"Missing column: {column}"
            
            print(f"✅ Table structure valid with {len(columns)} columns")
            
            # Check primary key and constraints
            constraints = db.fetch_query("""
                SELECT constraint_name, constraint_type
                FROM information_schema.table_constraints 
                WHERE table_schema = 'source' AND table_name = 'cash_flow'
            """)
            
            constraint_types = [row[1] for row in constraints]
            assert 'PRIMARY KEY' in constraint_types
            assert 'UNIQUE' in constraint_types  # Should have unique constraint
            
            print("✅ Table constraints valid")
            
    except Exception as e:
        print(f"❌ Table structure test failed: {e}")
        raise


def test_content_hashing():
    """Test content hashing functionality."""
    print("🧪 Testing content hashing...")
    
    try:
        extractor = CashFlowExtractor()
        
        # Test data transformation and hashing
        mock_api_response = {
            "quarterlyReports": [
                {
                    "fiscalDateEnding": "2024-06-30",
                    "reportedCurrency": "USD",
                    "operatingCashflow": "26000000000",
                    "capitalExpenditures": "-2100000000"
                }
            ]
        }
        
        records = extractor._transform_data("TEST", 9999, mock_api_response, "test_run")
        
        assert len(records) == 1
        record = records[0]
        
        assert record["content_hash"] is not None
        assert len(record["content_hash"]) > 0
        print(f"✅ Content hash generated: {record['content_hash'][:16]}...")
        
        # Test same data produces same hash
        records2 = extractor._transform_data("TEST", 9999, mock_api_response, "test_run_2")
        assert records[0]["content_hash"] == records2[0]["content_hash"]
        print("✅ Content hashing is deterministic")
        
    except Exception as e:
        print(f"❌ Content hashing test failed: {e}")
        raise


def test_data_quality_validation():
    """Test data quality and validation."""
    print("🧪 Testing data quality validation...")
    
    try:
        with PostgresDatabaseManager() as db:
            # Check for any existing cash flow data
            count_query = "SELECT COUNT(*) FROM source.cash_flow"
            result = db.fetch_query(count_query)
            total_records = result[0][0] if result else 0
            
            print(f"✅ Found {total_records} existing cash flow records")
            
            if total_records > 0:
                # Test data quality metrics
                quality_check = db.fetch_query("""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(DISTINCT symbol_id) as unique_symbols,
                        COUNT(DISTINCT fiscal_date_ending) as unique_dates,
                        SUM(CASE WHEN operating_cashflow IS NOT NULL THEN 1 ELSE 0 END) as has_operating_cashflow,
                        SUM(CASE WHEN content_hash IS NOT NULL THEN 1 ELSE 0 END) as has_content_hash,
                        MIN(fiscal_date_ending) as earliest_date,
                        MAX(fiscal_date_ending) as latest_date
                    FROM source.cash_flow
                """)
                
                if quality_check:
                    stats = quality_check[0]
                    print(f"✅ Data quality metrics:")
                    print(f"   Total records: {stats[0]:,}")
                    print(f"   Unique symbols: {stats[1]:,}")
                    print(f"   Unique dates: {stats[2]:,}")
                    print(f"   Has operating cashflow: {stats[3]:,} ({stats[3]/stats[0]*100:.1f}%)")
                    print(f"   Has content hash: {stats[4]:,} ({stats[4]/stats[0]*100:.1f}%)")
                    print(f"   Date range: {stats[5]} to {stats[6]}")
                    
                    # Basic quality assertions
                    assert stats[4] == stats[0], "All records should have content hash"
                    assert stats[5] is not None, "Should have earliest date"
                    assert stats[6] is not None, "Should have latest date"
            
    except Exception as e:
        print(f"❌ Data quality validation failed: {e}")
        raise


def run_integration_tests():
    """Run all integration tests."""
    print("🚀 Starting Cash Flow Extractor Integration Tests\n")
    
    tests = [
        test_database_connectivity,
        test_extractor_initialization,
        test_schema_initialization,
        test_watermark_system,
        test_table_structure,
        test_content_hashing,
        test_data_quality_validation
    ]
    
    passed = 0
    failed = 0
    
    for test_func in tests:
        try:
            test_func()
            passed += 1
            print(f"✅ {test_func.__name__} PASSED\n")
        except Exception as e:
            failed += 1
            print(f"❌ {test_func.__name__} FAILED: {e}\n")
    
    print(f"🎯 Integration Tests Summary:")
    print(f"   Passed: {passed}")
    print(f"   Failed: {failed}")
    print(f"   Total: {len(tests)}")
    
    if failed == 0:
        print("🎉 All integration tests passed!")
    else:
        print(f"⚠️ {failed} integration tests failed")
    
    return failed == 0


if __name__ == "__main__":
    success = run_integration_tests()
    exit(0 if success else 1)
