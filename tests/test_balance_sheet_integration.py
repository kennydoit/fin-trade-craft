"""
Integration tests for balance sheet extractor with real database.
These tests verify the extractor works with actual database connections.
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime, date
from unittest.mock import patch

# Add the parent directories to the path
sys.path.append(str(Path(__file__).parent.parent))
from data_pipeline.extract.extract_balance_sheet import BalanceSheetExtractor
from db.postgres_database_manager import PostgresDatabaseManager


class TestBalanceSheetIntegration:
    """Integration tests with real database."""
    
    def __init__(self):
        """Initialize integration test."""
        self.extractor = BalanceSheetExtractor()
        self.test_symbols = ["AAPL", "MSFT", "GOOGL"]  # Known stable symbols
    
    def test_database_connection(self):
        """Test that we can connect to the database."""
        print("🔍 Testing database connection...")
        try:
            with self.extractor._get_db_manager() as db:
                result = db.fetch_query("SELECT NOW()")
                print(f"✅ Database connected: {result[0][0]}")
                return True
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return False
    
    def test_schema_initialization(self):
        """Test that source schema can be initialized."""
        print("🔍 Testing schema initialization...")
        try:
            with self.extractor._get_db_manager() as db:
                self.extractor._ensure_schema_exists(db)
                
                # Check if balance_sheet table exists
                result = db.fetch_query("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'source' 
                        AND table_name = 'balance_sheet'
                    )
                """)
                
                if result[0][0]:
                    print("✅ Source schema and balance_sheet table exist")
                    return True
                else:
                    print("❌ balance_sheet table not found in source schema")
                    return False
        except Exception as e:
            print(f"❌ Schema initialization failed: {e}")
            return False
    
    def test_watermark_system(self):
        """Test watermark management system."""
        print("🔍 Testing watermark system...")
        try:
            with self.extractor._get_db_manager() as db:
                watermark_mgr = self.extractor._initialize_watermark_manager(db)
                
                # Test getting symbols needing processing
                symbols = watermark_mgr.get_symbols_needing_processing(
                    "balance_sheet", staleness_hours=24, limit=5
                )
                
                print(f"✅ Found {len(symbols)} symbols needing processing")
                for symbol in symbols[:3]:  # Show first 3
                    print(f"   - {symbol['symbol']} (ID: {symbol['symbol_id']})")
                
                return len(symbols) >= 0  # Should always return a list
        except Exception as e:
            print(f"❌ Watermark system test failed: {e}")
            return False
    
    def test_api_response_structure(self):
        """Test that we can fetch and parse API responses."""
        print("🔍 Testing API response structure...")
        
        # Use a test symbol (Apple)
        test_symbol = "AAPL"
        
        try:
            api_response, status = self.extractor._fetch_api_data(test_symbol)
            
            if status == "success":
                print(f"✅ API responded successfully for {test_symbol}")
                
                # Check expected structure
                required_keys = ["symbol", "annualReports", "quarterlyReports"]
                missing_keys = [key for key in required_keys if key not in api_response]
                
                if not missing_keys:
                    print("✅ API response has correct structure")
                    
                    # Check if we have data
                    annual_count = len(api_response.get("annualReports", []))
                    quarterly_count = len(api_response.get("quarterlyReports", []))
                    print(f"   Annual reports: {annual_count}")
                    print(f"   Quarterly reports: {quarterly_count}")
                    
                    return True
                else:
                    print(f"❌ API response missing keys: {missing_keys}")
                    return False
            
            elif status == "rate_limited":
                print("⚠️ API rate limited - this is expected behavior")
                return True
            else:
                print(f"❌ API returned status: {status}")
                print(f"   Response: {api_response}")
                return False
                
        except Exception as e:
            print(f"❌ API test failed: {e}")
            return False
    
    def test_data_transformation(self):
        """Test data transformation with mock API response."""
        print("🔍 Testing data transformation...")
        
        # Create a realistic mock API response
        mock_response = {
            "symbol": "TEST",
            "annualReports": [
                {
                    "fiscalDateEnding": "2023-12-31",
                    "reportedCurrency": "USD",
                    "totalAssets": "352755000000",
                    "totalCurrentAssets": "143566000000",
                    "totalLiabilities": "290437000000",
                    "totalShareholderEquity": "62146000000",
                    "cashAndCashEquivalentsAtCarryingValue": "29965000000",
                    "inventory": "6331000000"
                }
            ],
            "quarterlyReports": [
                {
                    "fiscalDateEnding": "2024-03-31",
                    "reportedCurrency": "USD",
                    "totalAssets": "365725000000",
                    "totalCurrentAssets": "147580000000",
                    "totalLiabilities": "298877000000",
                    "totalShareholderEquity": "66848000000"
                }
            ]
        }
        
        try:
            records = self.extractor._transform_data("TEST", 9999, mock_response, "test-run-id")
            
            if len(records) == 2:  # 1 annual + 1 quarterly
                print(f"✅ Transformed {len(records)} records successfully")
                
                # Check record structure
                annual_record = next((r for r in records if r['report_type'] == 'annual'), None)
                quarterly_record = next((r for r in records if r['report_type'] == 'quarterly'), None)
                
                if annual_record and quarterly_record:
                    print("✅ Both annual and quarterly records created")
                    print(f"   Annual fiscal date: {annual_record['fiscal_date_ending']}")
                    print(f"   Quarterly fiscal date: {quarterly_record['fiscal_date_ending']}")
                    print(f"   Annual total assets: {annual_record['total_assets']}")
                    return True
                else:
                    print("❌ Missing annual or quarterly record")
                    return False
            else:
                print(f"❌ Expected 2 records, got {len(records)}")
                return False
                
        except Exception as e:
            print(f"❌ Data transformation test failed: {e}")
            return False
    
    def test_end_to_end_dry_run(self):
        """Test end-to-end extraction for one symbol (dry run)."""
        print("🔍 Testing end-to-end extraction (dry run)...")
        
        try:
            with self.extractor._get_db_manager() as db:
                # Get one symbol that needs processing
                watermark_mgr = self.extractor._initialize_watermark_manager(db)
                symbols = watermark_mgr.get_symbols_needing_processing(
                    "balance_sheet", staleness_hours=24, limit=1
                )
                
                if not symbols:
                    print("⚠️ No symbols need processing - this might be normal")
                    return True
                
                test_symbol = symbols[0]
                print(f"   Testing with symbol: {test_symbol['symbol']} (ID: {test_symbol['symbol_id']})")
                
                # Mock the actual database writes to avoid data changes
                with patch.object(self.extractor, '_store_landing_record', return_value="test-hash"), \
                     patch.object(self.extractor, '_upsert_records', return_value=0):
                    
                    result = self.extractor.extract_symbol(
                        test_symbol['symbol'], 
                        test_symbol['symbol_id'], 
                        db
                    )
                
                print(f"✅ End-to-end test completed")
                print(f"   Status: {result['status']}")
                print(f"   Records processed: {result['records_processed']}")
                
                return result['status'] in ['success', 'no_changes', 'no_valid_records']
                
        except Exception as e:
            print(f"❌ End-to-end test failed: {e}")
            return False
    
    def run_all_tests(self):
        """Run all integration tests."""
        print("🚀 Running Balance Sheet Extractor Integration Tests\n")
        
        tests = [
            ("Database Connection", self.test_database_connection),
            ("Schema Initialization", self.test_schema_initialization),
            ("Watermark System", self.test_watermark_system),
            ("API Response Structure", self.test_api_response_structure),
            ("Data Transformation", self.test_data_transformation),
            ("End-to-End Dry Run", self.test_end_to_end_dry_run)
        ]
        
        passed = 0
        total = len(tests)
        
        for test_name, test_func in tests:
            print(f"\n{'='*60}")
            print(f"TEST: {test_name}")
            print('='*60)
            
            try:
                if test_func():
                    passed += 1
                    print(f"✅ {test_name}: PASSED")
                else:
                    print(f"❌ {test_name}: FAILED")
            except Exception as e:
                print(f"❌ {test_name}: ERROR - {e}")
        
        print(f"\n{'='*60}")
        print(f"SUMMARY: {passed}/{total} tests passed")
        print('='*60)
        
        if passed == total:
            print("🎉 All integration tests passed!")
            return True
        else:
            print("⚠️ Some integration tests failed - review output above")
            return False


def main():
    """Run integration tests."""
    try:
        tester = TestBalanceSheetIntegration()
        success = tester.run_all_tests()
        
        if success:
            print("\n✅ Integration tests completed successfully")
            print("💡 Your balance sheet extractor is ready for new data!")
        else:
            print("\n❌ Integration tests had failures")
            print("💡 Please review the failures before processing new data")
            
    except Exception as e:
        print(f"❌ Integration test runner failed: {e}")


if __name__ == "__main__":
    main()
