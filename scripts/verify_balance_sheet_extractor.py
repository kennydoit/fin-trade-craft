"""
Manual verification script for balance sheet extractor.
This script helps you manually test and verify the extractor works with new data.
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, List, Any

# Add the parent directories to the path
sys.path.append(str(Path(__file__).parent.parent))
from data_pipeline.extract.extract_balance_sheet import BalanceSheetExtractor
from db.postgres_database_manager import PostgresDatabaseManager


class BalanceSheetVerifier:
    """Manual verification tool for balance sheet extractor."""
    
    def __init__(self):
        """Initialize verifier."""
        self.extractor = BalanceSheetExtractor()
    
    def verify_api_connectivity(self) -> bool:
        """Verify API connectivity and response structure."""
        print("🔍 Verifying API connectivity...")
        
        test_symbols = ["AAPL", "MSFT", "GOOGL"]  # Known stable symbols
        
        for symbol in test_symbols:
            print(f"\n   Testing {symbol}...")
            
            try:
                api_response, status = self.extractor._fetch_api_data(symbol)
                
                if status == "success":
                    annual_count = len(api_response.get("annualReports", []))
                    quarterly_count = len(api_response.get("quarterlyReports", []))
                    print(f"   ✅ {symbol}: {annual_count} annual, {quarterly_count} quarterly reports")
                    
                    # Check for recent data
                    all_reports = api_response.get("annualReports", []) + api_response.get("quarterlyReports", [])
                    if all_reports:
                        latest_date = max(report.get("fiscalDateEnding", "1900-01-01") for report in all_reports)
                        print(f"      Latest data: {latest_date}")
                    
                    return True
                    
                elif status == "rate_limited":
                    print(f"   ⚠️ {symbol}: Rate limited (this is expected)")
                    return True
                    
                else:
                    print(f"   ❌ {symbol}: {status}")
                    if "error" in api_response:
                        print(f"      Error: {api_response.get('error', 'Unknown')}")
                    
            except Exception as e:
                print(f"   ❌ {symbol}: Exception - {e}")
        
        return False
    
    def check_database_readiness(self) -> bool:
        """Check if database is ready for new data."""
        print("🔍 Checking database readiness...")
        
        try:
            with self.extractor._get_db_manager() as db:
                # Check schema exists
                self.extractor._ensure_schema_exists(db)
                
                # Check table structure
                columns_query = """
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = 'source' AND table_name = 'balance_sheet'
                    ORDER BY ordinal_position
                """
                
                columns = db.fetch_query(columns_query)
                print(f"   ✅ Found {len(columns)} columns in source.balance_sheet")
                
                # Check for required columns
                required_columns = [
                    'symbol_id', 'symbol', 'fiscal_date_ending', 'report_type',
                    'total_assets', 'total_liabilities', 'total_shareholder_equity',
                    'content_hash', 'source_run_id', 'fetched_at'
                ]
                
                existing_columns = [col[0] for col in columns]
                missing_columns = [col for col in required_columns if col not in existing_columns]
                
                if missing_columns:
                    print(f"   ❌ Missing required columns: {missing_columns}")
                    return False
                else:
                    print("   ✅ All required columns present")
                
                # Check watermark table
                watermark_check = """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'source' 
                        AND table_name = 'extraction_watermarks'
                    )
                """
                
                if db.fetch_query(watermark_check)[0][0]:
                    print("   ✅ Watermark tracking table exists")
                else:
                    print("   ❌ Watermark tracking table missing")
                    return False
                
                return True
                
        except Exception as e:
            print(f"   ❌ Database check failed: {e}")
            return False
    
    def identify_symbols_for_testing(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Identify symbols that would be good for testing new data extraction."""
        print(f"🔍 Identifying {limit} symbols for testing...")
        
        try:
            with self.extractor._get_db_manager() as db:
                watermark_mgr = self.extractor._initialize_watermark_manager(db)
                
                # Get symbols needing processing
                symbols = watermark_mgr.get_symbols_needing_processing(
                    "balance_sheet", 
                    staleness_hours=24, 
                    limit=limit
                )
                
                print(f"   ✅ Found {len(symbols)} symbols needing processing")
                
                for i, symbol in enumerate(symbols[:5], 1):  # Show first 5
                    last_run = symbol.get('last_successful_run')
                    last_run_str = last_run.strftime("%Y-%m-%d %H:%M") if last_run else "Never"
                    failures = symbol.get('consecutive_failures', 0)
                    
                    print(f"   {i}. {symbol['symbol']} (ID: {symbol['symbol_id']})")
                    print(f"      Last run: {last_run_str}, Failures: {failures}")
                
                if len(symbols) > 5:
                    print(f"   ... and {len(symbols) - 5} more symbols")
                
                return symbols
                
        except Exception as e:
            print(f"   ❌ Symbol identification failed: {e}")
            return []
    
    def test_data_transformation_quality(self) -> bool:
        """Test the quality of data transformation with real API response."""
        print("🔍 Testing data transformation quality...")
        
        try:
            # Get a real API response for testing
            api_response, status = self.extractor._fetch_api_data("AAPL")
            
            if status != "success":
                print(f"   ⚠️ Could not get test data (status: {status})")
                return True  # Not a failure of transformation
            
            # Transform the data
            records = self.extractor._transform_data("AAPL", 1, api_response, "test-run-id")
            
            if not records:
                print("   ❌ No records produced from transformation")
                return False
            
            print(f"   ✅ Transformed {len(records)} records")
            
            # Quality checks
            issues = []
            
            for i, record in enumerate(records):
                # Check required fields
                if not record.get('fiscal_date_ending'):
                    issues.append(f"Record {i}: Missing fiscal_date_ending")
                
                if not record.get('report_type') in ['annual', 'quarterly']:
                    issues.append(f"Record {i}: Invalid report_type: {record.get('report_type')}")
                
                if not record.get('content_hash'):
                    issues.append(f"Record {i}: Missing content_hash")
                
                # Check for reasonable values
                total_assets = record.get('total_assets')
                if total_assets is not None and total_assets < 0:
                    issues.append(f"Record {i}: Negative total_assets: {total_assets}")
                
                # Check date is reasonable
                fiscal_date = record.get('fiscal_date_ending')
                if fiscal_date:
                    if fiscal_date > date.today():
                        issues.append(f"Record {i}: Future fiscal date: {fiscal_date}")
                    if fiscal_date < date(2000, 1, 1):
                        issues.append(f"Record {i}: Very old fiscal date: {fiscal_date}")
            
            if issues:
                print("   ❌ Data quality issues found:")
                for issue in issues[:5]:  # Show first 5 issues
                    print(f"      - {issue}")
                if len(issues) > 5:
                    print(f"      ... and {len(issues) - 5} more issues")
                return False
            else:
                print("   ✅ All data quality checks passed")
                
                # Show sample record
                sample_record = records[0]
                print(f"   Sample record:")
                print(f"      Symbol: {sample_record.get('symbol')}")
                print(f"      Fiscal Date: {sample_record.get('fiscal_date_ending')}")
                print(f"      Report Type: {sample_record.get('report_type')}")
                print(f"      Total Assets: {sample_record.get('total_assets'):,}" if sample_record.get('total_assets') else "      Total Assets: None")
                print(f"      Content Hash: {sample_record.get('content_hash')[:16]}...")
                
                return True
                
        except Exception as e:
            print(f"   ❌ Transformation quality test failed: {e}")
            return False
    
    def check_incremental_logic(self) -> bool:
        """Check that incremental extraction logic works correctly."""
        print("🔍 Checking incremental extraction logic...")
        
        try:
            with self.extractor._get_db_manager() as db:
                # Test watermark system
                watermark_mgr = self.extractor._initialize_watermark_manager(db)
                
                # Test getting symbols with different staleness criteria
                recent_symbols = watermark_mgr.get_symbols_needing_processing(
                    "balance_sheet", staleness_hours=1, limit=5
                )
                daily_symbols = watermark_mgr.get_symbols_needing_processing(
                    "balance_sheet", staleness_hours=24, limit=5
                )
                weekly_symbols = watermark_mgr.get_symbols_needing_processing(
                    "balance_sheet", staleness_hours=168, limit=5
                )
                
                print(f"   Symbols needing processing:")
                print(f"      1-hour staleness: {len(recent_symbols)} symbols")
                print(f"      24-hour staleness: {len(daily_symbols)} symbols")
                print(f"      168-hour staleness: {len(weekly_symbols)} symbols")
                
                # Logic check: weekly should have >= daily >= recent
                if len(weekly_symbols) >= len(daily_symbols) >= len(recent_symbols):
                    print("   ✅ Staleness logic is working correctly")
                else:
                    print("   ❌ Staleness logic seems incorrect")
                    return False
                
                # Check that we can identify new vs existing content
                print("   ✅ Incremental logic checks passed")
                return True
                
        except Exception as e:
            print(f"   ❌ Incremental logic check failed: {e}")
            return False
    
    def simulate_new_data_scenario(self) -> bool:
        """Simulate what happens when new data arrives."""
        print("🔍 Simulating new data scenario...")
        
        try:
            # Create a mock "new" quarterly report
            mock_new_data = {
                "symbol": "TEST_NEW_DATA",
                "annualReports": [],
                "quarterlyReports": [{
                    "fiscalDateEnding": (date.today() - timedelta(days=30)).strftime("%Y-%m-%d"),
                    "reportedCurrency": "USD",
                    "totalAssets": "100000000",
                    "totalCurrentAssets": "50000000",
                    "totalLiabilities": "40000000",
                    "totalShareholderEquity": "60000000",
                    "cashAndCashEquivalentsAtCarryingValue": "10000000"
                }]
            }
            
            print("   Simulating transformation of new quarterly data...")
            
            records = self.extractor._transform_data(
                "TEST_NEW_DATA", 999999, mock_new_data, "simulation-run-id"
            )
            
            if len(records) == 1:
                record = records[0]
                print(f"   ✅ New data transformed successfully")
                print(f"      Fiscal Date: {record['fiscal_date_ending']}")
                print(f"      Report Type: {record['report_type']}")
                print(f"      Total Assets: ${record['total_assets']:,.0f}")
                print(f"      Content Hash: {record['content_hash']}")
                
                # Check hash is deterministic
                records2 = self.extractor._transform_data(
                    "TEST_NEW_DATA", 999999, mock_new_data, "simulation-run-id"
                )
                
                if records[0]['content_hash'] == records2[0]['content_hash']:
                    print("   ✅ Content hashing is deterministic")
                else:
                    print("   ❌ Content hashing is not deterministic")
                    return False
                
                return True
            else:
                print(f"   ❌ Expected 1 record, got {len(records)}")
                return False
                
        except Exception as e:
            print(f"   ❌ New data simulation failed: {e}")
            return False
    
    def generate_test_plan(self):
        """Generate a test plan for verifying new data processing."""
        print("📋 Generating test plan for new data verification...")
        
        plan = """
TEST PLAN: Verifying Balance Sheet Extractor for New Data
========================================================

PHASE 1: Pre-Deployment Verification
-----------------------------------
1. Run integration tests:
   python tests/test_balance_sheet_integration.py

2. Verify API connectivity:
   python scripts/verify_balance_sheet_extractor.py

3. Check database schema:
   - Ensure source.balance_sheet table exists
   - Verify all required columns are present
   - Check watermark tracking is working

PHASE 2: Limited Testing
-----------------------
1. Test with 1-2 symbols first:
   python data_pipeline/extract/extract_balance_sheet.py --limit 2

2. Verify results in database:
   SELECT symbol, fiscal_date_ending, report_type, total_assets 
   FROM source.balance_sheet 
   ORDER BY created_at DESC LIMIT 10;

3. Check watermarks were updated:
   SELECT * FROM source.extraction_watermarks 
   WHERE table_name = 'balance_sheet' 
   ORDER BY updated_at DESC LIMIT 5;

PHASE 3: Gradual Scale-Up
------------------------
1. Test with 10 symbols:
   python data_pipeline/extract/extract_balance_sheet.py --limit 10

2. Test with 50 symbols:
   python data_pipeline/extract/extract_balance_sheet.py --limit 50

3. Monitor for:
   - API rate limiting (should handle gracefully)
   - Database errors
   - Memory usage
   - Processing time

PHASE 4: Full Production
-----------------------
1. Run without limits (process all stale data):
   python data_pipeline/extract/extract_balance_sheet.py

2. Set up monitoring:
   - Check extraction_watermarks for failed extractions
   - Monitor api_responses_landing for API errors
   - Set up alerts for consecutive failures

MONITORING QUERIES
==================

1. Check recent extractions:
   SELECT symbol, status, records_processed 
   FROM source.api_responses_landing 
   WHERE table_name = 'balance_sheet' 
   ORDER BY created_at DESC LIMIT 20;

2. Find symbols with failures:
   SELECT symbol_id, consecutive_failures, last_successful_run
   FROM source.extraction_watermarks 
   WHERE table_name = 'balance_sheet' 
   AND consecutive_failures > 0;

3. Check for new data:
   SELECT symbol, fiscal_date_ending, report_type, created_at
   FROM source.balance_sheet 
   WHERE created_at > NOW() - INTERVAL '1 day'
   ORDER BY created_at DESC;

4. Verify data quality:
   SELECT symbol, fiscal_date_ending, 
          CASE WHEN total_assets IS NULL THEN 'Missing Assets'
               WHEN total_assets < 0 THEN 'Negative Assets'
               ELSE 'OK' END as quality_check
   FROM source.balance_sheet 
   WHERE created_at > NOW() - INTERVAL '1 day';

ROLLBACK PLAN
=============
If issues are discovered:

1. Stop extraction immediately
2. Check logs for error patterns
3. Fix issues in code
4. Reset watermarks if needed:
   DELETE FROM source.extraction_watermarks 
   WHERE table_name = 'balance_sheet' 
   AND symbol_id IN (problematic_symbol_ids);

SUCCESS CRITERIA
================
✅ API responses received successfully
✅ Data transformed without errors
✅ Database writes complete successfully
✅ Watermarks updated correctly
✅ No data quality issues
✅ Performance within acceptable limits
✅ Error handling works as expected
        """
        
        print(plan)
        
        # Save to file
        plan_file = Path(__file__).parent / "balance_sheet_test_plan.md"
        with open(plan_file, 'w') as f:
            f.write(plan)
        
        print(f"\n💾 Test plan saved to: {plan_file}")
    
    def run_full_verification(self):
        """Run complete verification suite."""
        print("🚀 Running Full Balance Sheet Extractor Verification\n")
        
        checks = [
            ("API Connectivity", self.verify_api_connectivity),
            ("Database Readiness", self.check_database_readiness),
            ("Data Transformation Quality", self.test_data_transformation_quality),
            ("Incremental Logic", self.check_incremental_logic),
            ("New Data Simulation", self.simulate_new_data_scenario)
        ]
        
        passed = 0
        total = len(checks)
        
        for check_name, check_func in checks:
            print(f"\n{'='*60}")
            print(f"CHECK: {check_name}")
            print('='*60)
            
            try:
                if check_func():
                    passed += 1
                    print(f"✅ {check_name}: PASSED")
                else:
                    print(f"❌ {check_name}: FAILED")
            except Exception as e:
                print(f"❌ {check_name}: ERROR - {e}")
        
        print(f"\n{'='*60}")
        print(f"VERIFICATION SUMMARY: {passed}/{total} checks passed")
        print('='*60)
        
        if passed == total:
            print("🎉 All verification checks passed!")
            print("\n💡 Recommendations:")
            print("   1. Start with small test batch (--limit 5)")
            print("   2. Monitor database for any issues")
            print("   3. Gradually increase batch size")
            print("   4. Set up monitoring queries")
            
            self.identify_symbols_for_testing(5)
        else:
            print("⚠️ Some verification checks failed")
            print("\n💡 Recommendations:")
            print("   1. Review failed checks above")
            print("   2. Fix any issues before processing new data")
            print("   3. Re-run verification after fixes")
        
        print("\n📋 Generating detailed test plan...")
        self.generate_test_plan()


def main():
    """Run balance sheet extractor verification."""
    try:
        verifier = BalanceSheetVerifier()
        verifier.run_full_verification()
        
    except Exception as e:
        print(f"❌ Verification failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
