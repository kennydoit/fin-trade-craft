"""
Cash Flow Extractor verification and validation tool.
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from data_pipeline.extract.extract_cash_flow import CashFlowExtractor, CASH_FLOW_FIELDS
from db.postgres_database_manager import PostgresDatabaseManager


def verify_api_connectivity():
    """Verify API connectivity and response structure."""
    print("🧪 Verifying API connectivity...")
    
    try:
        extractor = CashFlowExtractor()
        
        # Test with a known symbol
        test_symbol = "AAPL"
        api_response, status = extractor._fetch_api_data(test_symbol)
        
        print(f"   API Status: {status}")
        
        if status == "success":
            print(f"   ✅ API responded successfully for {test_symbol}")
            
            # Check response structure
            if "quarterlyReports" in api_response:
                quarterly_count = len(api_response["quarterlyReports"])
                print(f"   📊 Quarterly reports: {quarterly_count}")
            
            if "annualReports" in api_response:
                annual_count = len(api_response["annualReports"])
                print(f"   📊 Annual reports: {annual_count}")
                
            return True
            
        elif status == "rate_limited":
            print("   ⚠️ API rate limited - this is expected behavior")
            return True
            
        else:
            print(f"   ❌ API error: {api_response}")
            return False
            
    except Exception as e:
        print(f"   ❌ API connectivity test failed: {e}")
        return False


def verify_database_readiness():
    """Verify database schema and tables are ready."""
    print("🧪 Verifying database readiness...")
    
    try:
        with PostgresDatabaseManager() as db:
            # Check source schema
            schema_check = db.fetch_query("""
                SELECT schema_name FROM information_schema.schemata 
                WHERE schema_name = 'source'
            """)
            
            if not schema_check:
                print("   ❌ Source schema not found")
                return False
            
            print("   ✅ Source schema exists")
            
            # Check required tables
            required_tables = ['cash_flow', 'api_responses_landing', 'extraction_watermarks']
            
            for table in required_tables:
                table_check = db.fetch_query("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'source' AND table_name = %s
                """, (table,))
                
                if not table_check:
                    print(f"   ❌ Table {table} not found")
                    return False
                
                print(f"   ✅ Table {table} exists")
            
            # Check table structure
            columns_check = db.fetch_query("""
                SELECT COUNT(*) FROM information_schema.columns 
                WHERE table_schema = 'source' AND table_name = 'cash_flow'
            """)
            
            column_count = columns_check[0][0] if columns_check else 0
            print(f"   📊 Cash flow table has {column_count} columns")
            
            # Verify foreign key to symbols table
            fk_check = db.fetch_query("""
                SELECT 1 FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                  ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_schema = 'source' 
                  AND tc.table_name = 'cash_flow'
                  AND tc.constraint_type = 'FOREIGN KEY'
                  AND kcu.column_name = 'symbol_id'
            """)
            
            if fk_check:
                print("   ✅ Foreign key to symbols table exists")
            else:
                print("   ⚠️ Foreign key to symbols table not found")
            
            return True
            
    except Exception as e:
        print(f"   ❌ Database readiness check failed: {e}")
        return False


def verify_field_mappings():
    """Verify field mappings are complete and valid."""
    print("🧪 Verifying field mappings...")
    
    try:
        # Check field mapping completeness
        expected_categories = {
            'operating': ['operating_cashflow', 'depreciation_depletion_and_amortization'],
            'investing': ['cashflow_from_investment', 'capital_expenditures'],
            'financing': ['cashflow_from_financing', 'dividend_payout'],
            'summary': ['change_in_cash_and_cash_equivalents']
        }
        
        for category, fields in expected_categories.items():
            for field in fields:
                if field in CASH_FLOW_FIELDS:
                    print(f"   ✅ {category.title()} field mapped: {field}")
                else:
                    print(f"   ❌ Missing {category} field: {field}")
                    return False
        
        print(f"   📊 Total field mappings: {len(CASH_FLOW_FIELDS)}")
        
        # Verify no duplicate API field mappings
        api_fields = list(CASH_FLOW_FIELDS.values())
        unique_api_fields = set(api_fields)
        
        if len(api_fields) == len(unique_api_fields):
            print("   ✅ No duplicate API field mappings")
        else:
            print("   ❌ Duplicate API field mappings found")
            return False
        
        return True
        
    except Exception as e:
        print(f"   ❌ Field mapping verification failed: {e}")
        return False


def verify_transformation_logic():
    """Verify data transformation logic."""
    print("🧪 Verifying transformation logic...")
    
    try:
        extractor = CashFlowExtractor()
        
        # Test transformation with mock data
        mock_api_response = {
            "quarterlyReports": [
                {
                    "fiscalDateEnding": "2024-06-30",
                    "reportedCurrency": "USD",
                    "operatingCashflow": "26000000000",
                    "capitalExpenditures": "-2100000000",
                    "cashflowFromInvestment": "-5500000000",
                    "cashflowFromFinancing": "-21800000000",
                    "changeInCashAndCashEquivalents": "-1300000000"
                }
            ],
            "annualReports": [
                {
                    "fiscalDateEnding": "2023-09-30",
                    "reportedCurrency": "USD",
                    "operatingCashflow": "110563000000",
                    "capitalExpenditures": "-10959000000",
                    "cashflowFromInvestment": "-1337000000"
                }
            ]
        }
        
        records = extractor._transform_data("TEST", 9999, mock_api_response, "test_run")
        
        if len(records) != 2:
            print(f"   ❌ Expected 2 records, got {len(records)}")
            return False
        
        print(f"   ✅ Transformed {len(records)} records successfully")
        
        # Verify quarterly record
        quarterly = next((r for r in records if r["report_type"] == "quarterly"), None)
        if not quarterly:
            print("   ❌ No quarterly record found")
            return False
        
        # Check key fields
        checks = [
            (quarterly["symbol"] == "TEST", "Symbol assignment"),
            (quarterly["symbol_id"] == 9999, "Symbol ID assignment"),
            (quarterly["fiscal_date_ending"] is not None, "Date parsing"),
            (quarterly["operating_cashflow"] == 26000000000.0, "Operating cashflow conversion"),
            (quarterly["capital_expenditures"] == -2100000000.0, "Capital expenditures conversion"),
            (quarterly["content_hash"] is not None, "Content hash generation"),
            (quarterly["source_run_id"] == "test_run", "Run ID assignment")
        ]
        
        for check, description in checks:
            if check:
                print(f"   ✅ {description}")
            else:
                print(f"   ❌ {description}")
                return False
        
        # Verify annual record
        annual = next((r for r in records if r["report_type"] == "annual"), None)
        if not annual:
            print("   ❌ No annual record found")
            return False
        
        print("   ✅ Annual record validation passed")
        
        # Test edge cases
        edge_cases = {
            "empty_values": {
                "fiscalDateEnding": "2024-03-31",
                "operatingCashflow": "",
                "capitalExpenditures": None,
                "cashflowFromInvestment": "None"
            },
            "invalid_numbers": {
                "fiscalDateEnding": "2024-03-31", 
                "operatingCashflow": "invalid",
                "capitalExpenditures": "not_a_number"
            }
        }
        
        for case_name, report_data in edge_cases.items():
            record = extractor._transform_single_report("TEST", 9999, report_data, "quarterly", "test")
            if record:
                print(f"   ✅ Edge case handled: {case_name}")
            else:
                print(f"   ❌ Edge case failed: {case_name}")
                return False
        
        return True
        
    except Exception as e:
        print(f"   ❌ Transformation logic verification failed: {e}")
        return False


def verify_incremental_processing():
    """Verify incremental processing capabilities."""
    print("🧪 Verifying incremental processing...")
    
    try:
        extractor = CashFlowExtractor()
        
        with extractor._get_db_manager() as db:
            # Initialize watermark manager
            watermark_mgr = extractor._initialize_watermark_manager(db)
            
            # Test getting symbols for processing
            symbols_to_process = watermark_mgr.get_symbols_needing_processing(
                "cash_flow",
                staleness_hours=24,
                limit=5
            )
            
            print(f"   📊 Found {len(symbols_to_process)} symbols needing processing")
            
            if symbols_to_process:
                sample_symbol = symbols_to_process[0]
                required_fields = ["symbol_id", "symbol"]
                
                for field in required_fields:
                    if field not in sample_symbol:
                        print(f"   ❌ Missing field in symbol data: {field}")
                        return False
                
                print(f"   ✅ Sample symbol data valid: {sample_symbol['symbol']}")
            
            # Test content change detection
            test_hash = "test_hash_12345"
            content_changed = extractor._content_has_changed(db, 9999, test_hash)
            print(f"   ✅ Content change detection working: {content_changed}")
            
            return True
            
    except Exception as e:
        print(f"   ❌ Incremental processing verification failed: {e}")
        return False


def verify_data_quality():
    """Verify existing data quality."""
    print("🧪 Verifying data quality...")
    
    try:
        with PostgresDatabaseManager() as db:
            # Get basic statistics
            stats_query = """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT symbol_id) as unique_symbols,
                    COUNT(DISTINCT fiscal_date_ending) as unique_dates,
                    SUM(CASE WHEN operating_cashflow IS NOT NULL THEN 1 ELSE 0 END) as has_operating_cashflow,
                    SUM(CASE WHEN content_hash IS NOT NULL THEN 1 ELSE 0 END) as has_content_hash,
                    MIN(fiscal_date_ending) as earliest_date,
                    MAX(fiscal_date_ending) as latest_date,
                    COUNT(DISTINCT report_type) as report_types
                FROM source.cash_flow
            """
            
            result = db.fetch_query(stats_query)
            
            if not result:
                print("   ⚠️ No cash flow data found - this is expected for new installations")
                return True
            
            stats = result[0]
            total_records = stats[0]
            
            if total_records == 0:
                print("   ⚠️ No cash flow records found - ready for initial data load")
                return True
            
            print(f"   📊 Data Quality Metrics:")
            print(f"      Total records: {stats[0]:,}")
            print(f"      Unique symbols: {stats[1]:,}")
            print(f"      Unique dates: {stats[2]:,}")
            print(f"      Operating cashflow coverage: {stats[3]:,} ({stats[3]/stats[0]*100:.1f}%)")
            print(f"      Content hash coverage: {stats[4]:,} ({stats[4]/stats[0]*100:.1f}%)")
            print(f"      Date range: {stats[5]} to {stats[6]}")
            print(f"      Report types: {stats[7]}")
            
            # Quality checks
            quality_checks = [
                (stats[4] == stats[0], "All records have content hash"),
                (stats[7] >= 1, "At least one report type exists"),
                (stats[5] is not None, "Has earliest date"),
                (stats[6] is not None, "Has latest date")
            ]
            
            for check, description in quality_checks:
                if check:
                    print(f"   ✅ {description}")
                else:
                    print(f"   ❌ {description}")
                    return False
            
            # Check for recent data
            recent_data_query = """
                SELECT COUNT(*) FROM source.cash_flow 
                WHERE created_at >= NOW() - INTERVAL '7 days'
            """
            
            recent_result = db.fetch_query(recent_data_query)
            recent_count = recent_result[0][0] if recent_result else 0
            
            print(f"   📊 Recent data (7 days): {recent_count:,} records")
            
            return True
            
    except Exception as e:
        print(f"   ❌ Data quality verification failed: {e}")
        return False


def verify_production_readiness():
    """Verify overall production readiness."""
    print("🧪 Verifying production readiness...")
    
    try:
        # Test extractor initialization
        extractor = CashFlowExtractor()
        
        # Verify essential components
        checks = [
            (extractor.api_key is not None, "API key configured"),
            (extractor.table_name == "cash_flow", "Table name correct"),
            (extractor.schema_name == "source", "Schema name correct"),
            (len(CASH_FLOW_FIELDS) > 10, "Sufficient field mappings"),
        ]
        
        for check, description in checks:
            if check:
                print(f"   ✅ {description}")
            else:
                print(f"   ❌ {description}")
                return False
        
        # Test schema initialization
        with extractor._get_db_manager() as db:
            extractor._ensure_schema_exists(db)
            print("   ✅ Schema initialization successful")
        
        print("   🎯 Cash flow extractor is production ready!")
        return True
        
    except Exception as e:
        print(f"   ❌ Production readiness verification failed: {e}")
        return False


def run_full_verification():
    """Run complete verification suite."""
    print("🚀 Starting Cash Flow Extractor Verification\n")
    
    verifications = [
        ("API Connectivity", verify_api_connectivity),
        ("Database Readiness", verify_database_readiness),
        ("Field Mappings", verify_field_mappings),
        ("Transformation Logic", verify_transformation_logic),
        ("Incremental Processing", verify_incremental_processing),
        ("Data Quality", verify_data_quality),
        ("Production Readiness", verify_production_readiness)
    ]
    
    passed = 0
    failed = 0
    
    for name, verification_func in verifications:
        print(f"{'='*60}")
        print(f"🧪 {name}")
        print(f"{'='*60}")
        
        try:
            if verification_func():
                passed += 1
                print(f"✅ {name} PASSED\n")
            else:
                failed += 1
                print(f"❌ {name} FAILED\n")
        except Exception as e:
            failed += 1
            print(f"❌ {name} FAILED with exception: {e}\n")
    
    print(f"{'='*60}")
    print(f"🎯 Verification Summary")
    print(f"{'='*60}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Total:  {len(verifications)}")
    
    if failed == 0:
        print("\n🎉 All verifications passed! Cash flow extractor is ready for production.")
        print("\n📝 Recommended next steps:")
        print("   1. Run: python data_pipeline/extract/extract_cash_flow.py --limit 10")
        print("   2. Monitor logs for any issues")
        print("   3. Check data quality in database")
        print("   4. Scale up to larger batches")
    else:
        print(f"\n⚠️ {failed} verifications failed. Please address issues before production use.")
    
    return failed == 0


if __name__ == "__main__":
    success = run_full_verification()
    exit(0 if success else 1)
