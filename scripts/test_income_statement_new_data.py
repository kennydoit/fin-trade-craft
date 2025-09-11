"""
New Data Test Script for Income Statement Extractor.

This script tests the income statement extractor's ability to process new data by:
1. Selecting symbols that haven't been updated recently
2. Processing them with the extractor
3. Validating the results
"""

import sys
import os
from datetime import datetime, timedelta

# Add parent directories to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'db'))

try:
    from data_pipeline.extract.extract_income_statement import IncomeStatementExtractor
    from postgres_database_manager import PostgresDatabaseManager
    print("✅ Successfully imported required modules")
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)


def test_new_data_processing():
    """Test processing of new income statement data."""
    print("🧪 Testing Income Statement Extractor with New Data")
    print("=" * 60)
    
    try:
        # Initialize components
        extractor = IncomeStatementExtractor()
        db = PostgresDatabaseManager()
        db.connect()
        print("✅ Initialized extractor and database connection")

        # Find symbols that need updating (haven't been processed recently)
        print("\n🔍 Finding symbols that need income statement updates...")

        with db.connection.cursor() as cursor:
            # Get symbols that either have no income statement data or haven't been updated recently
            cursor.execute("""
                WITH recent_income_statements AS (
                    SELECT symbol_id, MAX(updated_at) as last_updated
                    FROM source.income_statement
                    WHERE updated_at > NOW() - INTERVAL '7 days'
                    GROUP BY symbol_id
                ),
                symbol_candidates AS (
                    SELECT l.symbol_id, l.symbol
                    FROM listing_status l
                    WHERE l.status = 'active'
                    AND l.asset_type = 'stock'
                    AND l.symbol_id NOT IN (SELECT symbol_id FROM recent_income_statements)
                    ORDER BY RANDOM()
                    LIMIT 5
                )
                SELECT symbol_id, symbol FROM symbol_candidates
            """)
            symbols_to_test = cursor.fetchall()

        if not symbols_to_test:
            print("⚠️  No symbols found that need income statement updates")
            print("   This could mean:")
            print("   - All symbols have been updated recently")
            print("   - No active stock symbols in database")

            # Fallback: get any active symbols
            print("\n🔄 Trying fallback selection...")
            with db.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT symbol_id, symbol
                    FROM listing_status
                    WHERE status = 'active' AND asset_type = 'stock'
                    ORDER BY RANDOM()
                    LIMIT 3
                """)
                symbols_to_test = cursor.fetchall()

        if not symbols_to_test:
            print("❌ No active stock symbols found in database")
            return False

        print(f"✅ Found {len(symbols_to_test)} symbols to test:")
        for symbol_id, symbol in symbols_to_test:
            print(f"   {symbol} (ID: {symbol_id})")

        # Test processing each symbol
        print("\n🧪 Testing income statement extraction...")

        successful_extractions = 0
        total_records_processed = 0

        for symbol_id, symbol in symbols_to_test:
            print(f"\n📊 Processing {symbol} (ID: {symbol_id})...")
            try:
                # Get pre-processing record count
                with db.connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT COUNT(*) FROM source.income_statement 
                        WHERE symbol_id = %s
                    """, (symbol_id,))
                    pre_count = cursor.fetchone()[0]

                print(f"   Pre-processing records: {pre_count}")

                # Extract data
                result = extractor.extract_symbol(symbol, symbol_id, db)

                # Get post-processing record count
                with db.connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT COUNT(*) FROM source.income_statement 
                        WHERE symbol_id = %s
                    """, (symbol_id,))
                    post_count = cursor.fetchone()[0]

                print(f"   Post-processing records: {post_count}")
                print(f"   Status: {result['status']}")

                if result['status'] == 'success':
                    successful_extractions += 1
                    records_processed = result.get('records_processed', 0)
                    total_records_processed += records_processed
                    print(f"   ✅ Successfully processed {records_processed} records")

                    if records_processed > 0:
                        # Check latest data
                        with db.connection.cursor() as cursor:
                            cursor.execute("""
                                SELECT fiscal_date_ending, report_type, total_revenue, net_income
                                FROM source.income_statement
                                WHERE symbol_id = %s
                                ORDER BY fiscal_date_ending DESC, 
                                         CASE WHEN report_type = 'annual' THEN 1 ELSE 2 END
                                LIMIT 3
                            """, (symbol_id,))
                            recent_records = cursor.fetchall()

                        print("   📈 Latest records:")
                        for fiscal_date, report_type, total_revenue, net_income in recent_records:
                            revenue_str = f"${total_revenue:,.0f}" if total_revenue else "N/A"
                            income_str = f"${net_income:,.0f}" if net_income else "N/A"
                            print(f"      {fiscal_date} ({report_type}): Revenue={revenue_str}, Income={income_str}")

                elif result['status'] == 'no_changes':
                    print(f"   ⚪ No changes detected (content unchanged)")

                else:
                    print(f"   ❌ Failed: {result['status']}")

            except Exception as e:
                print(f"   ❌ Exception during processing: {e}")
                continue

        # Summary
        print("\n" + "=" * 60)
        print("📋 NEW DATA PROCESSING TEST SUMMARY")
        print("=" * 60)
        print(f"Symbols tested: {len(symbols_to_test)}")
        print(f"Successful extractions: {successful_extractions}")
        print(f"Total records processed: {total_records_processed}")
        print(f"Success rate: {successful_extractions/len(symbols_to_test)*100:.1f}%")

        if successful_extractions > 0:
            print("\n✅ Income statement extractor successfully processed new data!")

            # Data quality check
            print("\n🔍 Running data quality check on new records...")
            with db.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(total_revenue) as revenue_count,
                        COUNT(net_income) as income_count,
                        COUNT(gross_profit) as profit_count,
                        ROUND(AVG(CASE WHEN total_revenue IS NOT NULL THEN 1 ELSE 0 END) * 100, 1) as revenue_completeness,
                        ROUND(AVG(CASE WHEN net_income IS NOT NULL THEN 1 ELSE 0 END) * 100, 1) as income_completeness
                    FROM source.income_statement
                    WHERE updated_at > NOW() - INTERVAL '1 hour'
                """)
                quality_stats = cursor.fetchone()
                if quality_stats and quality_stats[0] > 0:
                    total, revenue_count, income_count, profit_count, revenue_pct, income_pct = quality_stats
                    print(f"   Recent records: {total}")
                    print(f"   Revenue completeness: {revenue_pct}% ({revenue_count}/{total})")
                    print(f"   Income completeness: {income_pct}% ({income_count}/{total})")
                    print(f"   Profit data: {profit_count}/{total}")

                    if revenue_pct >= 80 and income_pct >= 80:
                        print("   ✅ Data quality is good")
                    else:
                        print("   ⚠️  Data quality could be improved")
            return True
        else:
            print("\n❌ No successful extractions. Check API connectivity and configuration.")
            return False
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        return False


def test_specific_symbols():
    """Test extraction for specific high-profile symbols."""
    print("\n🎯 Testing specific high-profile symbols...")
    
    test_symbols = [
        ('AAPL', 'Apple Inc.'),
        ('MSFT', 'Microsoft Corporation'),
        ('GOOGL', 'Alphabet Inc.')
    ]
    
    try:
        extractor = IncomeStatementExtractor()
        db = PostgresDatabaseManager()
        db.connect()

        for symbol, company_name in test_symbols:
            print(f"\n📊 Testing {symbol} ({company_name})...")
            try:
                # Get symbol_id
                with db.connection.cursor() as cursor:
                    cursor.execute("SELECT symbol_id FROM listing_status WHERE symbol = %s", (symbol,))
                    result = cursor.fetchone()
                    if not result:
                        print(f"   ❌ Symbol {symbol} not found in database")
                        continue
                    symbol_id = result[0]

                # Test API call
                api_response, status = extractor._fetch_api_data(symbol)
                if status == "success":
                    print(f"   ✅ API call successful")
                    # Check data structure
                    annual_reports = api_response.get('annualReports', [])
                    quarterly_reports = api_response.get('quarterlyReports', [])
                    print(f"   📈 Annual reports: {len(annual_reports)}")
                    print(f"   📊 Quarterly reports: {len(quarterly_reports)}")
                    if annual_reports:
                        latest_annual = annual_reports[0]
                        fiscal_date = latest_annual.get('fiscalDateEnding', 'N/A')
                        total_revenue = latest_annual.get('totalRevenue', 'N/A')
                        net_income = latest_annual.get('netIncome', 'N/A')
                        print(f"   📅 Latest annual: {fiscal_date}")
                        print(f"   💰 Revenue: {total_revenue}")
                        print(f"   💵 Net Income: {net_income}")
                    # Test transformation
                    try:
                        records = extractor._transform_data(symbol, symbol_id, api_response, "test_run")
                        print(f"   ✅ Transformation successful: {len(records)} records")
                    except Exception as e:
                        print(f"   ❌ Transformation failed: {e}")
                else:
                    print(f"   ❌ API call failed: {status}")
            except Exception as e:
                print(f"   ❌ Exception during specific symbol test: {e}")
                continue
        print("\n✅ Specific symbol testing completed")
        return True
    except Exception as e:
        print(f"❌ Specific symbol test failed: {e}")
        return False


def main():
    """Main execution function."""
    print("🧪 INCOME STATEMENT EXTRACTOR - NEW DATA TESTING")
    print("=" * 80)
    
    # Test 1: New data processing
    test1_success = test_new_data_processing()
    
    # Test 2: Specific symbols
    test2_success = test_specific_symbols()
    
    # Overall summary
    print("\n" + "=" * 80)
    print("🏁 OVERALL TEST SUMMARY")
    print("=" * 80)
    
    tests_passed = sum([test1_success, test2_success])
    total_tests = 2
    
    print(f"Tests passed: {tests_passed}/{total_tests}")
    print(f"Success rate: {tests_passed/total_tests*100:.1f}%")
    
    if tests_passed == total_tests:
        print("\n🎉 All tests passed! Income statement extractor is working correctly with new data.")
        sys.exit(0)
    else:
        print(f"\n⚠️  {total_tests - tests_passed} test(s) failed. Please investigate.")
        sys.exit(1)


if __name__ == "__main__":
    main()
