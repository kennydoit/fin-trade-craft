#!/usr/bin/env python3
"""
Test Quarterly Gap Detection Logic
=================================

This script tests whether symbols with current quarterly data (Q2 2025 ending 6/30/25) 
are correctly skipped from processing until 45 days after Q3 2025 ends (9/30/25).

Expected behavior:
- Symbols with Q2 2025 data (6/30/25) should NOT be processed on 9/15/25
- They should only be processed after 11/14/25 (45 days after 9/30/25)
"""

import sys
from pathlib import Path
from datetime import datetime, date

# Add the parent directories to the path
sys.path.append(str(Path(__file__).parent))
from db.postgres_database_manager import PostgresDatabaseManager
from utils.incremental_etl import WatermarkManager

def test_quarterly_gap_detection():
    """Test the quarterly gap detection logic for Q2 2025 data."""
    
    print("🧪 TESTING QUARTERLY GAP DETECTION LOGIC")
    print("=" * 60)
    print(f"Current Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Test Scenario: Symbols with Q2 2025 data (6/30/25) should be skipped")
    print(f"Expected Skip Until: 11/14/25 (45 days after Q3 end 9/30/25)")
    print()
    
    try:
        with PostgresDatabaseManager() as db_manager:
            watermark_mgr = WatermarkManager(db_manager)
            
            # First, let's find symbols that have Q2 2025 balance sheet data
            print("🔍 Finding symbols with Q2 2025 balance sheet data...")
            q2_symbols_query = """
                SELECT DISTINCT 
                    bs.symbol_id,
                    bs.symbol,
                    bs.fiscal_date_ending,
                    ew.last_fiscal_date,
                    ew.last_successful_run,
                    EXTRACT(EPOCH FROM (NOW() - ew.last_successful_run))/3600 as hours_since_last_run
                FROM source.balance_sheet bs
                JOIN source.extraction_watermarks ew ON bs.symbol_id = ew.symbol_id 
                    AND ew.table_name = 'balance_sheet'
                WHERE bs.fiscal_date_ending = '2025-06-30'  -- Q2 2025
                    AND bs.report_type = 'quarterly'
                ORDER BY bs.symbol
                LIMIT 10;
            """
            
            q2_symbols = db_manager.fetch_query(q2_symbols_query)
            
            if not q2_symbols:
                print("❌ No symbols found with Q2 2025 data")
                return
            
            print(f"✅ Found {len(q2_symbols)} symbols with Q2 2025 data:")
            for symbol_data in q2_symbols:
                symbol_id, symbol, fiscal_date, last_fiscal_date, last_run, hours_since = symbol_data
                print(f"   • {symbol}: Fiscal Date = {fiscal_date}, Last Run = {last_run}")
            
            print()
            
            # Now test the quarterly gap detection logic
            print("🎯 Testing quarterly gap detection...")
            symbols_needing_processing = watermark_mgr.get_symbols_needing_processing(
                table_name='balance_sheet',
                staleness_hours=24,
                max_failures=3,
                limit=None,  # Get all symbols
                quarterly_gap_detection=True,
                reporting_lag_days=45
            )
            
            # Check which of our Q2 2025 symbols are flagged for processing
            q2_symbol_ids = {row[0] for row in q2_symbols}
            q2_symbols_to_process = []
            q2_symbols_skipped = []
            
            for symbol_data in symbols_needing_processing:
                if symbol_data['symbol_id'] in q2_symbol_ids:
                    q2_symbols_to_process.append(symbol_data)
            
            # Find skipped symbols (those NOT in the processing list)
            processing_symbol_ids = {s['symbol_id'] for s in symbols_needing_processing}
            for row in q2_symbols:
                symbol_id, symbol, fiscal_date, last_fiscal_date, last_run, hours_since = row
                if symbol_id not in processing_symbol_ids:
                    q2_symbols_skipped.append({
                        'symbol_id': symbol_id,
                        'symbol': symbol,
                        'fiscal_date_ending': fiscal_date,
                        'last_fiscal_date': last_fiscal_date,
                        'last_successful_run': last_run,
                        'hours_since_last_run': hours_since
                    })
            
            print(f"📊 QUARTERLY GAP DETECTION RESULTS:")
            print(f"   Total symbols needing processing: {len(symbols_needing_processing)}")
            print(f"   Q2 2025 symbols flagged for processing: {len(q2_symbols_to_process)}")
            print(f"   Q2 2025 symbols correctly SKIPPED: {len(q2_symbols_skipped)}")
            print()
            
            # Show symbols that were correctly skipped
            if q2_symbols_skipped:
                print("✅ CORRECTLY SKIPPED (Q2 2025 data is current):")
                for i, symbol_data in enumerate(q2_symbols_skipped[:5], 1):
                    symbol = symbol_data['symbol']
                    fiscal_date = symbol_data['fiscal_date_ending']
                    last_run = symbol_data['last_successful_run']
                    hours = symbol_data.get('hours_since_last_run', 0)
                    print(f"   {i}. {symbol}")
                    print(f"      - Latest Fiscal Date: {fiscal_date}")
                    print(f"      - Last Successful Run: {last_run}")
                    print(f"      - Hours Since Last Run: {hours:.1f}")
                    print(f"      - Status: ✅ CORRECTLY SKIPPED (Q2 data is current)")
                    print()
            
            # Show symbols that are incorrectly flagged for processing
            if q2_symbols_to_process:
                print("⚠️  FLAGGED FOR PROCESSING (should these be skipped?):")
                for i, symbol_data in enumerate(q2_symbols_to_process, 1):
                    symbol = symbol_data['symbol']
                    last_fiscal = symbol_data['last_fiscal_date']
                    expected_quarter = symbol_data.get('expected_latest_quarter')
                    has_gap = symbol_data.get('has_quarterly_gap', False)
                    print(f"   {i}. {symbol}")
                    print(f"      - Last Fiscal Date: {last_fiscal}")
                    print(f"      - Expected Quarter: {expected_quarter}")
                    print(f"      - Has Quarterly Gap: {has_gap}")
                    print(f"      - Status: ⚠️ FLAGGED FOR PROCESSING")
                    print()
            
            # Calculate the expected skip date
            print("📅 EXPECTED BEHAVIOR:")
            print(f"   - Current Date: September 15, 2025")
            print(f"   - Q3 2025 End Date: September 30, 2025")
            print(f"   - 45-Day Reporting Lag: Until November 14, 2025")
            print(f"   - Expected Result: Q2 2025 symbols should be SKIPPED until 11/14/25")
            print()
            
            # Summary
            total_q2_symbols = len(q2_symbols)
            correctly_skipped = len(q2_symbols_skipped)
            incorrectly_flagged = len(q2_symbols_to_process)
            
            print("🎯 TEST RESULTS SUMMARY:")
            print(f"   Total Q2 2025 symbols tested: {total_q2_symbols}")
            print(f"   Correctly skipped: {correctly_skipped} ({correctly_skipped/total_q2_symbols*100:.1f}%)")
            print(f"   Incorrectly flagged: {incorrectly_flagged} ({incorrectly_flagged/total_q2_symbols*100:.1f}%)")
            
            if incorrectly_flagged == 0:
                print("   ✅ PASS: All Q2 2025 symbols correctly skipped!")
            else:
                print("   ⚠️  REVIEW: Some Q2 2025 symbols are flagged for processing")
            
            print()
            
            # Additional analysis: Show the expected latest quarter calculation
            print("🔍 DETAILED LOGIC ANALYSIS:")
            logic_query = """
                SELECT 
                    CURRENT_DATE as current_date,
                    DATE_TRUNC('quarter', CURRENT_DATE) as current_quarter_start,
                    DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '3 months' - INTERVAL '1 day' as current_quarter_end,
                    DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '1 day' as previous_quarter_end,
                    CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '45 days' as is_45_days_into_quarter,
                    CASE 
                        WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '45 days'
                        THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '1 day')::date
                        ELSE (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' - INTERVAL '1 day')::date
                    END as expected_latest_quarter;
            """
            
            result = db_manager.fetch_query(logic_query)[0]
            current_date, current_q_start, current_q_end, prev_q_end, is_45_days, expected_quarter = result
            
            print(f"   Current Date: {current_date}")
            print(f"   Current Quarter: {current_q_start} to {current_q_end}")
            print(f"   Previous Quarter End: {prev_q_end}")
            print(f"   45+ Days into Current Quarter: {is_45_days}")
            print(f"   Expected Latest Quarter End: {expected_quarter}")
            print()
            print(f"   Logic: Since we're {'' if is_45_days else 'NOT '}45+ days into Q3 2025,")
            print(f"          symbols should have data through: {expected_quarter}")
            print(f"          Q2 2025 data (6/30/25) {'meets' if expected_quarter >= date(2025, 6, 30) else 'does not meet'} this requirement")
                
    except Exception as e:
        print(f"❌ Error testing quarterly gap detection: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_quarterly_gap_detection()
