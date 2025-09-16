#!/usr/bin/env python3
"""
Deep Dive: Why Are Successfully Processed Symbols Still Appearing?
================================================================
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))
from db.postgres_database_manager import PostgresDatabaseManager
from utils.incremental_etl import WatermarkManager

def deep_dive_symbol_reappearance():
    """Check why successfully processed symbols reappear in the queue."""
    
    print("🔍 DEEP DIVE: Why Successfully Processed Symbols Reappear")
    print("=" * 60)
    
    try:
        with PostgresDatabaseManager() as db:
            watermark_mgr = WatermarkManager(db)
            
            # Get the current 524 symbols that are being selected
            print("1. Getting current processing queue...")
            current_symbols = watermark_mgr.get_symbols_needing_processing(
                table_name='balance_sheet',
                staleness_hours=168,
                limit=10,  # Just get a sample for analysis
                quarterly_gap_detection=True,
                reporting_lag_days=45,
                enable_pre_screening=True
            )
            
            print(f"   Sample of current queue: {len(current_symbols)} symbols")
            
            if current_symbols:
                print("\n2. Analyzing why these symbols are still selected:")
                print(f"   {'Symbol':<8} {'Last Run':<20} {'Last Fiscal':<12} {'Failures':<8} {'Reason':<15}")
                print(f"   {'-'*8} {'-'*20} {'-'*12} {'-'*8} {'-'*15}")
                
                for symbol_data in current_symbols[:10]:
                    symbol = symbol_data['symbol']
                    last_run = symbol_data.get('last_successful_run')
                    last_fiscal = symbol_data.get('last_fiscal_date')
                    failures = symbol_data.get('consecutive_failures', 0)
                    
                    # Check specific reasons
                    if last_run is None:
                        reason = "Never processed"
                    elif symbol_data.get('has_quarterly_gap', False):
                        reason = "Quarterly gap"
                    else:
                        # Check staleness
                        staleness_query = """
                            SELECT 
                                EXTRACT(EPOCH FROM (NOW() - %s))/3600 as hours_since_last
                            FROM dual;
                        """
                        try:
                            hours_since = db.fetch_query("SELECT EXTRACT(EPOCH FROM (NOW() - %s))/3600", [last_run])[0][0]
                            if hours_since >= 168:
                                reason = f"Stale ({hours_since:.1f}h)"
                            else:
                                reason = f"Current ({hours_since:.1f}h)"
                        except:
                            reason = "Unknown"
                    
                    last_run_str = str(last_run)[:19] if last_run else "Never"
                    last_fiscal_str = str(last_fiscal)[:10] if last_fiscal else "None"
                    
                    print(f"   {symbol:<8} {last_run_str:<20} {last_fiscal_str:<12} {failures:<8} {reason:<15}")
                
                print("\n3. Detailed quarterly gap analysis for sample symbols:")
                
                # Check quarterly gap logic for a specific symbol
                sample_symbol_id = current_symbols[0]['symbol_id']
                sample_symbol = current_symbols[0]['symbol']
                
                gap_analysis_query = """
                    SELECT 
                        ew.last_fiscal_date,
                        -- Calculate expected latest quarter
                        CASE 
                            WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '45 days'
                            THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '1 day')::date
                            ELSE (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' - INTERVAL '1 day')::date
                        END as expected_latest_quarter,
                        -- Check gap condition
                        CASE 
                            WHEN ew.last_fiscal_date IS NULL THEN TRUE
                            WHEN ew.last_fiscal_date < (
                                CASE 
                                    WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '45 days'
                                    THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '1 day')::date
                                    ELSE (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' - INTERVAL '1 day')::date
                                END
                            ) THEN TRUE
                            ELSE FALSE
                        END as has_quarterly_gap,
                        CURRENT_DATE,
                        DATE_TRUNC('quarter', CURRENT_DATE) as current_quarter_start
                    FROM source.extraction_watermarks ew
                    WHERE ew.symbol_id = %s AND ew.table_name = 'balance_sheet';
                """
                
                gap_result = db.fetch_query(gap_analysis_query, [sample_symbol_id])
                
                if gap_result:
                    last_fiscal, expected_quarter, has_gap, current_date, current_q = gap_result[0]
                    
                    print(f"\n   Sample: {sample_symbol} (ID: {sample_symbol_id})")
                    print(f"   • Last fiscal date: {last_fiscal}")
                    print(f"   • Expected latest quarter: {expected_quarter}")
                    print(f"   • Has quarterly gap: {has_gap}")
                    print(f"   • Current date: {current_date}")
                    print(f"   • Current quarter start: {current_q}")
                    
                    if has_gap:
                        print(f"   ➡️  REASON: Quarterly gap detected!")
                        print(f"      {last_fiscal} < {expected_quarter}")
                    else:
                        print(f"   ➡️  NO quarterly gap, must be time-stale")
                
                print("\n4. Root cause analysis:")
                
                # Check if this is a quarterly gap issue
                quarterly_gap_count = sum(1 for s in current_symbols if s.get('has_quarterly_gap', False))
                never_processed_count = sum(1 for s in current_symbols if s.get('last_successful_run') is None)
                
                print(f"   • Quarterly gaps: {quarterly_gap_count}/{len(current_symbols)}")
                print(f"   • Never processed: {never_processed_count}/{len(current_symbols)}")
                print(f"   • Time stale: {len(current_symbols) - quarterly_gap_count - never_processed_count}/{len(current_symbols)}")
                
                if quarterly_gap_count > len(current_symbols) * 0.8:
                    print("\n   🎯 PRIMARY ISSUE: QUARTERLY GAP LOGIC")
                    print("   Most symbols have quarterly gaps, meaning:")
                    print("   • They're missing expected quarterly data (Q2 or Q3 2025)")
                    print("   • Even after processing, they still don't have the latest expected quarter")
                    print("   • This is likely because the API doesn't have Q3 2025 data yet")
                    print("   • So they'll keep reappearing until Q3 data is available")
                
            print("\n💡 LIKELY EXPLANATION:")
            print("The same 524 symbols keep appearing because:")
            print("1. ✅ They are being successfully processed (watermarks updated)")
            print("2. ✅ Data is being inserted (24K+ records)")
            print("3. ❌ But they still have quarterly gaps (missing Q3 2025 data)")
            print("4. 🔄 So they immediately re-qualify for processing")
            print("\nThis creates an infinite loop until Q3 2025 data becomes available!")
                
    except Exception as e:
        print(f"❌ Error in deep dive: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    deep_dive_symbol_reappearance()
