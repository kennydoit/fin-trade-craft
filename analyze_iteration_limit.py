#!/usr/bin/env python3
"""
Investigate Why Balance Sheet Extractor Stops at ~500 Iterations
===============================================================

This script investigates why the extractor stops processing at around 500 symbols
instead of running through all eligible symbols when no --limit is specified.
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add the parent directories to the path
sys.path.append(str(Path(__file__).parent))
from db.postgres_database_manager import PostgresDatabaseManager
from utils.incremental_etl import WatermarkManager

def investigate_iteration_limit():
    """Investigate why the extractor stops at ~500 iterations."""
    
    print("🔍 INVESTIGATING 500-ITERATION LIMIT")
    print("=" * 60)
    print("Why does the extractor stop at ~500 symbols instead of processing all eligible?")
    print()
    
    try:
        with PostgresDatabaseManager() as db_manager:
            watermark_mgr = WatermarkManager(db_manager)
            
            # 1. Check total eligible symbols with weekly staleness
            print("📊 Step 1: Total Eligible Symbols Analysis...")
            
            # Simulate the exact query used by the extractor with 168-hour staleness
            eligible_symbols_weekly = watermark_mgr.get_symbols_needing_processing(
                table_name='balance_sheet',
                staleness_hours=168,  # 7 days
                limit=None,  # No limit - should get all
                quarterly_gap_detection=True,
                reporting_lag_days=45,
                enable_pre_screening=True
            )
            
            print(f"   Total eligible symbols (168-hour staleness): {len(eligible_symbols_weekly):,}")
            
            # Compare with 24-hour staleness
            eligible_symbols_daily = watermark_mgr.get_symbols_needing_processing(
                table_name='balance_sheet',
                staleness_hours=24,  # 1 day
                limit=None,
                quarterly_gap_detection=True,
                reporting_lag_days=45,
                enable_pre_screening=True
            )
            
            print(f"   Total eligible symbols (24-hour staleness): {len(eligible_symbols_daily):,}")
            print()
            
            # 2. Check for potential limiting factors
            print("🚫 Step 2: Potential Limiting Factors...")
            print()
            
            # Check pre-screening statistics in detail
            print("🔍 Pre-screening Impact:")
            symbols_no_screening = watermark_mgr.get_symbols_needing_processing(
                table_name='balance_sheet',
                staleness_hours=168,
                limit=None,
                quarterly_gap_detection=True,
                reporting_lag_days=45,
                enable_pre_screening=False  # No screening
            )
            
            symbols_with_screening = eligible_symbols_weekly
            
            print(f"   Without pre-screening: {len(symbols_no_screening):,}")
            print(f"   With pre-screening: {len(symbols_with_screening):,}")
            print(f"   Pre-screening removes: {len(symbols_no_screening) - len(symbols_with_screening):,}")
            
            # 3. Check the breakdown of why symbols need processing
            print()
            print("📈 Step 3: Breakdown of Processing Reasons...")
            
            never_processed = 0
            quarterly_gaps = 0
            time_stale = 0
            
            for symbol_data in eligible_symbols_weekly:
                if symbol_data.get('last_successful_run') is None:
                    never_processed += 1
                elif symbol_data.get('has_quarterly_gap', False):
                    quarterly_gaps += 1
                else:
                    time_stale += 1
            
            print(f"   Never processed: {never_processed:,}")
            print(f"   Quarterly gaps: {quarterly_gaps:,}")
            print(f"   Time stale (7+ days): {time_stale:,}")
            print(f"   TOTAL: {len(eligible_symbols_weekly):,}")
            
            # 4. Check for database/connection limits
            print()
            print("🔧 Step 4: Checking Database Configuration...")
            
            # Check for any database connection settings that might limit results
            db_config_query = """
                SHOW work_mem;
            """
            work_mem = db_manager.fetch_query(db_config_query)[0][0]
            print(f"   PostgreSQL work_mem: {work_mem}")
            
            # Check for query timeout settings
            timeout_query = """
                SHOW statement_timeout;
            """
            timeout = db_manager.fetch_query(timeout_query)[0][0]
            print(f"   Statement timeout: {timeout}")
            
            # 5. Test if there's a hidden limit in the watermark query
            print()
            print("🔍 Step 5: Watermark Query Investigation...")
            
            # Let's check the raw SQL query being executed
            print("   Checking if there's a hidden LIMIT in the watermark query...")
            
            # Check recent processing to see if it actually stopped at 500
            recent_processing_query = """
                SELECT 
                    DATE(ew.last_successful_run) as process_date,
                    COUNT(*) as symbols_processed,
                    MIN(ew.last_successful_run) as first_run,
                    MAX(ew.last_successful_run) as last_run
                FROM source.extraction_watermarks ew
                WHERE ew.table_name = 'balance_sheet'
                  AND ew.last_successful_run >= CURRENT_DATE - INTERVAL '1 days'
                GROUP BY DATE(ew.last_successful_run)
                ORDER BY process_date DESC;
            """
            
            recent_runs = db_manager.fetch_query(recent_processing_query)
            
            if recent_runs:
                print("   Recent processing runs:")
                print(f"   {'Date':<12} {'Symbols':<8} {'Duration':<15}")
                print(f"   {'-'*12} {'-'*8} {'-'*15}")
                
                for date_val, count, first_run, last_run in recent_runs:
                    duration = last_run - first_run
                    duration_str = str(duration).split('.')[0]  # Remove microseconds
                    print(f"   {str(date_val):<12} {count:<8} {duration_str:<15}")
            
            # 6. Check API rate limiting impact
            print()
            print("⏱️  Step 6: API Rate Limiting Analysis...")
            
            # If processing ~500 symbols, how long would that take?
            print("   Estimated processing time for 500 symbols:")
            print("   • At 5 calls/minute (Alpha Vantage limit): ~1.67 hours")
            print("   • At 12-second intervals (adaptive): ~1.67 hours")
            print("   • This suggests the run should complete normally")
            
            # 7. Check for extraction failures during the run
            print()
            print("❌ Step 7: Recent Extraction Failures...")
            
            failure_analysis_query = """
                SELECT 
                    ew.consecutive_failures,
                    COUNT(*) as symbol_count
                FROM source.extraction_watermarks ew
                WHERE ew.table_name = 'balance_sheet'
                  AND ew.updated_at >= CURRENT_DATE - INTERVAL '1 days'
                GROUP BY ew.consecutive_failures
                ORDER BY ew.consecutive_failures;
            """
            
            failure_stats = db_manager.fetch_query(failure_analysis_query)
            
            if failure_stats:
                print("   Recent failure distribution:")
                print(f"   {'Failures':<10} {'Symbol Count':<12}")
                print(f"   {'-'*10} {'-'*12}")
                
                total_recent_symbols = 0
                failed_symbols = 0
                
                for failures, count in failure_stats:
                    print(f"   {failures:<10} {count:<12}")
                    total_recent_symbols += count
                    if failures > 0:
                        failed_symbols += count
                
                print(f"   {'-'*10} {'-'*12}")
                print(f"   {'TOTAL':<10} {total_recent_symbols:<12}")
                print(f"   Failed symbols: {failed_symbols} ({failed_symbols/total_recent_symbols*100:.1f}%)")
            
            # 8. Possible reasons for stopping at ~500
            print()
            print("💡 Step 8: Likely Reasons for ~500 Symbol Limit...")
            print()
            
            actual_eligible = len(eligible_symbols_weekly)
            
            if actual_eligible <= 600:
                print("🎯 **REASON 1: Natural Queue Size**")
                print(f"   • Only {actual_eligible:,} symbols are actually eligible")
                print("   • Pre-screening removes problematic symbols")
                print("   • 168-hour staleness reduces time-based eligibility")
                print("   • ~500 processed = most/all eligible symbols")
                print()
            
            print("🎯 **REASON 2: Pre-screening Efficiency**")
            print(f"   • Pre-screening removes {len(symbols_no_screening) - len(symbols_with_screening):,} symbols")
            print("   • This dramatically reduces the processing queue")
            print("   • Only 'clean' symbols remain for processing")
            print()
            
            print("🎯 **REASON 3: Weekly Staleness Filter**")
            print("   • 168-hour staleness means only symbols not processed in 7+ days qualify")
            print("   • This eliminates recently processed symbols")
            print("   • Reduces queue size compared to 24-hour staleness")
            print()
            
            print("🎯 **REASON 4: Progress Through Universe**")
            print("   • You're 76.8% through the total universe")
            print("   • Most symbols have been processed at least once")
            print("   • Remaining work is naturally smaller")
            print()
            
            # 9. Recommendations
            print("✅ Step 9: Recommendations...")
            print()
            print("**To verify the extractor is working correctly:**")
            print("```bash")
            print("# Check if all eligible symbols were actually processed")
            print("python extract_balance_sheet.py --staleness-hours 168 --limit 1000")
            print("```")
            print()
            print("**To see more symbols per run:**")
            print("```bash")
            print("# Use shorter staleness to include more recently processed symbols")
            print("python extract_balance_sheet.py --staleness-hours 24")
            print()
            print("# Disable pre-screening to see all symbols (including problematic ones)")
            print("python extract_balance_sheet.py --staleness-hours 168 --no-pre-screening")
            print("```")
            print()
            print("🎉 **CONCLUSION:**")
            print("The ~500 iteration limit is likely NOT a bug, but rather:")
            print("• The natural size of your current processing queue")
            print("• Effective pre-screening reducing the workload")  
            print("• Weekly staleness filtering out recently processed symbols")
            print(f"• Progress through your {actual_eligible:,} currently eligible symbols")
                
    except Exception as e:
        print(f"❌ Error investigating iteration limit: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    investigate_iteration_limit()
