#!/usr/bin/env python3
"""
Test API Failure Handling - What happens after 3 failed API calls?
================================================================

This script demonstrates the failure handling mechanism in the balance sheet extractor.
It shows how the watermark system tracks consecutive failures and stops processing
symbols after they reach the max_failures threshold.
"""

import sys
from pathlib import Path

# Add the parent directories to the path
sys.path.append(str(Path(__file__).parent))
from db.postgres_database_manager import PostgresDatabaseManager
from utils.incremental_etl import WatermarkManager

def test_failure_handling():
    """Test what happens when a symbol has consecutive API failures."""
    
    print("🔥 TESTING API FAILURE HANDLING MECHANISM")
    print("=" * 60)
    print("This test shows what happens after 3 consecutive API failures")
    print()
    
    try:
        with PostgresDatabaseManager() as db_manager:
            watermark_mgr = WatermarkManager(db_manager)
            
            # 1. Find symbols with consecutive failures
            print("🔍 Step 1: Finding symbols with consecutive failures...")
            failures_query = """
                SELECT 
                    ls.symbol_id,
                    ls.symbol,
                    ls.name,
                    ls.asset_type,
                    ew.consecutive_failures,
                    ew.last_successful_run,
                    ew.updated_at,
                    CASE 
                        WHEN ew.consecutive_failures >= 3 THEN 'BLACKLISTED'
                        WHEN ew.consecutive_failures >= 2 THEN 'AT RISK'
                        WHEN ew.consecutive_failures = 1 THEN 'FAILING'
                        ELSE 'OK'
                    END as failure_status
                FROM source.listing_status ls
                JOIN source.extraction_watermarks ew ON ls.symbol_id = ew.symbol_id
                    AND ew.table_name = 'balance_sheet'
                WHERE ew.consecutive_failures > 0
                ORDER BY ew.consecutive_failures DESC, ls.symbol
                LIMIT 15;
            """
            
            failed_symbols = db_manager.fetch_query(failures_query)
            
            if failed_symbols:
                print(f"✅ Found {len(failed_symbols)} symbols with failures:")
                print(f"{'Symbol':<8} {'Name':<25} {'Failures':<8} {'Status':<12} {'Last Successful':<20}")
                print("-" * 85)
                
                blacklisted_count = 0
                at_risk_count = 0
                failing_count = 0
                
                for symbol_data in failed_symbols:
                    symbol_id, symbol, name, asset_type, failures, last_run, updated, status = symbol_data
                    name_short = (name[:22] + "...") if name and len(name) > 25 else (name or "N/A")
                    last_run_str = str(last_run)[:19] if last_run else "Never"
                    
                    print(f"{symbol:<8} {name_short:<25} {failures:<8} {status:<12} {last_run_str:<20}")
                    
                    if status == 'BLACKLISTED':
                        blacklisted_count += 1
                    elif status == 'AT RISK':
                        at_risk_count += 1
                    elif status == 'FAILING':
                        failing_count += 1
                
                print()
                print(f"📊 Failure Status Summary:")
                print(f"   • BLACKLISTED (3+ failures): {blacklisted_count} symbols")
                print(f"   • AT RISK (2 failures): {at_risk_count} symbols")
                print(f"   • FAILING (1 failure): {failing_count} symbols")
                
            else:
                print("✅ No symbols with consecutive failures found")
            
            print()
            
            # 2. Test the filtering logic - show how blacklisted symbols are excluded
            print("🎯 Step 2: Testing the failure filtering logic...")
            
            # Get all symbols that would be considered for processing (no failure filter)
            all_symbols_query = """
                SELECT COUNT(*) as total_symbols
                FROM source.listing_status ls
                LEFT JOIN source.extraction_watermarks ew ON ls.symbol_id = ew.symbol_id 
                    AND ew.table_name = 'balance_sheet'
                WHERE ls.asset_type = 'Stock'
                  AND LOWER(ls.status) = 'active';
            """
            
            total_symbols = db_manager.fetch_query(all_symbols_query)[0][0]
            
            # Get symbols that would actually be processed (with failure filter)
            eligible_symbols_query = """
                SELECT COUNT(*) as eligible_symbols
                FROM source.listing_status ls
                LEFT JOIN source.extraction_watermarks ew ON ls.symbol_id = ew.symbol_id 
                    AND ew.table_name = 'balance_sheet'
                WHERE ls.asset_type = 'Stock'
                  AND LOWER(ls.status) = 'active'
                  AND COALESCE(ew.consecutive_failures, 0) < 3;  -- max_failures = 3
            """
            
            eligible_symbols = db_manager.fetch_query(eligible_symbols_query)[0][0]
            
            blacklisted_symbols = total_symbols - eligible_symbols
            
            print(f"   Total active stock symbols: {total_symbols:,}")
            print(f"   Eligible for processing: {eligible_symbols:,}")
            print(f"   Blacklisted (3+ failures): {blacklisted_symbols:,}")
            print(f"   Exclusion rate: {blacklisted_symbols/total_symbols*100:.1f}%")
            
            print()
            
            # 3. Show what happens during the processing cycle
            print("🔄 Step 3: How the failure mechanism works...")
            print()
            print("API Call Failure Progression:")
            print("=" * 40)
            print("1️⃣  First API failure:")
            print("   • consecutive_failures = 1")
            print("   • Symbol remains eligible for next run")
            print("   • Status: 'FAILING'")
            print()
            print("2️⃣  Second API failure:")
            print("   • consecutive_failures = 2") 
            print("   • Symbol still eligible but flagged")
            print("   • Status: 'AT RISK'")
            print()
            print("3️⃣  Third API failure:")
            print("   • consecutive_failures = 3")
            print("   • Symbol EXCLUDED from future processing")
            print("   • Status: 'BLACKLISTED' 🚫")
            print()
            print("✅ Successful API call:")
            print("   • consecutive_failures reset to 0")
            print("   • last_successful_run updated")
            print("   • Symbol returns to normal processing")
            
            print()
            
            # 4. Show examples of blacklisted symbols
            if failed_symbols and blacklisted_count > 0:
                print("🚫 Step 4: Examples of blacklisted symbols...")
                blacklisted_examples = [s for s in failed_symbols if s[7] == 'BLACKLISTED'][:5]
                
                print(f"{'Symbol':<8} {'Name':<30} {'Failures':<8} {'Asset Type':<12}")
                print("-" * 70)
                for symbol_data in blacklisted_examples:
                    symbol_id, symbol, name, asset_type, failures, last_run, updated, status = symbol_data
                    name_short = (name[:27] + "...") if name and len(name) > 30 else (name or "N/A")
                    print(f"{symbol:<8} {name_short:<30} {failures:<8} {asset_type:<12}")
                
                print()
                print("These symbols will NOT be processed in future runs until:")
                print("• Manual intervention resets their consecutive_failures to 0, OR")
                print("• A successful API call resets the counter")
            
            print()
            
            # 5. Benefits of this system
            print("💡 Step 5: Benefits of the failure handling system...")
            print()
            print("✅ API Efficiency:")
            print("   • Stops wasting API calls on persistently failing symbols")
            print("   • Focuses resources on symbols likely to succeed")
            print()
            print("✅ Error Resilience:")
            print("   • Temporary failures (1-2) don't permanently exclude symbols")
            print("   • Persistent failures (3+) indicate systematic issues")
            print()
            print("✅ Resource Optimization:")
            print("   • Reduces processing time by skipping problematic symbols")
            print(f"   • Current exclusion saves ~{blacklisted_symbols} API calls per run")
            print()
            print("✅ Self-Healing:")
            print("   • Successful extraction resets failure counter")
            print("   • Symbols can recover from temporary issues")
            
            print()
            print("🎯 SUMMARY: After 3 consecutive API failures:")
            print("   📛 Symbol is BLACKLISTED from future processing")
            print("   🚫 Will not appear in symbols_needing_processing list")
            print("   💾 Failure count persists in extraction_watermarks table")
            print("   🔄 Can be re-enabled by successful API call or manual reset")
            
    except Exception as e:
        print(f"❌ Error testing failure handling: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_failure_handling()
