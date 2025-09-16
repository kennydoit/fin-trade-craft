#!/usr/bin/env python3
"""
Quick Diagnostic: Why Are the Same 524 Symbols Reprocessing?
==========================================================
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))
from db.postgres_database_manager import PostgresDatabaseManager

def diagnose_watermark_issue():
    """Check if watermarks are being updated properly."""
    
    print("🔍 DIAGNOSING WATERMARK UPDATE ISSUE")
    print("=" * 50)
    
    try:
        with PostgresDatabaseManager() as db:
            
            # Check recent watermark updates
            print("1. Recent watermark updates (last 10 minutes):")
            recent_updates_query = """
                SELECT ls.symbol, ew.last_successful_run, ew.consecutive_failures,
                       ew.updated_at, ew.last_fiscal_date
                FROM source.extraction_watermarks ew
                JOIN source.listing_status ls ON ew.symbol_id = ls.symbol_id
                WHERE ew.table_name = 'balance_sheet'
                  AND ew.updated_at >= NOW() - INTERVAL '10 minutes'
                ORDER BY ew.updated_at DESC
                LIMIT 10;
            """
            
            recent_updates = db.fetch_query(recent_updates_query)
            
            if recent_updates:
                print(f"   Found {len(recent_updates)} recent updates:")
                print(f"   {'Symbol':<8} {'Last Success':<20} {'Failures':<8} {'Updated':<20}")
                print(f"   {'-'*8} {'-'*20} {'-'*8} {'-'*20}")
                
                for symbol, last_success, failures, updated, fiscal_date in recent_updates:
                    last_success_str = str(last_success)[:19] if last_success else "Never"
                    updated_str = str(updated)[:19]
                    print(f"   {symbol:<8} {last_success_str:<20} {failures:<8} {updated_str:<20}")
            else:
                print("   ❌ NO RECENT WATERMARK UPDATES FOUND!")
                print("   This suggests watermarks are not being updated after processing.")
            
            print()
            
            # Check if any balance sheet records were inserted recently
            print("2. Recent balance sheet data inserts:")
            recent_inserts_query = """
                SELECT COUNT(*) as recent_inserts
                FROM source.balance_sheet
                WHERE updated_at >= NOW() - INTERVAL '10 minutes';
            """
            
            recent_inserts = db.fetch_query(recent_inserts_query)[0][0]
            print(f"   Recent balance sheet inserts: {recent_inserts}")
            
            if recent_inserts > 0:
                print("   ✅ Data is being inserted")
            else:
                print("   ❌ No recent data inserts - extraction may be failing")
            
            print()
            
            # Check API responses landing table
            print("3. Recent API response logs:")
            api_responses_query = """
                SELECT response_status, COUNT(*) as count
                FROM source.api_responses_landing
                WHERE table_name = 'balance_sheet'
                  AND created_at >= NOW() - INTERVAL '10 minutes'
                GROUP BY response_status
                ORDER BY count DESC;
            """
            
            api_responses = db.fetch_query(api_responses_query)
            
            if api_responses:
                print("   Recent API responses:")
                for status, count in api_responses:
                    print(f"   • {status}: {count} calls")
            else:
                print("   ❌ No recent API responses logged")
            
            print()
            
            # Check a specific symbol's current state
            print("4. Sample symbol analysis:")
            sample_query = """
                SELECT ls.symbol, ew.last_successful_run, ew.consecutive_failures,
                       ew.updated_at, ew.last_fiscal_date,
                       -- Check if this symbol would be selected again
                       CASE 
                           WHEN ew.last_successful_run IS NULL THEN 'Never processed'
                           WHEN ew.last_successful_run < NOW() - INTERVAL '168 hours' THEN 'Stale (>7 days)'
                           WHEN ew.consecutive_failures >= 3 THEN 'Blacklisted'
                           ELSE 'Should be skipped'
                       END as current_status
                FROM source.listing_status ls
                LEFT JOIN source.extraction_watermarks ew ON ew.symbol_id = ls.symbol_id 
                    AND ew.table_name = 'balance_sheet'
                WHERE ls.asset_type = 'Stock'
                  AND LOWER(ls.status) = 'active'
                ORDER BY RANDOM()
                LIMIT 5;
            """
            
            sample_symbols = db.fetch_query(sample_query)
            
            print(f"   {'Symbol':<8} {'Last Success':<20} {'Status':<20}")
            print(f"   {'-'*8} {'-'*20} {'-'*20}")
            
            for symbol, last_success, failures, updated, fiscal_date, status in sample_symbols:
                last_success_str = str(last_success)[:19] if last_success else "Never"
                print(f"   {symbol:<8} {last_success_str:<20} {status:<20}")
            
            print()
            print("🎯 DIAGNOSIS:")
            
            if not recent_updates:
                print("❌ PRIMARY ISSUE: Watermarks are not being updated!")
                print("   Possible causes:")
                print("   • Database transaction not being committed")
                print("   • Error in watermark update logic")
                print("   • Connection issues")
                print("   • All extractions are failing (not reaching watermark update)")
                
            elif recent_inserts == 0:
                print("❌ ISSUE: API calls succeeding but data not being inserted")
                print("   • Check for transformation errors")
                print("   • Check for database constraint violations")
                
            else:
                print("✅ Watermarks and data are being updated")
                print("   • The issue may be in the symbol selection logic")
                print("   • Or symbols are becoming eligible again for other reasons")
            
    except Exception as e:
        print(f"❌ Error in diagnosis: {e}")

if __name__ == "__main__":
    diagnose_watermark_issue()
