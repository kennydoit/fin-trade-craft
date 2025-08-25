#!/usr/bin/env python3
"""
Fixed Duplicate Resolution Script
Handles the case where multiple rows share the same symbol_id but have different symbols.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from db.postgres_database_manager import PostgresDatabaseManager


def resolve_all_duplicates_correctly():
    """Resolve ALL duplicate calculated_symbol_id values correctly."""
    
    with PostgresDatabaseManager() as db:
        
        # First, get ALL remaining duplicates
        print("🔍 Finding all remaining duplicates...")
        duplicates_query = """
        SELECT calculated_symbol_id, COUNT(*) as count, array_agg(symbol ORDER BY symbol) as symbols
        FROM extracted.listing_status
        WHERE calculated_symbol_id IS NOT NULL
        GROUP BY calculated_symbol_id
        HAVING COUNT(*) > 1
        ORDER BY calculated_symbol_id;
        """
        
        duplicates = db.fetch_query(duplicates_query)
        print(f"Found {len(duplicates)} duplicate groups to resolve")
        
        if not duplicates:
            print("✅ No duplicates found!")
            return True
            
        # Process each duplicate group
        total_updated = 0
        
        for dup_group in duplicates:
            calc_id = dup_group[0]
            count = dup_group[1]
            symbols = dup_group[2]
            
            print(f"\n  Processing calc_id {calc_id} with {count} symbols: {symbols}")
            
            # Get symbol details for this group (symbol_id AND symbol for precise targeting)
            symbol_details_query = """
            SELECT symbol_id, symbol
            FROM extracted.listing_status
            WHERE calculated_symbol_id = %s
            ORDER BY symbol;
            """
            symbol_details = db.fetch_query(symbol_details_query, (calc_id,))
            
            # First symbol keeps the original ID, others get incremented
            canonical_symbol = symbol_details[0]
            print(f"    Canonical: {canonical_symbol[1]} (keeps {calc_id})")
            
            # Update remaining symbols with incremented IDs
            for i, symbol_data in enumerate(symbol_details[1:], 1):
                symbol_id, symbol = symbol_data
                new_calc_id = calc_id + i
                
                # Find the next available ID
                while True:
                    check_query = "SELECT COUNT(*) FROM extracted.listing_status WHERE calculated_symbol_id = %s"
                    exists = db.fetch_query(check_query, (new_calc_id,))[0][0]
                    if exists == 0:
                        break
                    new_calc_id += 1
                
                print(f"    Updating: {symbol} -> {new_calc_id}")
                
                # Update this specific symbol in listing_status (using both symbol_id AND symbol for precision)
                update_listing = """
                UPDATE extracted.listing_status
                SET calculated_symbol_id = %s
                WHERE symbol_id = %s AND symbol = %s;
                """
                db.execute_query(update_listing, (new_calc_id, symbol_id, symbol))
                
                # Update all dependent tables (also using both symbol_id AND symbol for precision)
                dependent_tables = [
                    'overview',
                    'time_series_daily_adjusted',
                    'income_statement', 
                    'balance_sheet',
                    'cash_flow',
                    'historical_options',
                    'realtime_options',
                    'insider_transactions',
                    'earnings_call_transcripts'
                ]
                
                for table in dependent_tables:
                    # Check if this table has a symbol column
                    check_symbol_col_query = f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'extracted' AND table_name = '{table}' AND column_name = 'symbol';
                    """
                    has_symbol_col = db.fetch_query(check_symbol_col_query)
                    
                    if has_symbol_col:
                        # Update using both symbol_id and symbol for precision
                        update_query = f"""
                        UPDATE extracted.{table}
                        SET calculated_symbol_id = %s
                        WHERE symbol_id = %s AND symbol = %s;
                        """
                        db.execute_query(update_query, (new_calc_id, symbol_id, symbol))
                    else:
                        # Fallback to symbol_id only if no symbol column
                        update_query = f"""
                        UPDATE extracted.{table}
                        SET calculated_symbol_id = %s
                        WHERE symbol_id = %s;
                        """
                        db.execute_query(update_query, (new_calc_id, symbol_id))
                
                total_updated += 1
        
        print(f"\n✅ Updated {total_updated} symbols with new calculated IDs")
        
        # Verify uniqueness
        print("\nVerifying uniqueness...")
        verify_query = """
        SELECT
            COUNT(*) as total_symbols,
            COUNT(DISTINCT calculated_symbol_id) as unique_calc_ids,
            COUNT(*) - COUNT(DISTINCT calculated_symbol_id) as duplicates
        FROM extracted.listing_status
        WHERE calculated_symbol_id IS NOT NULL;
        """
        
        verification = db.fetch_query(verify_query)[0]
        total_symbols = verification[0]
        unique_calc_ids = verification[1]
        remaining_duplicates = verification[2]
        
        print(f"  Total symbols: {total_symbols:,}")
        print(f"  Unique calculated IDs: {unique_calc_ids:,}")
        
        if remaining_duplicates == 0:
            print("  ✅ All duplicates resolved!")
            return True
        else:
            print(f"  ❌ {remaining_duplicates} duplicates still exist!")
            return False


if __name__ == "__main__":
    print("=== Fixed Duplicate Resolution ===")
    print("Resolving ALL remaining duplicate calculated_symbol_id values correctly...")
    
    try:
        success = resolve_all_duplicates_correctly()
        
        if success:
            print("\n🎉 ALL DUPLICATES SUCCESSFULLY RESOLVED!")
            print("Database is ready for final migration.")
        else:
            print("\n❌ Some duplicates still remain - manual review required.")
            
    except Exception as e:
        print(f"\n❌ Error during duplicate resolution: {e}")
        sys.exit(1)
