"""
Fix Duplicate Calculated Symbol IDs

This script identifies and resolves duplicate calculated symbol IDs
before proceeding with the migration.

Strategy:
1. Identify all duplicates
2. Keep the "canonical" symbol (shortest/simplest form) 
3. Assign unique calculated IDs to variations using a suffix system
4. Update all dependent tables accordingly
"""

import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from db.postgres_database_manager import PostgresDatabaseManager
from data_pipeline.common.symbol_id_calculator import calculate_symbol_id


def identify_duplicates(db):
    """Identify all duplicate calculated symbol IDs."""
    query = """
    SELECT 
        calculated_symbol_id,
        array_agg(symbol ORDER BY LENGTH(symbol), symbol) as symbols,
        COUNT(*) as symbol_count
    FROM extracted.listing_status
    WHERE calculated_symbol_id IS NOT NULL
    GROUP BY calculated_symbol_id
    HAVING COUNT(*) > 1
    ORDER BY calculated_symbol_id;
    """
    
    result = db.execute_query(query)
    
    duplicates = []
    for row in result:
        calc_id = row[0]
        symbols = row[1].strip('{}').split(',')  # Parse PostgreSQL array
        count = row[2]
        
        duplicates.append({
            'calculated_symbol_id': calc_id,
            'symbols': [s.strip('"') for s in symbols],  # Remove quotes
            'count': count
        })
    
    return duplicates


def resolve_duplicate_group(db, duplicate_group):
    """Resolve a group of duplicate symbols by assigning unique IDs."""
    symbols = duplicate_group['symbols']
    base_calc_id = duplicate_group['calculated_symbol_id']
    
    print(f"\nResolving duplicate group: {symbols}")
    print(f"  Original calculated_symbol_id: {base_calc_id}")
    
    # Strategy: Keep the first (shortest/simplest) symbol with original ID
    # Assign incremental IDs to the others
    canonical_symbol = symbols[0]
    
    print(f"  Canonical symbol: {canonical_symbol} (keeps ID {base_calc_id})")
    
    # Update the other symbols with incremented IDs
    for i, symbol in enumerate(symbols[1:], 1):
        new_calc_id = base_calc_id + i
        
        # Check if this new ID conflicts with anything
        conflict_check = db.execute_query(
            "SELECT COUNT(*) FROM extracted.listing_status WHERE calculated_symbol_id = %s;",
            (new_calc_id,)
        )
        
        while conflict_check[0][0] > 0:
            new_calc_id += 1
            conflict_check = db.execute_query(
                "SELECT COUNT(*) FROM extracted.listing_status WHERE calculated_symbol_id = %s;",
                (new_calc_id,)
            )
        
        # Update listing_status
        update_query = """
        UPDATE extracted.listing_status 
        SET calculated_symbol_id = %s 
        WHERE symbol = %s;
        """
        db.execute_query(update_query, (new_calc_id, symbol))
        
        print(f"  Updated {symbol}: {base_calc_id} -> {new_calc_id}")
        
        # Update all dependent tables
        dependent_tables = [
            'overview', 'time_series_daily_adjusted', 'income_statement',
            'balance_sheet', 'cash_flow', 'historical_options',
            'realtime_options', 'insider_transactions', 'earnings_call_transcripts'
        ]
        
        for table in dependent_tables:
            # Get the old symbol_id for this symbol
            old_symbol_id_query = f"""
            SELECT symbol_id FROM extracted.{table} 
            WHERE symbol = %s LIMIT 1;
            """
            
            old_symbol_id_result = db.execute_query(old_symbol_id_query, (symbol,))
            
            if old_symbol_id_result:
                update_dependent_query = f"""
                UPDATE extracted.{table} 
                SET calculated_symbol_id = %s 
                WHERE symbol = %s;
                """
                db.execute_query(update_dependent_query, (new_calc_id, symbol))


def fix_all_duplicates(db):
    """Fix all duplicate calculated symbol IDs."""
    print("Identifying duplicate calculated symbol IDs...")
    
    duplicates = identify_duplicates(db)
    
    if not duplicates:
        print("✅ No duplicates found!")
        return True
    
    print(f"Found {len(duplicates)} duplicate groups affecting {sum(d['count'] for d in duplicates)} symbols")
    
    print("\nDuplicate Summary:")
    for dup in duplicates:
        print(f"  ID {dup['calculated_symbol_id']}: {dup['symbols']}")
    
    print(f"\nResolving {len(duplicates)} duplicate groups...")
    
    for duplicate_group in duplicates:
        try:
            resolve_duplicate_group(db, duplicate_group)
        except Exception as e:
            print(f"❌ Error resolving group {duplicate_group['symbols']}: {e}")
            return False
    
    print(f"\n✅ All {len(duplicates)} duplicate groups resolved!")
    return True


def verify_no_duplicates(db):
    """Verify that no duplicates remain."""
    duplicates = identify_duplicates(db)
    
    if duplicates:
        print(f"❌ {len(duplicates)} duplicate groups still exist:")
        for dup in duplicates:
            print(f"  ID {dup['calculated_symbol_id']}: {dup['symbols']}")
        return False
    else:
        print("✅ No duplicate calculated symbol IDs found!")
        return True


def show_resolution_summary(db):
    """Show summary of the resolution."""
    print("\nResolution Summary:")
    
    # Count total symbols and unique calculated IDs
    summary_query = """
    SELECT 
        COUNT(*) as total_symbols,
        COUNT(DISTINCT calculated_symbol_id) as unique_calc_ids
    FROM extracted.listing_status
    WHERE calculated_symbol_id IS NOT NULL;
    """
    
    result = db.execute_query(summary_query)
    total_symbols = result[0][0]
    unique_calc_ids = result[0][1]
    
    print(f"  Total symbols: {total_symbols:,}")
    print(f"  Unique calculated IDs: {unique_calc_ids:,}")
    print(f"  Match: {'✅ Yes' if total_symbols == unique_calc_ids else '❌ No'}")
    
    # Show range of calculated IDs
    range_query = """
    SELECT 
        MIN(calculated_symbol_id) as min_id,
        MAX(calculated_symbol_id) as max_id
    FROM extracted.listing_status
    WHERE calculated_symbol_id IS NOT NULL;
    """
    
    range_result = db.execute_query(range_query)
    min_id = range_result[0][0]
    max_id = range_result[0][1]
    
    print(f"  ID range: {min_id:,} to {max_id:,}")
    print(f"  Range span: {max_id - min_id:,}")


def main():
    """Fix duplicate calculated symbol IDs."""
    print("🔧 FIXING DUPLICATE CALCULATED SYMBOL IDs")
    print("=" * 60)
    print("This will resolve conflicts before final migration.")
    print()
    
    db = PostgresDatabaseManager()
    db.connect()
    
    try:
        # Fix duplicates
        if not fix_all_duplicates(db):
            print("❌ Failed to fix duplicates")
            return False
        
        # Verify no duplicates remain
        if not verify_no_duplicates(db):
            print("❌ Duplicates still exist after resolution")
            return False
        
        # Show summary
        show_resolution_summary(db)
        
        print("\n🎉 DUPLICATE RESOLUTION COMPLETED!")
        print("✅ All calculated symbol IDs are now unique")
        print("✅ Ready to proceed with final migration")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during duplicate resolution: {e}")
        return False
        
    finally:
        db.close()


if __name__ == "__main__":
    success = main()
    print("\n" + "=" * 60)
    if success:
        print("🚀 Ready to run final migration!")
        print("Run simplified_final_migration.py to complete the process.")
    else:
        print("🔴 Duplicate resolution failed - check errors above")
    
    sys.exit(0 if success else 1)
