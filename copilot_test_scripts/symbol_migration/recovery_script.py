"""
Recovery Script: Restore calculated_symbol_id to listing_status

The migration partially failed, leaving the database in an inconsistent state.
This script will:
1. Add back the calculated_symbol_id column to listing_status
2. Repopulate it with values
3. Fix duplicates
4. Prepare for a clean migration

This recovers from the failed migration attempt.
"""

import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from db.postgres_database_manager import PostgresDatabaseManager
from data_pipeline.common.symbol_id_calculator import calculate_symbol_id


def check_current_state(db):
    """Check the current state of the database."""
    print("Checking current database state...")
    
    # Check listing_status columns
    ls_columns_query = """
    SELECT column_name, data_type
    FROM information_schema.columns 
    WHERE table_schema = 'extracted' 
    AND table_name = 'listing_status'
    ORDER BY ordinal_position;
    """
    
    ls_columns = db.execute_query(ls_columns_query)
    print("listing_status columns:")
    for col in ls_columns:
        print(f"  {col[0]} ({col[1]})")
    
    # Check if calculated_symbol_id exists in listing_status
    has_calc_id = any(col[0] == 'calculated_symbol_id' for col in ls_columns)
    has_symbol_id = any(col[0] == 'symbol_id' for col in ls_columns)
    
    print(f"\nCurrent state:")
    print(f"  listing_status has symbol_id: {has_symbol_id}")
    print(f"  listing_status has calculated_symbol_id: {has_calc_id}")
    
    return {
        'has_symbol_id': has_symbol_id,
        'has_calculated_symbol_id': has_calc_id,
        'columns': [col[0] for col in ls_columns]
    }


def restore_calculated_symbol_id_column(db):
    """Restore the calculated_symbol_id column to listing_status."""
    print("\nRestoring calculated_symbol_id column to listing_status...")
    
    # Add the column back
    add_column_query = """
    ALTER TABLE extracted.listing_status 
    ADD COLUMN calculated_symbol_id BIGINT;
    """
    
    try:
        db.execute_query(add_column_query)
        print("  ✅ Added calculated_symbol_id column")
    except Exception as e:
        if "already exists" in str(e):
            print("  ✅ calculated_symbol_id column already exists")
        else:
            print(f"  ❌ Error adding column: {e}")
            return False
    
    # Populate with calculated values
    print("  Populating calculated_symbol_id values...")
    
    # Get all symbols
    symbols_query = "SELECT symbol_id, symbol FROM extracted.listing_status ORDER BY symbol;"
    symbols_result = db.execute_query(symbols_query)
    
    update_count = 0
    for row in symbols_result:
        old_symbol_id = row[0]
        symbol = row[1]
        calc_id = calculate_symbol_id(symbol)
        
        update_query = """
        UPDATE extracted.listing_status 
        SET calculated_symbol_id = %s 
        WHERE symbol_id = %s;
        """
        
        db.execute_query(update_query, (calc_id, old_symbol_id))
        update_count += 1
    
    print(f"  ✅ Populated {update_count:,} calculated symbol IDs")
    return True


def fix_duplicates_with_incremental_ids(db):
    """Fix duplicates by assigning incremental IDs to variations."""
    print("\nFixing duplicate calculated symbol IDs...")
    
    # Find duplicates using a simpler approach
    duplicates_query = """
    SELECT calculated_symbol_id, COUNT(*) as count
    FROM extracted.listing_status
    WHERE calculated_symbol_id IS NOT NULL
    GROUP BY calculated_symbol_id
    HAVING COUNT(*) > 1
    ORDER BY calculated_symbol_id;
    """
    
    duplicates_result = db.execute_query(duplicates_query)
    
    if not duplicates_result:
        print("  ✅ No duplicates found!")
        return True
    
    print(f"  Found {len(duplicates_result)} duplicate groups")
    
    # Process each duplicate group
    for row in duplicates_result:
        calc_id = row[0]
        count = row[1]
        
        print(f"    Processing calc_id {calc_id} with {count} symbols")
        
        # Get all symbols with this calc_id
        symbols_query = """
        SELECT symbol_id, symbol 
        FROM extracted.listing_status 
        WHERE calculated_symbol_id = %s 
        ORDER BY LENGTH(symbol), symbol;
        """
        
        symbols_result = db.execute_query(symbols_query, (calc_id,))
        
        if len(symbols_result) <= 1:
            continue
        
        # Keep the first symbol with original calc_id, increment others
        canonical_symbol_id = symbols_result[0][0]
        canonical_symbol = symbols_result[0][1]
        
        print(f"      Canonical: {canonical_symbol} (keeps {calc_id})")
        
        # Update the other symbols
        for i, (sym_id, symbol) in enumerate(symbols_result[1:], 1):
            new_calc_id = calc_id + i
            
            # Make sure this new calc_id doesn't conflict
            max_attempts = 1000
            attempts = 0
            while attempts < max_attempts:
                conflict_check = db.execute_query(
                    "SELECT COUNT(*) FROM extracted.listing_status WHERE calculated_symbol_id = %s;",
                    (new_calc_id,)
                )
                if conflict_check[0][0] == 0:
                    break
                new_calc_id += 1
                attempts += 1
            
            if attempts >= max_attempts:
                print(f"        ❌ Could not find unique ID for {symbol}")
                continue
            
            # Update listing_status
            update_query = """
            UPDATE extracted.listing_status 
            SET calculated_symbol_id = %s 
            WHERE symbol_id = %s;
            """
            db.execute_query(update_query, (new_calc_id, sym_id))
            
            print(f"      Updated: {symbol} -> {new_calc_id}")
            
            # Update all dependent tables
            dependent_tables = [
                'overview', 'time_series_daily_adjusted', 'income_statement',
                'balance_sheet', 'cash_flow', 'historical_options',
                'realtime_options', 'insider_transactions', 'earnings_call_transcripts'
            ]
            
            for table in dependent_tables:
                table_update_query = f"""
                UPDATE extracted.{table} 
                SET calculated_symbol_id = %s 
                WHERE symbol_id = %s;
                """
                
                try:
                    db.execute_query(table_update_query, (new_calc_id, sym_id))
                except Exception as e:
                    print(f"        Warning: {table} update failed: {e}")
    
    print("  ✅ All duplicates resolved!")
    return True


def verify_uniqueness(db):
    """Verify that all calculated symbol IDs are now unique."""
    print("\nVerifying uniqueness...")
    
    uniqueness_query = """
    SELECT 
        COUNT(*) as total_symbols,
        COUNT(DISTINCT calculated_symbol_id) as unique_calc_ids
    FROM extracted.listing_status
    WHERE calculated_symbol_id IS NOT NULL;
    """
    
    result = db.execute_query(uniqueness_query)
    total = result[0][0]
    unique = result[0][1]
    
    print(f"  Total symbols: {total:,}")
    print(f"  Unique calculated IDs: {unique:,}")
    
    if total == unique:
        print("  ✅ All calculated symbol IDs are unique!")
        return True
    else:
        print(f"  ❌ {total - unique} duplicates still exist!")
        return False


def main():
    """Execute the recovery process."""
    print("🔄 DATABASE RECOVERY: Restoring calculated_symbol_id")
    print("=" * 60)
    print("Recovering from partial migration failure...")
    print()
    
    db = PostgresDatabaseManager()
    db.connect()
    
    try:
        # Check current state
        state = check_current_state(db)
        
        if not state['has_calculated_symbol_id']:
            # Restore calculated_symbol_id column
            if not restore_calculated_symbol_id_column(db):
                print("❌ Failed to restore calculated_symbol_id column")
                return False
        else:
            print("✅ calculated_symbol_id column already exists")
        
        # Fix duplicates
        if not fix_duplicates_with_incremental_ids(db):
            print("❌ Failed to fix duplicates")
            return False
        
        # Verify uniqueness
        if not verify_uniqueness(db):
            print("❌ Uniqueness verification failed")
            return False
        
        print("\n🎉 RECOVERY COMPLETED SUCCESSFULLY!")
        print("✅ calculated_symbol_id column restored")
        print("✅ All duplicates resolved")
        print("✅ Database ready for migration")
        
        return True
        
    except Exception as e:
        print(f"❌ Recovery failed: {e}")
        return False
        
    finally:
        db.close()


if __name__ == "__main__":
    success = main()
    print("\n" + "=" * 60)
    if success:
        print("🚀 Database recovered! Ready to retry migration.")
        print("Run simplified_final_migration.py to complete the process.")
    else:
        print("🔴 Recovery failed - manual intervention may be required")
    
    sys.exit(0 if success else 1)
