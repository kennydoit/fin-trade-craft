"""
Extend calculated symbol ID to all dependent tables.

This script:
1. Adds calculated_symbol_id column as the FIRST column in all dependent tables
2. Populates calculated IDs by joining with listing_status
3. Verifies data integrity across all tables
4. Prepares for migration to replace symbol_id with calculated_symbol_id

Tables to update:
- extracted.overview
- extracted.time_series_daily_adjusted  
- extracted.income_statement
- extracted.balance_sheet
- extracted.cash_flow
- extracted.historical_options
- extracted.realtime_options
- extracted.insider_transactions
- extracted.earnings_call_transcripts
"""

import sys
import os
import pandas as pd
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from db.postgres_database_manager import PostgresDatabaseManager


def get_dependent_tables():
    """Get list of tables that have foreign key references to listing_status.symbol_id."""
    return [
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


def add_calculated_symbol_id_column(db, table_name):
    """Add calculated_symbol_id as the first column in the specified table."""
    print(f"Adding calculated_symbol_id column to {table_name}...")
    
    # Check if column already exists
    check_query = """
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_schema = 'extracted' 
    AND table_name = %s 
    AND column_name = 'calculated_symbol_id';
    """
    
    result = db.execute_query(check_query, (table_name,))
    
    if result and len(result) > 0:
        print(f"  ✓ calculated_symbol_id column already exists in {table_name}")
        return True
    
    # Add the column as the first column
    add_column_query = f"""
    ALTER TABLE extracted.{table_name} 
    ADD COLUMN calculated_symbol_id BIGINT FIRST;
    """
    
    try:
        db.execute_query(add_column_query)
        print(f"  ✓ Added calculated_symbol_id column to {table_name}")
        return True
    except Exception as e:
        # PostgreSQL doesn't support FIRST keyword, so add normally then reorganize
        print(f"  Note: Adding column normally (will reorganize later): {e}")
        try:
            add_column_query_pg = f"""
            ALTER TABLE extracted.{table_name} 
            ADD COLUMN calculated_symbol_id BIGINT;
            """
            db.execute_query(add_column_query_pg)
            print(f"  ✓ Added calculated_symbol_id column to {table_name}")
            return True
        except Exception as e2:
            print(f"  ✗ Error adding column to {table_name}: {e2}")
            return False


def populate_calculated_symbol_id(db, table_name):
    """Populate calculated_symbol_id by joining with listing_status table."""
    print(f"Populating calculated_symbol_id for {table_name}...")
    
    # Get row count before update
    count_query = f"SELECT COUNT(*) FROM extracted.{table_name};"
    before_count = db.execute_query(count_query)[0][0]
    
    # Update calculated_symbol_id using JOIN with listing_status
    update_query = f"""
    UPDATE extracted.{table_name} t
    SET calculated_symbol_id = ls.calculated_symbol_id
    FROM extracted.listing_status ls
    WHERE t.symbol_id = ls.symbol_id
    AND ls.calculated_symbol_id IS NOT NULL;
    """
    
    try:
        db.execute_query(update_query)
        
        # Verify update worked
        updated_count_query = f"""
        SELECT COUNT(*) 
        FROM extracted.{table_name} 
        WHERE calculated_symbol_id IS NOT NULL;
        """
        updated_count = db.execute_query(updated_count_query)[0][0]
        
        print(f"  ✓ Updated {updated_count:,} of {before_count:,} rows in {table_name}")
        
        if updated_count != before_count:
            missing_count = before_count - updated_count
            print(f"  ⚠️  {missing_count} rows in {table_name} have symbol_ids not found in listing_status")
        
        return True
        
    except Exception as e:
        print(f"  ✗ Error updating {table_name}: {e}")
        return False


def verify_table_integrity(db, table_name):
    """Verify data integrity for the updated table."""
    print(f"Verifying integrity for {table_name}...")
    
    # Check for orphaned records (symbol_id exists but no calculated_symbol_id)
    orphan_query = f"""
    SELECT COUNT(*) 
    FROM extracted.{table_name} t
    LEFT JOIN extracted.listing_status ls ON t.symbol_id = ls.symbol_id
    WHERE ls.symbol_id IS NULL;
    """
    
    orphan_count = db.execute_query(orphan_query)[0][0]
    
    if orphan_count > 0:
        print(f"  ⚠️  {orphan_count} orphaned records found in {table_name}")
        
        # Show sample orphaned records
        sample_orphan_query = f"""
        SELECT DISTINCT t.symbol_id
        FROM extracted.{table_name} t
        LEFT JOIN extracted.listing_status ls ON t.symbol_id = ls.symbol_id
        WHERE ls.symbol_id IS NULL
        LIMIT 10;
        """
        
        orphan_samples = db.execute_query(sample_orphan_query)
        print(f"    Sample orphaned symbol_ids: {[row[0] for row in orphan_samples]}")
    else:
        print(f"  ✓ No orphaned records in {table_name}")
    
    # Check calculated_symbol_id population rate
    total_query = f"SELECT COUNT(*) FROM extracted.{table_name};"
    with_calc_id_query = f"SELECT COUNT(*) FROM extracted.{table_name} WHERE calculated_symbol_id IS NOT NULL;"
    
    total_rows = db.execute_query(total_query)[0][0]
    with_calc_rows = db.execute_query(with_calc_id_query)[0][0]
    
    population_rate = (with_calc_rows / total_rows * 100) if total_rows > 0 else 0
    print(f"  Population rate: {with_calc_rows:,}/{total_rows:,} ({population_rate:.1f}%)")
    
    return orphan_count == 0


def show_sample_data(db, table_name, limit=5):
    """Show sample data from the table with both old and new symbol IDs."""
    print(f"Sample data from {table_name}:")
    
    # Get table columns to build appropriate query
    columns_query = """
    SELECT column_name, ordinal_position, data_type
    FROM information_schema.columns 
    WHERE table_schema = 'extracted' 
    AND table_name = %s 
    ORDER BY ordinal_position
    LIMIT 10;
    """
    
    columns_result = db.execute_query(columns_query, (table_name,))
    column_names = [row[0] for row in columns_result[:6]]  # First 6 columns
    
    # Build sample query
    columns_str = ', '.join(column_names)
    sample_query = f"""
    SELECT {columns_str}
    FROM extracted.{table_name} 
    WHERE calculated_symbol_id IS NOT NULL
    ORDER BY calculated_symbol_id 
    LIMIT {limit};
    """
    
    try:
        sample_df = db.fetch_dataframe(sample_query)
        print(sample_df.to_string(index=False))
        print()
    except Exception as e:
        print(f"  ✗ Error fetching sample data: {e}")


def analyze_cross_table_consistency(db):
    """Analyze consistency of calculated_symbol_id across all tables."""
    print("Analyzing cross-table consistency...")
    
    tables = get_dependent_tables()
    
    # Get calculated_symbol_id counts per table
    print("Calculated Symbol ID counts by table:")
    print(f"{'Table':<25} {'Total Rows':<12} {'With Calc ID':<12} {'Coverage %':<10}")
    print("-" * 65)
    
    for table in tables:
        total_query = f"SELECT COUNT(*) FROM extracted.{table};"
        calc_id_query = f"SELECT COUNT(*) FROM extracted.{table} WHERE calculated_symbol_id IS NOT NULL;"
        
        try:
            total_count = db.execute_query(total_query)[0][0]
            calc_id_count = db.execute_query(calc_id_query)[0][0]
            coverage = (calc_id_count / total_count * 100) if total_count > 0 else 0
            
            print(f"{table:<25} {total_count:<12,} {calc_id_count:<12,} {coverage:<10.1f}")
            
        except Exception as e:
            print(f"{table:<25} Error: {e}")
    
    print()
    
    # Check for calculated_symbol_ids that exist in dependent tables but not in listing_status
    print("Checking for invalid calculated_symbol_ids...")
    
    for table in tables:
        invalid_query = f"""
        SELECT COUNT(*)
        FROM extracted.{table} t
        LEFT JOIN extracted.listing_status ls ON t.calculated_symbol_id = ls.calculated_symbol_id
        WHERE t.calculated_symbol_id IS NOT NULL 
        AND ls.calculated_symbol_id IS NULL;
        """
        
        try:
            invalid_count = db.execute_query(invalid_query)[0][0]
            if invalid_count > 0:
                print(f"  ⚠️  {table}: {invalid_count} invalid calculated_symbol_ids")
            else:
                print(f"  ✓ {table}: All calculated_symbol_ids valid")
        except Exception as e:
            print(f"  ✗ {table}: Error checking - {e}")


def main():
    """Main function to extend calculated symbol ID to all dependent tables."""
    print("Extending Calculated Symbol ID to All Dependent Tables")
    print("=" * 70)
    
    db = PostgresDatabaseManager()
    db.connect()
    
    try:
        tables = get_dependent_tables()
        success_count = 0
        
        print(f"Processing {len(tables)} dependent tables...\n")
        
        # Step 1: Add calculated_symbol_id columns
        for table in tables:
            print(f"--- Processing {table} ---")
            
            if add_calculated_symbol_id_column(db, table):
                if populate_calculated_symbol_id(db, table):
                    if verify_table_integrity(db, table):
                        success_count += 1
                    show_sample_data(db, table)
                else:
                    print(f"  ✗ Failed to populate {table}")
            else:
                print(f"  ✗ Failed to add column to {table}")
            
            print()
        
        # Step 2: Cross-table analysis
        analyze_cross_table_consistency(db)
        
        # Step 3: Summary
        print(f"Summary:")
        print(f"  Successfully processed: {success_count}/{len(tables)} tables")
        print(f"  Ready for migration: {'Yes' if success_count == len(tables) else 'No'}")
        
        if success_count == len(tables):
            print("\n✅ All tables ready for calculated symbol ID migration!")
            print("\nNext steps:")
            print("1. Review the data integrity results above")
            print("2. Test queries using calculated_symbol_id")
            print("3. When ready, proceed with full migration to replace symbol_id")
        else:
            print(f"\n⚠️  {len(tables) - success_count} tables need attention before migration")
        
        return success_count == len(tables)
        
    except Exception as e:
        print(f"✗ Error during processing: {e}")
        return False
        
    finally:
        db.close()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
