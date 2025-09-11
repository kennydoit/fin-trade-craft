"""
Test script to add calculated symbol ID to listing_status table.

This script:
1. Adds a calculated_symbol_id column to listing_status
2. Populates it using the symbol ID calculation function
3. Verifies uniqueness and alphabetical ordering
4. Provides analysis comparing old vs new IDs

Run this as a test before implementing the full migration.
"""

import sys
import os
import pandas as pd
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from data_pipeline.common.symbol_id_calculator import calculate_symbol_id, validate_symbol_id_uniqueness, test_alphabetical_ordering
from db.postgres_database_manager import PostgresDatabaseManager


def add_calculated_symbol_id_column():
    """Add calculated_symbol_id column to listing_status table."""
    db = PostgresDatabaseManager()
    db.connect()  # Explicitly connect
    
    try:
        print("Adding calculated_symbol_id column to listing_status table...")
        
        # Check if column already exists
        check_query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'extracted' 
        AND table_name = 'listing_status' 
        AND column_name = 'calculated_symbol_id';
        """
        
        result = db.execute_query(check_query)
        
        if result and len(result) > 0:
            print("✓ calculated_symbol_id column already exists")
            return True
        
        # Add the column
        add_column_query = """
        ALTER TABLE extracted.listing_status 
        ADD COLUMN calculated_symbol_id BIGINT;
        """
        
        db.execute_query(add_column_query)
        print("✓ Added calculated_symbol_id column successfully")
        return True
        
    except Exception as e:
        print(f"✗ Error adding column: {e}")
        return False
    finally:
        db.close()


def populate_calculated_symbol_ids():
    """Populate calculated_symbol_id values for all symbols."""
    db = PostgresDatabaseManager()
    db.connect()  # Explicitly connect
    
    try:
        print("\nPopulating calculated symbol IDs...")
        
        # Get all symbols from listing_status
        query = """
        SELECT symbol_id, symbol 
        FROM extracted.listing_status 
        ORDER BY symbol;
        """
        
        symbols_df = db.fetch_dataframe(query)
        
        if symbols_df.empty:
            print("✗ No symbols found in listing_status table")
            return False
        
        print(f"Processing {len(symbols_df)} symbols...")
        
        # Calculate IDs for all symbols
        symbols_df['calculated_symbol_id'] = symbols_df['symbol'].apply(calculate_symbol_id)
        
        # Update the database
        update_count = 0
        for _, row in symbols_df.iterrows():
            update_query = """
            UPDATE extracted.listing_status 
            SET calculated_symbol_id = %s 
            WHERE symbol_id = %s;
            """
            
            db.execute_query(update_query, (row['calculated_symbol_id'], row['symbol_id']))
            update_count += 1
        
        print(f"✓ Updated {update_count} symbols with calculated IDs")
        return True
        
    except Exception as e:
        print(f"✗ Error updating symbols: {e}")
        return False
    finally:
        db.close()


def verify_calculated_ids():
    """Verify the calculated IDs for uniqueness and ordering."""
    db = PostgresDatabaseManager()
    db.connect()  # Explicitly connect
    
    try:
        print("\nVerifying calculated symbol IDs...")
        
        # Get all symbols with both old and new IDs
        query = """
        SELECT symbol_id, symbol, calculated_symbol_id 
        FROM extracted.listing_status 
        WHERE calculated_symbol_id IS NOT NULL
        ORDER BY symbol;
        """
        
        df = db.fetch_dataframe(query)
        
        if df.empty:
            print("✗ No calculated symbol IDs found")
            return False
        
        print(f"Analyzing {len(df)} symbols...")
        
        # Test uniqueness
        symbols_list = df['symbol'].tolist()
        uniqueness_test = validate_symbol_id_uniqueness(symbols_list)
        
        print(f"\nUniqueness Test:")
        print(f"  Total symbols: {uniqueness_test['total_symbols']}")
        print(f"  Unique IDs: {uniqueness_test['unique_ids']}")
        print(f"  Valid (no conflicts): {uniqueness_test['is_valid']}")
        
        if not uniqueness_test['is_valid']:
            print("  ⚠️  CONFLICTS FOUND:")
            for conflict in uniqueness_test['conflicts']:
                print(f"    ID {conflict['id']}: {conflict['symbols']}")
        
        # Test alphabetical ordering
        ordering_test = test_alphabetical_ordering(symbols_list)
        
        print(f"\nAlphabetical Ordering Test:")
        print(f"  Orders match: {ordering_test['orders_match']}")
        
        if not ordering_test['orders_match']:
            print("  ⚠️  ORDERING MISMATCH FOUND")
            print(f"  First 10 alphabetical: {ordering_test['alphabetical_order'][:10]}")
            print(f"  First 10 by calc ID: {ordering_test['id_order'][:10]}")
        
        # Show sample comparisons
        print(f"\nSample Symbol ID Comparisons:")
        print(f"{'Symbol':<8} {'Old ID':<10} {'Calc ID':<12} {'Difference':<12}")
        print("-" * 50)
        
        for _, row in df.head(15).iterrows():
            old_id = row['symbol_id']
            calc_id = row['calculated_symbol_id']
            diff = calc_id - old_id
            print(f"{row['symbol']:<8} {old_id:<10} {calc_id:<12} {diff:+<12}")
        
        # Check ordering by calculated ID
        print(f"\nOrdering Verification (by calculated_symbol_id):")
        ordered_query = """
        SELECT symbol, calculated_symbol_id 
        FROM extracted.listing_status 
        WHERE calculated_symbol_id IS NOT NULL
        ORDER BY calculated_symbol_id 
        LIMIT 20;
        """
        
        ordered_df = db.fetch_dataframe(ordered_query)
        
        print("First 20 symbols by calculated ID:")
        for _, row in ordered_df.iterrows():
            print(f"  {row['symbol']:<8} -> {row['calculated_symbol_id']:,}")
        
        return uniqueness_test['is_valid'] and ordering_test['orders_match']
        
    except Exception as e:
        print(f"✗ Error during verification: {e}")
        return False
    finally:
        db.close()


def analyze_id_distribution():
    """Analyze the distribution and patterns of calculated IDs."""
    db = PostgresDatabaseManager()
    db.connect()  # Explicitly connect
    
    try:
        print("\nAnalyzing ID Distribution...")
        
        query = """
        SELECT 
            symbol,
            symbol_id,
            calculated_symbol_id,
            LENGTH(symbol) as symbol_length
        FROM extracted.listing_status 
        WHERE calculated_symbol_id IS NOT NULL
        ORDER BY calculated_symbol_id;
        """
        
        df = db.fetch_dataframe(query)
        
        if df.empty:
            return
        
        # Distribution by symbol length
        print("\nDistribution by Symbol Length:")
        length_dist = df['symbol_length'].value_counts().sort_index()
        for length, count in length_dist.items():
            print(f"  {length} characters: {count:,} symbols")
        
        # ID ranges
        print(f"\nID Ranges:")
        print(f"  Minimum calculated ID: {df['calculated_symbol_id'].min():,}")
        print(f"  Maximum calculated ID: {df['calculated_symbol_id'].max():,}")
        print(f"  Range span: {df['calculated_symbol_id'].max() - df['calculated_symbol_id'].min():,}")
        
        # Check for gaps or patterns
        id_diffs = df['calculated_symbol_id'].diff().dropna()
        print(f"\nID Gap Analysis:")
        print(f"  Minimum gap: {id_diffs.min():,}")
        print(f"  Maximum gap: {id_diffs.max():,}")
        print(f"  Average gap: {id_diffs.mean():.0f}")
        
    except Exception as e:
        print(f"✗ Error during analysis: {e}")
    finally:
        db.close()


def main():
    """Run the complete test of calculated symbol IDs."""
    print("Testing Calculated Symbol ID Implementation")
    print("=" * 60)
    
    try:
        # Step 1: Add column
        if not add_calculated_symbol_id_column():
            return False
        
        # Step 2: Populate values
        if not populate_calculated_symbol_ids():
            return False
        
        # Step 3: Verify results
        if not verify_calculated_ids():
            print("\n⚠️  Verification failed - review results above")
            return False
        
        # Step 4: Analyze distribution
        analyze_id_distribution()
        
        print("\n✓ All tests passed! Calculated symbol IDs are working correctly.")
        print("\nNext steps:")
        print("1. Review the sample comparisons above")
        print("2. Verify alphabetical ordering meets your needs")
        print("3. If satisfied, proceed with full migration plan")
        
        return True
        
    except Exception as e:
        print(f"\n✗ Error during testing: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
