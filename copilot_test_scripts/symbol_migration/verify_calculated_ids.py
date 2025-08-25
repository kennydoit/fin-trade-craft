"""
Simple verification script to check calculated symbol IDs in listing_status table.

This script provides quick queries to verify the calculated symbol ID implementation
without making any modifications to the database.
"""

import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from db.postgres_database_manager import PostgresDatabaseManager


def check_column_exists():
    """Check if calculated_symbol_id column exists."""
    db = PostgresDatabaseManager()
    db.connect()
    
    try:
        query = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns 
        WHERE table_schema = 'extracted' 
        AND table_name = 'listing_status' 
        AND column_name IN ('symbol_id', 'calculated_symbol_id', 'symbol')
        ORDER BY column_name;
        """
        
        result = db.fetch_dataframe(query)
        print("Columns in listing_status table:")
        print(result.to_string(index=False))
        print()
    finally:
        db.close()


def check_sample_data():
    """Show sample data with both old and new IDs."""
    db = PostgresDatabaseManager()
    db.connect()
    
    try:
        query = """
        SELECT 
            symbol,
            symbol_id,
            calculated_symbol_id,
            CASE 
                WHEN calculated_symbol_id IS NULL THEN 'Missing'
                ELSE 'Present'
            END as calc_id_status
        FROM extracted.listing_status 
        ORDER BY symbol 
        LIMIT 20;
        """
        
        result = db.fetch_dataframe(query)
        print("Sample data from listing_status:")
        print(result.to_string(index=False))
        print()
    finally:
        db.close()


def check_alphabetical_ordering():
    """Verify alphabetical ordering by calculated ID."""
    db = PostgresDatabaseManager()
    db.connect()
    
    try:
        query = """
        SELECT 
            symbol,
            calculated_symbol_id
        FROM extracted.listing_status 
        WHERE calculated_symbol_id IS NOT NULL
        ORDER BY calculated_symbol_id 
        LIMIT 15;
        """
        
        result = db.fetch_dataframe(query)
        print("First 15 symbols ordered by calculated_symbol_id:")
        print(result.to_string(index=False))
        print()
    finally:
        db.close()


def check_uniqueness():
    """Check for duplicate calculated symbol IDs."""
    db = PostgresDatabaseManager()
    db.connect()
    
    try:
        query = """
        SELECT 
            calculated_symbol_id,
            COUNT(*) as symbol_count,
            STRING_AGG(symbol, ', ') as symbols
        FROM extracted.listing_status 
        WHERE calculated_symbol_id IS NOT NULL
        GROUP BY calculated_symbol_id 
        HAVING COUNT(*) > 1
        ORDER BY calculated_symbol_id;
        """
        
        result = db.fetch_dataframe(query)
        
        if result.empty:
            print("✓ No duplicate calculated symbol IDs found")
        else:
            print("⚠️  Duplicate calculated symbol IDs found:")
            print(result.to_string(index=False))
        print()
    finally:
        db.close()


def summary_stats():
    """Show summary statistics."""
    db = PostgresDatabaseManager()
    db.connect()
    
    try:
        query = """
        SELECT 
            COUNT(*) as total_symbols,
            COUNT(calculated_symbol_id) as with_calc_id,
            COUNT(*) - COUNT(calculated_symbol_id) as missing_calc_id,
            MIN(calculated_symbol_id) as min_calc_id,
            MAX(calculated_symbol_id) as max_calc_id
        FROM extracted.listing_status;
        """
        
        result = db.fetch_dataframe(query)
        print("Summary Statistics:")
        print(result.to_string(index=False))
        print()
    finally:
        db.close()


def main():
    """Run all verification checks."""
    print("Calculated Symbol ID Verification")
    print("=" * 50)
    
    try:
        check_column_exists()
        check_sample_data()
        check_alphabetical_ordering()
        check_uniqueness()
        summary_stats()
        
        print("Verification complete. Review results above.")
        
    except Exception as e:
        print(f"Error during verification: {e}")
        return False
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
