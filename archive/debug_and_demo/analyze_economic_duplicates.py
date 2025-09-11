#!/usr/bin/env python3

import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from db.postgres_database_manager import PostgresDatabaseManager

def main():
    print("Checking economic_indicators table structures...")
    
    db_manager = PostgresDatabaseManager()
    with db_manager as db:
        # Check source table structure
        print("\n=== SOURCE TABLE STRUCTURE ===")
        cols = db.fetch_query("""
            SELECT column_name, data_type, character_maximum_length 
            FROM information_schema.columns 
            WHERE table_schema = 'source' AND table_name = 'economic_indicators' 
            ORDER BY ordinal_position
        """)
        
        if cols:
            for row in cols:
                col_name, data_type, max_length = row
                length_info = f"({max_length})" if max_length else ""
                print(f"  {col_name}: {data_type}{length_info}")
        else:
            print("  Table not found or no columns")
        
        # Check extracted table structure
        print("\n=== EXTRACTED TABLE STRUCTURE ===")
        ecols = db.fetch_query("""
            SELECT column_name, data_type, character_maximum_length 
            FROM information_schema.columns 
            WHERE table_schema = 'extracted' AND table_name = 'economic_indicators' 
            ORDER BY ordinal_position
        """)
        
        if ecols:
            for row in ecols:
                col_name, data_type, max_length = row
                length_info = f"({max_length})" if max_length else ""
                print(f"  {col_name}: {data_type}{length_info}")
        else:
            print("  Table not found or no columns")
        
        # Check for the worst duplicates
        print("\n=== WORST DUPLICATE ANALYSIS ===")
        worst_dups = db.fetch_query("""
            SELECT 
                content_hash,
                COUNT(*) as dup_count,
                SUM(LENGTH(value::text)) as total_text_size
            FROM source.economic_indicators 
            GROUP BY content_hash 
            HAVING COUNT(*) > 1 
            ORDER BY dup_count DESC 
            LIMIT 5
        """)
        
        if worst_dups:
            total_wasted_space = 0
            for row in worst_dups:
                hash_val, dup_count, text_size = row
                wasted = text_size * (dup_count - 1)  # Space wasted by duplicates
                total_wasted_space += wasted
                print(f"  Hash {hash_val}: {dup_count} duplicates, ~{wasted:,} bytes wasted")
            print(f"\n  Total wasted space from top 5 duplicates: ~{total_wasted_space:,} bytes")
        
        # Check if there's a pattern to the duplicates
        print("\n=== DUPLICATE PATTERN ANALYSIS ===")
        sample_dup = db.fetch_query("""
            SELECT 
                indicator_name, date, value, content_hash, created_at, updated_at
            FROM source.economic_indicators 
            WHERE content_hash IN (
                SELECT content_hash 
                FROM source.economic_indicators 
                GROUP BY content_hash 
                HAVING COUNT(*) > 1 
                LIMIT 1
            )
            ORDER BY created_at
        """)
        
        if sample_dup:
            print("  Sample duplicate records:")
            for i, row in enumerate(sample_dup):
                indicator, date, value, hash_val, created, updated = row
                print(f"    {i+1}. {indicator} | {date} | {value} | Created: {created}")

if __name__ == "__main__":
    main()
