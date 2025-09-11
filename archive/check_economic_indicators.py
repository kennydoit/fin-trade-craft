#!/usr/bin/env python3

import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from db.postgres_database_manager import PostgresDatabaseManager

def main():
    print("Checking economic_indicators table sizes...")
    
    db_manager = PostgresDatabaseManager()
    with db_manager as db:
        # Check table sizes
        size_query = """
            SELECT 
                schemaname, 
                tablename, 
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                pg_total_relation_size(schemaname||'.'||tablename) as bytes
            FROM pg_tables 
            WHERE tablename LIKE '%economic_indicators%' 
            ORDER BY bytes DESC
        """
        
        sizes = db.fetch_query(size_query)
        print("\n=== TABLE SIZES ===")
        for row in sizes:
            schema, table, size, bytes_size = row
            print(f"{schema}.{table}: {size} ({bytes_size:,} bytes)")
        
        # Check record counts
        print("\n=== RECORD COUNTS ===")
        for schema, table, _, _ in sizes:
            count_query = f"SELECT COUNT(*) FROM {schema}.{table}"
            result = db.fetch_query(count_query)
            count = result[0][0] if result else 0
            print(f"{schema}.{table}: {count:,} records")
        
        # Check for duplicate data in source table
        if any(row[0] == 'source' and row[1] == 'economic_indicators' for row in sizes):
            print("\n=== CHECKING FOR DUPLICATES IN SOURCE TABLE ===")
            
            # Check for duplicate indicator/date combinations
            dup_query = """
                SELECT 
                    indicator_name,
                    date,
                    COUNT(*) as duplicate_count
                FROM source.economic_indicators 
                GROUP BY indicator_name, date 
                HAVING COUNT(*) > 1 
                ORDER BY duplicate_count DESC 
                LIMIT 10
            """
            
            duplicates = db.fetch_query(dup_query)
            if duplicates:
                print("Found duplicate indicator/date combinations:")
                for row in duplicates:
                    indicator, date, count = row
                    print(f"  {indicator} on {date}: {count} records")
            else:
                print("No duplicate indicator/date combinations found")
            
            # Check for records with same content hash
            hash_dup_query = """
                SELECT 
                    content_hash,
                    COUNT(*) as duplicate_count
                FROM source.economic_indicators 
                GROUP BY content_hash 
                HAVING COUNT(*) > 1 
                ORDER BY duplicate_count DESC 
                LIMIT 10
            """
            
            hash_duplicates = db.fetch_query(hash_dup_query)
            if hash_duplicates:
                print("\nFound duplicate content hashes:")
                for row in hash_duplicates:
                    hash_val, count = row
                    print(f"  Hash {hash_val}: {count} records")
            
            # Check run_id distribution
            run_id_query = """
                SELECT 
                    source_run_id,
                    COUNT(*) as record_count,
                    MIN(created_at) as first_created,
                    MAX(created_at) as last_created
                FROM source.economic_indicators 
                GROUP BY source_run_id 
                ORDER BY record_count DESC 
                LIMIT 10
            """
            
            run_ids = db.fetch_query(run_id_query)
            if run_ids:
                print(f"\nRun ID distribution (top 10):")
                for row in run_ids:
                    run_id, count, first, last = row
                    print(f"  {run_id}: {count:,} records ({first} to {last})")

if __name__ == "__main__":
    main()
