#!/usr/bin/env python3

import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from db.postgres_database_manager import PostgresDatabaseManager

def main():
    print("Checking database schema...")
    
    db_manager = PostgresDatabaseManager()
    with db_manager as db:
        # Check for listing_status table
        tables_to_check = [
            'listing_status',
            'extracted.listing_status', 
            'source.extraction_watermarks',
            'extracted.earnings_call_transcripts',
            'source.earnings_call_transcripts'
        ]
        
        for table in tables_to_check:
            exists = db.table_exists(table)
            print(f"{table}: {'EXISTS' if exists else 'MISSING'}")
        
        # Get all tables with relevant names
        query = """
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_name IN ('listing_status', 'earnings_call_transcripts', 'extraction_watermarks')
            ORDER BY table_schema, table_name
        """
        result = db.fetch_query(query)
        print(f"\nFound relevant tables: {result}")
        
        # Check specifically for the source schema
        source_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'source'
        """
        source_result = db.fetch_query(source_query)
        print(f"\nTables in source schema: {[row[0] for row in source_result]}")

if __name__ == "__main__":
    main()
