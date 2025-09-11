#!/usr/bin/env python3

import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from db.postgres_database_manager import PostgresDatabaseManager

def main():
    print("Checking data validity for migration...")
    
    db_manager = PostgresDatabaseManager()
    with db_manager as db:
        # Get total records
        total = db.fetch_query('SELECT COUNT(*) FROM extracted.earnings_call_transcripts')[0][0]
        
        # Get valid records (with existing symbol_ids)
        valid = db.fetch_query('''
            SELECT COUNT(*) 
            FROM extracted.earnings_call_transcripts e
            INNER JOIN extracted.listing_status ls ON e.symbol_id = ls.symbol_id
        ''')[0][0]
        
        invalid = total - valid
        
        print(f"Total records: {total:,}")
        print(f"Valid records: {valid:,}")
        print(f"Invalid records: {invalid:,}")
        print(f"Percentage valid: {(valid/total*100):.1f}%")

if __name__ == "__main__":
    main()
