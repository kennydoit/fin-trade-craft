#!/usr/bin/env python3

import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from db.postgres_database_manager import PostgresDatabaseManager

def main():
    print("Checking JSON response sizes in source.economic_indicators...")
    
    db_manager = PostgresDatabaseManager()
    with db_manager as db:
        # Check largest JSON responses
        print("\n=== LARGEST JSON RESPONSES ===")
        result = db.fetch_query("""
            SELECT 
                indicator_name, 
                LENGTH(api_response::text) as json_size 
            FROM source.economic_indicators 
            ORDER BY json_size DESC 
            LIMIT 5
        """)
        
        total_json_size = 0
        for row in result:
            indicator, json_size = row
            total_json_size += json_size
            print(f"  {indicator}: {json_size:,} characters")
        
        # Check average JSON size
        avg_result = db.fetch_query("SELECT AVG(LENGTH(api_response::text)) FROM source.economic_indicators")
        avg_size = int(avg_result[0][0]) if avg_result[0][0] else 0
        print(f"\nAverage JSON size: {avg_size:,} characters")
        
        # Calculate total JSON storage
        total_result = db.fetch_query("SELECT SUM(LENGTH(api_response::text)) FROM source.economic_indicators")
        total_size = total_result[0][0] if total_result[0][0] else 0
        print(f"Total JSON storage: {total_size:,} characters (~{total_size/1024/1024:.1f} MB)")
        
        # Check if we can see what's in a sample JSON
        sample_result = db.fetch_query("""
            SELECT 
                indicator_name, 
                LEFT(api_response::text, 200) as json_sample,
                LENGTH(api_response::text) as json_size
            FROM source.economic_indicators 
            ORDER BY LENGTH(api_response::text) DESC 
            LIMIT 1
        """)
        
        if sample_result:
            indicator, json_sample, size = sample_result[0]
            print(f"\nSample JSON from {indicator} ({size:,} chars):")
            print(f"  {json_sample}...")

if __name__ == "__main__":
    main()
