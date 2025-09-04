import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager

with PostgresDatabaseManager() as db:
    # Check api_responses_landing table structure
    result = db.fetch_query("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'api_responses_landing' 
        AND table_schema = 'source' 
        ORDER BY ordinal_position
    """)
    
    print("api_responses_landing columns:")
    for row in result:
        print(f"  {row[0]}: {row[1]}")
    
    # Check recent API responses
    result2 = db.fetch_query("""
        SELECT * 
        FROM source.api_responses_landing 
        WHERE table_name = 'balance_sheet'
        ORDER BY landing_id DESC 
        LIMIT 3
    """)
    
    print(f"\nRecent API responses ({len(result2)} found):")
    for row in result2:
        print(f"  Symbol: {row[2]}, Status: {row[8]}, ID: {row[0]}")
