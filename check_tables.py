"""Quick script to check database tables."""
import os
from dotenv import load_dotenv
from db.postgres_database_manager import PostgresDatabaseManager

load_dotenv()

with PostgresDatabaseManager() as db:
    tables = db.fetch_query("""
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_schema IN ('raw', 'source', 'transforms') 
        ORDER BY table_schema, table_name
    """)
    
    print("\n📊 Database Tables:")
    print("=" * 50)
    for schema, table in tables:
        print(f"{schema}.{table}")
    
    # Check specifically for listing_status
    print("\n🔍 Looking for listing_status table...")
    listing_tables = [f"{s}.{t}" for s, t in tables if 'listing' in t.lower()]
    if listing_tables:
        print(f"Found: {', '.join(listing_tables)}")
    else:
        print("Not found")
