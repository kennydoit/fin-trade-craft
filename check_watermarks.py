"""Check the structure of pg_etl_watermarks table."""
from db.postgres_database_manager import PostgresDatabaseManager
from dotenv import load_dotenv

load_dotenv()

with PostgresDatabaseManager() as db:
    # Check columns
    cols = db.fetch_query("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = 'raw' AND table_name = 'pg_etl_watermarks' 
        ORDER BY ordinal_position
    """)
    
    print("\n📋 raw.pg_etl_watermarks columns:")
    for col, dtype in cols:
        print(f"  {col}: {dtype}")
    
    # Sample data
    sample = db.fetch_query("SELECT * FROM raw.pg_etl_watermarks LIMIT 3")
    print(f"\n📊 Sample rows: {len(sample)}")
    if sample:
        print(f"First row: {sample[0]}")
