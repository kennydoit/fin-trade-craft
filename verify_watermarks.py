"""Verify the transformation_watermarks table has all fields."""
from db.postgres_database_manager import PostgresDatabaseManager
from dotenv import load_dotenv

load_dotenv()

with PostgresDatabaseManager() as db:
    # Check columns
    cols = db.fetch_query("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = 'transforms' 
          AND table_name = 'transformation_watermarks' 
        ORDER BY ordinal_position
    """)
    
    print("\n📋 transforms.transformation_watermarks columns:")
    for col, dtype in cols:
        print(f"  {col}: {dtype}")
    
    # Sample data
    sample = db.fetch_query("""
        SELECT 
            symbol_id, 
            symbol, 
            transformation_group,
            listing_status,
            ipo_date,
            delisting_date,
            exchange
        FROM transforms.transformation_watermarks 
        WHERE transformation_group = 'insider_transactions'
        LIMIT 5
    """)
    
    print(f"\n📊 Sample records (showing first 5):")
    print(f"{'Symbol':<10} {'Status':<10} {'IPO Date':<12} {'Delisting':<12} {'Exchange':<10}")
    print("=" * 60)
    for row in sample:
        symbol_id, symbol, group, status, ipo, delist, exchange = row
        ipo_str = str(ipo) if ipo else 'N/A'
        delist_str = str(delist) if delist else 'N/A'
        print(f"{symbol:<10} {status:<10} {ipo_str:<12} {delist_str:<12} {exchange:<10}")
