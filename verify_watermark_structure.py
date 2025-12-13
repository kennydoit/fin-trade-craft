"""Verify watermark structure without next_eligible_date."""
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
    
    # Check sample data
    result = db.fetch_query("""
        SELECT 
            symbol,
            first_date_processed,
            last_date_processed,
            last_successful_run,
            last_run_status
        FROM transforms.transformation_watermarks
        WHERE transformation_group = 'insider_transactions'
          AND last_successful_run IS NOT NULL
        ORDER BY last_successful_run DESC
        LIMIT 5
    """)
    
    print(f"\n📅 Sample Watermark Data (Recent):")
    print("=" * 100)
    print(f"{'Symbol':<10} {'First Date':<15} {'Last Date':<15} {'Status':<10} {'Last Run':<25}")
    print("=" * 100)
    
    for symbol, first_date, last_date, last_run, status in result:
        first_str = str(first_date) if first_date else "N/A"
        last_str = str(last_date) if last_date else "N/A"
        run_str = str(last_run) if last_run else "N/A"
        
        print(f"{symbol:<10} {first_str:<15} {last_str:<15} {status:<10} {run_str:<25}")
