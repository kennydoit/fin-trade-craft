"""Check watermark date fields."""
from db.postgres_database_manager import PostgresDatabaseManager
from dotenv import load_dotenv

load_dotenv()

with PostgresDatabaseManager() as db:
    result = db.fetch_query("""
        SELECT 
            symbol,
            first_date_processed,
            last_date_processed,
            next_eligible_date,
            last_successful_run,
            last_run_status
        FROM transforms.transformation_watermarks
        WHERE transformation_group = 'insider_transactions'
          AND last_successful_run IS NOT NULL
        ORDER BY last_successful_run DESC
        LIMIT 10
    """)
    
    print("\n📅 Watermark Date Fields (Recent):")
    print("=" * 120)
    print(f"{'Symbol':<10} {'First Date':<15} {'Last Date':<15} {'Next Eligible':<15} {'Status':<10}")
    print("=" * 120)
    
    for symbol, first_date, last_date, next_date, last_run, status in result:
        first_str = str(first_date) if first_date else "N/A"
        last_str = str(last_date) if last_date else "N/A"
        next_str = str(next_date) if next_date else "N/A"
        
        print(f"{symbol:<10} {first_str:<15} {last_str:<15} {next_str:<15} {status:<10}")
