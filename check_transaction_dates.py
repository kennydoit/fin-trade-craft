"""Check transaction date ranges per symbol."""
from db.postgres_database_manager import PostgresDatabaseManager
from dotenv import load_dotenv

load_dotenv()

with PostgresDatabaseManager() as db:
    # Sample date ranges
    result = db.fetch_query("""
        SELECT 
            symbol_id,
            symbol,
            MIN(transaction_date) as first_date,
            MAX(transaction_date) as last_date,
            COUNT(*) as transaction_count
        FROM raw.insider_transactions
        WHERE symbol_id IN (
            SELECT symbol_id FROM raw.pg_etl_watermarks ORDER BY symbol_id LIMIT 5
        )
        GROUP BY symbol_id, symbol
        ORDER BY symbol_id
    """)
    
    print("\n📅 Transaction Date Ranges (sample):")
    print("=" * 80)
    print(f"{'Symbol':<10} {'First Date':<15} {'Last Date':<15} {'Count':<10}")
    print("=" * 80)
    
    for symbol_id, symbol, first_date, last_date, count in result:
        print(f"{symbol:<10} {str(first_date):<15} {str(last_date):<15} {count:<10}")
