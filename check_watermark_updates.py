"""Check watermark update details."""
from db.postgres_database_manager import PostgresDatabaseManager
from dotenv import load_dotenv

load_dotenv()

with PostgresDatabaseManager() as db:
    # Check recent updates
    result = db.fetch_query("""
        SELECT 
            symbol_id,
            symbol,
            transformation_group,
            last_run_status,
            consecutive_failures,
            last_successful_run,
            updated_at,
            created_at
        FROM transforms.transformation_watermarks
        WHERE transformation_group = 'insider_transactions'
        ORDER BY updated_at DESC NULLS LAST
        LIMIT 10
    """)
    
    print("\n📊 Most Recently Updated Watermarks:")
    print("=" * 120)
    print(f"{'Symbol':<10} {'Status':<10} {'Failures':<10} {'Last Success':<25} {'Updated At':<25}")
    print("=" * 120)
    
    for row in result:
        symbol_id, symbol, group, status, failures, last_success, updated, created = row
        status_str = status if status else "N/A"
        failures_str = str(failures) if failures is not None else "0"
        last_success_str = str(last_success) if last_success else "Never"
        updated_str = str(updated) if updated else str(created)
        
        print(f"{symbol:<10} {status_str:<10} {failures_str:<10} {last_success_str:<25} {updated_str:<25}")
    
    # Count by status
    stats = db.fetch_query("""
        SELECT 
            last_run_status,
            COUNT(*) as count
        FROM transforms.transformation_watermarks
        WHERE transformation_group = 'insider_transactions'
        GROUP BY last_run_status
        ORDER BY last_run_status
    """)
    
    print(f"\n📈 Status Breakdown:")
    print("=" * 40)
    for status, count in stats:
        status_str = status if status else "Not Run"
        print(f"{status_str:<20} {count:>10,}")
