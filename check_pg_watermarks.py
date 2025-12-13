"""Check pg_etl_watermarks table contents."""
from db.postgres_database_manager import PostgresDatabaseManager

db = PostgresDatabaseManager()
db.connect()

# Total symbols
total = db.fetch_query("SELECT COUNT(DISTINCT symbol_id) FROM raw.pg_etl_watermarks")
print(f"Total symbols in pg_etl_watermarks: {total[0][0]:,}")

# Breakdown by status and asset_type
breakdown = db.fetch_query("""
    SELECT status, asset_type, COUNT(*) as cnt
    FROM raw.pg_etl_watermarks 
    GROUP BY status, asset_type 
    ORDER BY cnt DESC
""")

print("\nBreakdown by status and asset_type:")
print(f"{'Status':<15} {'Asset Type':<15} {'Count':>10}")
print("-" * 42)
for status, asset_type, count in breakdown:
    status_str = status if status else "NULL"
    asset_str = asset_type if asset_type else "NULL"
    print(f"{status_str:<15} {asset_str:<15} {count:>10,}")

# Check Active stocks
active_stocks = db.fetch_query("""
    SELECT COUNT(*) 
    FROM raw.pg_etl_watermarks 
    WHERE status = 'Active' AND asset_type = 'Stock'
""")
print(f"\nActive Stocks: {active_stocks[0][0]:,}")

db.close()
