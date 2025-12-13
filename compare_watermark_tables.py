"""Compare etl_watermarks vs pg_etl_watermarks."""
from db.postgres_database_manager import PostgresDatabaseManager

db = PostgresDatabaseManager()
db.connect()

print("=== raw.etl_watermarks ===")
cols1 = db.fetch_query("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_schema = 'raw' AND table_name = 'etl_watermarks'
    ORDER BY ordinal_position
""")
for col, dtype in cols1:
    print(f"  {col:<30} {dtype}")

total1 = db.fetch_query("SELECT COUNT(DISTINCT symbol_id) FROM raw.etl_watermarks")
print(f"\nTotal distinct symbols: {total1[0][0]:,}")

# Count with time series data
with_ts = db.fetch_query("""
    SELECT COUNT(DISTINCT e.symbol_id)
    FROM raw.etl_watermarks e
    JOIN raw.time_series_daily_adjusted ts 
        ON e.symbol_id::text = ts.symbol_id
""")
print(f"Symbols with time series data: {with_ts[0][0]:,}")

# Sample
sample = db.fetch_query("""
    SELECT symbol_id, symbol, status, asset_type, exchange
    FROM raw.etl_watermarks
    WHERE status = 'Active' AND asset_type = 'Stock'
    LIMIT 5
""")
print(f"\nSample Active Stocks:")
for row in sample:
    print(f"  {row}")

print("\n=== raw.pg_etl_watermarks ===")
cols2 = db.fetch_query("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_schema = 'raw' AND table_name = 'pg_etl_watermarks'
    ORDER BY ordinal_position
""")
for col, dtype in cols2:
    print(f"  {col:<30} {dtype}")

total2 = db.fetch_query("SELECT COUNT(DISTINCT symbol_id) FROM raw.pg_etl_watermarks")
print(f"\nTotal distinct symbols: {total2[0][0]:,}")

db.close()
