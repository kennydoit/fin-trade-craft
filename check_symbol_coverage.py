"""Check symbol coverage between tables."""
from db.postgres_database_manager import PostgresDatabaseManager

db = PostgresDatabaseManager()
db.connect()

# Get counts
watermarks = db.fetch_query("""
    SELECT COUNT(DISTINCT symbol_id) 
    FROM raw.pg_etl_watermarks 
    WHERE status = 'Active' AND asset_type = 'Stock'
""")

time_series = db.fetch_query("""
    SELECT COUNT(DISTINCT symbol_id) 
    FROM raw.time_series_daily_adjusted
""")

# Symbols with time series data but not in active watermarks
not_in_watermarks = db.fetch_query("""
    SELECT COUNT(DISTINCT ts.symbol_id)
    FROM raw.time_series_daily_adjusted ts
    LEFT JOIN raw.pg_etl_watermarks w 
        ON ts.symbol_id::integer = w.symbol_id
    WHERE w.symbol_id IS NULL
       OR w.status != 'Active'
       OR w.asset_type != 'Stock'
""")

# Sample of symbols not in active watermarks
sample = db.fetch_query("""
    SELECT DISTINCT ts.symbol_id, ts.symbol, w.status, w.asset_type
    FROM raw.time_series_daily_adjusted ts
    LEFT JOIN raw.pg_etl_watermarks w 
        ON ts.symbol_id::integer = w.symbol_id
    WHERE w.symbol_id IS NULL
       OR w.status != 'Active'
       OR w.asset_type != 'Stock'
    LIMIT 20
""")

print(f"Active stocks in pg_etl_watermarks: {watermarks[0][0]:,}")
print(f"Symbols with time series data: {time_series[0][0]:,}")
print(f"Symbols with time series but not Active/Stock: {not_in_watermarks[0][0]:,}")
print(f"\nSample of excluded symbols:")
print(f"{'Symbol ID':<12} {'Symbol':<10} {'Status':<15} {'Asset Type'}")
print("-" * 60)
for row in sample:
    symbol_id = row[0]
    symbol = row[1]
    status = row[2] if row[2] else 'NOT IN TABLE'
    asset_type = row[3] if row[3] else 'NOT IN TABLE'
    print(f"{symbol_id:<12} {symbol:<10} {status:<15} {asset_type}")

db.close()
