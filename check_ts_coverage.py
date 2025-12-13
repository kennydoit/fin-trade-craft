"""Check coverage of initialized symbols with time series data."""
from db.postgres_database_manager import PostgresDatabaseManager

db = PostgresDatabaseManager()
db.connect()

# Symbols in watermark table
watermark_count = db.fetch_query("""
    SELECT COUNT(*) 
    FROM transforms.transformation_watermarks 
    WHERE transformation_group = 'time_series_daily_adjusted'
""")

# Symbols with both watermark and time series data
with_data = db.fetch_query("""
    SELECT COUNT(DISTINCT w.symbol_id)
    FROM transforms.transformation_watermarks w
    JOIN raw.time_series_daily_adjusted ts 
        ON w.symbol_id::text = ts.symbol_id
    WHERE w.transformation_group = 'time_series_daily_adjusted'
""")

# Total symbols with time series data
ts_total = db.fetch_query("""
    SELECT COUNT(DISTINCT symbol_id) 
    FROM raw.time_series_daily_adjusted
""")

print(f"Symbols in transformation_watermarks: {watermark_count[0][0]:,}")
print(f"Symbols with both watermark AND time series data: {with_data[0][0]:,}")
print(f"Total symbols with time series data: {ts_total[0][0]:,}")
print(f"\nCoverage: {with_data[0][0] / ts_total[0][0] * 100:.1f}% of time series symbols have watermarks")

db.close()
