"""Quick script to check raw.time_series_daily_adjusted table structure."""
from db.postgres_database_manager import PostgresDatabaseManager

db = PostgresDatabaseManager()
db.connect()

cols = db.fetch_query("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_schema = 'raw' 
      AND table_name = 'time_series_daily_adjusted' 
    ORDER BY ordinal_position
""")

print("raw.time_series_daily_adjusted columns:")
for col_name, col_type in cols:
    print(f"  {col_name}: {col_type}")

# Check watermark table symbol_id type
watermark_cols = db.fetch_query("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_schema = 'transforms' 
      AND table_name = 'transformation_watermarks'
      AND column_name = 'symbol_id'
""")

print("\ntransforms.transformation_watermarks symbol_id:")
for col_name, col_type in watermark_cols:
    print(f"  {col_name}: {col_type}")

# Get sample watermark data
watermark_sample = db.fetch_query("""
    SELECT symbol_id, symbol 
    FROM transforms.transformation_watermarks 
    WHERE transformation_group = 'time_series_daily_adjusted'
    LIMIT 3
""")

print("\nWatermark sample data:")
for symbol_id, symbol in watermark_sample:
    print(f"  {type(symbol_id).__name__}: {symbol_id} = {symbol}")

db.close()
