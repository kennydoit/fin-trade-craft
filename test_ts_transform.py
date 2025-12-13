"""Test time series transformation with a single symbol."""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from transforms.transform_time_series_daily_adjusted import TimeSeriesDailyAdjustedTransformer
from db.postgres_database_manager import PostgresDatabaseManager

# Get first symbol from watermarks
db = PostgresDatabaseManager()
db.connect()

result = db.fetch_query("""
    SELECT symbol_id, symbol 
    FROM transforms.transformation_watermarks 
    WHERE transformation_group = 'time_series_daily_adjusted'
    LIMIT 1
""")

if not result:
    print("No symbols found in watermark table")
    db.close()
    sys.exit(1)

symbol_id, symbol = result[0]
print(f"Testing with symbol: {symbol} (ID: {symbol_id})")

# Test transformation
transformer = TimeSeriesDailyAdjustedTransformer()

print("\nCreating transforms table...")
transformer.create_transforms_table()

# Keep db connection open for the entire process
transformer.db.connect()

print(f"\nTransforming {symbol}...")
result = transformer.transform_and_load(symbol_id, symbol, mode='full')

print(f"\nResult: {result}")

transformer.db.close()
db.close()

if result['success']:
    print(f"\n✅ Successfully loaded {result['records_loaded']} records")
else:
    print(f"\n❌ Failed: {result['error']}")
