"""Test with symbols that actually have data."""
from db.postgres_database_manager import PostgresDatabaseManager

db = PostgresDatabaseManager()
db.connect()

# Get 5 symbols that have time series data
query = """
    SELECT DISTINCT ts.symbol_id::integer, ts.symbol
    FROM raw.time_series_daily_adjusted ts
    JOIN transforms.transformation_watermarks w 
        ON ts.symbol_id::integer = w.symbol_id
    WHERE w.transformation_group = 'time_series_daily_adjusted'
    LIMIT 5
"""

result = db.fetch_query(query)
print(f"Found {len(result)} symbols with data:")
for symbol_id, symbol in result:
    print(f"  {symbol} (ID: {symbol_id})")

db.close()
