"""Test transform_and_load for JHX."""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from transforms.transform_time_series_daily_adjusted import TimeSeriesDailyAdjustedTransformer

transformer = TimeSeriesDailyAdjustedTransformer()
transformer.db.connect()

# Ensure table exists
transformer.create_transforms_table()

symbol_id = 372856
symbol = 'JHX'

print(f"Testing transform_and_load for {symbol} (ID: {symbol_id})")
result = transformer.transform_and_load(symbol_id, symbol, mode='full')

print(f"\nResult:")
print(f"  Success: {result['success']}")
print(f"  Records loaded: {result['records_loaded']}")
if not result['success']:
    print(f"  Error: {result['error']}")

transformer.db.close()
