"""Test time series transformation in full mode with limited symbols."""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from transforms.transform_time_series_daily_adjusted import TimeSeriesDailyAdjustedTransformer

# Test transformation with first 10 symbols
transformer = TimeSeriesDailyAdjustedTransformer()
transformer.db.connect()

# Get first 10 symbols
symbols = transformer.watermark_mgr.get_symbols_needing_transformation(
    'time_series_daily_adjusted',
    staleness_hours=999999,
    limit=10
)

print(f"Processing {len(symbols)} symbols...")

# Create table
transformer.create_transforms_table()

# Process each symbol
for idx, symbol_data in enumerate(symbols, 1):
    symbol_id = symbol_data['symbol_id']
    symbol = symbol_data['symbol']
    
    print(f"\n[{idx}/{len(symbols)}] Processing {symbol} (ID: {symbol_id})")
    result = transformer.transform_and_load(symbol_id, symbol, mode='full')
    print(f"  Result: {result['success']}, Records: {result['records_loaded']}")
    if not result['success']:
        print(f"  Error: {result['error']}")

# Update watermarks
print("\nUpdating watermarks...")
transformer.update_all_watermarks()

print("\nDone!")
transformer.db.close()
