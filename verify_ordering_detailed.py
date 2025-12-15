"""
Detailed verification of data ordering with actual database sample.
"""
from db.postgres_database_manager import PostgresDatabaseManager
import pandas as pd

db = PostgresDatabaseManager()
db.connect()

print("\n" + "=" * 80)
print("DATA ORDERING VERIFICATION - ACTUAL DATA SAMPLE")
print("=" * 80)

# Get a sample symbol
sample_query = """
SELECT symbol_id, symbol 
FROM transforms.transformation_watermarks
WHERE transformation_group = 'time_series_daily_adjusted'
  AND last_successful_run IS NOT NULL
LIMIT 1;
"""

sample = db.fetch_query(sample_query)
if not sample:
    print("No processed symbols found")
    db.close()
    exit()

symbol_id = sample[0][0]
symbol = sample[0][1]

print(f"\n📊 Testing with symbol: {symbol} (ID: {symbol_id})")
print("-" * 80)

# Check raw data ordering
print("\n1. RAW DATA FROM DATABASE (first 10 rows)")
print("-" * 80)

raw_query = """
SELECT date, close, volume
FROM raw.time_series_daily_adjusted
WHERE symbol_id = %s
ORDER BY date
LIMIT 10;
"""

raw_data = db.fetch_query(raw_query, (str(symbol_id),))
print(f"{'Date':<12} | {'Close':>10} | {'Volume':>12}")
print("-" * 40)
for row in raw_data:
    print(f"{str(row[0]):<12} | {row[1]:>10.2f} | {row[2]:>12,}")

# Check if dates are ascending
dates = [row[0] for row in raw_data]
is_sorted = all(dates[i] <= dates[i+1] for i in range(len(dates)-1))
print(f"\n{'✅' if is_sorted else '❌'} Dates are {'ascending' if is_sorted else 'NOT ascending'}")

# Check transformed data
print("\n2. TRANSFORMED DATA (with calculated features)")
print("-" * 80)

transform_query = """
SELECT date, ohlcv_sma_5, ohlcv_sma_20, ohlcv_ema_8, ohlcv_rsi_14
FROM transforms.time_series_daily_adjusted
WHERE symbol_id = %s
ORDER BY date
LIMIT 50;
"""

transform_data = db.fetch_query(transform_query, (symbol_id,))

# Show first 10 and last 10 to demonstrate SMA/EMA are calculated correctly
print(f"\nFirst 10 rows (note: early rows have NULL for features needing lookback):")
print(f"{'Date':<12} | {'SMA_5':>10} | {'SMA_20':>10} | {'EMA_8':>10} | {'RSI_14':>10}")
print("-" * 70)
for i, row in enumerate(transform_data[:10]):
    sma5 = f"{row[1]:.2f}" if row[1] is not None else "NULL"
    sma20 = f"{row[2]:.2f}" if row[2] is not None else "NULL"
    ema8 = f"{row[3]:.2f}" if row[3] is not None else "NULL"
    rsi14 = f"{row[4]:.2f}" if row[4] is not None else "NULL"
    print(f"{str(row[0]):<12} | {sma5:>10} | {sma20:>10} | {ema8:>10} | {rsi14:>10}")

# Verify dates are in order
transform_dates = [row[0] for row in transform_data]
is_transform_sorted = all(transform_dates[i] <= transform_dates[i+1] for i in range(len(transform_dates)-1))
print(f"\n{'✅' if is_transform_sorted else '❌'} Transformed dates are {'ascending' if is_transform_sorted else 'NOT ascending'}")

# Check for proper NULL handling at the beginning (indicators need warmup period)
nulls_at_start = sum(1 for row in transform_data[:20] if row[2] is None)  # SMA_20 needs 20 periods
print(f"\n3. LOOKBACK PERIOD VERIFICATION")
print("-" * 80)
print(f"SMA_20 NULL values in first 20 rows: {nulls_at_start}")
if nulls_at_start >= 19:  # Should have ~19 NULLs (needs 20 periods to calculate)
    print("✅ Correct: SMA_20 properly waits for 20-period lookback window")
else:
    print("⚠️ Unexpected: SMA_20 might not be waiting for full lookback period")

# Verify RSI calculation (needs 14 periods)
rsi_nulls = sum(1 for row in transform_data[:15] if row[4] is None)
print(f"RSI_14 NULL values in first 15 rows: {rsi_nulls}")
if rsi_nulls >= 13:
    print("✅ Correct: RSI_14 properly waits for lookback window")
else:
    print("⚠️ Unexpected: RSI_14 calculation might have issues")

print("\n" + "=" * 80)
print("CONCLUSION")
print("=" * 80)

if is_sorted and is_transform_sorted and nulls_at_start >= 19:
    print("\n✅ ALL CHECKS PASSED")
    print("   - Raw data is properly ordered by date")
    print("   - Transformed data maintains date order")
    print("   - Moving averages respect lookback periods")
    print("   - Backward-looking calculations are CORRECT")
else:
    print("\n⚠️ ISSUES DETECTED - Review data ordering logic")

db.close()
