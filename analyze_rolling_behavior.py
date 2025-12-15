"""
Check if pandas_ta uses min_periods for rolling calculations.
"""
from db.postgres_database_manager import PostgresDatabaseManager
import pandas as pd
import numpy as np

db = PostgresDatabaseManager()
db.connect()

# Get a sample with enough history
sample_query = """
SELECT symbol_id, symbol 
FROM transforms.transformation_watermarks
WHERE transformation_group = 'time_series_daily_adjusted'
  AND last_successful_run IS NOT NULL
LIMIT 1;
"""

sample = db.fetch_query(sample_query)
symbol_id = sample[0][0]
symbol = sample[0][1]

print(f"\n📊 Analyzing {symbol} - Manual SMA calculation vs pandas_ta")
print("=" * 80)

# Get raw data
raw_query = """
SELECT date, close
FROM raw.time_series_daily_adjusted
WHERE symbol_id = %s
ORDER BY date
LIMIT 30;
"""

raw_data = db.fetch_query(raw_query, (str(symbol_id),))
dates = [row[0] for row in raw_data]
closes = [row[1] for row in raw_data]

# Manual SMA_5 calculation (strict - needs 5 periods)
print("\nMANUAL SMA_5 CALCULATION (strict mode):")
print("-" * 80)
print(f"{'Row':<4} | {'Date':<12} | {'Close':>10} | {'Manual SMA_5':>14}")
print("-" * 50)

for i in range(min(10, len(closes))):
    if i < 4:  # First 4 rows don't have enough data
        manual_sma = "NULL"
    else:
        manual_sma = f"{sum(closes[i-4:i+1])/5:.2f}"
    
    print(f"{i+1:<4} | {str(dates[i]):<12} | {closes[i]:>10.2f} | {manual_sma:>14}")

# Check what pandas rolling does by default
print("\n\nPANDAS ROLLING BEHAVIOR TEST:")
print("-" * 80)

df_test = pd.DataFrame({'close': closes[:10]})

# Default rolling (min_periods=window by default)
sma5_default = df_test['close'].rolling(window=5).mean()
print("\nrolling(5).mean() - default:")
for i in range(10):
    val = "NULL" if pd.isna(sma5_default.iloc[i]) else f"{sma5_default.iloc[i]:.2f}"
    print(f"Row {i+1}: {val}")

# Rolling with min_periods=1 (calculates with whatever data available)
sma5_partial = df_test['close'].rolling(window=5, min_periods=1).mean()
print("\nrolling(5, min_periods=1).mean():")
for i in range(10):
    val = "NULL" if pd.isna(sma5_partial.iloc[i]) else f"{sma5_partial.iloc[i]:.2f}"
    print(f"Row {i+1}: {val}")

# Check what's actually in the database
print("\n\nACTUAL DATABASE VALUES:")
print("-" * 80)

db_query = """
SELECT date, ohlcv_sma_5, ohlcv_sma_20
FROM transforms.time_series_daily_adjusted
WHERE symbol_id = %s
ORDER BY date
LIMIT 10;
"""

db_data = db.fetch_query(db_query, (symbol_id,))
print(f"{'Row':<4} | {'Date':<12} | {'DB SMA_5':>12} | {'DB SMA_20':>12}")
print("-" * 50)
for i, row in enumerate(db_data):
    sma5 = "NULL" if row[1] is None else f"{row[1]:.2f}"
    sma20 = "NULL" if row[2] is None else f"{row[2]:.2f}"
    print(f"{i+1:<4} | {str(row[0]):<12} | {sma5:>12} | {sma20:>12}")

print("\n" + "=" * 80)
print("ANALYSIS:")
print("=" * 80)

# Check if database has values in first 5 rows
has_early_values = any(row[1] is not None for row in db_data[:4])

if has_early_values:
    print("\n⚠️ Database has SMA values before sufficient lookback period")
    print("   This means pandas rolling is using min_periods < window")
    print("   Early values may be calculated on incomplete windows")
    print("\n💡 RECOMMENDATION: Explicitly set min_periods=window in rolling calculations")
else:
    print("\n✅ Database correctly waits for full lookback period")
    print("   SMA calculations only start after sufficient data")

db.close()
