"""
Verify the modified transformation fetches only recent periods.
"""
from db.postgres_database_manager import PostgresDatabaseManager
import pandas as pd

db = PostgresDatabaseManager()
db.connect()

print("\n" + "=" * 80)
print("VERIFY LAST 250 PERIODS OPTIMIZATION")
print("=" * 80)

# Get a sample symbol with lots of history
sample_query = """
SELECT 
    t.symbol_id, 
    t.symbol,
    COUNT(*) as total_raw_records
FROM raw.time_series_daily_adjusted t
JOIN transforms.transformation_watermarks w ON t.symbol_id = w.symbol_id::text
WHERE w.transformation_group = 'time_series_daily_adjusted'
GROUP BY t.symbol_id, t.symbol
HAVING COUNT(*) > 1000
LIMIT 1;
"""

sample = db.fetch_query(sample_query)
if not sample:
    print("No suitable test symbol found")
    db.close()
    exit()

symbol_id = sample[0][0]
symbol = sample[0][1]
total_raw = sample[0][2]

print(f"\n📊 Test Symbol: {symbol} (ID: {symbol_id})")
print(f"Total raw records available: {total_raw:,}")
print("-" * 80)

# Test the new query
test_query = """
SELECT symbol_id, symbol, date, open, high, low, 
       close, adjusted_close, volume
FROM (
    SELECT symbol_id, symbol, date, open, high, low, 
           close, adjusted_close, volume
    FROM raw.time_series_daily_adjusted
    WHERE symbol_id = %s
    ORDER BY date DESC
    LIMIT 250
) subq
ORDER BY date ASC
"""

print("\nExecuting optimized query (LIMIT 250)...")
df = pd.read_sql(test_query, db.connection, params=(symbol_id,))

print(f"\n✅ Query returned: {len(df)} records")
print(f"Date range: {df['date'].min()} to {df['date'].max()}")
print(f"Reduction: {total_raw:,} → {len(df)} records ({100*(1-len(df)/total_raw):.1f}% less)")

# Verify we have enough for calculations
print("\n" + "=" * 80)
print("ADEQUACY CHECK")
print("=" * 80)

required_lookback = 55  # Longest EMA period
required_forward = 40   # Longest target horizon
min_required = required_lookback + required_forward

if len(df) >= min_required:
    print(f"\n✅ {len(df)} periods is sufficient")
    print(f"   Required: {min_required} (55 lookback + 40 forward)")
    print(f"   Buffer: {len(df) - min_required} extra periods")
else:
    print(f"\n❌ {len(df)} periods is INSUFFICIENT")
    print(f"   Required: {min_required} (55 lookback + 40 forward)")
    print(f"   Shortage: {min_required - len(df)} periods")

# Estimate performance improvement
print("\n" + "=" * 80)
print("PERFORMANCE ESTIMATE")
print("=" * 80)

avg_raw_query = """
SELECT AVG(cnt)::integer as avg_records
FROM (
    SELECT COUNT(*) as cnt
    FROM raw.time_series_daily_adjusted
    GROUP BY symbol_id
) subq;
"""

avg_result = db.fetch_query(avg_raw_query)
avg_records = avg_result[0][0] if avg_result else 0

if avg_records > 0:
    reduction_pct = 100 * (1 - 250 / avg_records)
    print(f"\nAverage records per symbol: {avg_records:,}")
    print(f"New limit: 250 records")
    print(f"Average reduction: {reduction_pct:.1f}%")
    print(f"\n💡 Expected speedup: ~{reduction_pct/100:.1f}x faster per symbol")

db.close()
