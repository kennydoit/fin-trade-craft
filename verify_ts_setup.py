"""Verify time series transformation setup."""
from db.postgres_database_manager import PostgresDatabaseManager

db = PostgresDatabaseManager()
db.connect()

print("=== Time Series Transformation Setup Verification ===\n")

# 1. Check watermarks by status and asset_type
print("1. Watermarks breakdown:")
breakdown = db.fetch_query("""
    SELECT 
        e.status,
        e.asset_type,
        COUNT(*) as count
    FROM transforms.transformation_watermarks w
    JOIN raw.etl_watermarks e ON w.symbol_id = e.symbol_id
    WHERE w.transformation_group = 'time_series_daily_adjusted'
    GROUP BY e.status, e.asset_type
    ORDER BY count DESC
""")

print(f"{'Status':<15} {'Asset Type':<15} {'Count':>10}")
print("-" * 42)
for status, asset_type, count in breakdown:
    print(f"{status:<15} {asset_type:<15} {count:>10,}")

# 2. Check how many have time series data
print("\n2. Symbols with time series data:")
with_data = db.fetch_query("""
    SELECT 
        e.status,
        e.asset_type,
        COUNT(DISTINCT w.symbol_id) as count
    FROM transforms.transformation_watermarks w
    JOIN raw.etl_watermarks e ON w.symbol_id = e.symbol_id
    JOIN raw.time_series_daily_adjusted ts ON w.symbol_id::text = ts.symbol_id
    WHERE w.transformation_group = 'time_series_daily_adjusted'
    GROUP BY e.status, e.asset_type
    ORDER BY count DESC
""")

print(f"{'Status':<15} {'Asset Type':<15} {'With Data':>10}")
print("-" * 42)
total_with_data = 0
for status, asset_type, count in with_data:
    print(f"{status:<15} {asset_type:<15} {count:>10,}")
    total_with_data += count

print(f"\nTotal symbols with time series data: {total_with_data:,}")

# 3. Sample of each category
print("\n3. Sample symbols from each category:")
samples = db.fetch_query("""
    SELECT 
        w.symbol,
        e.status,
        e.asset_type,
        CASE 
            WHEN ts.symbol_id IS NOT NULL THEN 'Yes'
            ELSE 'No'
        END as has_ts_data
    FROM transforms.transformation_watermarks w
    JOIN raw.etl_watermarks e ON w.symbol_id = e.symbol_id
    LEFT JOIN (
        SELECT DISTINCT symbol_id FROM raw.time_series_daily_adjusted LIMIT 100000
    ) ts ON w.symbol_id::text = ts.symbol_id
    WHERE w.transformation_group = 'time_series_daily_adjusted'
    ORDER BY e.status, e.asset_type, w.symbol
    LIMIT 20
""")

print(f"{'Symbol':<10} {'Status':<15} {'Asset Type':<15} {'Has TS Data'}")
print("-" * 60)
for symbol, status, asset_type, has_data in samples:
    print(f"{symbol:<10} {status:<15} {asset_type:<15} {has_data}")

db.close()

print("\n✅ Verification complete!")
print("\nReady to run:")
print("  python transforms/transform_time_series_daily_adjusted.py --mode full")
