"""
Check which symbols have newer raw data than their transformed data.
This helps identify what needs incremental updating.
"""
from db.postgres_database_manager import PostgresDatabaseManager

db = PostgresDatabaseManager()
db.connect()

print("\n" + "=" * 80)
print("INCREMENTAL UPDATE ANALYSIS")
print("=" * 80)

# Check symbols with raw data newer than transformation
print("\n1. SYMBOLS WITH NEW RAW DATA")
print("-" * 80)

new_data_query = """
SELECT 
    COUNT(*) as symbols_with_new_data
FROM transforms.transformation_watermarks tw
JOIN (
    SELECT symbol_id, MAX(date) as latest_raw_date
    FROM raw.time_series_daily_adjusted
    GROUP BY symbol_id
) r ON tw.symbol_id = r.symbol_id::integer
WHERE tw.transformation_group = 'time_series_daily_adjusted'
  AND (
    tw.last_date_processed IS NULL
    OR r.latest_raw_date > tw.last_date_processed
  );
"""

result = db.fetch_query(new_data_query)
symbols_with_new_data = result[0][0] if result else 0
print(f"Symbols with raw data newer than transformed: {symbols_with_new_data:,}")

# Check symbols that haven't been processed yet
print("\n2. NEVER PROCESSED SYMBOLS")
print("-" * 80)

never_processed_query = """
SELECT COUNT(*)
FROM transforms.transformation_watermarks
WHERE transformation_group = 'time_series_daily_adjusted'
  AND last_successful_run IS NULL;
"""

result = db.fetch_query(never_processed_query)
never_processed = result[0][0] if result else 0
print(f"Symbols never processed: {never_processed:,}")

# Check staleness (symbols not processed in last 24 hours)
print("\n3. STALE SYMBOLS (not processed in 24h)")
print("-" * 80)

stale_query = """
SELECT COUNT(*)
FROM transforms.transformation_watermarks
WHERE transformation_group = 'time_series_daily_adjusted'
  AND last_successful_run IS NOT NULL
  AND last_successful_run < NOW() - INTERVAL '24 hours';
"""

result = db.fetch_query(stale_query)
stale_24h = result[0][0] if result else 0
print(f"Symbols stale (>24h): {stale_24h:,}")

# Sample of symbols with new data
print("\n4. SAMPLE SYMBOLS WITH NEW RAW DATA (10 examples)")
print("-" * 80)

sample_query = """
SELECT 
    tw.symbol,
    tw.symbol_id,
    tw.last_date_processed,
    r.latest_raw_date,
    r.latest_raw_date - tw.last_date_processed as days_behind,
    tw.last_successful_run
FROM transforms.transformation_watermarks tw
JOIN (
    SELECT symbol_id, MAX(date) as latest_raw_date
    FROM raw.time_series_daily_adjusted
    GROUP BY symbol_id
) r ON tw.symbol_id = r.symbol_id::integer
WHERE tw.transformation_group = 'time_series_daily_adjusted'
  AND tw.last_date_processed IS NOT NULL
  AND r.latest_raw_date > tw.last_date_processed
ORDER BY (r.latest_raw_date - tw.last_date_processed) DESC
LIMIT 10;
"""

sample_result = db.fetch_query(sample_query)
if sample_result:
    print(f"{'Symbol':<10} | {'Last Transformed':<15} | {'Latest Raw':<15} | {'Days Behind':>12} | {'Last Run':<20}")
    print("-" * 95)
    for row in sample_result:
        days_behind = row[4] if isinstance(row[4], int) else (row[4].days if row[4] else 0)
        print(f"{row[0]:<10} | {str(row[2]):<15} | {str(row[3]):<15} | {days_behind:>12} | {str(row[5])[:19]:<20}")
else:
    print("No symbols with new raw data found")

# Check raw.etl_watermarks for last update times
print("\n5. RAW ETL WATERMARKS STATUS")
print("-" * 80)

etl_watermark_query = """
SELECT 
    COUNT(*) as total_symbols,
    MIN(last_successful_run::timestamp) as earliest_update,
    MAX(last_successful_run::timestamp) as latest_update,
    COUNT(CASE WHEN last_successful_run::timestamp > NOW() - INTERVAL '24 hours' THEN 1 END) as updated_last_24h
FROM raw.etl_watermarks
WHERE status IN ('Active', 'Delisted')
  AND asset_type IN ('Stock', 'ETF');
"""

etl_result = db.fetch_query(etl_watermark_query)
if etl_result:
    row = etl_result[0]
    print(f"Total symbols in ETL watermarks: {row[0]:,}")
    print(f"Earliest update: {row[1]}")
    print(f"Latest update: {row[2]}")
    print(f"Updated in last 24h: {row[3]:,}")

print("\n" + "=" * 80)
print("RECOMMENDED ACTION")
print("=" * 80)

if never_processed > 0:
    print(f"\n⚠️ {never_processed:,} symbols have NEVER been processed")
    print("   Run: python transforms/transform_time_series_daily_adjusted.py --mode incremental")
elif symbols_with_new_data > 0:
    print(f"\n✅ {symbols_with_new_data:,} symbols have new raw data")
    print("   Run: python transforms/transform_time_series_daily_adjusted.py --mode incremental")
elif stale_24h > 0:
    print(f"\n📅 {stale_24h:,} symbols haven't been processed in 24+ hours")
    print("   Run: python transforms/transform_time_series_daily_adjusted.py --mode incremental --staleness-hours 24")
else:
    print("\n✅ All symbols are up to date!")
    print("   No incremental update needed")

db.close()
