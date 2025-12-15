"""
Verify if delisted symbols are being processed in incremental mode.
"""
from db.postgres_database_manager import PostgresDatabaseManager

db = PostgresDatabaseManager()
db.connect()

print("\n" + "=" * 80)
print("VERIFY DELISTED SYMBOL PROCESSING")
print("=" * 80)

# Check watermark table for delisted symbols
print("\n1. DELISTED SYMBOLS IN WATERMARK TABLE")
print("-" * 80)

watermark_delisted_query = """
SELECT 
    COUNT(*) as total_delisted,
    COUNT(CASE WHEN w.last_successful_run IS NOT NULL THEN 1 END) as processed_delisted,
    COUNT(CASE WHEN w.last_successful_run IS NULL THEN 1 END) as unprocessed_delisted,
    MIN(w.last_successful_run) as earliest_run,
    MAX(w.last_successful_run) as latest_run
FROM transforms.transformation_watermarks w
JOIN raw.etl_watermarks e ON w.symbol_id = e.symbol_id
WHERE w.transformation_group = 'time_series_daily_adjusted'
  AND e.status = 'Delisted';
"""

result = db.fetch_query(watermark_delisted_query)
if result:
    row = result[0]
    print(f"Total delisted symbols in watermarks: {row[0]:,}")
    print(f"  - Processed: {row[1]:,}")
    print(f"  - Unprocessed: {row[2]:,}")
    print(f"  - Earliest run: {row[3]}")
    print(f"  - Latest run: {row[4]}")

# Check what would be returned by get_symbols_needing_transformation
print("\n2. SYMBOLS SELECTED BY INCREMENTAL MODE (staleness_hours=168)")
print("-" * 80)

incremental_query = """
SELECT 
    e.status,
    COUNT(*) as symbol_count
FROM transforms.transformation_watermarks w
JOIN raw.etl_watermarks e ON w.symbol_id = e.symbol_id
WHERE w.transformation_group = 'time_series_daily_adjusted'
  AND w.transformation_eligible = true
  AND w.consecutive_failures < 3
  AND (
      w.last_successful_run IS NULL 
      OR w.last_successful_run < NOW() - INTERVAL '168 hours'
  )
  AND e.status IN ('Active', 'Delisted')
  AND e.asset_type IN ('Stock', 'ETF')
GROUP BY e.status
ORDER BY e.status;
"""

result = db.fetch_query(incremental_query)
if result:
    print(f"{'Status':<12} | {'Count':>10}")
    print("-" * 25)
    for row in result:
        print(f"{row[0]:<12} | {row[1]:>10,}")

# Check the actual filter being used
print("\n3. FILTER VERIFICATION IN WATERMARK MANAGER")
print("-" * 80)

filter_check_query = """
SELECT DISTINCT e.status
FROM raw.etl_watermarks e
WHERE e.status IN ('Active', 'Delisted')
  AND e.asset_type IN ('Stock', 'ETF')
ORDER BY e.status;
"""

result = db.fetch_query(filter_check_query)
if result:
    statuses = [row[0] for row in result]
    print(f"Status values included in filter: {', '.join(statuses)}")
    
    if 'Delisted' in statuses:
        print("\n⚠️ 'Delisted' IS INCLUDED in the filter")
        print("   Delisted symbols WILL be processed")
    else:
        print("\n✅ 'Delisted' is NOT in the filter")
        print("   Delisted symbols will be skipped")

# Check recent transformation runs
print("\n4. RECENT TRANSFORMATION ACTIVITY")
print("-" * 80)

recent_activity_query = """
SELECT 
    e.status,
    COUNT(*) as symbols_processed,
    MIN(w.last_successful_run) as earliest_in_batch,
    MAX(w.last_successful_run) as latest_in_batch
FROM transforms.transformation_watermarks w
JOIN raw.etl_watermarks e ON w.symbol_id = e.symbol_id
WHERE w.transformation_group = 'time_series_daily_adjusted'
  AND w.last_successful_run > NOW() - INTERVAL '24 hours'
GROUP BY e.status
ORDER BY e.status;
"""

result = db.fetch_query(recent_activity_query)
if result:
    print(f"{'Status':<12} | {'Processed (24h)':>16} | {'Earliest':>20} | {'Latest':>20}")
    print("-" * 75)
    for row in result:
        print(f"{row[0]:<12} | {row[1]:>16,} | {str(row[2])[:19]:>20} | {str(row[3])[:19]:>20}")
else:
    print("No processing activity in last 24 hours")

# Sample of recently processed delisted symbols
print("\n5. SAMPLE RECENTLY PROCESSED DELISTED SYMBOLS (10 examples)")
print("-" * 80)

sample_delisted_query = """
SELECT 
    w.symbol,
    w.symbol_id,
    e.status,
    w.last_successful_run,
    (SELECT COUNT(*) FROM transforms.time_series_daily_adjusted t WHERE t.symbol_id = w.symbol_id) as record_count
FROM transforms.transformation_watermarks w
JOIN raw.etl_watermarks e ON w.symbol_id = e.symbol_id
WHERE w.transformation_group = 'time_series_daily_adjusted'
  AND e.status = 'Delisted'
  AND w.last_successful_run > NOW() - INTERVAL '24 hours'
ORDER BY w.last_successful_run DESC
LIMIT 10;
"""

result = db.fetch_query(sample_delisted_query)
if result:
    print(f"{'Symbol':<10} | {'Status':<10} | {'Last Run':>20} | {'Records':>10}")
    print("-" * 60)
    for row in result:
        print(f"{row[0]:<10} | {row[2]:<10} | {str(row[3])[:19]:>20} | {row[4]:>10,}")
else:
    print("No delisted symbols processed in last 24 hours")

print("\n" + "=" * 80)
print("CONCLUSION")
print("=" * 80)

# Get the final verdict
verdict_query = """
SELECT 
    COUNT(CASE WHEN e.status = 'Delisted' THEN 1 END) as delisted_count,
    COUNT(CASE WHEN e.status = 'Active' THEN 1 END) as active_count
FROM transforms.transformation_watermarks w
JOIN raw.etl_watermarks e ON w.symbol_id = e.symbol_id
WHERE w.transformation_group = 'time_series_daily_adjusted'
  AND w.last_successful_run > NOW() - INTERVAL '24 hours';
"""

result = db.fetch_query(verdict_query)
if result and result[0]:
    delisted_count = result[0][0]
    active_count = result[0][1]
    
    if delisted_count > 0:
        print(f"\n❌ DELISTED SYMBOLS ARE BEING PROCESSED")
        print(f"   {delisted_count:,} delisted symbols processed in last 24h")
        print(f"   {active_count:,} active symbols processed in last 24h")
        print(f"\n💡 The filter includes 'Delisted' status:")
        print(f"   status IN ('Active', 'Delisted')")
    else:
        print(f"\n✅ DELISTED SYMBOLS ARE BEING SKIPPED")
        print(f"   0 delisted symbols processed in last 24h")
        print(f"   {active_count:,} active symbols processed in last 24h")
else:
    print("\n⚠️ No recent processing activity detected")

db.close()
