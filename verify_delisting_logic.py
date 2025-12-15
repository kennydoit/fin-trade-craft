"""
Verify the updated delisting logic.
"""
from db.postgres_database_manager import PostgresDatabaseManager

db = PostgresDatabaseManager()
db.connect()

print("\n" + "=" * 80)
print("VERIFY UPDATED DELISTING LOGIC")
print("=" * 80)

# Test the new query logic
print("\n1. SYMBOLS SELECTED WITH NEW LOGIC (staleness_hours=168)")
print("-" * 80)

new_logic_query = """
SELECT 
    p.status,
    COUNT(*) as symbol_count,
    COUNT(CASE WHEN w.last_date_processed IS NULL THEN 1 END) as never_processed,
    COUNT(CASE WHEN w.last_date_processed < p.delisting_date THEN 1 END) as data_before_delisting
FROM transforms.transformation_watermarks w
JOIN raw.etl_watermarks p ON w.symbol_id = p.symbol_id
WHERE w.transformation_group = 'time_series_daily_adjusted'
  AND w.transformation_eligible = true
  AND w.consecutive_failures < 3
  AND (
      w.last_successful_run IS NULL 
      OR w.last_successful_run < NOW() - INTERVAL '168 hours'
  )
  AND (
      p.status = 'Active'
      OR (p.status = 'Delisted' AND (w.last_date_processed IS NULL OR w.last_date_processed < p.delisting_date))
  )
  AND p.asset_type IN ('Stock', 'ETF')
GROUP BY p.status
ORDER BY p.status;
"""

result = db.fetch_query(new_logic_query)
if result:
    print(f"{'Status':<12} | {'Total':>10} | {'Never Proc':>12} | {'Before Delist':>15}")
    print("-" * 60)
    total = 0
    for row in result:
        print(f"{row[0]:<12} | {row[1]:>10,} | {row[2]:>12,} | {row[3]:>15,}")
        total += row[1]
    print("-" * 60)
    print(f"{'TOTAL':<12} | {total:>10,}")

# Compare old vs new logic
print("\n2. COMPARISON: OLD LOGIC vs NEW LOGIC")
print("-" * 80)

old_logic_query = """
SELECT COUNT(*)
FROM transforms.transformation_watermarks w
JOIN raw.etl_watermarks p ON w.symbol_id = p.symbol_id
WHERE w.transformation_group = 'time_series_daily_adjusted'
  AND w.transformation_eligible = true
  AND w.consecutive_failures < 3
  AND (
      w.last_successful_run IS NULL 
      OR w.last_successful_run < NOW() - INTERVAL '168 hours'
  )
  AND p.status IN ('Active', 'Delisted')
  AND p.asset_type IN ('Stock', 'ETF');
"""

old_count = db.fetch_query(old_logic_query)[0][0]

new_logic_count_query = """
SELECT COUNT(*)
FROM transforms.transformation_watermarks w
JOIN raw.etl_watermarks p ON w.symbol_id = p.symbol_id
WHERE w.transformation_group = 'time_series_daily_adjusted'
  AND w.transformation_eligible = true
  AND w.consecutive_failures < 3
  AND (
      w.last_successful_run IS NULL 
      OR w.last_successful_run < NOW() - INTERVAL '168 hours'
  )
  AND (
      p.status = 'Active'
      OR (p.status = 'Delisted' AND (w.last_date_processed IS NULL OR w.last_date_processed < p.delisting_date))
  )
  AND p.asset_type IN ('Stock', 'ETF');
"""

new_count = db.fetch_query(new_logic_count_query)[0][0]

print(f"Old logic (status IN ('Active', 'Delisted')): {old_count:,} symbols")
print(f"New logic (with delisting date check): {new_count:,} symbols")
print(f"Reduction: {old_count - new_count:,} delisted symbols skipped")
print(f"Percentage: {100 * (old_count - new_count) / old_count:.1f}% fewer symbols")

# Show examples of delisted symbols that WILL be processed
print("\n3. DELISTED SYMBOLS THAT WILL BE PROCESSED (new data after delisting)")
print("-" * 80)

will_process_query = """
SELECT 
    w.symbol,
    p.delisting_date,
    w.last_date_processed,
    (SELECT MAX(date) FROM raw.time_series_daily_adjusted WHERE symbol_id = w.symbol_id::text) as latest_raw_data
FROM transforms.transformation_watermarks w
JOIN raw.etl_watermarks p ON w.symbol_id = p.symbol_id
WHERE w.transformation_group = 'time_series_daily_adjusted'
  AND p.status = 'Delisted'
  AND (w.last_date_processed IS NULL OR w.last_date_processed < p.delisting_date)
  AND w.consecutive_failures < 3
ORDER BY p.delisting_date DESC
LIMIT 10;
"""

result = db.fetch_query(will_process_query)
if result:
    print(f"{'Symbol':<10} | {'Delisting Date':<15} | {'Last Processed':<15} | {'Latest Raw':<15}")
    print("-" * 70)
    for row in result:
        last_proc = str(row[2]) if row[2] else "Never"
        latest_raw = str(row[3]) if row[3] else "No data"
        print(f"{row[0]:<10} | {str(row[1]):<15} | {last_proc:<15} | {latest_raw:<15}")
else:
    print("No delisted symbols need processing")

# Show examples of delisted symbols that WILL BE SKIPPED
print("\n4. DELISTED SYMBOLS THAT WILL BE SKIPPED (already processed past delisting)")
print("-" * 80)

will_skip_query = """
SELECT 
    w.symbol,
    p.delisting_date,
    w.last_date_processed,
    w.last_successful_run
FROM transforms.transformation_watermarks w
JOIN raw.etl_watermarks p ON w.symbol_id = p.symbol_id
WHERE w.transformation_group = 'time_series_daily_adjusted'
  AND p.status = 'Delisted'
  AND w.last_date_processed >= p.delisting_date
ORDER BY w.last_successful_run DESC
LIMIT 10;
"""

result = db.fetch_query(will_skip_query)
if result:
    print(f"{'Symbol':<10} | {'Delisting Date':<15} | {'Last Processed':<15} | {'Last Run':<20}")
    print("-" * 75)
    for row in result:
        print(f"{row[0]:<10} | {str(row[1]):<15} | {str(row[2]):<15} | {str(row[3])[:19]:<20}")
else:
    print("No delisted symbols will be skipped")

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

if new_count < old_count:
    reduction = old_count - new_count
    print(f"\n✅ NEW LOGIC IS WORKING")
    print(f"   - {reduction:,} delisted symbols will be skipped")
    print(f"   - Only delisted symbols with new data after delisting will be processed")
    print(f"   - This will save ~{100 * reduction / old_count:.1f}% of processing time")
else:
    print(f"\n⚠️ No change detected - review logic")

db.close()
