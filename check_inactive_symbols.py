"""Check if inactive symbols were processed in the transformation."""
from db.postgres_database_manager import PostgresDatabaseManager

db = PostgresDatabaseManager()
db.connect()

print("\n=== INACTIVE SYMBOLS IN WATERMARK TABLE ===")
# Check how many inactive symbols are in the watermark table
watermark_inactive = """
SELECT 
    COUNT(DISTINCT w.symbol_id) as total_inactive,
    COUNT(DISTINCT CASE WHEN w.last_successful_run IS NOT NULL THEN w.symbol_id END) as processed_inactive,
    COUNT(DISTINCT CASE WHEN w.listing_status = 'DEL' THEN w.symbol_id END) as marked_del
FROM transforms.transformation_watermarks w
JOIN raw.etl_watermarks e ON w.symbol_id = e.symbol_id
WHERE w.transformation_group = 'time_series_daily_adjusted'
  AND e.status = 'Inactive';
"""

result = db.fetch_query(watermark_inactive)
if result:
    row = result[0]
    print(f"Total Inactive symbols in watermark table: {row[0]:,}")
    print(f"Inactive symbols processed successfully: {row[1]:,}")
    print(f"Inactive symbols marked as 'DEL': {row[2]:,}")

print("\n=== INACTIVE SYMBOLS WITH TRANSFORMED DATA ===")
# Check if any inactive symbols have data in the transformed table
data_inactive = """
SELECT 
    COUNT(DISTINCT t.symbol_id) as inactive_with_data,
    COUNT(*) as total_records
FROM transforms.time_series_daily_adjusted t
JOIN raw.etl_watermarks e ON t.symbol_id = e.symbol_id
WHERE e.status = 'Inactive';
"""

data_result = db.fetch_query(data_inactive)
if data_result:
    row = data_result[0]
    print(f"Inactive symbols with transformed data: {row[0]:,}")
    print(f"Total records for inactive symbols: {row[1]:,}")

print("\n=== ACTIVE vs INACTIVE BREAKDOWN ===")
# Full breakdown
breakdown = """
SELECT 
    e.status,
    COUNT(DISTINCT w.symbol_id) as in_watermarks,
    COUNT(DISTINCT CASE WHEN w.last_successful_run IS NOT NULL THEN w.symbol_id END) as processed,
    COUNT(DISTINCT t.symbol_id) as with_data,
    SUM(CASE WHEN t.symbol_id IS NOT NULL THEN 1 ELSE 0 END) as total_records
FROM transforms.transformation_watermarks w
JOIN raw.etl_watermarks e ON w.symbol_id = e.symbol_id
LEFT JOIN (
    SELECT DISTINCT symbol_id, COUNT(*) as cnt
    FROM transforms.time_series_daily_adjusted
    GROUP BY symbol_id
) t ON w.symbol_id = t.symbol_id
WHERE w.transformation_group = 'time_series_daily_adjusted'
GROUP BY e.status
ORDER BY e.status;
"""

breakdown_result = db.fetch_query(breakdown)
if breakdown_result:
    print(f"{'Status':<12} | {'In Watermarks':>15} | {'Processed':>10} | {'With Data':>10} | {'Records':>12}")
    print("-" * 75)
    for row in breakdown_result:
        status = row[0] if row[0] else 'NULL'
        records = row[4] if row[4] else 0
        print(f"{status:<12} | {row[1]:>15,} | {row[2]:>10,} | {row[3] or 0:>10,} | {records:>12,}")

print("\n=== SAMPLE INACTIVE SYMBOLS (10 random) ===")
# Sample of inactive symbols
sample = """
SELECT 
    w.symbol,
    w.symbol_id,
    e.status,
    w.last_successful_run IS NOT NULL as processed,
    w.listing_status,
    (SELECT COUNT(*) FROM transforms.time_series_daily_adjusted t WHERE t.symbol_id = w.symbol_id) as record_count
FROM transforms.transformation_watermarks w
JOIN raw.etl_watermarks e ON w.symbol_id = e.symbol_id
WHERE w.transformation_group = 'time_series_daily_adjusted'
  AND e.status = 'Inactive'
ORDER BY RANDOM()
LIMIT 10;
"""

sample_result = db.fetch_query(sample)
if sample_result:
    print(f"{'Symbol':<10} | {'Status':<10} | {'Processed':>10} | {'Listing':>10} | {'Records':>10}")
    print("-" * 65)
    for row in sample_result:
        processed = 'Yes' if row[3] else 'No'
        listing = row[4] if row[4] else 'N/A'
        print(f"{row[0]:<10} | {row[2]:<10} | {processed:>10} | {listing:>10} | {row[5]:>10,}")

db.close()
