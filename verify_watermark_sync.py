"""Verify watermark updates and data in transformed table."""
from db.postgres_database_manager import PostgresDatabaseManager

db = PostgresDatabaseManager()
db.connect()

print("\n=== WATERMARK TABLE STATUS ===")
# Check watermark table
watermark_query = """
SELECT 
    COUNT(*) as total_symbols,
    COUNT(CASE WHEN last_successful_run IS NOT NULL THEN 1 END) as with_successful_run,
    COUNT(CASE WHEN first_date_processed IS NOT NULL THEN 1 END) as with_first_date,
    COUNT(CASE WHEN last_date_processed IS NOT NULL THEN 1 END) as with_last_date,
    COUNT(CASE WHEN listing_status = 'DEL' THEN 1 END) as marked_as_del,
    COUNT(CASE WHEN last_run_status = 'success' THEN 1 END) as success_status,
    MIN(last_successful_run)::text as earliest_run,
    MAX(last_successful_run)::text as latest_run
FROM transforms.transformation_watermarks
WHERE transformation_group = 'time_series_daily_adjusted';
"""

result = db.fetch_query(watermark_query)
if result:
    row = result[0]
    print(f"Total Symbols in Watermark Table: {row[0]:,}")
    print(f"  - With Successful Run: {row[1]:,}")
    print(f"  - With First Date: {row[2]:,}")
    print(f"  - With Last Date: {row[3]:,}")
    print(f"  - Marked as DEL: {row[4]:,}")
    print(f"  - Success Status: {row[5]:,}")
    print(f"  - Earliest Run: {row[6]}")
    print(f"  - Latest Run: {row[7]}")

print("\n=== TRANSFORMED DATA TABLE STATUS ===")
# Check transformed data table
data_query = """
SELECT 
    COUNT(DISTINCT symbol_id) as unique_symbols,
    COUNT(*) as total_records,
    MIN(date)::text as earliest_date,
    MAX(date)::text as latest_date
FROM transforms.time_series_daily_adjusted;
"""

data_result = db.fetch_query(data_query)
if data_result:
    row = data_result[0]
    print(f"Unique Symbols with Data: {row[0]:,}")
    print(f"Total Records: {row[1]:,}")
    print(f"Date Range: {row[2]} to {row[3]}")

print("\n=== COMPARISON ===")
# Compare symbols in both tables
comparison_query = """
SELECT 
    (SELECT COUNT(DISTINCT symbol_id) 
     FROM transforms.time_series_daily_adjusted) as symbols_with_data,
    (SELECT COUNT(*) 
     FROM transforms.transformation_watermarks 
     WHERE transformation_group = 'time_series_daily_adjusted') as symbols_in_watermarks,
    (SELECT COUNT(*) 
     FROM transforms.transformation_watermarks 
     WHERE transformation_group = 'time_series_daily_adjusted'
       AND last_successful_run IS NOT NULL) as watermarks_updated;
"""

comp_result = db.fetch_query(comparison_query)
if comp_result:
    row = comp_result[0]
    print(f"Symbols with transformed data: {row[0]:,}")
    print(f"Symbols in watermark table: {row[1]:,}")
    print(f"Watermarks actually updated: {row[2]:,}")
    
    if row[2] == 0:
        print("\n❌ PROBLEM: Watermarks were NOT updated despite transformation completing!")
    elif row[2] == row[0]:
        print(f"\n✅ SUCCESS: All {row[2]:,} symbols with data have updated watermarks")
    else:
        print(f"\n⚠️ PARTIAL: Only {row[2]:,} of {row[0]:,} symbols have updated watermarks")

# Sample of actual data
print("\n=== SAMPLE TRANSFORMED DATA (5 symbols) ===")
sample_data = """
SELECT 
    t.symbol_id,
    w.symbol,
    COUNT(*) as record_count,
    MIN(t.date)::text as first_date,
    MAX(t.date)::text as last_date
FROM transforms.time_series_daily_adjusted t
LEFT JOIN transforms.transformation_watermarks w 
    ON t.symbol_id = w.symbol_id 
    AND w.transformation_group = 'time_series_daily_adjusted'
GROUP BY t.symbol_id, w.symbol
ORDER BY record_count DESC
LIMIT 5;
"""

sample_result = db.fetch_query(sample_data)
for row in sample_result:
    symbol = str(row[1]) if row[1] else 'UNKNOWN'
    symbol_id = str(row[0])
    print(f"{symbol:8s} | {symbol_id:15s} | {row[2]:6,} records | {row[3]} to {row[4]}")

db.close()
