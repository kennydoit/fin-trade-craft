"""Check inactive symbols in source table."""
from db.postgres_database_manager import PostgresDatabaseManager

db = PostgresDatabaseManager()
db.connect()

print("\n=== RAW.ETL_WATERMARKS STATUS BREAKDOWN ===")
# Check the source table
source_status = """
SELECT 
    status,
    asset_type,
    COUNT(*) as symbol_count,
    COUNT(DISTINCT symbol_id) as unique_symbol_ids
FROM raw.etl_watermarks
WHERE status IN ('Active', 'Inactive')
  AND asset_type IN ('Stock', 'ETF')
GROUP BY status, asset_type
ORDER BY status, asset_type;
"""

result = db.fetch_query(source_status)
if result:
    print(f"{'Status':<12} | {'Asset Type':<12} | {'Total Rows':>12} | {'Unique IDs':>12}")
    print("-" * 65)
    total_rows = 0
    for row in result:
        print(f"{row[0]:<12} | {row[1]:<12} | {row[2]:>12,} | {row[3]:>12,}")
        total_rows += row[2]
    print("-" * 65)
    print(f"{'TOTAL':<12} | {'':12} | {total_rows:>12,}")

print("\n=== CHECKING IF INACTIVE SYMBOLS EXIST ===")
# Check if there are any inactive symbols at all
inactive_check = """
SELECT 
    COUNT(*) as total_inactive,
    COUNT(DISTINCT symbol_id) as unique_inactive,
    COUNT(DISTINCT symbol) as unique_symbols
FROM raw.etl_watermarks
WHERE status = 'Inactive'
  AND asset_type IN ('Stock', 'ETF');
"""

inactive_result = db.fetch_query(inactive_check)
if inactive_result:
    row = inactive_result[0]
    print(f"Total Inactive rows: {row[0]:,}")
    print(f"Unique Inactive symbol_ids: {row[1]:,}")
    print(f"Unique Inactive symbols: {row[2]:,}")
    
    if row[0] == 0:
        print("\n❌ NO INACTIVE SYMBOLS found in raw.etl_watermarks!")
        print("   The status filter includes 'Inactive', but none exist in the source data.")
    else:
        print("\n✅ Inactive symbols DO exist in source table")

print("\n=== SAMPLE INACTIVE SYMBOLS FROM SOURCE (if any) ===")
# Sample inactive symbols
sample = """
SELECT 
    symbol,
    symbol_id,
    status,
    asset_type,
    exchange
FROM raw.etl_watermarks
WHERE status = 'Inactive'
  AND asset_type IN ('Stock', 'ETF')
LIMIT 10;
"""

sample_result = db.fetch_query(sample)
if sample_result:
    print(f"{'Symbol':<10} | {'Symbol ID':<15} | {'Status':<10} | {'Asset Type':<12} | {'Exchange':<10}")
    print("-" * 75)
    for row in sample_result:
        print(f"{row[0]:<10} | {row[1]:<15} | {row[2]:<10} | {row[3]:<12} | {row[4]:<10}")
else:
    print("No inactive symbols found.")

print("\n=== ALL UNIQUE STATUS VALUES ===")
# Check what status values actually exist
all_statuses = """
SELECT 
    status,
    COUNT(*) as count,
    COUNT(DISTINCT symbol_id) as unique_ids
FROM raw.etl_watermarks
WHERE asset_type IN ('Stock', 'ETF')
GROUP BY status
ORDER BY count DESC;
"""

status_result = db.fetch_query(all_statuses)
if status_result:
    print(f"{'Status':<20} | {'Row Count':>12} | {'Unique IDs':>12}")
    print("-" * 50)
    for row in status_result:
        status = row[0] if row[0] else 'NULL'
        print(f"{status:<20} | {row[1]:>12,} | {row[2]:>12,}")

db.close()
