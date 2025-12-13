"""Check for duplicates in etl_watermarks."""
from db.postgres_database_manager import PostgresDatabaseManager

db = PostgresDatabaseManager()
db.connect()

# Check for duplicate symbol_ids
duplicates = db.fetch_query("""
    SELECT symbol_id, COUNT(*) as cnt
    FROM raw.etl_watermarks
    WHERE status IN ('Active', 'Inactive')
      AND asset_type IN ('Stock', 'ETF')
    GROUP BY symbol_id
    HAVING COUNT(*) > 1
    ORDER BY cnt DESC
    LIMIT 20
""")

print(f"Found {len(duplicates)} symbol_ids with duplicates in raw.etl_watermarks")
print(f"\n{'Symbol ID':<15} {'Count'}")
print("-" * 30)
for symbol_id, count in duplicates[:10]:
    print(f"{symbol_id:<15} {count}")

# Sample duplicates
if duplicates:
    sample_id = duplicates[0][0]
    sample = db.fetch_query("""
        SELECT symbol_id, symbol, status, asset_type, exchange, source
        FROM raw.etl_watermarks
        WHERE symbol_id = %s
    """, (sample_id,))
    
    print(f"\nSample duplicate records for symbol_id {sample_id}:")
    print(f"{'Symbol':<10} {'Status':<12} {'Asset Type':<12} {'Exchange':<10} {'Source'}")
    print("-" * 70)
    for row in sample:
        print(f"{row[1]:<10} {row[2]:<12} {row[3]:<12} {row[4]:<10} {row[5] if len(row) > 5 else 'N/A'}")

db.close()
