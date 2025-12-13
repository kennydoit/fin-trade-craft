"""Check watermark counts."""
from db.postgres_database_manager import PostgresDatabaseManager

db = PostgresDatabaseManager()
db.connect()

result = db.fetch_query("""
    SELECT transformation_group, COUNT(*) 
    FROM transforms.transformation_watermarks 
    GROUP BY transformation_group
""")

print("Watermarks by group:")
for group, count in result:
    print(f"  {group}: {count:,}")

# Check if there are duplicates
insider_check = db.fetch_query("""
    SELECT COUNT(*), COUNT(DISTINCT symbol_id) 
    FROM transforms.transformation_watermarks 
    WHERE transformation_group = 'insider_transactions'
""")
print(f"\nInsider transactions: {insider_check[0][0]:,} total, {insider_check[0][1]:,} unique symbols")

ts_check = db.fetch_query("""
    SELECT COUNT(*), COUNT(DISTINCT symbol_id) 
    FROM transforms.transformation_watermarks 
    WHERE transformation_group = 'time_series_daily_adjusted'
""")
print(f"Time series: {ts_check[0][0]:,} total, {ts_check[0][1]:,} unique symbols")

db.close()
