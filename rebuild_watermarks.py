"""Rebuild transformation watermarks from scratch."""
from db.postgres_database_manager import PostgresDatabaseManager

db = PostgresDatabaseManager()
db.connect()

print("🔄 Rebuilding transformation watermarks from scratch...")
print()

# Step 1: Drop the entire transformation_watermarks table
print("Step 1: Dropping transforms.transformation_watermarks table...")
drop_query = "DROP TABLE IF EXISTS transforms.transformation_watermarks CASCADE"
db.execute_query(drop_query)
print("✅ Table dropped")
print()

# Step 2: Recreate the table (will be done by watermark manager)
print("Step 2: Table will be recreated when you run --init-group")
print()

print("Next steps:")
print("1. Run: python transforms/transformation_watermark_manager.py --init-group insider_transactions")
print("2. Run: python transforms/transform_time_series_daily_adjusted.py --init-group time_series_daily_adjusted")
print()
print("Or use the watermark manager directly:")
print("  python transforms/transformation_watermark_manager.py --create-table")
print("  python transforms/transformation_watermark_manager.py --init-group insider_transactions")
print("  python transforms/transformation_watermark_manager.py --init-group time_series_daily_adjusted")

db.close()
