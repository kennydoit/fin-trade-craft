from db.postgres_database_manager import PostgresDatabaseManager

db = PostgresDatabaseManager()
db.connect()

# Check if schema exists
result = db.fetch_query("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'extracted'")
print(f"Extracted schema exists: {bool(result)}")

# Check tables in extracted schema
result = db.fetch_query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'extracted'")
print(f"Tables in extracted schema: {[r[0] for r in result]}")

# Test table_exists method
exists = db.table_exists("extracted.balance_sheet")
print(f"extracted.balance_sheet exists (via table_exists): {exists}")

db.close()
