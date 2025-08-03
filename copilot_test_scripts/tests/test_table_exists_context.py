from db.postgres_database_manager import PostgresDatabaseManager

# Test with fresh connection
db = PostgresDatabaseManager()
with db as connection:
    print("Testing table_exists method...")
    exists = connection.table_exists("extracted.balance_sheet")
    print(f"extracted.balance_sheet exists: {exists}")
    
    # Also test the raw query
    result = connection.fetch_query(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'extracted' AND table_name = 'balance_sheet')"
    )
    print(f"Raw query result: {result[0][0]}")
