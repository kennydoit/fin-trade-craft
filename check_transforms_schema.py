from db.postgres_database_manager import PostgresDatabaseManager

db = PostgresDatabaseManager()
db.connect()

result = db.fetch_query("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_schema = 'transforms' 
      AND table_name = 'time_series_daily_adjusted' 
      AND column_name = 'symbol_id'
""")

if result:
    print(f"symbol_id type in transforms.time_series_daily_adjusted: {result[0][1]}")
else:
    print("Table not found or column doesn't exist")

db.close()
