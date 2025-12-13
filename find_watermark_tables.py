"""Find watermark and listing tables."""
from db.postgres_database_manager import PostgresDatabaseManager

db = PostgresDatabaseManager()
db.connect()

# Find relevant tables
tables = db.fetch_query("""
    SELECT table_schema, table_name 
    FROM information_schema.tables 
    WHERE table_schema IN ('raw', 'source') 
      AND (table_name LIKE '%water%' 
           OR table_name LIKE '%list%' 
           OR table_name LIKE '%status%'
           OR table_name LIKE '%symbol%')
    ORDER BY table_schema, table_name
""")

print("Relevant tables:")
for schema, table in tables:
    count = db.fetch_query(f"SELECT COUNT(*) FROM {schema}.{table}")
    print(f"  {schema}.{table:<40} {count[0][0]:>10,} rows")

db.close()
