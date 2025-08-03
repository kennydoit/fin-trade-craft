from db.postgres_database_manager import PostgresDatabaseManager

db = PostgresDatabaseManager()
db.connect()

# Test the split logic
table_name = "extracted.balance_sheet"
if '.' in table_name:
    schema_name, table_name_only = table_name.split('.', 1)
    print(f"Schema: '{schema_name}', Table: '{table_name_only}'")
    
    cursor = db.connection.cursor()
    cursor.execute(
        """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_name = %s
        )
    """,
        (schema_name, table_name_only),
    )
    result = cursor.fetchone()[0]
    print(f"Query result: {result}")
    cursor.close()

db.close()
