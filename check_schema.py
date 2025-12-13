"""Check if transforms schema exists."""
from db.postgres_database_manager import PostgresDatabaseManager
from dotenv import load_dotenv

load_dotenv()

with PostgresDatabaseManager() as db:
    result = db.fetch_query("""
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name = 'transforms'
    """)
    
    if result:
        print("✅ transforms schema exists")
    else:
        print("❌ transforms schema does NOT exist")
        print("\n Creating transforms schema...")
        db.execute_query("CREATE SCHEMA IF NOT EXISTS transforms;")
        print("✅ transforms schema created")
