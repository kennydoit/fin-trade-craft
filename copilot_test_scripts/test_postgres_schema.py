"""
Test script to create PostgreSQL schema and verify tables are created.
"""

import psycopg2
import os
from dotenv import load_dotenv

def create_postgres_schema():
    """Create PostgreSQL schema and verify tables."""
    
    # Load environment variables
    load_dotenv()
    
    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = os.getenv('POSTGRES_PORT', '5432')
    user = os.getenv('POSTGRES_USER', 'postgres')
    password = os.getenv('POSTGRES_PASSWORD')
    database = os.getenv('POSTGRES_DATABASE', 'fin_trade_craft')
    
    if not password:
        print("Error: POSTGRES_PASSWORD not found in environment variables")
        return False
    
    try:
        # Connect to the database
        print(f"Connecting to database '{database}' at {host}:{port}")
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        
        cursor = conn.cursor()
        
        # Read and execute the schema file
        schema_file = 'db/schema/postgres_stock_db_schema.sql'
        print(f"Reading schema from {schema_file}")
        
        with open(schema_file, 'r') as f:
            schema_sql = f.read()
        
        print("Creating PostgreSQL schema...")
        cursor.execute(schema_sql)
        conn.commit()
        
        print("✅ Schema created successfully!")
        
        # Verify tables were created
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            ORDER BY table_name
        """)
        
        tables = cursor.fetchall()
        print(f"\nCreated {len(tables)} tables:")
        for table in tables:
            print(f"  - {table[0]}")
        
        # Check indexes
        cursor.execute("""
            SELECT indexname, tablename 
            FROM pg_indexes 
            WHERE schemaname = 'public' 
            AND indexname NOT LIKE '%pkey'
            ORDER BY tablename, indexname
        """)
        
        indexes = cursor.fetchall()
        print(f"\nCreated {len(indexes)} indexes:")
        for index in indexes:
            print(f"  - {index[0]} on {index[1]}")
        
        # Check triggers
        cursor.execute("""
            SELECT trigger_name, event_object_table 
            FROM information_schema.triggers 
            WHERE trigger_schema = 'public'
            ORDER BY event_object_table, trigger_name
        """)
        
        triggers = cursor.fetchall()
        print(f"\nCreated {len(triggers)} triggers:")
        for trigger in triggers:
            print(f"  - {trigger[0]} on {trigger[1]}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except psycopg2.Error as e:
        print(f"❌ PostgreSQL error: {e}")
        return False
    except FileNotFoundError:
        print(f"❌ Schema file not found: {schema_file}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

if __name__ == "__main__":
    create_postgres_schema()
