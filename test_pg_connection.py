#!/usr/bin/env python3
"""
Test PostgreSQL connection and check database status
"""
import os
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_connection():
    """Test PostgreSQL connection and database status"""
    
    # Connection parameters from .env file
    conn_params = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'database': 'postgres'  # Connect to default postgres database first
    }
    
    try:
        print("Testing PostgreSQL connection...")
        print(f"Host: {conn_params['host']}")
        print(f"Port: {conn_params['port']}")
        print(f"User: {conn_params['user']}")
        
        # Test connection to PostgreSQL server
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True
        cursor = conn.cursor()
        
        print("✅ Successfully connected to PostgreSQL!")
        
        # Check PostgreSQL version
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"PostgreSQL Version: {version}")
        
        # Check if fin_trade_craft database exists
        target_db = os.getenv('POSTGRES_DATABASE', 'fin_trade_craft')
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (target_db,))
        db_exists = cursor.fetchone()
        
        if db_exists:
            print(f"✅ Database '{target_db}' already exists!")
            
            # Connect to the target database to check tables
            conn.close()
            conn_params['database'] = target_db
            conn = psycopg2.connect(**conn_params)
            cursor = conn.cursor()
            
            # Count tables in the database
            cursor.execute("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = 'public';
            """)
            table_count = cursor.fetchone()[0]
            print(f"📊 Found {table_count} tables in '{target_db}' database")
            
            if table_count > 0:
                # List some tables
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    ORDER BY table_name 
                    LIMIT 10;
                """)
                tables = cursor.fetchall()
                print("Sample tables:")
                for table in tables:
                    print(f"  - {table[0]}")
                    
        else:
            print(f"❌ Database '{target_db}' does not exist yet")
            print("We'll need to create it and restore from backup")
        
        cursor.close()
        conn.close()
        return True
        
    except psycopg2.Error as e:
        print(f"❌ PostgreSQL connection failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

if __name__ == "__main__":
    test_connection()
