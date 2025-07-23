#!/usr/bin/env python3
"""
Quick verification of PostgreSQL database status
"""
import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def verify_database():
    """Verify the PostgreSQL database setup"""
    
    conn_params = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'database': os.getenv('POSTGRES_DATABASE', 'fin_trade_craft')
    }
    
    try:
        print("🔍 Verifying PostgreSQL database...")
        print(f"Connecting to: {conn_params['database']} at {conn_params['host']}:{conn_params['port']}")
        
        # Connect to the database
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        print("✅ Successfully connected to fin_trade_craft database!")
        
        # Count tables
        cursor.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'public';
        """)
        table_count = cursor.fetchone()[0]
        print(f"📊 Found {table_count} tables in the database")
        
        if table_count > 0:
            # List tables
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                ORDER BY table_name;
            """)
            tables = cursor.fetchall()
            print("\n📋 Tables in the database:")
            for table in tables:
                # Count rows in each table
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table[0]};")
                    row_count = cursor.fetchone()[0]
                    print(f"  - {table[0]}: {row_count:,} rows")
                except Exception as e:
                    print(f"  - {table[0]}: Unable to count rows ({str(e)[:50]}...)")
        
        cursor.close()
        conn.close()
        
        print("\n🎉 Database verification completed successfully!")
        print("✅ Your PostgreSQL migration appears to be working correctly!")
        
        return True
        
    except psycopg2.Error as e:
        print(f"❌ PostgreSQL error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

if __name__ == "__main__":
    verify_database()
