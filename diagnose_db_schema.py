#!/usr/bin/env python3
"""
Diagnose database schema issues
"""

from db.postgres_database_manager import PostgresDatabaseManager

def main():
    print("🔍 Diagnosing database schema issues...")
    
    with PostgresDatabaseManager() as db:
        cursor = db.connection.cursor()
        
        # 1. Check if listing_status exists and what type it is
        print("\n1. Checking listing_status table/view status:")
        cursor.execute("""
            SELECT table_name, table_type 
            FROM information_schema.tables 
            WHERE table_name = 'listing_status'
        """)
        tables = cursor.fetchall()
        print(f"   Tables found: {tables}")
        
        # Check views specifically
        cursor.execute("""
            SELECT schemaname, viewname 
            FROM pg_views 
            WHERE viewname = 'listing_status'
        """)
        views = cursor.fetchall()
        print(f"   Views found: {views}")
        
        # 2. Check economic_indicators table structure
        print("\n2. Checking economic_indicators table structure:")
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'economic_indicators' 
            AND table_schema = 'source'
            ORDER BY ordinal_position
        """)
        econ_columns = cursor.fetchall()
        print(f"   Economic indicators columns: {econ_columns}")
        
        # 3. Check if source schema exists
        print("\n3. Checking source schema:")
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name = 'source'
        """)
        source_schema = cursor.fetchall()
        print(f"   Source schema exists: {source_schema}")
        
        # 4. List all tables in source schema
        print("\n4. Tables in source schema:")
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'source'
            ORDER BY table_name
        """)
        source_tables = cursor.fetchall()
        print(f"   Source tables: {source_tables}")
        
        # 5. Check extraction_watermarks table
        print("\n5. Checking extraction_watermarks table:")
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'extraction_watermarks'
            AND table_schema = 'source'
            ORDER BY ordinal_position
        """)
        watermarks_columns = cursor.fetchall()
        print(f"   Watermarks columns: {watermarks_columns}")

if __name__ == "__main__":
    main()
