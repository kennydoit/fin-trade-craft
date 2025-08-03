#!/usr/bin/env python3

import sys
sys.path.append('.')

from db.postgres_database_manager import PostgresDatabaseManager

def check_dependencies():
    """Check for foreign key dependencies on balance sheet tables."""
    db = PostgresDatabaseManager()
    db.connect()
    
    print("Checking CASCADE safety for balance sheet tables...")
    print("=" * 60)
    
    # Check for foreign keys pointing TO balance sheet tables
    print("\n1. Foreign keys pointing TO balance sheet tables:")
    result = db.execute_query("""
        SELECT 
            tc.table_schema, 
            tc.table_name, 
            kcu.column_name,
            ccu.table_schema AS foreign_table_schema,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu 
            ON tc.constraint_name = kcu.constraint_name 
            AND tc.table_schema = kcu.table_schema 
        JOIN information_schema.constraint_column_usage AS ccu 
            ON ccu.constraint_name = tc.constraint_name 
            AND ccu.table_schema = tc.table_schema 
        WHERE tc.constraint_type = 'FOREIGN KEY' 
            AND (ccu.table_name = 'balance_sheet' OR ccu.table_name = 'balance_sheet_daily');
    """)
    
    if result:
        print("   ⚠️  DEPENDENCIES FOUND:")
        for row in result:
            print(f"   {row[0]}.{row[1]}.{row[2]} -> {row[3]}.{row[4]}.{row[5]}")
        print("   CASCADE will affect these tables!")
    else:
        print("   ✅ None found - CASCADE is SAFE")
    
    # Check for foreign keys FROM balance sheet tables
    print("\n2. Foreign keys FROM balance sheet tables:")
    result = db.execute_query("""
        SELECT 
            tc.table_schema, 
            tc.table_name, 
            kcu.column_name,
            ccu.table_schema AS foreign_table_schema,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu 
            ON tc.constraint_name = kcu.constraint_name 
            AND tc.table_schema = kcu.table_schema 
        JOIN information_schema.constraint_column_usage AS ccu 
            ON ccu.constraint_name = tc.constraint_name 
            AND ccu.table_schema = tc.table_schema 
        WHERE tc.constraint_type = 'FOREIGN KEY' 
            AND (tc.table_name = 'balance_sheet' OR tc.table_name = 'balance_sheet_daily');
    """)
    
    if result:
        print("   Dependencies FROM balance sheet tables:")
        for row in result:
            print(f"   {row[0]}.{row[1]}.{row[2]} -> {row[3]}.{row[4]}.{row[5]}")
    else:
        print("   ✅ None found")
    
    # Check for views that depend on balance sheet tables
    print("\n3. Views depending on balance sheet tables:")
    result = db.execute_query("""
        SELECT DISTINCT 
            v.table_schema as view_schema,
            v.table_name as view_name,
            v.view_definition
        FROM information_schema.views v
        WHERE v.view_definition ILIKE '%balance_sheet%'
            OR v.view_definition ILIKE '%balance_sheet_daily%';
    """)
    
    if result:
        print("   ⚠️  VIEWS FOUND:")
        for row in result:
            print(f"   {row[0]}.{row[1]}")
        print("   CASCADE will drop these views!")
    else:
        print("   ✅ None found - CASCADE is SAFE")
    
    # Check current table schemas
    print("\n4. Current balance sheet table locations:")
    result = db.execute_query("""
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_name IN ('balance_sheet', 'balance_sheet_daily')
        ORDER BY table_schema, table_name;
    """)
    
    for row in result:
        print(f"   {row[0]}.{row[1]}")
    
    db.close()
    
    print("\n" + "=" * 60)
    print("CASCADE SAFETY ASSESSMENT:")
    print("If you see ✅ for items 1 and 3, CASCADE is safe to use.")
    print("If you see ⚠️, CASCADE will affect other objects.")

if __name__ == "__main__":
    check_dependencies()
