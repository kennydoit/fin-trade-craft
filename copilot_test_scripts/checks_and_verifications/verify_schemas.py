#!/usr/bin/env python3

import sys
import argparse
sys.path.append('.')

from db.postgres_database_manager import PostgresDatabaseManager

def reset_balance_sheet_tables(db):
    """Drop and recreate balance sheet tables for a fresh start."""
    print("Resetting balance sheet tables...")
    print("=" * 50)
    
    # First check for dependencies
    print("Checking for dependencies...")
    views_result = db.execute_query("""
        SELECT DISTINCT 
            v.table_schema as view_schema,
            v.table_name as view_name
        FROM information_schema.views v
        WHERE v.view_definition ILIKE '%balance_sheet%'
            OR v.view_definition ILIKE '%balance_sheet_daily%';
    """)
    
    if views_result:
        print("⚠️  WARNING: The following views depend on balance sheet tables:")
        for row in views_result:
            print(f"   {row[0]}.{row[1]}")
        print("\nOptions:")
        print("1. Use RESTRICT (safer) - will fail if dependencies exist")
        print("2. Use CASCADE (dangerous) - will drop dependent views")
        print("3. Cancel and handle dependencies manually")
        
        choice = input("\nChoose option (1/2/3): ").strip()
        
        if choice == "3":
            print("Reset cancelled. Handle dependencies manually first.")
            return
        elif choice == "2":
            drop_mode = "CASCADE"
            print("⚠️  Using CASCADE - dependent views will be dropped!")
        else:
            drop_mode = "RESTRICT"
            print("Using RESTRICT - will fail safely if dependencies exist")
    else:
        drop_mode = "RESTRICT"
        print("✅ No dependencies found - using RESTRICT mode")
    
    # Drop tables in correct order (daily first due to potential dependencies)
    tables_to_drop = [
        'extracted.balance_sheet_daily',
        'extracted.balance_sheet'
    ]
    
    for table in tables_to_drop:
        try:
            db.execute_query(f"DROP TABLE IF EXISTS {table} {drop_mode};")
            db.connection.commit()
            print(f"✓ Dropped {table}")
        except Exception as e:
            print(f"✗ Error dropping {table}: {e}")
            if "still referenced" in str(e).lower():
                print("  💡 Tip: Use CASCADE option or drop dependent objects first")
    
    print("\nTables reset complete! You can now run the balance sheet extractor.")
    print("Command: .\\venv\\Scripts\\python.exe .\\data_pipeline\\extract\\extract_balance_sheet.py")

def main():
    print("Checking schema placement for balance sheet tables...")
    print("=" * 60)
    
    db = PostgresDatabaseManager()
    db.connect()
    
    # Check balance_sheet table
    print("\nbalance_sheet table:")
    result = db.execute_query("""
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_name = 'balance_sheet'
        ORDER BY table_schema;
    """)
    for row in result:
        print(f"  Schema: {row[0]}, Table: {row[1]}")
    
    # Check balance_sheet_daily table
    print("\nbalance_sheet_daily table:")
    result = db.execute_query("""
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_name = 'balance_sheet_daily'
        ORDER BY table_schema;
    """)
    for row in result:
        print(f"  Schema: {row[0]}, Table: {row[1]}")
    
    # Check record counts
    print("\nRecord counts:")
    try:
        result = db.execute_query("SELECT COUNT(*) FROM extracted.balance_sheet;")
        print(f"  extracted.balance_sheet: {result[0][0]} records")
    except Exception as e:
        print(f"  extracted.balance_sheet: Error - {e}")
    
    try:
        result = db.execute_query("SELECT COUNT(*) FROM extracted.balance_sheet_daily;")
        print(f"  extracted.balance_sheet_daily: {result[0][0]} records")
    except Exception as e:
        print(f"  extracted.balance_sheet_daily: Error - {e}")
    
    # Check if old tables exist in public schema
    print("\nChecking for old tables in public schema:")
    try:
        result = db.execute_query("SELECT COUNT(*) FROM public.balance_sheet;")
        print(f"  public.balance_sheet: {result[0][0]} records (should be 0 or not exist)")
    except Exception as e:
        print(f"  public.balance_sheet: Does not exist (good!)")
    
    try:
        result = db.execute_query("SELECT COUNT(*) FROM public.balance_sheet_daily;")
        print(f"  public.balance_sheet_daily: {result[0][0]} records (should be 0 or not exist)")
    except Exception as e:
        print(f"  public.balance_sheet_daily: Does not exist (good!)")
    
    print("\nSchema verification completed!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Verify or reset balance sheet table schemas')
    parser.add_argument('--reset', action='store_true', help='Reset (drop) balance sheet tables for fresh start')
    args = parser.parse_args()
    
    db = PostgresDatabaseManager()
    db.connect()
    
    try:
        if args.reset:
            reset_balance_sheet_tables(db)
        else:
            main()
    finally:
        db.close()
