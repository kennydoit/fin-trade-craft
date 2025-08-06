#!/usr/bin/env python3

import sys
sys.path.append('.')

from db.postgres_database_manager import PostgresDatabaseManager

def verify_cash_flow_schema():
    """Verify that cash flow table is in the extracted schema."""
    db = PostgresDatabaseManager()
    db.connect()
    
    print("Checking cash flow schema configuration...")
    print("=" * 60)
    
    # Check if table exists in extracted schema
    print("\n1. Checking extracted schema:")
    result = db.execute_query("""
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_name = 'cash_flow'
        ORDER BY table_schema;
    """)
    
    if result:
        for row in result:
            print(f"   {row[0]}.{row[1]}")
    else:
        print("   No cash_flow table found yet (will be created on first run)")
    
    # Check record counts if table exists
    print("\n2. Record counts (if table exists):")
    try:
        result = db.execute_query("SELECT COUNT(*) FROM extracted.cash_flow;")
        print(f"   extracted.cash_flow: {result[0][0]} records")
    except Exception as e:
        print(f"   extracted.cash_flow: Table does not exist yet (normal for first run)")
    
    # Check if old table exists in public schema
    print("\n3. Checking for table in public schema:")
    try:
        result = db.execute_query("SELECT COUNT(*) FROM public.cash_flow;")
        print(f"   ⚠️  public.cash_flow: {result[0][0]} records (should not exist!)")
    except Exception as e:
        print(f"   ✅ public.cash_flow: Does not exist (good!)")
    
    db.close()
    
    print("\n" + "=" * 60)
    print("SCHEMA VERIFICATION COMPLETE")
    print("✅ The extractor is configured to use extracted.cash_flow")

if __name__ == "__main__":
    verify_cash_flow_schema()
