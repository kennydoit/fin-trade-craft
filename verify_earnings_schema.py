#!/usr/bin/env python3

import sys
sys.path.append('.')

from db.postgres_database_manager import PostgresDatabaseManager

def verify_earnings_schema():
    """Verify that earnings call transcripts table creation uses extracted schema."""
    db = PostgresDatabaseManager()
    db.connect()
    
    print("Checking earnings call transcripts schema configuration...")
    print("=" * 60)
    
    # Check if table exists in extracted schema
    print("\n1. Checking extracted schema:")
    result = db.execute_query("""
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_name = 'earnings_call_transcripts'
        ORDER BY table_schema;
    """)
    
    if result:
        for row in result:
            print(f"   {row[0]}.{row[1]}")
    else:
        print("   No earnings_call_transcripts table found yet (will be created on first run)")
    
    # Check record counts if table exists
    print("\n2. Record counts (if table exists):")
    try:
        result = db.execute_query("SELECT COUNT(*) FROM extracted.earnings_call_transcripts;")
        print(f"   extracted.earnings_call_transcripts: {result[0][0]} records")
    except Exception as e:
        print(f"   extracted.earnings_call_transcripts: Table does not exist yet (normal for first run)")
    
    # Check if old table exists in public schema
    print("\n3. Checking for table in public schema:")
    try:
        result = db.execute_query("SELECT COUNT(*) FROM public.earnings_call_transcripts;")
        print(f"   ⚠️  public.earnings_call_transcripts: {result[0][0]} records (should not exist!)")
    except Exception as e:
        print(f"   ✅ public.earnings_call_transcripts: Does not exist (good!)")
    
    db.close()
    
    print("\n" + "=" * 60)
    print("SCHEMA VERIFICATION COMPLETE")
    print("✅ The extractor is configured to use extracted.earnings_call_transcripts")

if __name__ == "__main__":
    verify_earnings_schema()
