#!/usr/bin/env python3
"""
Check source table columns to understand the structure
"""
import os
import sys
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def check_source_columns():
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        database=os.getenv('POSTGRES_DB', 'fin_trade_craft'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD')
    )
    cursor = conn.cursor()
    
    print("🔍 Checking Source Table Columns")
    print("=" * 50)
    
    # Overview columns
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'extracted' AND table_name = 'overview'
        ORDER BY ordinal_position
    """)
    overview_cols = [r[0] for r in cursor.fetchall()]
    
    print("📈 Overview table columns:")
    for col in overview_cols:
        print(f"  {col}")
    
    # Listing status columns
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'extracted' AND table_name = 'listing_status'
        ORDER BY ordinal_position
    """)
    listing_cols = [r[0] for r in cursor.fetchall()]
    
    print(f"\n📋 Listing_status table columns:")
    for col in listing_cols:
        print(f"  {col}")
    
    # Sample data to understand the structure
    print(f"\n📊 Sample overview data:")
    cursor.execute("SELECT symbol, name, sector, exchange FROM extracted.overview LIMIT 3")
    overview_samples = cursor.fetchall()
    for row in overview_samples:
        print(f"  {row}")
    
    print(f"\n📊 Sample listing_status data:")
    cursor.execute("SELECT symbol, name, asset_type, exchange FROM extracted.listing_status LIMIT 3")
    listing_samples = cursor.fetchall()
    for row in listing_samples:
        print(f"  {row}")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_source_columns()
