#!/usr/bin/env python3
"""
Check if time_series_daily_adjusted table exists in extracted schema
"""
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def check_table():
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        database=os.getenv('POSTGRES_DB', 'fin_trade_craft'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD')
    )
    cursor = conn.cursor()
    
    # Check if table exists in extracted schema
    cursor.execute("""
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_schema = 'extracted' 
        AND table_name = 'time_series_daily_adjusted'
    """)
    table_exists = cursor.fetchone()[0] > 0
    
    print(f"Time series table exists in extracted schema: {table_exists}")
    
    # If it doesn't exist, check if it exists in public schema
    if not table_exists:
        cursor.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'time_series_daily_adjusted'
        """)
        public_exists = cursor.fetchone()[0] > 0
        print(f"Time series table exists in public schema: {public_exists}")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_table()
