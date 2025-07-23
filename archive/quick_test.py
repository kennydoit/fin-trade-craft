#!/usr/bin/env python3
"""
Simple test to verify PostgreSQL connection works
"""
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

# Quick connection test
try:
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        database=os.getenv('POSTGRES_DATABASE')
    )
    
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM overview;")
    count = cursor.fetchone()[0]
    
    print(f"✅ Connection successful! Overview table has {count:,} rows")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"❌ Error: {e}")
