#!/usr/bin/env python3

import psycopg2

try:
    conn = psycopg2.connect(host='localhost', database='fin_trade_craft', user='postgres', password='postgres')
    cursor = conn.cursor()
    
    # Check which schemas exist
    cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('source', 'extracted', 'public');")
    schemas = cursor.fetchall()
    print('Available schemas:', [s[0] for s in schemas])
    
    # Check if overview view exists and in which schema
    cursor.execute("""
    SELECT schemaname, viewname 
    FROM pg_views 
    WHERE viewname = 'overview';
    """)
    views = cursor.fetchall()
    print('Overview views found:', views)
    
    # Check source.company_overview table structure
    cursor.execute("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_schema = 'source' AND table_name = 'company_overview'
    ORDER BY ordinal_position;
    """)
    columns = cursor.fetchall()
    print('source.company_overview columns:')
    for col, dtype in columns:
        print(f'  {col}: {dtype}')
    
    conn.close()
except Exception as e:
    print(f'Error: {e}')
