#!/usr/bin/env python3

from db.postgres_database_manager import PostgresDatabaseManager

try:
    db = PostgresDatabaseManager()
    db.connect()
    
    # Check which schemas exist
    schemas = db.fetch_dataframe("SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('source', 'extracted', 'public');")
    print('Available schemas:', list(schemas['schema_name']))
    
    # Check if overview view exists and in which schema
    views = db.fetch_dataframe("""
    SELECT schemaname, viewname 
    FROM pg_views 
    WHERE viewname = 'overview';
    """)
    print('Overview views found:', views.to_dict('records') if not views.empty else 'None')
    
    # Check source.company_overview table structure
    columns = db.fetch_dataframe("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_schema = 'source' AND table_name = 'company_overview'
    ORDER BY ordinal_position;
    """)
    print('source.company_overview columns:')
    for _, row in columns.iterrows():
        print(f'  {row["column_name"]}: {row["data_type"]}')
    
    db.close()
except Exception as e:
    print(f'Error: {e}')
