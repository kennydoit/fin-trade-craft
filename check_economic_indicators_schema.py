#!/usr/bin/env python3

from db.postgres_database_manager import PostgresDatabaseManager

try:
    db = PostgresDatabaseManager()
    db.connect()

    # Check the actual column names in source.economic_indicators  
    columns = db.fetch_dataframe("""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_schema = 'source' AND table_name = 'economic_indicators'
    ORDER BY ordinal_position;
    """)
    print('source.economic_indicators columns:')
    for _, row in columns.iterrows():
        print(f'  {row["column_name"]}')

    # Check the constraint
    constraints = db.fetch_dataframe("""
    SELECT conname, pg_get_constraintdef(oid) as definition
    FROM pg_constraint 
    WHERE conrelid = 'source.economic_indicators'::regclass
    AND contype = 'u';
    """)
    print('\nUNIQUE constraints:')
    for _, row in constraints.iterrows():
        print(f'  {row["conname"]}: {row["definition"]}')

    db.close()
    
except Exception as e:
    print(f'Error: {e}')
