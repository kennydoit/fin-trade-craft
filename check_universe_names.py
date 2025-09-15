#!/usr/bin/env python3

from db.postgres_database_manager import PostgresDatabaseManager

try:
    db = PostgresDatabaseManager()
    db.connect()

    # Get all unique universe names
    universe_names = db.fetch_dataframe("""
    SELECT DISTINCT universe_name, universe_id
    FROM transformed.symbol_universes 
    ORDER BY universe_name;
    """)
    
    print('Available universe names:')
    for _, row in universe_names.iterrows():
        print(f'  {row["universe_name"]} (ID: {row["universe_id"]})')
        
    # Check if the specific name exists with partial matching
    partial_matches = db.fetch_dataframe("""
    SELECT DISTINCT universe_name, universe_id
    FROM transformed.symbol_universes 
    WHERE universe_name ILIKE '%IPO%500%' OR universe_name ILIKE '%500M%' OR universe_name ILIKE '%1B%'
    ORDER BY universe_name;
    """)
    
    if not partial_matches.empty:
        print('\nPartial matches for IPO/500M/1B:')
        for _, row in partial_matches.iterrows():
            print(f'  {row["universe_name"]} (ID: {row["universe_id"]})')
    else:
        print('\nNo partial matches found')
            
except Exception as e:
    print(f'Error: {e}')
finally:
    if 'db' in locals():
        db.close()
