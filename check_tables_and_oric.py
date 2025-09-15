#!/usr/bin/env python3

from db.postgres_database_manager import PostgresDatabaseManager

try:
    db = PostgresDatabaseManager()
    db.connect()
    
    # Get all tables
    tables_query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
    ORDER BY table_name
    """
    
    tables = db.execute_query(tables_query)
    print('Available tables:')
    for table in tables:
        print(f'  - {table[0]}')
        
    print()
    
    # Check if ORIC exists in balance_sheet
    oric_query = """
    SELECT 
        symbol,
        fiscal_date_ending,
        reported_currency,
        total_assets
    FROM balance_sheet 
    WHERE symbol = 'ORIC' 
    ORDER BY fiscal_date_ending DESC 
    LIMIT 5
    """
    
    oric_result = db.execute_query(oric_query)
    if oric_result:
        print('ORIC Balance Sheet Data:')
        for row in oric_result:
            print(f'  Fiscal Date: {row[1]}, Assets: ${row[3]:,}')
    else:
        print('❌ No ORIC data in balance_sheet')
        
    print()
    
    # Check listing_status for ORIC
    listing_query = """
    SELECT symbol, status, exchange 
    FROM listing_status 
    WHERE symbol = 'ORIC'
    """
    
    listing_result = db.execute_query(listing_query)
    if listing_result:
        for row in listing_result:
            print(f'ORIC Listing: Status={row[1]}, Exchange={row[2]}')
    else:
        print('❌ ORIC not in listing_status')

except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()
finally:
    if 'db' in locals():
        db.close()
