#!/usr/bin/env python3

from db.postgres_database_manager import PostgresDatabaseManager

try:
    db = PostgresDatabaseManager()
    db.connect()
    
    print('=== SIMPLE ORIC CHECK ===')
    
    # First, just check if ORIC exists in the basic joins
    basic_query = """
    SELECT 
        ls.symbol_id, 
        ls.symbol,
        ls.asset_type,
        ls.status,
        ew.last_fiscal_date,
        ew.last_successful_run,
        ew.consecutive_failures
    FROM source.listing_status ls
    LEFT JOIN source.extraction_watermarks ew ON ew.symbol_id = ls.symbol_id 
                                               AND ew.table_name = 'balance_sheet'
    WHERE ls.symbol = 'ORIC'
    """
    
    basic_result = db.execute_query(basic_query)
    if basic_result:
        print('ORIC basic data:')
        for row in basic_result:
            print(f'  Symbol ID: {row[0]}')
            print(f'  Symbol: {row[1]}')
            print(f'  Asset Type: {row[2]}')
            print(f'  Status: {row[3]}')
            print(f'  Last Fiscal: {row[4]}')
            print(f'  Last Run: {row[5]}')
            print(f'  Failures: {row[6]}')
            
            # Check filters
            print(f'\nFILTER CHECKS:')
            print(f'  Asset Type = Stock: {row[2] == "Stock"}')
            print(f'  Status = Active: {row[3].lower() == "active"}')
            print(f'  Failures < 3: {(row[6] or 0) < 3}')
    else:
        print('❌ No ORIC found in basic query')
        
    print('\n=== TESTING QUARTERLY CALCULATION ===')
    
    # Test the quarterly calculation logic separately
    quarter_query = """
    SELECT 
        CURRENT_DATE as current_date,
        DATE_TRUNC('quarter', CURRENT_DATE) as current_quarter_start,
        DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '3 months' - INTERVAL '1 day' as current_quarter_end,
        DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '1 day' as prev_quarter_end,
        CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '45 days' as in_reporting_period,
        CASE 
            WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '45 days'
            THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '1 day')::date
            ELSE (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' - INTERVAL '1 day')::date
        END as expected_quarter
    """
    
    quarter_result = db.execute_query(quarter_query)
    if quarter_result:
        row = quarter_result[0]
        print(f'Current Date: {row[0]}')
        print(f'Current Quarter Start: {row[1]}')
        print(f'Current Quarter End: {row[2]}')
        print(f'Previous Quarter End: {row[3]}')
        print(f'In Reporting Period: {row[4]}')
        print(f'Expected Quarter: {row[5]}')

except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()
finally:
    if 'db' in locals():
        db.close()
