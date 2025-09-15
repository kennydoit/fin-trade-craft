#!/usr/bin/env python3

from db.postgres_database_manager import PostgresDatabaseManager

try:
    db = PostgresDatabaseManager()
    db.connect()
    
    # Find ORIC's symbol_id
    oric_id_query = """
    SELECT symbol_id, symbol, status, exchange
    FROM source.listing_status 
    WHERE symbol = 'ORIC'
    """
    
    oric_result = db.execute_query(oric_id_query)
    if oric_result:
        oric_symbol_id = oric_result[0][0]
        print(f'ORIC symbol_id: {oric_symbol_id}')
        
        # Check if ORIC has watermark data
        watermark_query = """
        SELECT 
            table_name,
            symbol_id,
            last_fiscal_date,
            last_successful_run,
            consecutive_failures
        FROM source.extraction_watermarks 
        WHERE symbol_id = %s AND table_name = 'balance_sheet'
        """
        
        watermark_result = db.execute_query(watermark_query, (oric_symbol_id,))
        if watermark_result:
            print('\nORIC watermark data:')
            for row in watermark_result:
                print(f'  Table: {row[0]}, Symbol ID: {row[1]}')
                print(f'  Last Fiscal: {row[2]}, Last Run: {row[3]}')
                print(f'  Failures: {row[4]}')
        else:
            print('\n❌ No ORIC watermark in source.extraction_watermarks')
            
        # Check ORIC's actual balance sheet data
        balance_query = """
        SELECT 
            fiscal_date_ending,
            total_assets,
            DATE_TRUNC('quarter', fiscal_date_ending) as quarter_date
        FROM source.balance_sheet 
        WHERE symbol_id = %s 
        ORDER BY fiscal_date_ending DESC 
        LIMIT 5
        """
        
        balance_result = db.execute_query(balance_query, (oric_symbol_id,))
        if balance_result:
            print('\nORIC Balance Sheet quarters:')
            for row in balance_result:
                print(f'  Fiscal Date: {row[0]}, Quarter: {row[2]}, Assets: ${row[1]:,}')
                
            # Check what quarter we should expect next
            latest_fiscal = balance_result[0][0]
            print(f'\nLatest fiscal date: {latest_fiscal}')
            
            # Q1 2025 ends March 31, so Q2 2025 should end June 30
            # Today is September 15, 2025, so Q2 data should be available
            print('Expected Q2 2025 data should be available by August 14, 2025')
            print('Today is September 15, 2025 - so ORIC should need Q2 extraction')
                
    else:
        print('❌ ORIC not found in source.listing_status')

except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()
finally:
    if 'db' in locals():
        db.close()
