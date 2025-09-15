#!/usr/bin/env python3

from db.postgres_database_manager import PostgresDatabaseManager

try:
    db = PostgresDatabaseManager()
    db.connect()
    
    print('=== CHECKING ORIC DATA DIRECTLY ===')
    
    # Check extraction_watermarks for ORIC
    watermark_query = """
    SELECT 
        symbol,
        table_name,
        last_fiscal_date,
        last_extracted_at,
        failure_count,
        EXTRACT(DAYS FROM CURRENT_DATE - last_extracted_at) as days_since_extraction,
        CASE 
            WHEN last_fiscal_date IS NULL THEN 'Never processed'
            ELSE 'Has data'
        END as data_status
    FROM extraction_watermarks 
    WHERE symbol = 'ORIC' AND table_name = 'balance_sheet'
    """
    
    watermark_result = db.execute_query(watermark_query)
    if watermark_result:
        for row in watermark_result:
            print(f'Watermark - Symbol: {row[0]}, Table: {row[1]}')
            print(f'           Last Fiscal: {row[2]}')
            print(f'           Last Extracted: {row[3]}')
            print(f'           Failures: {row[4]}')
            print(f'           Days Since Extract: {row[5]}')
            print(f'           Status: {row[6]}')
    else:
        print('❌ No watermark record for ORIC balance_sheet')
    
    print()
    
    # Check actual balance sheet data for ORIC
    data_query = """
    SELECT 
        symbol,
        fiscal_date_ending,
        reported_currency,
        total_assets,
        ROW_NUMBER() OVER (ORDER BY fiscal_date_ending DESC) as row_num
    FROM balance_sheet 
    WHERE symbol = 'ORIC' 
    ORDER BY fiscal_date_ending DESC 
    LIMIT 5
    """
    
    data_result = db.execute_query(data_query)
    if data_result:
        print('Balance Sheet Data for ORIC:')
        for row in data_result:
            print(f'  {row[4]}. Fiscal Date: {row[1]}, Currency: {row[2]}, Assets: ${row[3]:,}')
    else:
        print('❌ No balance sheet data for ORIC')
    
    print()
    
    # Check if ORIC is in listing_status
    listing_query = """
    SELECT symbol, status, exchange, asset_type, ipo_date, delisting_date
    FROM listing_status 
    WHERE symbol = 'ORIC'
    """
    
    listing_result = db.execute_query(listing_query)
    if listing_result:
        for row in listing_result:
            print(f'Listing Status - Symbol: {row[0]}, Status: {row[1]}')
            print(f'                Exchange: {row[2]}, Type: {row[3]}')
            print(f'                IPO: {row[4]}, Delisting: {row[5]}')
    else:
        print('❌ ORIC not found in listing_status')

except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()
finally:
    if 'db' in locals():
        db.close()
