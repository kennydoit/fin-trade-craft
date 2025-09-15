#!/usr/bin/env python3

from db.postgres_database_manager import PostgresDatabaseManager

try:
    db = PostgresDatabaseManager()
    db.connect()
    
    print('=== TESTING WATERMARK FILTERS ON ORIC ===')
    
    # Skip individual filter tests and go straight to full WHERE clause
    print('Skipping individual filter tests - testing full WHERE clause...')
        
    print('\n=== TESTING FULL WHERE CLAUSE ON ORIC ===')
    
    full_where_query = """
    SELECT 
        ls.symbol,
        ls.asset_type = 'Stock' as is_stock,
        LOWER(ls.status) = 'active' as is_active,
        ls.symbol NOT LIKE '%WS%' as not_warrant,
        ls.symbol NOT LIKE '%R' as not_right_ending_r,  
        ls.symbol NOT LIKE '%.R%' as not_right_with_dot,
        ls.symbol NOT LIKE '%P%' as not_preferred,
        ls.symbol NOT LIKE '%U' as not_unit,
        COALESCE(ew.consecutive_failures, 0) < 3 as not_failed,
        -- Overall check
        (ls.asset_type = 'Stock'
         AND LOWER(ls.status) = 'active'
         AND ls.symbol NOT LIKE '%WS%'
         AND ls.symbol NOT LIKE '%R'  
         AND ls.symbol NOT LIKE '%.R%'
         AND ls.symbol NOT LIKE '%P%'
         AND ls.symbol NOT LIKE '%U'
         AND COALESCE(ew.consecutive_failures, 0) < 3) as passes_all_filters
    FROM source.listing_status ls
    LEFT JOIN source.extraction_watermarks ew ON ew.symbol_id = ls.symbol_id 
                                               AND ew.table_name = 'balance_sheet'
    WHERE ls.symbol = 'ORIC'
    """
    
    where_result = db.execute_query(full_where_query)
    if where_result:
        row = where_result[0]
        print(f'Symbol: {row[0]}')
        print(f'Is Stock: {row[1]}')
        print(f'Is Active: {row[2]}')
        print(f'Not Warrant: {row[3]}')
        print(f'Not Right (ending R): {row[4]}')
        print(f'Not Right (with dot): {row[5]}')
        print(f'Not Preferred: {row[6]}')
        print(f'Not Unit: {row[7]}')
        print(f'Not Failed: {row[8]}')
        print(f'✅ PASSES ALL FILTERS: {row[9]}')
        
        if not row[9]:
            print('\n❌ ORIC is being filtered out by WHERE clause')
        else:
            print('\n✅ ORIC passes WHERE clause - issue must be elsewhere')

except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()
finally:
    if 'db' in locals():
        db.close()
