#!/usr/bin/env python3

from db.postgres_database_manager import PostgresDatabaseManager

try:
    db = PostgresDatabaseManager()
    db.connect()
    
    print('=== DEBUGGING ORIC QUARTERLY COMPARISON ===')
    
    # Test the exact quarterly logic from WatermarkManager for ORIC
    debug_query = """
    SELECT 
        ls.symbol,
        ew.last_fiscal_date,
        ew.last_successful_run,
        -- Calculate expected latest quarter (simplified)
        CASE 
            WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '45 days'
            THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '1 day')::date
            WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' + INTERVAL '45 days'
            THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' - INTERVAL '1 day')::date
            ELSE (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '6 months' - INTERVAL '1 day')::date
        END as expected_latest_quarter,
        -- Test the gap comparison
        ew.last_fiscal_date < (
            CASE 
                WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '45 days'
                THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '1 day')::date
                WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' + INTERVAL '45 days'
                THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' - INTERVAL '1 day')::date
                ELSE (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '6 months' - INTERVAL '1 day')::date
            END
        ) as has_quarterly_gap,
        -- Test time staleness
        ew.last_successful_run < NOW() - INTERVAL '24 hours' as is_time_stale,
        -- Priority calculation
        CASE 
            WHEN ew.last_fiscal_date IS NULL THEN 0 -- Never processed
            WHEN ew.last_fiscal_date < (
                CASE 
                    WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '45 days'
                    THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '1 day')::date
                    WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' + INTERVAL '45 days'
                    THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' - INTERVAL '1 day')::date
                    ELSE (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '6 months' - INTERVAL '1 day')::date
                END
            ) THEN 1 -- Has quarterly gap
            ELSE 2 -- Time-stale only
        END as calculated_priority
    FROM source.listing_status ls
    LEFT JOIN source.extraction_watermarks ew ON ew.symbol_id = ls.symbol_id 
                                               AND ew.table_name = 'balance_sheet'
    WHERE ls.symbol = 'ORIC'
    """
    
    result = db.execute_query(debug_query)
    if result:
        row = result[0]
        print(f'Symbol: {row[0]}')
        print(f'Last Fiscal Date: {row[1]}')
        print(f'Last Run: {row[2]}')
        print(f'Expected Latest Quarter: {row[3]}')
        print(f'Has Quarterly Gap: {row[4]}')
        print(f'Is Time Stale: {row[5]}')
        print(f'Calculated Priority: {row[6]}')
        
        if row[4]:
            print('✅ ORIC should have quarterly gap priority (1)')
        elif row[5]:
            print('⚠️ ORIC only time-stale priority (2)')
        else:
            print('❓ ORIC should not need processing')
            
        # Manual verification
        print(f'\nMANUAL VERIFICATION:')
        print(f'  ORIC last fiscal: {row[1]}')
        print(f'  Expected quarter: {row[3]}')
        print(f'  Is {row[1]} < {row[3]}? {row[1] < row[3] if row[1] and row[3] else "N/A"}')

except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()
finally:
    if 'db' in locals():
        db.close()
