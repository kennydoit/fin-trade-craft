#!/usr/bin/env python3

from db.postgres_database_manager import PostgresDatabaseManager

try:
    db = PostgresDatabaseManager()
    db.connect()
    
    print('=== DEBUGGING WATERMARK MANAGER QUERY ===')
    
    # Run the exact same query structure as WatermarkManager but simplified for ORIC
    debug_query = """
        WITH quarterly_analysis AS (
            SELECT 
                ls.symbol_id, 
                ls.symbol,
                ew.last_fiscal_date,
                ew.last_successful_run,
                ew.consecutive_failures,
                -- Calculate expected latest quarter based on current date and reporting lag
                CASE 
                    -- Current quarter minus 1 day = previous quarter end
                    WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '45 days'
                    THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '1 day')::date
                    -- Previous quarter minus 1 day = quarter before that  
                    WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' + INTERVAL '45 days'
                    THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' - INTERVAL '1 day')::date
                    -- Two quarters ago
                    WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '6 months' + INTERVAL '45 days'
                    THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '6 months' - INTERVAL '1 day')::date
                    -- Three quarters ago
                    ELSE (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '9 months' - INTERVAL '1 day')::date
                END as expected_latest_quarter,
                -- Check if there's a quarterly gap or staleness
                CASE 
                    WHEN ew.last_fiscal_date IS NULL THEN TRUE -- Never processed
                    WHEN ew.last_fiscal_date < (
                        CASE 
                            WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '45 days'
                            THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '1 day')::date
                            WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' + INTERVAL '45 days'
                            THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' - INTERVAL '1 day')::date
                            WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '6 months' + INTERVAL '45 days'
                            THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '6 months' - INTERVAL '1 day')::date
                            ELSE (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '9 months' - INTERVAL '1 day')::date
                        END
                    ) THEN TRUE -- Has quarterly gap
                    WHEN ew.last_successful_run < NOW() - INTERVAL '24 hours' THEN TRUE -- Time-based staleness
                    ELSE FALSE
                END as needs_processing,
                -- Add debug info
                CURRENT_DATE as current_date,
                DATE_TRUNC('quarter', CURRENT_DATE) as current_quarter_start,
                (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '1 day')::date as prev_quarter_end,
                (CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '45 days') as in_current_quarter_reporting_period
            FROM source.listing_status ls
            LEFT JOIN source.extraction_watermarks ew ON ew.symbol_id = ls.symbol_id 
                                                       AND ew.table_name = 'balance_sheet'
            WHERE ls.asset_type = 'Stock'
              AND LOWER(ls.status) = 'active'
              AND ls.symbol = 'ORIC'  -- Focus on ORIC only
              AND COALESCE(ew.consecutive_failures, 0) < 3  -- Not permanently failed
        )
        SELECT symbol_id, symbol, last_fiscal_date, last_successful_run, consecutive_failures,
               expected_latest_quarter, needs_processing, current_date, current_quarter_start, 
               prev_quarter_end, in_current_quarter_reporting_period
        FROM quarterly_analysis
    """
    
    debug_result = db.execute_query(debug_query)
    if debug_result and len(debug_result) > 0:
        row = debug_result[0]
        print(f'ORIC Debug Analysis:')
        print(f'  Symbol ID: {row[0]}')
        print(f'  Symbol: {row[1]}')
        print(f'  Last Fiscal Date: {row[2]}')
        print(f'  Last Run: {row[3]}')
        print(f'  Failures: {row[4]}')
        print(f'  Expected Quarter: {row[5]}')
        print(f'  Needs Processing: {row[6]}')
        print()
        print(f'DEBUG INFO:')
        print(f'  Current Date: {row[7]}')
        print(f'  Current Quarter Start: {row[8]}')
        print(f'  Previous Quarter End: {row[9]}')
        print(f'  In Current Quarter Reporting Period: {row[10]}')
        
        if row[6]:  # needs_processing = True
            print('\n✅ ORIC identified as needing processing by this query')
        else:
            print('\n❌ ORIC NOT identified as needing processing by this query')
            
        # Check the WHERE clause manually
        print(f'\nWHERE CLAUSE CHECKS:')
        print(f'  Asset Type = Stock: ✅')
        print(f'  Status = Active: ✅') 
        print(f'  Symbol = ORIC: ✅')
        print(f'  Consecutive Failures < 3: {row[4]} < 3 = {(row[4] or 0) < 3}')
        
    else:
        print('❌ No result from debug query - ORIC filtered out by WHERE clause')

except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()
finally:
    if 'db' in locals():
        db.close()
