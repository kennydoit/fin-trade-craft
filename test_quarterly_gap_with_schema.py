#!/usr/bin/env python3

from db.postgres_database_manager import PostgresDatabaseManager
from utils.incremental_etl import WatermarkManager

try:
    db = PostgresDatabaseManager()
    db.connect()
    
    watermark_mgr = WatermarkManager(db)
    
    print('=== TESTING QUARTERLY GAP DETECTION WITH CORRECT SCHEMA ===')
    
    # Test quarterly gap detection for balance_sheet
    symbols_needing_processing = watermark_mgr.get_symbols_needing_processing(
        table_name='balance_sheet',
        staleness_hours=24,
        max_failures=3,
        limit=20,
        quarterly_gap_detection=True,
        reporting_lag_days=45
    )
    
    print(f'Found {len(symbols_needing_processing)} symbols needing processing')
    print()
    
    # Look for ORIC specifically
    oric_found = False
    for i, symbol_info in enumerate(symbols_needing_processing, 1):
        print(f'{i}. Symbol ID: {symbol_info.get("symbol_id")}')
        print(f'   Symbol: {symbol_info.get("symbol")}')  
        print(f'   Last Fiscal Date: {symbol_info.get("last_fiscal_date")}')
        print(f'   Last Run: {symbol_info.get("last_successful_run")}')
        print(f'   Expected Quarter: {symbol_info.get("expected_latest_quarter")}')
        print(f'   Needs Processing: {symbol_info.get("needs_processing")}')
        
        if symbol_info.get("symbol") == 'ORIC':
            oric_found = True
            print('   🎯 FOUND ORIC!')
            
        print()
        
        if i >= 10:  # Show first 10
            break
            
    if not oric_found:
        print('❌ ORIC not found in results - checking why...')
        
        # Check ORIC directly with raw query
        oric_check_query = """
        SELECT 
            ls.symbol_id,
            ls.symbol,
            ew.last_fiscal_date,
            ew.last_successful_run,
            ew.consecutive_failures,
            
            -- Calculate the expected latest quarter based on current date and reporting lag
            CASE 
                WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '3 months' + INTERVAL '45 days' THEN
                    DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '3 months' - INTERVAL '1 day'  -- Current quarter end
                WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '45 days' THEN
                    DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '1 day'  -- Previous quarter end
                ELSE
                    DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' - INTERVAL '1 day'  -- Two quarters ago
            END::date as expected_latest_quarter,
            
            CASE 
                WHEN ew.last_fiscal_date IS NULL THEN TRUE -- Never processed
                WHEN ew.last_fiscal_date < (
                    CASE 
                        WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '3 months' + INTERVAL '45 days' THEN
                            DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '3 months' - INTERVAL '1 day'
                        WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '45 days' THEN
                            DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '1 day'
                        ELSE
                            DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' - INTERVAL '1 day'
                    END
                ) THEN TRUE -- Has quarterly gap
                WHEN ew.last_successful_run < NOW() - INTERVAL '24 hours' THEN TRUE -- Time-based staleness
                ELSE FALSE
            END as should_process
            
        FROM source.listing_status ls
        LEFT JOIN source.extraction_watermarks ew ON ew.symbol_id = ls.symbol_id 
                                                   AND ew.table_name = 'balance_sheet'
        WHERE ls.symbol = 'ORIC'
        """
        
        oric_result = db.execute_query(oric_check_query)
        if oric_result:
            row = oric_result[0]
            print(f'ORIC Direct Check:')
            print(f'  Symbol ID: {row[0]}')
            print(f'  Symbol: {row[1]}')
            print(f'  Last Fiscal Date: {row[2]}')
            print(f'  Last Run: {row[3]}')
            print(f'  Failures: {row[4]}')
            print(f'  Expected Quarter: {row[5]}')
            print(f'  Should Process: {row[6]}')
            
            if row[6]:  # should_process = True
                print('  ✅ ORIC should be processed according to logic')
            else:
                print('  ❌ ORIC not flagged for processing - logic issue?')
        
except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()
finally:
    if 'db' in locals():
        db.close()
