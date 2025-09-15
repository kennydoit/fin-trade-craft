#!/usr/bin/env python3

from db.postgres_database_manager import PostgresDatabaseManager
from utils.incremental_etl import WatermarkManager

try:
    db = PostgresDatabaseManager()
    db.connect()
    
    watermark_mgr = WatermarkManager(db)
    
    print('=== TESTING QUARTERLY GAP DETECTION WITH LARGER LIMIT ===')
    
    # Get more results to see if ORIC appears later
    symbols_needing_processing = watermark_mgr.get_symbols_needing_processing(
        table_name='balance_sheet',
        staleness_hours=24,
        max_failures=3,
        limit=100,  # Increased limit
        quarterly_gap_detection=True,
        reporting_lag_days=45
    )
    
    print(f'Found {len(symbols_needing_processing)} symbols needing processing')
    print()
    
    # Look for ORIC and analyze priority levels
    never_processed_count = 0
    quarterly_gap_count = 0
    time_stale_count = 0
    oric_found = False
    
    for i, symbol_info in enumerate(symbols_needing_processing, 1):
        symbol = symbol_info.get("symbol")
        last_fiscal = symbol_info.get("last_fiscal_date")
        expected_quarter = symbol_info.get("expected_latest_quarter")
        
        # Determine priority category
        if last_fiscal is None:
            never_processed_count += 1
            priority = "Never processed (0)"
        elif last_fiscal and expected_quarter and last_fiscal < expected_quarter:
            quarterly_gap_count += 1
            priority = "Quarterly gap (1)"
        else:
            time_stale_count += 1
            priority = "Time stale (2)"
            
        if symbol == 'ORIC':
            oric_found = True
            print(f'🎯 FOUND ORIC at position {i}!')
            print(f'   Symbol: {symbol}')
            print(f'   Last Fiscal Date: {last_fiscal}')
            print(f'   Expected Quarter: {expected_quarter}')
            print(f'   Priority: {priority}')
            print()
            
        # Show first few from each category for debugging
        if (never_processed_count <= 3 and last_fiscal is None) or \
           (quarterly_gap_count <= 3 and last_fiscal and expected_quarter and last_fiscal < expected_quarter) or \
           (time_stale_count <= 3 and not (last_fiscal is None) and not (last_fiscal and expected_quarter and last_fiscal < expected_quarter)):
            print(f'{i}. {symbol} - {priority}')
            if last_fiscal:
                print(f'   Last Fiscal: {last_fiscal}, Expected: {expected_quarter}')
                print(f'   Gap: {last_fiscal < expected_quarter if expected_quarter else "N/A"}')
            print()
        
    print(f'SUMMARY:')
    print(f'  Never processed (priority 0): {never_processed_count}')
    print(f'  Quarterly gaps (priority 1): {quarterly_gap_count}')
    print(f'  Time stale only (priority 2): {time_stale_count}')
    print(f'  Total: {len(symbols_needing_processing)}')
    
    if not oric_found:
        print('\n❌ ORIC not found in results')
        
        # Let's manually run a query to see if ORIC would be returned with no limit
        print('\n=== CHECKING IF ORIC IS IN UNLIMITED RESULTS ===')
        
        unlimited_symbols = watermark_mgr.get_symbols_needing_processing(
            table_name='balance_sheet',
            staleness_hours=24,
            max_failures=3,
            limit=None,  # No limit
            quarterly_gap_detection=True,
            reporting_lag_days=45
        )
        
        oric_in_unlimited = any(s.get("symbol") == 'ORIC' for s in unlimited_symbols)
        print(f'ORIC in unlimited results: {oric_in_unlimited}')
        print(f'Total unlimited results: {len(unlimited_symbols)}')

except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()
finally:
    if 'db' in locals():
        db.close()
