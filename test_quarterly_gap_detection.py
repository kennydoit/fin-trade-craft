#!/usr/bin/env python3

from db.postgres_database_manager import PostgresDatabaseManager
from utils.incremental_etl import WatermarkManager

try:
    db = PostgresDatabaseManager()
    db.connect()
    
    watermark_mgr = WatermarkManager(db)
    
    print('=== TESTING ENHANCED QUARTERLY GAP DETECTION ===')
    print('Testing balance_sheet table with quarterly gap detection enabled...\n')
    
    # Test with quarterly gap detection enabled (new logic)
    symbols_with_gaps = watermark_mgr.get_symbols_needing_processing(
        table_name='balance_sheet',
        staleness_hours=24,
        max_failures=3,
        limit=10,
        quarterly_gap_detection=True,
        reporting_lag_days=45
    )
    
    print(f'Found {len(symbols_with_gaps)} symbols needing processing with quarterly gap detection:')
    print()
    
    for i, symbol_data in enumerate(symbols_with_gaps, 1):
        symbol = symbol_data['symbol']
        last_fiscal = symbol_data['last_fiscal_date']
        expected_quarter = symbol_data.get('expected_latest_quarter', 'N/A')
        has_gap = symbol_data.get('has_quarterly_gap', False)
        
        gap_status = "📅 HAS QUARTERLY GAP" if has_gap else "⏰ TIME-STALE ONLY"
        
        print(f'{i}. {symbol}:')
        print(f'   Last Fiscal Date: {last_fiscal}')
        print(f'   Expected Quarter: {expected_quarter}')
        print(f'   Status: {gap_status}')
        print()
        
        if i >= 5:  # Show first 5 for clarity
            break
    
    # Compare with original logic
    print('\n=== COMPARISON: ORIGINAL TIME-BASED LOGIC ===')
    
    symbols_time_based = watermark_mgr.get_symbols_needing_processing(
        table_name='balance_sheet',
        staleness_hours=24,
        max_failures=3,
        limit=10,
        quarterly_gap_detection=False  # Use original logic
    )
    
    print(f'Original time-based logic found {len(symbols_time_based)} symbols')
    
    # Check if ORIC is prioritized differently
    oric_in_gaps = next((s for s in symbols_with_gaps if s['symbol'] == 'ORIC'), None)
    oric_in_time = next((s for s in symbols_time_based if s['symbol'] == 'ORIC'), None)
    
    if oric_in_gaps:
        gap_position = next(i for i, s in enumerate(symbols_with_gaps, 1) if s['symbol'] == 'ORIC')
        print(f"\nORIC with gap detection: Position #{gap_position}")
        print(f"  Last Fiscal: {oric_in_gaps['last_fiscal_date']}")
        print(f"  Expected: {oric_in_gaps.get('expected_latest_quarter')}")
        print(f"  Has Gap: {oric_in_gaps.get('has_quarterly_gap')}")
    
    if oric_in_time:
        time_position = next(i for i, s in enumerate(symbols_time_based, 1) if s['symbol'] == 'ORIC')
        print(f"\nORIC with time-based: Position #{time_position}")
        print(f"  Last Fiscal: {oric_in_time['last_fiscal_date']}")

except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()
finally:
    if 'db' in locals():
        db.close()
