#!/usr/bin/env python3

from db.postgres_database_manager import PostgresDatabaseManager
from utils.incremental_etl import WatermarkManager

try:
    db = PostgresDatabaseManager()
    db.connect()
    
    watermark_mgr = WatermarkManager(db)
    
    print('=== QUARTERLY GAP DETECTION FOR SPECIFIC SYMBOLS ===')
    
    # Test specifically for symbols that have Q1 but should have Q2 data
    symbols_with_gaps = watermark_mgr.get_symbols_needing_processing(
        table_name='balance_sheet',
        staleness_hours=24,
        max_failures=3,
        limit=50,
        quarterly_gap_detection=True,
        reporting_lag_days=45
    )
    
    # Look for symbols with actual fiscal dates (not null) that still have gaps
    symbols_with_actual_gaps = [s for s in symbols_with_gaps if s['last_fiscal_date'] is not None]
    
    print(f'Found {len(symbols_with_actual_gaps)} symbols with actual quarterly gaps (not never-processed):')
    print()
    
    for i, symbol_data in enumerate(symbols_with_actual_gaps[:10], 1):
        symbol = symbol_data['symbol']
        last_fiscal = symbol_data['last_fiscal_date']
        expected_quarter = symbol_data.get('expected_latest_quarter')
        has_gap = symbol_data.get('has_quarterly_gap', False)
        
        print(f'{i}. {symbol}:')
        print(f'   Last Fiscal Date: {last_fiscal}')
        print(f'   Expected Quarter: {expected_quarter}')
        print(f'   Gap: {has_gap}')
        print()
    
    # Specifically look for ORIC
    oric_data = next((s for s in symbols_with_gaps if s['symbol'] == 'ORIC'), None)
    if oric_data:
        print('\n🎯 ORIC ANALYSIS:')
        print(f'   Symbol: ORIC')
        print(f'   Last Fiscal Date: {oric_data["last_fiscal_date"]}') 
        print(f'   Expected Quarter: {oric_data.get("expected_latest_quarter")}')
        print(f'   Has Quarterly Gap: {oric_data.get("has_quarterly_gap")}')
        position = next(i for i, s in enumerate(symbols_with_gaps, 1) if s['symbol'] == 'ORIC')
        print(f'   Priority Position: #{position}')
    else:
        print('\n⚠️ ORIC not found in symbols needing processing')
        
    # Check if the system correctly identifies Q2 2025 as expected quarter
    print(f'\n📅 CURRENT DATE: September 15, 2025')
    print(f'📅 Q2 2025 END: June 30, 2025 (77 days ago)')
    print(f'📅 REPORTING LAG: 45 days')
    print(f'📅 Q2 DATA SHOULD BE AVAILABLE: August 14, 2025 (32 days ago)')

except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()
finally:
    if 'db' in locals():
        db.close()
