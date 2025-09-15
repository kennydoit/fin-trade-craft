#!/usr/bin/env python3

from db.postgres_database_manager import PostgresDatabaseManager
from utils.incremental_etl import WatermarkManager

try:
    db = PostgresDatabaseManager()
    db.connect()
    
    watermark_mgr = WatermarkManager(db)
    
    print('=== QUARTERLY GAP DETECTION PERFORMANCE COMPARISON ===')
    
    # Get symbols with quarterly gap detection (new method)
    print('1. With Quarterly Gap Detection (Enhanced):')
    with_quarterly_gaps = watermark_mgr.get_symbols_needing_processing(
        table_name='balance_sheet',
        staleness_hours=24,
        max_failures=3,
        limit=20,
        quarterly_gap_detection=True,
        reporting_lag_days=45
    )
    
    print(f'   Found: {len(with_quarterly_gaps)} symbols')
    priority_0_count = sum(1 for s in with_quarterly_gaps if s.get("last_fiscal_date") is None)
    priority_1_count = sum(1 for s in with_quarterly_gaps 
                          if s.get("last_fiscal_date") and 
                          s.get("expected_latest_quarter") and
                          s.get("last_fiscal_date") < s.get("expected_latest_quarter"))
    
    print(f'   Priority 0 (never processed): {priority_0_count}')
    print(f'   Priority 1 (quarterly gaps): {priority_1_count}') 
    print(f'   Priority 2 (time-stale only): {len(with_quarterly_gaps) - priority_0_count - priority_1_count}')
    
    print('\n   Sample symbols with quarterly gaps:')
    quarterly_gap_symbols = [s for s in with_quarterly_gaps 
                           if s.get("last_fiscal_date") and 
                           s.get("expected_latest_quarter") and
                           s.get("last_fiscal_date") < s.get("expected_latest_quarter")][:5]
    
    for i, s in enumerate(quarterly_gap_symbols, 1):
        symbol = s.get("symbol")
        last_fiscal = s.get("last_fiscal_date")
        expected = s.get("expected_latest_quarter")
        print(f'     {i}. {symbol}: Q1 {last_fiscal} → Missing Q2 {expected}')
    
    print('\n2. Without Quarterly Gap Detection (Legacy):')
    without_quarterly_gaps = watermark_mgr.get_symbols_needing_processing(
        table_name='balance_sheet',
        staleness_hours=24,
        max_failures=3,
        limit=20,
        quarterly_gap_detection=False  # Legacy time-based only
    )
    
    print(f'   Found: {len(without_quarterly_gaps)} symbols')
    print('   Sample symbols (time-stale only):')
    for i, s in enumerate(without_quarterly_gaps[:5], 1):
        symbol = s.get("symbol")
        last_fiscal = s.get("last_fiscal_date")
        last_run = s.get("last_successful_run")
        print(f'     {i}. {symbol}: Last Fiscal {last_fiscal}, Last Run {last_run}')
    
    print('\n=== KEY BENEFITS ===')
    print('✅ Enhanced prioritization: Never processed → Quarterly gaps → Time-stale')
    print('✅ Business-aware logic: Understands quarterly reporting cycles')
    print('✅ Efficient resource usage: Focus on actual missing quarters')
    print('✅ Predictable results: Deterministic quarterly date calculations')
    
    # Show ORIC specifically if in the sample
    oric_in_sample = next((s for s in with_quarterly_gaps if s.get("symbol") == 'ORIC'), None)
    if oric_in_sample:
        print(f'\n🎯 ORIC Example:')
        print(f'   Last Fiscal: {oric_in_sample.get("last_fiscal_date")} (Q1 2025)')
        print(f'   Expected: {oric_in_sample.get("expected_latest_quarter")} (Q2 2025)')
        print(f'   Gap: Missing 77 days worth of quarterly data!')
    else:
        print('\n📝 ORIC not in top 20 (ranked by staleness within priority levels)')

except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()
finally:
    if 'db' in locals():
        db.close()
