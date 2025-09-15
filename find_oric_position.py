#!/usr/bin/env python3

from db.postgres_database_manager import PostgresDatabaseManager
from utils.incremental_etl import WatermarkManager

try:
    db = PostgresDatabaseManager()
    db.connect()
    
    watermark_mgr = WatermarkManager(db)
    
    print('=== FINDING ORIC EXACT POSITION ===')
    
    # Get unlimited results to find ORIC's exact position
    all_symbols = watermark_mgr.get_symbols_needing_processing(
        table_name='balance_sheet',
        staleness_hours=24,
        max_failures=3,
        limit=None,  # No limit
        quarterly_gap_detection=True,
        reporting_lag_days=45
    )
    
    print(f'Total symbols needing processing: {len(all_symbols)}')
    
    # Find ORIC's position
    oric_position = None
    for i, symbol_info in enumerate(all_symbols, 1):
        if symbol_info.get("symbol") == 'ORIC':
            oric_position = i
            break
    
    if oric_position:
        print(f'\n🎯 ORIC found at position #{oric_position}')
        
        # Show context around ORIC
        start_idx = max(0, oric_position - 6)
        end_idx = min(len(all_symbols), oric_position + 5)
        
        print(f'\nContext around ORIC (positions {start_idx + 1}-{end_idx}):')
        for i in range(start_idx, end_idx):
            symbol_info = all_symbols[i]
            symbol = symbol_info.get("symbol")
            last_fiscal = symbol_info.get("last_fiscal_date")
            last_run = symbol_info.get("last_successful_run")
            expected_quarter = symbol_info.get("expected_latest_quarter")
            
            # Determine priority
            if last_fiscal is None:
                priority = 0
            elif last_fiscal and expected_quarter and last_fiscal < expected_quarter:
                priority = 1
            else:
                priority = 2
                
            marker = '🎯' if symbol == 'ORIC' else '  '
            print(f'{marker} #{i+1}: {symbol} (P{priority})')
            print(f'    Last Fiscal: {last_fiscal}, Expected: {expected_quarter}')
            print(f'    Last Run: {last_run}')
            print()
            
        # Analyze why ORIC is ranked so low among priority 1
        print('=== ANALYSIS ===')
        
        priority_0_count = sum(1 for s in all_symbols if s.get("last_fiscal_date") is None)
        print(f'Priority 0 (never processed) count: {priority_0_count}')
        
        if oric_position > priority_0_count:
            print(f'ORIC position {oric_position} > priority 0 count {priority_0_count}')
            print('✅ ORIC correctly ranked after never-processed symbols')
            
            # Check ORIC's rank within priority 1
            priority_1_symbols = []
            for s in all_symbols[priority_0_count:]:
                last_fiscal = s.get("last_fiscal_date")
                expected_quarter = s.get("expected_latest_quarter")
                if last_fiscal and expected_quarter and last_fiscal < expected_quarter:
                    priority_1_symbols.append(s)
                    
            oric_rank_in_p1 = None
            for i, s in enumerate(priority_1_symbols, 1):
                if s.get("symbol") == 'ORIC':
                    oric_rank_in_p1 = i
                    break
                    
            if oric_rank_in_p1:
                print(f'ORIC rank within priority 1: #{oric_rank_in_p1} of {len(priority_1_symbols)}')
                
                # Show why ORIC ranks low within priority 1
                print('\nTop 5 priority 1 symbols (ORIC comparison):')
                for i, s in enumerate(priority_1_symbols[:5], 1):
                    symbol = s.get("symbol")
                    last_run = s.get("last_successful_run")
                    print(f'  #{i}: {symbol}, Last Run: {last_run}')
                    
                oric_data = next(s for s in priority_1_symbols if s.get("symbol") == 'ORIC')
                oric_last_run = oric_data.get("last_successful_run")
                print(f'  ORIC: Last Run: {oric_last_run}')
                print('\n📝 ORIC ranks low because it was run recently (2025-09-03)')
                print('   Priority 1 symbols are sorted by oldest last_run first')
                
    else:
        print('❌ ORIC not found in unlimited results')

except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()
finally:
    if 'db' in locals():
        db.close()
