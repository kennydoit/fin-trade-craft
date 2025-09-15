#!/usr/bin/env python3

from db.postgres_database_manager import PostgresDatabaseManager
from datetime import datetime, date, timedelta

try:
    db = PostgresDatabaseManager()
    db.connect()

    # Check ORIC's latest quarterly data
    oric_data = db.fetch_dataframe("""
    SELECT 
        bs.symbol, 
        bs.fiscal_date_ending,
        bs.report_type,
        ew.last_fiscal_date,
        ew.last_successful_run,
        EXTRACT(EPOCH FROM (NOW() - ew.last_successful_run))/3600 as hours_since_last_run
    FROM source.balance_sheet bs
    RIGHT JOIN source.extraction_watermarks ew ON bs.symbol_id = ew.symbol_id AND ew.table_name = 'balance_sheet'
    JOIN source.listing_status ls ON ls.symbol_id = ew.symbol_id
    WHERE ls.symbol = 'ORIC'
    ORDER BY bs.fiscal_date_ending DESC
    LIMIT 10;
    """)
    
    if not oric_data.empty:
        print('=== ORIC Balance Sheet Data ===')
        for _, row in oric_data.iterrows():
            print(f'  Fiscal Date: {row["fiscal_date_ending"]} | Report: {row["report_type"]}')
            print(f'  Watermark Last Fiscal: {row["last_fiscal_date"]}')
            print(f'  Last Run: {row["last_successful_run"]} ({row["hours_since_last_run"]:.1f}h ago)')
            print()
    
    # Check what the current logic would determine for quarterly gap detection
    print('=== QUARTERLY GAP ANALYSIS ===')
    
    # Get current date
    current_date = datetime.now().date()
    print(f'Current Date: {current_date} (September 15, 2025)')
    
    # Simulate quarterly dates
    q1_2025 = date(2025, 3, 31)
    q2_2025 = date(2025, 6, 30)
    q3_2025 = date(2025, 9, 30)
    
    print(f'Q1 2025: {q1_2025} | Q2 2025: {q2_2025} | Q3 2025: {q3_2025}')
    
    # Check if we should expect Q2 2025 data to be available
    days_since_q2 = (current_date - q2_2025).days
    days_since_q1 = (current_date - q1_2025).days
    
    print(f'Days since Q1 2025: {days_since_q1}')
    print(f'Days since Q2 2025: {days_since_q2}')
    
    # Typical quarterly reporting lag is 45-90 days
    if days_since_q2 >= 45:
        print('✅ Q2 2025 data should be available (45+ days since quarter end)')
    else:
        print('⏳ Q2 2025 data may not be available yet')
        
    if days_since_q1 >= 45:
        print('✅ Q1 2025 data should definitely be available')
    
    # Check current watermarking logic for symbols needing processing
    print('\n=== CURRENT WATERMARKING LOGIC ===')
    symbols_needing_processing = db.fetch_dataframe("""
    SELECT 
        ls.symbol,
        ew.last_fiscal_date,
        ew.last_successful_run,
        EXTRACT(EPOCH FROM (NOW() - ew.last_successful_run))/3600 as hours_since_last_run,
        CASE 
            WHEN ew.last_successful_run IS NULL THEN 'Never processed'
            WHEN ew.last_successful_run < NOW() - INTERVAL '24 hours' THEN 'Stale (>24h)'
            ELSE 'Recent'
        END as processing_status
    FROM source.listing_status ls
    LEFT JOIN source.extraction_watermarks ew ON ls.symbol_id = ew.symbol_id AND ew.table_name = 'balance_sheet'
    WHERE ls.symbol IN ('ORIC', 'AAPL', 'MSFT', 'TSLA')
    ORDER BY ls.symbol;
    """)
    
    print('Sample symbols processing status:')
    for _, row in symbols_needing_processing.iterrows():
        print(f'  {row["symbol"]}: {row["processing_status"]} | Last Fiscal: {row["last_fiscal_date"]} | Last Run: {row["hours_since_last_run"]:.1f}h ago')
        
except Exception as e:
    print(f'Error: {e}')
finally:
    if 'db' in locals():
        db.close()
