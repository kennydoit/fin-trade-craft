import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager

with PostgresDatabaseManager() as db:
    # Check the latest balance sheet data
    result = db.fetch_query("""
        SELECT symbol, fiscal_date_ending, report_type, total_assets, 
               total_liabilities, total_shareholder_equity, created_at
        FROM source.balance_sheet 
        WHERE symbol = 'AMZN'
        ORDER BY created_at DESC, fiscal_date_ending DESC 
        LIMIT 5
    """)
    
    print("Latest AMZN balance sheet data:")
    print("Symbol | Fiscal Date  | Type     | Total Assets    | Total Liab     | Equity         | Created")
    print("-" * 95)
    
    for row in result:
        symbol, fiscal_date, report_type, assets, liab, equity, created = row
        assets_str = f"${float(assets)/1e9:.1f}B" if assets else "None"
        liab_str = f"${float(liab)/1e9:.1f}B" if liab else "None"
        equity_str = f"${float(equity)/1e9:.1f}B" if equity else "None"
        created_str = created.strftime("%Y-%m-%d %H:%M")
        
        print(f"{symbol:6} | {fiscal_date} | {report_type:8} | {assets_str:15} | {liab_str:14} | {equity_str:14} | {created_str}")
    
    # Check watermark
    watermark_result = db.fetch_query("""
        SELECT last_fiscal_date, last_successful_run, consecutive_failures
        FROM source.extraction_watermarks 
        WHERE table_name = 'balance_sheet' AND symbol_id = 22779604
    """)
    
    if watermark_result:
        last_fiscal, last_run, failures = watermark_result[0]
        print(f"\nWatermark status:")
        print(f"  Last fiscal date: {last_fiscal}")
        print(f"  Last successful run: {last_run}")
        print(f"  Consecutive failures: {failures}")
    
    # Check total count
    count_result = db.fetch_query("SELECT COUNT(*) FROM source.balance_sheet WHERE symbol = 'AMZN'")
    print(f"\nTotal AMZN records in database: {count_result[0][0]}")
