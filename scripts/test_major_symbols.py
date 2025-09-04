import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from data_pipeline.extract.extract_balance_sheet import BalanceSheetExtractor
from db.postgres_database_manager import PostgresDatabaseManager

# Test the extractor with known good symbols
extractor = BalanceSheetExtractor()

# Test symbols: AAPL (already done), MSFT, GOOGL  
test_cases = [
    ('MSFT', 197765848),
    ('GOOGL', 109714636),
]

with PostgresDatabaseManager() as db:
    for symbol, symbol_id in test_cases:
        print(f"\n{'='*50}")
        print(f"Testing {symbol} (ID: {symbol_id})")
        print('='*50)
        
        # Extract the symbol
        result = extractor.extract_symbol(symbol, symbol_id, db)
        
        print(f"Result: {result}")
        
        if result['status'] == 'success':
            # Verify data was inserted
            count_result = db.fetch_query("SELECT COUNT(*) FROM source.balance_sheet WHERE symbol = %s", (symbol,))
            print(f"Total {symbol} records in database: {count_result[0][0]}")
            
            # Show latest records
            latest_result = db.fetch_query("""
                SELECT fiscal_date_ending, report_type, total_assets 
                FROM source.balance_sheet 
                WHERE symbol = %s 
                ORDER BY fiscal_date_ending DESC 
                LIMIT 3
            """, (symbol,))
            
            print(f"Latest {symbol} data:")
            for row in latest_result:
                fiscal_date, report_type, assets = row
                assets_str = f"${float(assets)/1e9:.1f}B" if assets else "None"
                print(f"  {fiscal_date} ({report_type}): {assets_str}")
                
        print(f"\n{symbol} processing completed.")
