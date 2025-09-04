import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager
from utils.incremental_etl import WatermarkManager

# Reset watermarks for some major companies to test with
major_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META']

with PostgresDatabaseManager() as db:
    for symbol in major_symbols:
        result = db.fetch_query("SELECT symbol_id FROM extracted.listing_status WHERE symbol = %s", (symbol,))
        if result:
            symbol_id = result[0][0]
            print(f'Found {symbol} (ID: {symbol_id})')
            
            # Reset watermark
            db.execute_query("""
                DELETE FROM source.extraction_watermarks 
                WHERE table_name = 'balance_sheet' AND symbol_id = %s
            """, (symbol_id,))
            
            print(f'  Reset watermark for {symbol}')
        else:
            print(f'{symbol} not found')
    
    print('\nChecking which symbols will be processed first...')
    wm = WatermarkManager(db)
    symbols = wm.get_symbols_needing_processing('balance_sheet', staleness_hours=24, limit=5)
    for s in symbols:
        print(f'  {s["symbol"]} (ID: {s["symbol_id"]})')
