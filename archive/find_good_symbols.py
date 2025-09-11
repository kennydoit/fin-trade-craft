import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager
from utils.incremental_etl import WatermarkManager

with PostgresDatabaseManager() as db:
    wm = WatermarkManager(db)
    symbols = wm.get_symbols_needing_processing('balance_sheet', staleness_hours=1, limit=10)
    print(f'Found {len(symbols)} symbols needing processing:')
    for s in symbols[:10]:
        print(f'  {s["symbol"]} (ID: {s["symbol_id"]})')
        
    # Find symbols that are likely to have data (larger companies)
    big_symbols = [s for s in symbols if len(s['symbol']) <= 4 and not any(x in s['symbol'] for x in ['U', 'WS', 'R', 'P'])]
    print(f'\nLikely to have data ({len(big_symbols)} symbols):')
    for s in big_symbols[:5]:
        print(f'  {s["symbol"]} (ID: {s["symbol_id"]})')
