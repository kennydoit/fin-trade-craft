import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager

with PostgresDatabaseManager() as db:
    # Get AAPL's symbol_id
    result = db.fetch_query("SELECT symbol_id FROM extracted.listing_status WHERE symbol = %s", ('AAPL',))
    if result:
        aapl_id = result[0][0]
        print(f'AAPL symbol_id: {aapl_id}')
        
        # Reset AAPL's watermark to force processing
        db.execute_query("""
            DELETE FROM source.extraction_watermarks 
            WHERE table_name = 'balance_sheet' AND symbol_id = %s
        """, (aapl_id,))
        
        print("Reset AAPL's watermark - it will now be processed on next run")
    else:
        print("AAPL not found in listing_status")
