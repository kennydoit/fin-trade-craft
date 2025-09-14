#!/usr/bin/env python3
"""
Check the gap between listing_status symbols and watermarked symbols.
"""

from db.postgres_database_manager import PostgresDatabaseManager
from utils.incremental_etl import WatermarkManager

def main():
    db = PostgresDatabaseManager()
    db.connect()
    
    try:
        # Get watermark counts
        processed = db.fetch_query(
            "SELECT COUNT(*) FROM source.extraction_watermarks WHERE table_name = %s", 
            ('time_series_daily_adjusted',)
        )[0][0]
        
        # Get qualifying symbols using WatermarkManager logic
        wm = WatermarkManager(db)
        symbols = wm.get_symbols_needing_processing_with_filters(
            table_name='time_series_daily_adjusted',
            staleness_hours=24*365,  # Very stale to get all unprocessed
            max_failures=100,        # High to include all
            limit=None
        )
        
        # Get total active counts
        total_active = db.fetch_query(
            "SELECT COUNT(*) FROM source.listing_status WHERE LOWER(status) = 'active'"
        )[0][0]
        
        active_stock_etf = db.fetch_query(
            "SELECT COUNT(*) FROM source.listing_status WHERE LOWER(status) = 'active' AND asset_type IN ('Stock', 'ETF')"
        )[0][0]
        
        print("=== WATERMARK GAP ANALYSIS ===")
        print(f"Total active symbols: {total_active:,}")
        print(f"Active Stock/ETF: {active_stock_etf:,}")
        print(f"Already processed (have watermarks): {processed:,}")
        print(f"Qualifying for processing: {len(symbols):,}")
        print(f"Total that could be processed: {processed + len(symbols):,}")
        
        if len(symbols) > 0:
            print(f"\nSymbols never processed: {len(symbols):,}")
            print("Sample symbols needing processing:")
            for i, sym in enumerate(symbols[:10]):
                print(f"  {sym['symbol']} (ID: {sym['symbol_id']}, Failures: {sym['consecutive_failures']})")
                
        # Check what's excluded
        excluded_breakdown = [
            ("Warrants (WS)", "symbol LIKE '%WS%'"),
            ("Rights (R ending)", "symbol LIKE '%R'"),
            ("Rights (R containing)", "symbol LIKE '%R%'"),
            ("Preferred (P)", "symbol LIKE '%P%'"),
            ("Units (U ending)", "symbol LIKE '%U'")
        ]
        
        print(f"\nExclusion breakdown (from {active_stock_etf:,} active Stock/ETF):")
        for name, condition in excluded_breakdown:
            count = db.fetch_query(
                f"SELECT COUNT(*) FROM source.listing_status WHERE LOWER(status) = 'active' AND asset_type IN ('Stock', 'ETF') AND {condition}"
            )[0][0]
            print(f"  {name}: {count:,}")
            
    finally:
        db.close()

if __name__ == "__main__":
    main()
