#!/usr/bin/env python3
"""
Check earnings call transcript data vs watermarks.
"""

from db.postgres_database_manager import PostgresDatabaseManager

def main():
    db = PostgresDatabaseManager()
    db.connect()
    
    try:
        # Get watermark info
        watermark_data = db.fetch_query("""
            SELECT ew.symbol_id, ls.symbol, ew.last_successful_run 
            FROM source.extraction_watermarks ew 
            JOIN source.listing_status ls ON ew.symbol_id = ls.symbol_id 
            WHERE ew.table_name = %s
        """, ('earnings_call_transcripts',))
        
        # Get transcript summary
        total_records = db.fetch_query("SELECT COUNT(*) FROM source.earnings_call_transcripts")[0][0]
        unique_symbols = db.fetch_query("SELECT COUNT(DISTINCT symbol_id) FROM source.earnings_call_transcripts")[0][0]
        
        print("=== EARNINGS CALL TRANSCRIPT ANALYSIS ===")
        print(f"Total transcript records: {total_records:,}")
        print(f"Unique symbols with transcripts: {unique_symbols:,}")
        print(f"Watermark records: {len(watermark_data)}")
        
        if watermark_data:
            symbol_id, symbol, last_run = watermark_data[0]
            print(f"\nSingle watermark:")
            print(f"  Symbol: {symbol} (ID: {symbol_id})")
            print(f"  Last run: {last_run}")
            
            # Check if this symbol has transcript data
            symbol_transcripts = db.fetch_query(
                "SELECT COUNT(*) FROM source.earnings_call_transcripts WHERE symbol_id = %s", 
                (symbol_id,)
            )[0][0]
            print(f"  Transcript records for this symbol: {symbol_transcripts:,}")
        
        # Check if most symbols have NO watermarks
        symbols_without_watermarks = db.fetch_query("""
            SELECT COUNT(DISTINCT ect.symbol_id)
            FROM source.earnings_call_transcripts ect
            LEFT JOIN source.extraction_watermarks ew ON ect.symbol_id = ew.symbol_id 
                                                        AND ew.table_name = 'earnings_call_transcripts'
            WHERE ew.symbol_id IS NULL
        """)[0][0]
        
        print(f"\nSymbols with transcript data but NO watermarks: {symbols_without_watermarks:,}")
        print(f"Gap: {unique_symbols - len(watermark_data):,} symbols processed but not tracked in watermarks")
        
        # Sample symbols without watermarks
        sample_no_watermarks = db.fetch_query("""
            SELECT ect.symbol_id, ls.symbol, COUNT(ect.transcript_id) as transcript_count
            FROM source.earnings_call_transcripts ect
            LEFT JOIN source.listing_status ls ON ect.symbol_id = ls.symbol_id
            LEFT JOIN source.extraction_watermarks ew ON ect.symbol_id = ew.symbol_id 
                                                        AND ew.table_name = 'earnings_call_transcripts'
            WHERE ew.symbol_id IS NULL
            GROUP BY ect.symbol_id, ls.symbol
            ORDER BY transcript_count DESC
            LIMIT 5
        """)
        
        print(f"\nTop symbols with transcripts but no watermarks:")
        for row in sample_no_watermarks:
            print(f"  {row[1]} (ID: {row[0]}): {row[2]:,} transcripts")
            
    finally:
        db.close()

if __name__ == "__main__":
    main()
