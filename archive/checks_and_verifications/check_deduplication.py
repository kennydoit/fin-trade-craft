#!/usr/bin/env python3
"""
Verify deduplication mechanisms in earnings call transcripts.
"""

from db.postgres_database_manager import PostgresDatabaseManager

def main():
    db = PostgresDatabaseManager()
    db.connect()
    
    try:
        print("=== EARNINGS CALL TRANSCRIPT DEDUPLICATION ANALYSIS ===")
        
        # Check total counts
        total_records = db.fetch_query("SELECT COUNT(*) FROM source.earnings_call_transcripts")[0][0]
        total_symbols = db.fetch_query("SELECT COUNT(DISTINCT symbol_id) FROM source.earnings_call_transcripts")[0][0]
        
        print(f"Total transcript records: {total_records:,}")
        print(f"Unique symbols: {total_symbols:,}")
        
        # Check for duplicates based on unique constraint
        duplicate_groups = db.fetch_query("""
            SELECT COUNT(*) FROM (
                SELECT symbol_id, quarter, speaker, content_hash 
                FROM source.earnings_call_transcripts 
                GROUP BY symbol_id, quarter, speaker, content_hash 
                HAVING COUNT(*) > 1
            ) as dups
        """)[0][0]
        
        print(f"Duplicate constraint violations: {duplicate_groups}")
        
        # Check deduplication for recently processed symbols
        recent_symbols = db.fetch_query("""
            SELECT ls.symbol, ect.symbol_id, 
                   COUNT(CASE WHEN ect.created_at < '2025-09-12' THEN 1 END) as old_records,
                   COUNT(CASE WHEN ect.created_at >= '2025-09-12' THEN 1 END) as new_records,
                   COUNT(*) as total_records
            FROM source.earnings_call_transcripts ect
            JOIN source.listing_status ls ON ect.symbol_id = ls.symbol_id
            WHERE ect.symbol_id IN (
                SELECT symbol_id FROM source.extraction_watermarks 
                WHERE table_name = 'earnings_call_transcripts'
                AND updated_at >= '2025-09-12'
            )
            GROUP BY ls.symbol, ect.symbol_id
            ORDER BY ls.symbol
        """)
        
        print(f"\n--- Recently Processed Symbols ---")
        for symbol, symbol_id, old_records, new_records, total_records in recent_symbols:
            print(f"{symbol} (ID: {symbol_id}):")
            print(f"  Records before 2025-09-12: {old_records:,}")
            print(f"  Records added on 2025-09-12: {new_records:,}")
            print(f"  Total records: {total_records:,}")
            print(f"  Status: {'✅ Added new data only' if old_records > 0 and new_records > 0 else '📝 New symbol or no old data'}")
        
        # Check quarter-level deduplication
        print(f"\n--- Quarter-Level Analysis ---")
        quarter_analysis = db.fetch_query("""
            SELECT symbol_id, quarter, COUNT(DISTINCT content_hash) as unique_contents, COUNT(*) as total_records
            FROM source.earnings_call_transcripts 
            WHERE symbol_id IN (15348907, 44046721)  -- A and C symbols
            GROUP BY symbol_id, quarter 
            HAVING COUNT(*) > COUNT(DISTINCT content_hash)
            LIMIT 5
        """)
        
        if quarter_analysis:
            print("Quarters with potentially duplicate content:")
            for row in quarter_analysis:
                print(f"  Symbol ID {row[0]}, Quarter {row[1]}: {row[2]} unique vs {row[3]} total")
        else:
            print("✅ No quarter-level content duplicates found")
            
    finally:
        db.close()

if __name__ == "__main__":
    main()
