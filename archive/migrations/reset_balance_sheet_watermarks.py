"""
Reset Balance Sheet Watermarks for Missing Data

This script resets watermarks for symbols that have data in extracted.balance_sheet 
but are missing from source.balance_sheet. This allows the modern extractor to 
re-process these symbols.

IMPACT: This will cause ~10,238 symbols to be re-processed by the balance sheet extractor.
"""

import sys
from pathlib import Path

# Add the parent directories to the path
sys.path.append(str(Path(__file__).parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager

def analyze_impact():
    """Analyze the impact before making changes."""
    print("🔍 ANALYZING WATERMARK RESET IMPACT...")
    print("=" * 60)
    
    db_manager = PostgresDatabaseManager()
    with db_manager as db:
        # Count symbols to be affected
        query = """
            SELECT COUNT(DISTINCT ew.symbol_id) as symbols_to_reset
            FROM source.extraction_watermarks ew
            WHERE ew.table_name = 'balance_sheet'
              AND ew.symbol_id IN (
                  SELECT DISTINCT eb.symbol_id 
                  FROM extracted.balance_sheet eb
                  WHERE eb.symbol_id NOT IN (
                      SELECT DISTINCT symbol_id 
                      FROM source.balance_sheet
                      WHERE symbol_id IS NOT NULL
                  )
              )
        """
        result = db.fetch_query(query)
        symbols_to_reset = result[0][0]
        
        # Count total missing symbols
        query = """
            SELECT COUNT(DISTINCT eb.symbol_id) as total_missing
            FROM extracted.balance_sheet eb
            WHERE eb.symbol_id NOT IN (
                SELECT DISTINCT symbol_id 
                FROM source.balance_sheet
                WHERE symbol_id IS NOT NULL
            )
        """
        result = db.fetch_query(query)
        total_missing = result[0][0]
        
        print(f"📊 IMPACT ANALYSIS:")
        print(f"   • Symbols missing from source: {total_missing:,}")
        print(f"   • Symbols with watermarks to reset: {symbols_to_reset:,}")
        print(f"   • Symbols without watermarks (will be auto-discovered): {total_missing - symbols_to_reset:,}")
        print()
        
        # Show sample symbols that will be reset
        query = """
            SELECT ls.symbol, ew.last_successful_run, ew.consecutive_failures
            FROM source.extraction_watermarks ew
            JOIN extracted.listing_status ls ON ew.symbol_id = ls.symbol_id
            WHERE ew.table_name = 'balance_sheet'
              AND ew.symbol_id IN (
                  SELECT DISTINCT eb.symbol_id 
                  FROM extracted.balance_sheet eb
                  WHERE eb.symbol_id NOT IN (
                      SELECT DISTINCT symbol_id 
                      FROM source.balance_sheet
                      WHERE symbol_id IS NOT NULL
                  )
              )
            ORDER BY ew.last_successful_run DESC NULLS LAST
            LIMIT 10
        """
        results = db.fetch_query(query)
        
        print(f"📋 SAMPLE SYMBOLS TO BE RESET:")
        for row in results:
            symbol, last_run, failures = row
            print(f"   • {symbol} - Last run: {last_run}, Failures: {failures}")
        
        return symbols_to_reset

def reset_watermarks():
    """Reset the watermarks for missing symbols."""
    print("\n🔄 RESETTING WATERMARKS...")
    print("=" * 60)
    
    db_manager = PostgresDatabaseManager()
    with db_manager as db:
        # The reset query
        reset_query = """
            UPDATE source.extraction_watermarks 
            SET last_successful_run = NULL,
                consecutive_failures = 0,
                updated_at = NOW()
            WHERE table_name = 'balance_sheet'
              AND symbol_id IN (
                  SELECT DISTINCT eb.symbol_id 
                  FROM extracted.balance_sheet eb
                  WHERE eb.symbol_id NOT IN (
                      SELECT DISTINCT symbol_id 
                      FROM source.balance_sheet
                      WHERE symbol_id IS NOT NULL
                  )
              )
        """
        
        # Execute the reset
        rows_affected = db.execute_query(reset_query)
        
        print(f"✅ WATERMARKS RESET SUCCESSFULLY!")
        print(f"   • Rows affected: {rows_affected}")
        print()
        
        # Verify the reset
        verify_query = """
            SELECT COUNT(*) as reset_count
            FROM source.extraction_watermarks 
            WHERE table_name = 'balance_sheet' 
              AND last_successful_run IS NULL
        """
        result = db.fetch_query(verify_query)
        reset_count = result[0][0]
        
        print(f"🔍 VERIFICATION:")
        print(f"   • Watermarks now showing NULL (ready for processing): {reset_count:,}")
        
        return rows_affected

def main():
    """Main execution function."""
    print("🚀 BALANCE SHEET WATERMARK RESET TOOL")
    print("=" * 60)
    print()
    
    try:
        # Analyze impact first
        symbols_to_reset = analyze_impact()
        
        if symbols_to_reset == 0:
            print("✅ No watermarks need resetting. Exiting.")
            return
        
        # Confirm action
        print(f"\n⚠️  WARNING: This will reset {symbols_to_reset:,} watermarks.")
        print("   The modern balance sheet extractor will re-process these symbols.")
        print("   This could take several hours for API extraction.")
        print()
        
        confirm = input("🤔 Continue with watermark reset? (yes/no): ").lower().strip()
        
        if confirm in ['yes', 'y']:
            rows_affected = reset_watermarks()
            
            print("\n🎯 NEXT STEPS:")
            print("   1. Run balance sheet extractor with appropriate limits:")
            print("      python data_pipeline/extract/extract_balance_sheet.py --limit 100")
            print("   2. Monitor progress and adjust batch sizes as needed")
            print("   3. Consider using larger staleness hours for less frequent updates")
            print()
            print("✅ Watermark reset completed successfully!")
            
        else:
            print("❌ Operation cancelled by user.")
            
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
