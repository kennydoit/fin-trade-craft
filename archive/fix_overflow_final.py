"""
Fix the persistent numeric overflow by increasing precision further
and adding debugging to identify problematic values.
"""

import sys
from pathlib import Path

# Add the parent directories to the path
sys.path.append(str(Path(__file__).parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager

def fix_overflow_again():
    """Fix numeric overflow by increasing precision to DECIMAL(28,8)."""
    print("🔧 Fixing persistent numeric overflow in time_series_daily_adjusted table...")
    print("   Current limit: DECIMAL(20,6) = 10^14 (100 trillion)")
    print("   New limit: DECIMAL(28,8) = 10^20 (100 quintillion)")
    
    with PostgresDatabaseManager() as db:
        # First, let's check what values are causing the overflow
        print("\n🔍 Checking for extreme values in extracted table...")
        check_query = """
            SELECT symbol, date, open, high, low, close, adjusted_close
            FROM extracted.time_series_daily_adjusted 
            WHERE open > 1000000000000 OR high > 1000000000000 OR low > 1000000000000 
               OR close > 1000000000000 OR adjusted_close > 1000000000000
            ORDER BY GREATEST(
                COALESCE(open, 0), 
                COALESCE(high, 0), 
                COALESCE(low, 0), 
                COALESCE(close, 0), 
                COALESCE(adjusted_close, 0)
            ) DESC
            LIMIT 10
        """
        
        extreme_values = db.fetch_query(check_query)
        if extreme_values:
            print("📊 Found extreme values (> 1 trillion):")
            for row in extreme_values:
                symbol, date, open_val, high_val, low_val, close_val, adj_close = row
                print(f"  {symbol} ({date}): open={open_val}, high={high_val}, low={low_val}, close={close_val}, adj_close={adj_close}")
        else:
            print("✅ No extreme values found in extracted table")
        
        # Update schema to handle even larger values
        fix_sql = """
            -- Increase precision to DECIMAL(28,8) to handle extreme values
            -- This allows values up to 99,999,999,999,999,999,999.99999999
            ALTER TABLE source.time_series_daily_adjusted 
            ALTER COLUMN open TYPE DECIMAL(28,8),
            ALTER COLUMN high TYPE DECIMAL(28,8),
            ALTER COLUMN low TYPE DECIMAL(28,8),
            ALTER COLUMN close TYPE DECIMAL(28,8),
            ALTER COLUMN adjusted_close TYPE DECIMAL(28,8);
        """
        
        try:
            print("\n🛠️ Updating column types to DECIMAL(28,8)...")
            db.execute_script(fix_sql)
            print("✅ Schema updated successfully!")
            
            # Verify the changes
            verify_sql = """
                SELECT column_name, data_type, numeric_precision, numeric_scale
                FROM information_schema.columns 
                WHERE table_schema = 'source' 
                AND table_name = 'time_series_daily_adjusted'
                AND column_name IN ('open', 'high', 'low', 'close', 'adjusted_close')
                ORDER BY column_name
            """
            
            result = db.fetch_query(verify_sql)
            if result:
                print("\n📋 Updated schema:")
                for col_info in result:
                    col_name, data_type, precision, scale = col_info
                    print(f"  {col_name}: {data_type}({precision},{scale})")
            
            print(f"\n🎯 Schema now supports values up to 10^20 (100 quintillion)!")
            print("   This should handle any real-world stock price, including extreme cases.")
            
        except Exception as e:
            print(f"❌ Error updating schema: {e}")
            raise

if __name__ == "__main__":
    fix_overflow_again()
