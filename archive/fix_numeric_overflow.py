"""
Fix numeric overflow in time_series_daily_adjusted table.
The current DECIMAL(15,4) fields are too small for some stock prices.
This script will increase precision to DECIMAL(20,6) to handle larger values.
"""

import sys
from pathlib import Path

# Add the parent directories to the path
sys.path.append(str(Path(__file__).parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager

def fix_numeric_overflow():
    """Fix numeric overflow by increasing decimal precision."""
    print("🔧 Fixing numeric overflow in time_series_daily_adjusted table...")
    
    with PostgresDatabaseManager() as db:
        # Update source table schema to handle larger values
        fix_sql = """
            -- Increase precision for price fields to handle larger values
            -- DECIMAL(20,6) allows values up to 99,999,999,999,999.999999
            ALTER TABLE source.time_series_daily_adjusted 
            ALTER COLUMN open TYPE DECIMAL(20,6),
            ALTER COLUMN high TYPE DECIMAL(20,6),
            ALTER COLUMN low TYPE DECIMAL(20,6),
            ALTER COLUMN close TYPE DECIMAL(20,6),
            ALTER COLUMN adjusted_close TYPE DECIMAL(20,6);
            
            -- Keep dividend_amount as DECIMAL(15,6) - it's already sufficient
            -- Keep split_coefficient as DECIMAL(10,6) - it's already sufficient
        """
        
        try:
            print("Updating column types to DECIMAL(20,6)...")
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
            
            print("\n🎯 The time series extractor should now work without overflow errors!")
            
        except Exception as e:
            print(f"❌ Error updating schema: {e}")
            raise

if __name__ == "__main__":
    fix_numeric_overflow()
