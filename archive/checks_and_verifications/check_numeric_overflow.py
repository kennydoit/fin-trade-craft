"""
Check for numeric values that would cause overflow in DECIMAL(15,4) fields.
The error indicates values exceeding 10^11 (99,999,999,999.9999).
"""

import sys
from pathlib import Path
import pandas as pd

# Add the parent directories to the path
sys.path.append(str(Path(__file__).parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager

def check_overflow_values():
    """Check for values that would overflow DECIMAL(15,4) fields."""
    print("🔍 Checking for numeric overflow in time series data...")
    
    # Maximum value for DECIMAL(15,4): 10^11 - 1
    max_value = 99999999999.9999
    
    with PostgresDatabaseManager() as db:
        # Check extracted table for overflow values
        query = """
            SELECT symbol, date, open, high, low, close, adjusted_close, volume
            FROM extracted.time_series_daily_adjusted 
            WHERE open > %s OR high > %s OR low > %s OR close > %s OR adjusted_close > %s
            ORDER BY GREATEST(
                COALESCE(open, 0), 
                COALESCE(high, 0), 
                COALESCE(low, 0), 
                COALESCE(close, 0), 
                COALESCE(adjusted_close, 0)
            ) DESC
            LIMIT 20
        """
        
        result = db.fetch_query(query, (max_value, max_value, max_value, max_value, max_value))
        
        if result:
            print(f"📊 Found {len(result)} records with values exceeding DECIMAL(15,4) limits:")
            print()
            
            df = pd.DataFrame(result, columns=['symbol', 'date', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume'])
            
            for _, row in df.iterrows():
                print(f"Symbol: {row['symbol']}, Date: {row['date']}")
                for field in ['open', 'high', 'low', 'close', 'adjusted_close']:
                    value = row[field]
                    if value and value > max_value:
                        print(f"  {field}: {value:,.2f} (OVERFLOW!)")
                    elif value:
                        print(f"  {field}: {value:,.2f}")
                print()
        else:
            print("✅ No overflow values found in extracted table")
        
        # Check current max values
        print("📈 Current maximum values in extracted table:")
        max_query = """
            SELECT 
                MAX(open) as max_open,
                MAX(high) as max_high, 
                MAX(low) as max_low,
                MAX(close) as max_close,
                MAX(adjusted_close) as max_adjusted_close
            FROM extracted.time_series_daily_adjusted
        """
        
        max_result = db.fetch_query(max_query)
        if max_result:
            max_values = max_result[0]
            print(f"  Max open: {max_values[0]:,.2f}")
            print(f"  Max high: {max_values[1]:,.2f}")
            print(f"  Max low: {max_values[2]:,.2f}")
            print(f"  Max close: {max_values[3]:,.2f}")
            print(f"  Max adjusted_close: {max_values[4]:,.2f}")
            print(f"  DECIMAL(15,4) limit: {max_value:,.2f}")
        
        # Check if source table exists and has different schema
        source_check = db.fetch_query("""
            SELECT column_name, data_type, numeric_precision, numeric_scale
            FROM information_schema.columns 
            WHERE table_schema = 'source' 
            AND table_name = 'time_series_daily_adjusted'
            AND column_name IN ('open', 'high', 'low', 'close', 'adjusted_close')
            ORDER BY column_name
        """)
        
        if source_check:
            print("\n📋 Current source table schema:")
            for col_info in source_check:
                col_name, data_type, precision, scale = col_info
                print(f"  {col_name}: {data_type}({precision},{scale})")

if __name__ == "__main__":
    check_overflow_values()
