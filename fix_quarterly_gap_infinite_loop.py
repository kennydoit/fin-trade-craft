#!/usr/bin/env python3
"""
Fix the quarterly gap infinite loop issue.

The problem: Symbols keep reappearing in the processing queue because they have
quarterly gaps (missing Q3 2025 data), even after successful processing.

Solution: Modify the quarterly gap logic to include a "recent processing" check
that prevents symbols from being reprocessed too frequently for quarterly gaps.
"""

import os
import sys
from datetime import datetime, timedelta

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db.postgres_database_manager import PostgresDatabaseManager

def analyze_current_quarterly_gap_logic():
    """Analyze the current quarterly gap logic in the database."""
    print("🔍 ANALYZING CURRENT QUARTERLY GAP LOGIC")
    print("=" * 60)
    
    db_manager = PostgresDatabaseManager()
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get the current quarterly gap logic from the database
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_symbols,
                    COUNT(CASE WHEN last_fiscal_date < 
                        CASE 
                            WHEN EXTRACT(month FROM CURRENT_DATE) <= 3 THEN 
                                DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '3 months'  -- Q4 of previous year
                            WHEN EXTRACT(month FROM CURRENT_DATE) <= 6 THEN 
                                DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '0 months'  -- Q1 of current year
                            WHEN EXTRACT(month FROM CURRENT_DATE) <= 9 THEN 
                                DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '3 months'  -- Q2 of current year
                            ELSE 
                                DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '6 months'  -- Q3 of current year
                        END 
                        + INTERVAL '2 months' + INTERVAL '30 days'  -- Last day of the expected quarter
                    THEN 1 END) as symbols_with_quarterly_gaps
                FROM source.watermarks w
                JOIN source.stock_universes u ON w.stock_universe_id = u.id
                WHERE u.name = 'russell_3000'
                  AND w.consecutive_failures < 3
                  AND w.last_fiscal_date IS NOT NULL;
            """)
            
            result = cursor.fetchone()
            print(f"📊 Current state:")
            print(f"   Total Russell 3000 symbols: {result[0]}")
            print(f"   Symbols with quarterly gaps: {result[1]}")
            print(f"   Percentage with gaps: {result[1]/result[0]*100:.1f}%")
            
            return result
            
    except Exception as e:
        print(f"❌ Error analyzing quarterly gap logic: {e}")
        return None, None
    finally:
        db_manager.close()

def create_improved_quarterly_gap_logic():
    """Create an improved quarterly gap logic that prevents infinite loops."""
    print("\n🔧 CREATING IMPROVED QUARTERLY GAP LOGIC")
    print("=" * 60)
    
    # The improved logic will add a "recent processing" check
    improved_logic = """
    -- Improved quarterly gap logic that prevents infinite loops
    -- Add this condition to the balance sheet extraction query:
    
    AND (
        -- Regular quarterly gap logic
        w.last_fiscal_date < 
        CASE 
            WHEN EXTRACT(month FROM CURRENT_DATE) <= 3 THEN 
                DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '3 months'  -- Q4 of previous year
            WHEN EXTRACT(month FROM CURRENT_DATE) <= 6 THEN 
                DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '0 months'  -- Q1 of current year
            WHEN EXTRACT(month FROM CURRENT_DATE) <= 9 THEN 
                DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '3 months'  -- Q2 of current year
            ELSE 
                DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '6 months'  -- Q3 of current year
        END + INTERVAL '2 months' + INTERVAL '30 days'  -- Last day of the expected quarter
        
        -- BUT ONLY if it hasn't been processed recently for quarterly gaps
        AND (
            w.last_successful_run IS NULL 
            OR w.last_successful_run < CURRENT_DATE - INTERVAL '7 days'
        )
    )
    """
    
    print("✅ Improved logic created!")
    print("Key improvements:")
    print("1. Maintains quarterly gap detection")
    print("2. Adds 7-day cooling-off period for quarterly gap processing")
    print("3. Prevents infinite loops when Q3 2025 data isn't available yet")
    
    return improved_logic

def simulate_improved_logic():
    """Simulate how the improved logic would affect current processing."""
    print("\n🎯 SIMULATING IMPROVED LOGIC")
    print("=" * 60)
    
    db_manager = PostgresDatabaseManager()
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # Count symbols that would still qualify under improved logic
            cursor.execute("""
                SELECT 
                    COUNT(*) as would_still_process,
                    COUNT(CASE WHEN w.last_successful_run >= CURRENT_DATE - INTERVAL '7 days' 
                          THEN 1 END) as recently_processed
                FROM source.watermarks w
                JOIN source.stock_universes u ON w.stock_universe_id = u.id
                WHERE u.name = 'russell_3000'
                  AND w.consecutive_failures < 3
                  AND w.last_fiscal_date IS NOT NULL
                  AND w.last_fiscal_date < 
                      CASE 
                          WHEN EXTRACT(month FROM CURRENT_DATE) <= 3 THEN 
                              DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '3 months'
                          WHEN EXTRACT(month FROM CURRENT_DATE) <= 6 THEN 
                              DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '0 months'
                          WHEN EXTRACT(month FROM CURRENT_DATE) <= 9 THEN 
                              DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '3 months'
                          ELSE 
                              DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '6 months'
                      END + INTERVAL '2 months' + INTERVAL '30 days'
                  AND (
                      w.last_successful_run IS NULL 
                      OR w.last_successful_run < CURRENT_DATE - INTERVAL '7 days'
                  );
            """)
            
            result = cursor.fetchone()
            would_process = result[0]
            recently_processed = result[1]
            
            print(f"📊 Impact of improved logic:")
            print(f"   Symbols that would still process: {would_process}")
            print(f"   Recently processed (excluded): {recently_processed}")
            print(f"   Reduction in processing queue: {recently_processed} symbols")
            
            # Show some examples of recently processed symbols
            cursor.execute("""
                SELECT 
                    u.symbol,
                    w.last_successful_run,
                    w.last_fiscal_date
                FROM source.watermarks w
                JOIN source.stock_universes u ON w.stock_universe_id = u.id
                WHERE u.name = 'russell_3000'
                  AND w.consecutive_failures < 3
                  AND w.last_fiscal_date IS NOT NULL
                  AND w.last_fiscal_date < 
                      CASE 
                          WHEN EXTRACT(month FROM CURRENT_DATE) <= 3 THEN 
                              DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '3 months'
                          WHEN EXTRACT(month FROM CURRENT_DATE) <= 6 THEN 
                              DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '0 months'
                          WHEN EXTRACT(month FROM CURRENT_DATE) <= 9 THEN 
                              DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '3 months'
                          ELSE 
                              DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '6 months'
                      END + INTERVAL '2 months' + INTERVAL '30 days'
                  AND w.last_successful_run >= CURRENT_DATE - INTERVAL '7 days'
                ORDER BY w.last_successful_run DESC
                LIMIT 10;
            """)
            
            examples = cursor.fetchall()
            if examples:
                print(f"\n📝 Examples of recently processed symbols (would be excluded):")
                print("   Symbol   Last Run             Last Fiscal")
                print("   -------- -------------------- -----------")
                for symbol, last_run, last_fiscal in examples:
                    print(f"   {symbol:<8} {last_run} {last_fiscal}")
            
            return would_process, recently_processed
            
    except Exception as e:
        print(f"❌ Error simulating improved logic: {e}")
        return None, None
    finally:
        db_manager.close()

def create_implementation_script():
    """Create a script to implement the fix in the balance sheet extractor."""
    print("\n🚀 CREATING IMPLEMENTATION SCRIPT")
    print("=" * 60)
    
    implementation_code = """
# To implement this fix in the balance sheet extractor, modify the quarterly gap condition:

# BEFORE (causes infinite loop):
quarterly_gap_condition = \"\"\"
AND w.last_fiscal_date < 
    CASE 
        WHEN EXTRACT(month FROM CURRENT_DATE) <= 3 THEN 
            DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '3 months'
        WHEN EXTRACT(month FROM CURRENT_DATE) <= 6 THEN 
            DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '0 months'
        WHEN EXTRACT(month FROM CURRENT_DATE) <= 9 THEN 
            DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '3 months'
        ELSE 
            DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '6 months'
    END + INTERVAL '2 months' + INTERVAL '30 days'
\"\"\"

# AFTER (prevents infinite loop):
quarterly_gap_condition = \"\"\"
AND (
    w.last_fiscal_date < 
    CASE 
        WHEN EXTRACT(month FROM CURRENT_DATE) <= 3 THEN 
            DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '3 months'
        WHEN EXTRACT(month FROM CURRENT_DATE) <= 6 THEN 
            DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '0 months'
        WHEN EXTRACT(month FROM CURRENT_DATE) <= 9 THEN 
            DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '3 months'
        ELSE 
            DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '6 months'
    END + INTERVAL '2 months' + INTERVAL '30 days'
    
    AND (
        w.last_successful_run IS NULL 
        OR w.last_successful_run < CURRENT_DATE - INTERVAL '7 days'
    )
)
\"\"\"
"""
    
    print("✅ Implementation script created!")
    print("Key changes:")
    print("1. Wrap the quarterly gap condition in parentheses")
    print("2. Add AND condition for 7-day cooling-off period")
    print("3. This prevents recently processed symbols from reappearing")
    
    return implementation_code

def main():
    """Main function to analyze and fix the quarterly gap infinite loop."""
    print("🔄 QUARTERLY GAP INFINITE LOOP ANALYSIS & FIX")
    print("=" * 60)
    print(f"Timestamp: {datetime.now()}")
    print()
    
    # Step 1: Analyze current state
    total, with_gaps = analyze_current_quarterly_gap_logic()
    
    # Step 2: Create improved logic
    improved_logic = create_improved_quarterly_gap_logic()
    
    # Step 3: Simulate the impact
    would_process, excluded = simulate_improved_logic()
    
    # Step 4: Create implementation guide
    implementation = create_implementation_script()
    
    print(f"\n🎯 SUMMARY & RECOMMENDATION")
    print("=" * 60)
    print("✅ Problem identified: Quarterly gap logic causes infinite loop")
    print("✅ Root cause: Symbols reprocess immediately after successful runs")
    print("✅ Solution: Add 7-day cooling-off period for quarterly gap processing")
    print(f"✅ Impact: Would reduce processing queue by ~{excluded} symbols")
    print()
    print("📋 NEXT STEPS:")
    print("1. Locate the balance sheet extractor code")
    print("2. Find the quarterly gap condition in the SQL query")
    print("3. Replace it with the improved version shown above")
    print("4. Test with --limit 100 to verify fix works")
    print("5. Run full extraction without infinite loops!")

if __name__ == "__main__":
    main()
