"""
Analyze financial statement coverage to identify symbols with incomplete data sets.
This script examines which symbols have income statements but missing balance sheets or cash flows.
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from db.postgres_database_manager import PostgresDatabaseManager
from utils.database_safety import DatabaseSafety

def analyze_statement_coverage():
    """Analyze coverage patterns across financial statements."""
    
    print("📊 FINANCIAL STATEMENT COVERAGE ANALYSIS")
    print("=" * 60)
    
    db_manager = PostgresDatabaseManager()
    
    try:
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                # Check if we have the financial statement tables
                print("🔍 Checking available financial statement tables...")
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name IN ('income_statement', 'balance_sheet', 'cash_flow')
                    ORDER BY table_name;
                """)
                
                available_tables = [row[0] for row in cursor.fetchall()]
                print(f"Available tables: {', '.join(available_tables)}")
                
                if not available_tables:
                    print("❌ No financial statement tables found")
                    return
                
                # For each available table, get symbol counts and recent data
                statement_data = {}
                for table in available_tables:
                    print(f"\n📈 Analyzing {table}...")
                    
                    # Get total unique symbols
                    cursor.execute(f"""
                        SELECT COUNT(DISTINCT symbol) as symbol_count
                        FROM {table};
                    """)
                    total_symbols = cursor.fetchone()[0]
                    
                    # Get symbols with recent data (last 2 years)
                    cursor.execute(f"""
                        SELECT COUNT(DISTINCT symbol) as recent_symbol_count
                        FROM {table}
                        WHERE date >= CURRENT_DATE - INTERVAL '2 years';
                    """)
                    recent_symbols = cursor.fetchone()[0]
                    
                    # Get date range
                    cursor.execute(f"""
                        SELECT MIN(date) as min_date, MAX(date) as max_date
                        FROM {table};
                    """)
                    date_range = cursor.fetchone()
                    
                    statement_data[table] = {
                        'total_symbols': total_symbols,
                        'recent_symbols': recent_symbols,
                        'min_date': date_range[0],
                        'max_date': date_range[1]
                    }
                    
                    print(f"   Total symbols: {total_symbols:,}")
                    print(f"   Recent symbols (2y): {recent_symbols:,}")
                    print(f"   Date range: {date_range[0]} to {date_range[1]}")
                
                # Now analyze coverage overlaps if we have multiple tables
                if len(available_tables) > 1:
                    print(f"\n🔄 COVERAGE OVERLAP ANALYSIS")
                    print("-" * 40)
                    
                    # Find symbols that exist in some but not all tables
                    for table1 in available_tables:
                        for table2 in available_tables:
                            if table1 >= table2:  # Avoid duplicates
                                continue
                                
                            print(f"\n📊 Comparing {table1} vs {table2}:")
                            
                            # Symbols in table1 but not table2
                            cursor.execute(f"""
                                SELECT COUNT(*) 
                                FROM (
                                    SELECT DISTINCT symbol FROM {table1}
                                    WHERE date >= CURRENT_DATE - INTERVAL '2 years'
                                    EXCEPT
                                    SELECT DISTINCT symbol FROM {table2}
                                    WHERE date >= CURRENT_DATE - INTERVAL '2 years'
                                ) as diff;
                            """)
                            only_in_table1 = cursor.fetchone()[0]
                            
                            # Symbols in table2 but not table1
                            cursor.execute(f"""
                                SELECT COUNT(*) 
                                FROM (
                                    SELECT DISTINCT symbol FROM {table2}
                                    WHERE date >= CURRENT_DATE - INTERVAL '2 years'
                                    EXCEPT
                                    SELECT DISTINCT symbol FROM {table1}
                                    WHERE date >= CURRENT_DATE - INTERVAL '2 years'
                                ) as diff;
                            """)
                            only_in_table2 = cursor.fetchone()[0]
                            
                            # Common symbols
                            cursor.execute(f"""
                                SELECT COUNT(*) 
                                FROM (
                                    SELECT DISTINCT symbol FROM {table1}
                                    WHERE date >= CURRENT_DATE - INTERVAL '2 years'
                                    INTERSECT
                                    SELECT DISTINCT symbol FROM {table2}
                                    WHERE date >= CURRENT_DATE - INTERVAL '2 years'
                                ) as common;
                            """)
                            common_symbols = cursor.fetchone()[0]
                            
                            print(f"   Only in {table1}: {only_in_table1:,}")
                            print(f"   Only in {table2}: {only_in_table2:,}")
                            print(f"   Common to both: {common_symbols:,}")
                            
                            # Get some examples of symbols only in table1
                            if only_in_table1 > 0:
                                cursor.execute(f"""
                                    SELECT DISTINCT symbol 
                                    FROM {table1}
                                    WHERE date >= CURRENT_DATE - INTERVAL '2 years'
                                    AND symbol NOT IN (
                                        SELECT DISTINCT symbol FROM {table2}
                                        WHERE date >= CURRENT_DATE - INTERVAL '2 years'
                                    )
                                    ORDER BY symbol
                                    LIMIT 10;
                                """)
                                examples = [row[0] for row in cursor.fetchall()]
                                print(f"   Examples only in {table1}: {', '.join(examples)}")
                
                # If we have all three tables, do a comprehensive analysis
                if len(available_tables) == 3:
                    print(f"\n🎯 COMPREHENSIVE COVERAGE ANALYSIS")
                    print("-" * 40)
                    
                    # Symbols with all three statements
                    cursor.execute("""
                        SELECT COUNT(*) 
                        FROM (
                            SELECT DISTINCT symbol FROM income_statement
                            WHERE date >= CURRENT_DATE - INTERVAL '2 years'
                            INTERSECT
                            SELECT DISTINCT symbol FROM balance_sheet
                            WHERE date >= CURRENT_DATE - INTERVAL '2 years'
                            INTERSECT
                            SELECT DISTINCT symbol FROM cash_flow
                            WHERE date >= CURRENT_DATE - INTERVAL '2 years'
                        ) as complete;
                    """)
                    complete_coverage = cursor.fetchone()[0]
                    
                    print(f"   Symbols with ALL three statements: {complete_coverage:,}")
                    
                    # Symbols with only income statement
                    cursor.execute("""
                        SELECT COUNT(*) 
                        FROM (
                            SELECT DISTINCT symbol FROM income_statement
                            WHERE date >= CURRENT_DATE - INTERVAL '2 years'
                            EXCEPT
                            (SELECT DISTINCT symbol FROM balance_sheet
                             WHERE date >= CURRENT_DATE - INTERVAL '2 years'
                             UNION
                             SELECT DISTINCT symbol FROM cash_flow
                             WHERE date >= CURRENT_DATE - INTERVAL '2 years')
                        ) as income_only;
                    """)
                    income_only = cursor.fetchone()[0]
                    
                    print(f"   Symbols with ONLY income statement: {income_only:,}")
                    
                    if income_only > 0:
                        cursor.execute("""
                            SELECT DISTINCT symbol 
                            FROM income_statement
                            WHERE date >= CURRENT_DATE - INTERVAL '2 years'
                            AND symbol NOT IN (
                                SELECT DISTINCT symbol FROM balance_sheet
                                WHERE date >= CURRENT_DATE - INTERVAL '2 years'
                                UNION
                                SELECT DISTINCT symbol FROM cash_flow
                                WHERE date >= CURRENT_DATE - INTERVAL '2 years'
                            )
                            ORDER BY symbol
                            LIMIT 10;
                        """)
                        examples = [row[0] for row in cursor.fetchall()]
                        print(f"   Examples (income only): {', '.join(examples)}")
                
    except Exception as e:
        print(f"❌ Error analyzing coverage: {e}")
    
    print(f"\n✅ Analysis complete!")

if __name__ == "__main__":
    analyze_statement_coverage()
