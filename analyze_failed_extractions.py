"""
Analyze failed balance sheet extractions to identify patterns and optimization opportunities.
This will help us understand what types of symbols should be pre-screened out.
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from db.postgres_database_manager import PostgresDatabaseManager

def analyze_failed_extractions():
    """Analyze patterns in failed extractions to identify pre-screening opportunities."""
    
    with PostgresDatabaseManager() as db:
        print("🔍 Analyzing failed balance sheet extractions...")
        
        # 1. Check symbols with consecutive failures
        query1 = """
        SELECT ls.symbol, ls.asset_type, ls.exchange, ls.status, ls.delisting_date,
               ew.consecutive_failures, ew.last_successful_run,
               ls.ipo_date,
               -- Check if symbol has any characteristics that suggest no fundamentals
               CASE 
                   WHEN ls.symbol LIKE '%WS%' THEN 'Warrant'
                   WHEN ls.symbol LIKE '%R' THEN 'Rights'
                   WHEN ls.symbol LIKE '%P%' THEN 'Preferred'
                   WHEN ls.symbol LIKE '%U' THEN 'Unit/SPAC'
                   WHEN ls.symbol ~ '[.-]' THEN 'Complex Symbol'
                   WHEN LENGTH(ls.symbol) > 5 THEN 'Long Symbol'
                   ELSE 'Regular'
               END as symbol_type
        FROM source.listing_status ls
        LEFT JOIN source.extraction_watermarks ew ON ew.symbol_id = ls.symbol_id 
                                                   AND ew.table_name = 'balance_sheet'
        WHERE ew.consecutive_failures >= 2
        ORDER BY ew.consecutive_failures DESC, ls.symbol
        LIMIT 30;
        """
        
        print("\n📊 Top 30 symbols with consecutive failures:")
        print(f"{'Symbol':<10} {'Type':<12} {'Asset':<8} {'Exchange':<10} {'Status':<8} {'Failures':<8} {'Delisted':<12} {'IPO Date':<12}")
        print("-" * 100)
        
        results1 = db.fetch_query(query1)
        for row in results1:
            symbol, asset_type, exchange, status, delisting, failures, last_run, ipo_date, symbol_type = row
            delisting_str = str(delisting)[:10] if delisting else "Active"
            ipo_str = str(ipo_date)[:10] if ipo_date else "Unknown"
            print(f"{symbol:<10} {symbol_type:<12} {asset_type or 'N/A':<8} {exchange or 'N/A':<10} {status or 'N/A':<8} {failures or 0:<8} {delisting_str:<12} {ipo_str:<12}")
        
        # 2. Check symbols with error responses
        query2 = """
        SELECT DISTINCT ls.symbol, ls.asset_type, ls.exchange, 
               ar.response_status, 
               ar.api_response->>'Error Message' as error_msg,
               CASE 
                   WHEN ls.symbol LIKE '%WS%' THEN 'Warrant'
                   WHEN ls.symbol LIKE '%R' THEN 'Rights'
                   WHEN ls.symbol LIKE '%P%' THEN 'Preferred'
                   WHEN ls.symbol LIKE '%U' THEN 'Unit/SPAC'
                   WHEN ls.symbol ~ '[.-]' THEN 'Complex Symbol'
                   WHEN LENGTH(ls.symbol) > 5 THEN 'Long Symbol'
                   ELSE 'Regular'
               END as symbol_type
        FROM source.listing_status ls
        JOIN source.api_responses_landing ar ON ar.symbol_id = ls.symbol_id 
                                             AND ar.table_name = 'balance_sheet'
        WHERE ar.response_status IN ('error', 'empty')
        ORDER BY symbol_type, ls.symbol
        LIMIT 25;
        """
        
        print(f"\n🚫 Symbols with API errors:")
        print(f"{'Symbol':<10} {'Type':<12} {'Asset':<8} {'Exchange':<10} {'Status':<8} {'Error':<50}")
        print("-" * 100)
        
        results2 = db.fetch_query(query2)
        for row in results2:
            symbol, asset_type, exchange, resp_status, error_msg, symbol_type = row
            error_display = (error_msg or resp_status or "Unknown")[:47] + "..." if (error_msg or resp_status or "Unknown") and len(error_msg or resp_status or "Unknown") > 50 else (error_msg or resp_status or "Unknown")
            print(f"{symbol:<10} {symbol_type:<12} {asset_type or 'N/A':<8} {exchange or 'N/A':<10} {resp_status:<8} {error_display:<50}")
        
        # 3. Analyze symbol patterns and their success rates
        query3 = """
        WITH symbol_analysis AS (
            SELECT 
                CASE 
                    WHEN ls.symbol LIKE '%WS%' THEN 'Warrant'
                    WHEN ls.symbol LIKE '%R' AND ls.symbol NOT LIKE '%AR%' AND ls.symbol NOT LIKE '%ER%' AND ls.symbol NOT LIKE '%OR%' THEN 'Rights'
                    WHEN ls.symbol LIKE '%P%' AND ls.symbol NOT LIKE '%APP%' AND ls.symbol NOT LIKE '%EPP%' THEN 'Preferred'
                    WHEN ls.symbol LIKE '%U' AND ls.symbol NOT LIKE '%FU%' AND ls.symbol NOT LIKE '%AU%' THEN 'Unit/SPAC'
                    WHEN ls.symbol ~ '[.-]' THEN 'Complex Symbol'
                    WHEN LENGTH(ls.symbol) > 5 THEN 'Long Symbol (>5 chars)'
                    WHEN LENGTH(ls.symbol) = 1 THEN 'Single Character'
                    WHEN ls.delisting_date IS NOT NULL THEN 'Delisted'
                    WHEN ls.asset_type != 'Stock' THEN 'Non-Stock Asset'
                    WHEN LOWER(ls.status) != 'active' THEN 'Inactive Status'
                    ELSE 'Regular Stock'
                END as category,
                COUNT(*) as total_symbols,
                COUNT(CASE WHEN ew.consecutive_failures >= 2 THEN 1 END) as high_failure_symbols,
                COUNT(CASE WHEN ew.last_successful_run IS NOT NULL THEN 1 END) as ever_successful,
                AVG(COALESCE(ew.consecutive_failures, 0)) as avg_failures
            FROM source.listing_status ls
            LEFT JOIN source.extraction_watermarks ew ON ew.symbol_id = ls.symbol_id 
                                                       AND ew.table_name = 'balance_sheet'
            GROUP BY category
        )
        SELECT category, total_symbols, high_failure_symbols, ever_successful,
               ROUND(avg_failures, 2) as avg_failures,
               ROUND(100.0 * high_failure_symbols / total_symbols, 1) as failure_rate_pct,
               ROUND(100.0 * ever_successful / total_symbols, 1) as success_rate_pct
        FROM symbol_analysis
        ORDER BY failure_rate_pct DESC, total_symbols DESC;
        """
        
        print(f"\n📈 Symbol Category Analysis (Failure Patterns):")
        print(f"{'Category':<20} {'Total':<8} {'High Fail':<10} {'Ever OK':<8} {'Avg Fail':<10} {'Fail %':<8} {'Success %':<10}")
        print("-" * 90)
        
        results3 = db.fetch_query(query3)
        for row in results3:
            category, total, high_fail, ever_ok, avg_fail, fail_pct, success_pct = row
            print(f"{category:<20} {total:<8} {high_fail:<10} {ever_ok:<8} {avg_fail:<10} {fail_pct:<8} {success_pct:<10}")
        
        # 4. Check specific problem symbols like METBV
        query4 = """
        SELECT ls.symbol, ls.name, ls.asset_type, ls.exchange, ls.status, 
               ls.delisting_date, ls.ipo_date,
               ew.consecutive_failures, ew.last_successful_run,
               ar.response_status, ar.api_response->>'Error Message' as error_msg
        FROM source.listing_status ls
        LEFT JOIN source.extraction_watermarks ew ON ew.symbol_id = ls.symbol_id 
                                                   AND ew.table_name = 'balance_sheet'
        LEFT JOIN source.api_responses_landing ar ON ar.symbol_id = ls.symbol_id 
                                                   AND ar.table_name = 'balance_sheet'
        WHERE ls.symbol IN ('METBV', 'GOOGL', 'AAPL', 'MSFT', 'META')  -- Mix of problem and good symbols
        ORDER BY ls.symbol;
        """
        
        print(f"\n🔍 Specific Symbol Analysis:")
        print(f"{'Symbol':<8} {'Name':<25} {'Asset':<8} {'Exchange':<8} {'Status':<8} {'Failures':<8} {'Error':<30}")
        print("-" * 100)
        
        results4 = db.fetch_query(query4)
        for row in results4:
            symbol, name, asset_type, exchange, status, delisting, ipo, failures, last_run, resp_status, error_msg = row
            name_display = (name or "Unknown")[:22] + "..." if name and len(name) > 25 else (name or "Unknown")
            error_display = (error_msg or resp_status or "N/A")[:27] + "..." if (error_msg or resp_status) and len(error_msg or resp_status or "") > 30 else (error_msg or resp_status or "N/A")
            print(f"{symbol:<8} {name_display:<25} {asset_type or 'N/A':<8} {exchange or 'N/A':<8} {status or 'N/A':<8} {failures or 0:<8} {error_display:<30}")
        
        print(f"\n💡 RECOMMENDATIONS FOR PRE-SCREENING:")
        print("=" * 60)
        print("Based on the analysis above, consider excluding these symbol types:")
        print("1. Warrants (symbols containing 'WS')")
        print("2. Rights (symbols ending in 'R' without common letter combos)")
        print("3. Preferred shares (symbols containing 'P' without common combos)")
        print("4. Units/SPACs (symbols ending in 'U')")
        print("5. Complex symbols (containing dots or dashes)")
        print("6. Very long symbols (>5 characters)")
        print("7. Delisted symbols")
        print("8. Non-Stock assets")
        print("9. Inactive status symbols")
        print("10. Symbols with >3 consecutive failures (permanent blacklist)")

if __name__ == "__main__":
    analyze_failed_extractions()
