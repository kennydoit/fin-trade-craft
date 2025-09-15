"""
Analyze the recently failed symbols to improve pre-screening.
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from db.postgres_database_manager import PostgresDatabaseManager

def analyze_recent_failures():
    """Analyze the symbols that just failed extraction."""
    
    with PostgresDatabaseManager() as db:
        query = """
        SELECT ls.symbol, ls.name, ls.asset_type, ls.exchange, ls.status, 
               ls.delisting_date, ls.ipo_date,
               ew.consecutive_failures
        FROM source.listing_status ls
        LEFT JOIN source.extraction_watermarks ew ON ew.symbol_id = ls.symbol_id AND ew.table_name = 'balance_sheet'
        WHERE ls.symbol IN ('GBRG', 'GRCL', 'GRFX', 'GRIN', 'GSRM')
        ORDER BY ls.symbol;
        """
        
        results = db.fetch_query(query)
        print("🔍 Recent failed symbols analysis:")
        print(f"{'Symbol':<8} {'Name':<30} {'Asset':<8} {'Exchange':<10} {'Status':<8} {'Delisted':<12} {'IPO Date':<12} {'Failures':<8}")
        print("-" * 100)
        
        for row in results:
            symbol, name, asset_type, exchange, status, delisting, ipo, failures = row
            name_display = (name or "Unknown")[:27] + "..." if name and len(name) > 30 else (name or "Unknown")
            delisting_str = str(delisting)[:10] if delisting else "N/A"
            ipo_str = str(ipo)[:10] if ipo else "N/A"
            print(f"{symbol:<8} {name_display:<30} {asset_type or 'N/A':<8} {exchange or 'N/A':<10} {status or 'N/A':<8} {delisting_str:<12} {ipo_str:<12} {failures or 0:<8}")
        
        print("\n💡 Analysis:")
        print("These symbols passed pre-screening but still failed.")
        print("They appear to be legitimate stocks that simply don't have fundamental data available.")
        print("This suggests our API source may not have comprehensive coverage for all listed stocks.")
        
        # Check if these are recent IPOs or small companies
        print("\n📊 Additional insights:")
        for row in results:
            symbol, name, asset_type, exchange, status, delisting, ipo, failures = row
            if ipo:
                from datetime import datetime
                ipo_date = ipo if isinstance(ipo, datetime) else datetime.strptime(str(ipo)[:10], '%Y-%m-%d')
                days_since_ipo = (datetime.now() - ipo_date).days
                if days_since_ipo < 365:
                    print(f"• {symbol}: Recent IPO ({days_since_ipo} days ago) - may lack historical fundamentals")
                else:
                    print(f"• {symbol}: Mature company ({days_since_ipo} days since IPO) - possibly small/limited reporting")
            else:
                print(f"• {symbol}: No IPO date available - may be legacy listing or data quality issue")

if __name__ == "__main__":
    analyze_recent_failures()
