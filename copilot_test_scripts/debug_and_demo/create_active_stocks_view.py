#!/usr/bin/env python3
"""
Create Active Stocks View - Filtered Company Master
Focus on NYSE/NASDAQ stocks, excluding ETFs and other asset types
"""
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def create_active_stocks_view():
    """Create a view for active stocks only"""
    
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        database=os.getenv('POSTGRES_DB', 'fin_trade_craft'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD')
    )
    
    cursor = conn.cursor()
    
    try:
        print("🎯 Creating Active Stocks View")
        print("=" * 50)
        
        # First, let's check what asset types and exchanges we have
        print("📊 Analyzing current data...")
        
        cursor.execute("""
            SELECT asset_type, COUNT(*) as count
            FROM transformed.company_master 
            WHERE asset_type IS NOT NULL
            GROUP BY asset_type 
            ORDER BY count DESC
        """)
        asset_types = cursor.fetchall()
        
        print("Asset Types in database:")
        for asset_type, count in asset_types:
            print(f"  - {asset_type}: {count:,} companies")
        
        cursor.execute("""
            SELECT exchange, COUNT(*) as count
            FROM transformed.company_master 
            WHERE exchange IS NOT NULL
            GROUP BY exchange 
            ORDER BY count DESC
            LIMIT 10
        """)
        exchanges = cursor.fetchall()
        
        print("\nTop Exchanges:")
        for exchange, count in exchanges:
            print(f"  - {exchange}: {count:,} companies")
        
        # Create the view
        print("\n🏗️  Creating active_stocks view...")
        
        # Drop existing view if it exists
        cursor.execute("DROP VIEW IF EXISTS transformed.active_stocks;")
        
        # Create the filtered view
        create_view_sql = """
            CREATE VIEW transformed.active_stocks AS
            SELECT 
                company_master_id,
                symbol,
                name,
                description,
                cik,
                exchange,
                currency,
                country,
                sector,
                industry,
                address,
                ipo_date,
                status,
                -- Data availability counts
                cash_flow_count,
                income_statement_count,
                insider_transactions_count,
                balance_sheet_count,
                earnings_call_transcripts_count,
                time_series_daily_adjusted_count,
                -- Metadata
                created_at,
                updated_at
            FROM transformed.company_master
            WHERE 
                -- Focus on stocks only (exclude ETFs, REITs, etc.)
                (asset_type = 'Common Stock' OR asset_type = 'Stock' OR asset_type IS NULL)
                -- Focus on major US exchanges
                AND exchange IN ('NYSE', 'NASDAQ', 'NASDAQ Global Select', 'NASDAQ Global Market', 'NASDAQ Capital Market')
                -- Exclude delisted companies
                AND (status IS NULL OR status != 'Delisted')
                -- Must have a valid symbol
                AND symbol IS NOT NULL
                AND symbol != ''
            ORDER BY symbol;
        """
        
        cursor.execute(create_view_sql)
        conn.commit()
        
        print("✅ Created transformed.active_stocks view")
        
        # Get statistics on the filtered view
        cursor.execute("SELECT COUNT(*) FROM transformed.active_stocks")
        active_count = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN cash_flow_count > 0 THEN 1 END) as with_financials,
                COUNT(CASE WHEN time_series_daily_adjusted_count > 0 THEN 1 END) as with_price_data
            FROM transformed.active_stocks
        """)
        stats = cursor.fetchone()
        
        print(f"\n📈 Active Stocks View Statistics:")
        print(f"✅ Total active stocks: {active_count:,}")
        print(f"📊 With financial data: {stats[1]:,}")
        print(f"📉 With price data: {stats[2]:,}")
        
        # Show some examples
        print("\n📋 Sample Active Stocks:")
        cursor.execute("""
            SELECT symbol, name, exchange, sector, 
                   cash_flow_count, time_series_daily_adjusted_count
            FROM transformed.active_stocks 
            WHERE name IS NOT NULL 
            ORDER BY time_series_daily_adjusted_count DESC
            LIMIT 10
        """)
        
        samples = cursor.fetchall()
        for s in samples:
            name = s[1][:35] + "..." if s[1] and len(s[1]) > 35 else s[1]
            print(f"  {s[0]}: {name} ({s[2]}) - CF:{s[4]}, TS:{s[5]}")
        
        # Create an even more focused view for analysis-ready stocks
        print("\n🎯 Creating analysis_ready_stocks view...")
        
        cursor.execute("DROP VIEW IF EXISTS transformed.analysis_ready_stocks;")
        
        analysis_view_sql = """
            CREATE VIEW transformed.analysis_ready_stocks AS
            SELECT *
            FROM transformed.active_stocks
            WHERE 
                -- Must have financial data
                cash_flow_count > 0 
                AND income_statement_count > 0
                AND balance_sheet_count > 0
                -- Must have price data
                AND time_series_daily_adjusted_count > 0
                -- Must have basic company info
                AND name IS NOT NULL
                AND sector IS NOT NULL
            ORDER BY symbol;
        """
        
        cursor.execute(analysis_view_sql)
        conn.commit()
        
        cursor.execute("SELECT COUNT(*) FROM transformed.analysis_ready_stocks")
        analysis_ready_count = cursor.fetchone()[0]
        
        print(f"✅ Created transformed.analysis_ready_stocks view")
        print(f"📊 Analysis-ready stocks: {analysis_ready_count:,}")
        
        print("\n" + "=" * 50)
        print("🎉 Active Stocks Views Created Successfully!")
        print("\n📋 Available Views:")
        print("1. transformed.active_stocks - NYSE/NASDAQ stocks (excludes ETFs)")
        print("2. transformed.analysis_ready_stocks - Active stocks with complete data")
        print("\n💡 Usage Examples:")
        print("  SELECT * FROM transformed.active_stocks WHERE sector = 'TECHNOLOGY';")
        print("  SELECT * FROM transformed.analysis_ready_stocks LIMIT 100;")
        print("=" * 50)
        
    except Exception as e:
        print(f"❌ Error creating views: {str(e)}")
        conn.rollback()
        raise
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    create_active_stocks_view()
