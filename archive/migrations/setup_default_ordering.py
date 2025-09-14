#!/usr/bin/env python3
"""
Database Default Ordering Setup
Creates ordered views and configures default table settings for automatic alphabetical ordering.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from db.postgres_database_manager import PostgresDatabaseManager


def setup_default_ordering():
    """Set up default ordering for database tables."""
    
    with PostgresDatabaseManager() as db:
        
        print("🔧 Setting up default ordering for database tables...")
        
        # Create ordered views for all major tables
        views_config = [
            {
                'view_name': 'listing_status_ordered',
                'table_name': 'listing_status',
                'description': 'Listing status ordered alphabetically by symbol'
            },
            {
                'view_name': 'overview_ordered', 
                'table_name': 'overview',
                'description': 'Company overview ordered alphabetically by symbol'
            },
            {
                'view_name': 'time_series_ordered',
                'table_name': 'time_series_daily_adjusted', 
                'description': 'Time series data ordered by symbol, then date'
            },
            {
                'view_name': 'income_statement_ordered',
                'table_name': 'income_statement',
                'description': 'Income statements ordered by symbol, then date'
            },
            {
                'view_name': 'balance_sheet_ordered',
                'table_name': 'balance_sheet', 
                'description': 'Balance sheets ordered by symbol, then date'
            },
            {
                'view_name': 'cash_flow_ordered',
                'table_name': 'cash_flow',
                'description': 'Cash flow statements ordered by symbol, then date'
            },
            {
                'view_name': 'insider_transactions_ordered',
                'table_name': 'insider_transactions',
                'description': 'Insider transactions ordered by symbol, then date'
            },
            {
                'view_name': 'earnings_transcripts_ordered',
                'table_name': 'earnings_call_transcripts',
                'description': 'Earnings transcripts ordered by symbol, then date'
            }
        ]
        
        print(f"Creating {len(views_config)} ordered views...")
        
        for config in views_config:
            view_name = config['view_name']
            table_name = config['table_name']
            description = config['description']
            
            # Determine ordering columns based on table type
            if 'listing_status' in table_name or 'overview' in table_name:
                order_clause = "ORDER BY symbol_id, symbol"
            else:
                # For time-series tables, order by symbol then date
                order_clause = "ORDER BY symbol_id, symbol, date DESC"
            
            create_view_sql = f'''
            CREATE OR REPLACE VIEW extracted.{view_name} AS
            SELECT * FROM extracted.{table_name}
            {order_clause};
            '''
            
            # Add comment to view
            comment_sql = f'''
            COMMENT ON VIEW extracted.{view_name} IS '{description}';
            '''
            
            try:
                db.execute_query(create_view_sql)
                db.execute_query(comment_sql)
                print(f"  ✅ {view_name}")
            except Exception as e:
                print(f"  ❌ {view_name}: {e}")
        
        # Create a master view that shows table statistics
        master_view_sql = '''
        CREATE OR REPLACE VIEW extracted.database_summary AS
        SELECT 
            'listing_status' as table_name,
            COUNT(*) as record_count,
            MIN(symbol) as first_symbol,
            MAX(symbol) as last_symbol
        FROM extracted.listing_status
        UNION ALL
        SELECT 
            'overview' as table_name,
            COUNT(*) as record_count, 
            MIN(symbol) as first_symbol,
            MAX(symbol) as last_symbol
        FROM extracted.overview
        UNION ALL
        SELECT
            'time_series_daily_adjusted' as table_name,
            COUNT(*) as record_count,
            MIN(symbol) as first_symbol, 
            MAX(symbol) as last_symbol
        FROM extracted.time_series_daily_adjusted
        ORDER BY table_name;
        '''
        
        db.execute_query(master_view_sql)
        print("  ✅ database_summary")
        
        # Test the views
        print("\n📊 Testing ordered views...")
        
        # Test listing_status_ordered
        test_query = '''
        SELECT symbol_id, symbol, name
        FROM extracted.listing_status_ordered 
        LIMIT 5;
        '''
        result = db.fetch_query(test_query)
        print("\n📋 listing_status_ordered (first 5 rows):")
        print("   Symbol ID  | Symbol | Name")
        print("   -----------|--------|--------------------------------")
        for row in result:
            name = (row[2] or '')[:30] + ('...' if len(row[2] or '') > 30 else '')
            print(f"   {row[0]:>10} | {row[1]:<6} | {name}")
        
        # Test overview_ordered  
        test_query2 = '''
        SELECT symbol_id, symbol, name
        FROM extracted.overview_ordered 
        LIMIT 5;
        '''
        result2 = db.fetch_query(test_query2)
        print("\n📋 overview_ordered (first 5 rows):")
        print("   Symbol ID  | Symbol | Name")
        print("   -----------|--------|--------------------------------")
        for row in result2:
            name = (row[2] or '')[:30] + ('...' if len(row[2] or '') > 30 else '')
            print(f"   {row[0]:>10} | {row[1]:<6} | {name}")
        
        # Show database summary
        summary_query = '''
        SELECT * FROM extracted.database_summary;
        '''
        summary_result = db.fetch_query(summary_query)
        print("\n📊 Database Summary:")
        print("   Table Name                    | Records     | First  | Last")
        print("   ------------------------------|-------------|--------|--------")
        for row in summary_result:
            print(f"   {row[0]:<29} | {row[1]:>11,} | {row[2]:<6} | {row[3]}")
        
        print(f"\n🎉 Setup Complete!")
        print(f"📋 Now you have {len(views_config)} ordered views available:")
        print(f"   • Use 'listing_status_ordered' instead of 'listing_status'")
        print(f"   • Use 'overview_ordered' instead of 'overview'") 
        print(f"   • Use 'time_series_ordered' instead of 'time_series_daily_adjusted'")
        print(f"   • And so on...")
        print(f"\n✨ These views automatically sort alphabetically when opened!")


if __name__ == "__main__":
    print("=== Database Default Ordering Setup ===")
    
    try:
        setup_default_ordering()
        
    except Exception as e:
        print(f"\n❌ Error during setup: {e}")
        sys.exit(1)
