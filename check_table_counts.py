#!/usr/bin/env python3
"""
Check record counts in all extracted tables
"""

from db.postgres_database_manager import PostgresDatabaseManager

def main():
    db_manager = PostgresDatabaseManager()
    
    with db_manager as db:
        tables = [
            'listing_status',
            'balance_sheet', 
            'cash_flow', 
            'earnings_call_transcripts', 
            'insider_transactions', 
            'income_statement',
            'overview',
            'time_series_daily_adjusted'
        ]
        
        print("📊 Record counts in extracted tables:")
        for table in tables:
            try:
                result = db.fetch_query(f"SELECT COUNT(*) FROM extracted.{table}")
                count = result[0][0] if result else 0
                print(f"  {table}: {count:,} records")
            except Exception as e:
                print(f"  {table}: ERROR - {str(e)}")

        # Check for foreign key constraints that might explain the CASCADE
        print("\n🔍 Checking foreign key constraints on listing_status:")
        fk_query = """
        SELECT 
            tc.table_name, 
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name 
        FROM 
            information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY' 
        AND (ccu.table_name = 'listing_status' OR tc.table_name = 'listing_status')
        AND tc.table_schema = 'extracted';
        """
        
        try:
            fk_result = db.fetch_query(fk_query)
            if fk_result:
                print("  Found foreign key relationships:")
                for row in fk_result:
                    print(f"    {row[0]}.{row[1]} -> {row[2]}.{row[3]}")
            else:
                print("  No foreign key constraints found involving listing_status")
        except Exception as e:
            print(f"  Error checking foreign keys: {e}")

if __name__ == "__main__":
    main()
