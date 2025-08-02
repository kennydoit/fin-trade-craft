#!/usr/bin/env python3
"""
Check insider_transactions table status
"""

from db.postgres_database_manager import PostgresDatabaseManager

def main():
    db_manager = PostgresDatabaseManager()
    
    with db_manager as db:
        # Check if the table exists in the extracted schema
        result = db.fetch_query("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'extracted' 
                AND table_name = 'insider_transactions'
            );
        """)
        print(f'Table exists in extracted schema: {result[0][0] if result else False}')
        
        # Check if the table exists in any schema
        result2 = db.fetch_query("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_name = 'insider_transactions'
        """)
        print(f'Table found in schemas: {result2}')
        
        # If table exists, check records
        if result and result[0][0]:
            # Check total records
            result3 = db.fetch_query("SELECT COUNT(*) FROM extracted.insider_transactions")
            print(f'Total records in insider_transactions: {result3[0][0] if result3 else 0}')
            
            # Check for any records with placeholder dates
            result4 = db.fetch_query("SELECT COUNT(*) FROM extracted.insider_transactions WHERE transaction_date = '1900-01-01'")
            print(f'Records with placeholder date: {result4[0][0] if result4 else 0}')
            
            # Check for actual transaction records
            result5 = db.fetch_query("SELECT COUNT(*) FROM extracted.insider_transactions WHERE api_response_status = 'pass' AND transaction_date != '1900-01-01'")
            print(f'Actual transaction records: {result5[0][0] if result5 else 0}')
            
            # Show sample records
            samples = db.fetch_query("SELECT symbol, transaction_date, executive, api_response_status FROM extracted.insider_transactions LIMIT 5")
            print('Sample records:')
            for sample in samples:
                print(f'  {sample}')
        else:
            print("❌ Table doesn't exist in extracted schema!")
            
            # Check if it exists in public schema
            result_public = db.fetch_query("SELECT COUNT(*) FROM insider_transactions")
            if result_public:
                print(f"✅ Found {result_public[0][0]} records in public.insider_transactions")
            else:
                print("❌ Table doesn't exist in public schema either")

if __name__ == "__main__":
    main()
