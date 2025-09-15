#!/usr/bin/env python3

from db.postgres_database_manager import PostgresDatabaseManager

def check_constraints():
    """Check constraints on source.company_overview table"""
    
    try:
        db = PostgresDatabaseManager()
        db.connect()
        
        # Check constraints on source.company_overview
        constraints = db.fetch_dataframe("""
        SELECT 
            conname as constraint_name,
            contype as constraint_type,
            pg_get_constraintdef(oid) as definition
        FROM pg_constraint 
        WHERE conrelid = 'source.company_overview'::regclass;
        """)
        
        print("Constraints on source.company_overview:")
        for _, row in constraints.iterrows():
            print(f"  {row['constraint_name']} ({row['constraint_type']}): {row['definition']}")
        
        # Check indexes
        indexes = db.fetch_dataframe("""
        SELECT 
            indexname,
            indexdef
        FROM pg_indexes 
        WHERE schemaname = 'source' AND tablename = 'company_overview';
        """)
        
        print("\nIndexes on source.company_overview:")
        for _, row in indexes.iterrows():
            print(f"  {row['indexname']}: {row['indexdef']}")
            
        db.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_constraints()
