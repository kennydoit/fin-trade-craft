#!/usr/bin/env python3

"""
Fix Economic Indicators - Make source_run_id nullable
Since the extractor doesn't provide source_run_id and it's causing insertion failures
"""

from db.postgres_database_manager import PostgresDatabaseManager

def fix_economic_indicators_schema():
    """Make source_run_id nullable in economic_indicators table"""
    
    try:
        db = PostgresDatabaseManager()
        db.connect()
        
        print("✅ Connected to database")
        
        # Make source_run_id nullable
        alter_sql = """
        ALTER TABLE source.economic_indicators 
        ALTER COLUMN source_run_id DROP NOT NULL;
        """
        
        db.execute_query(alter_sql)
        print("✅ Made source_run_id nullable in economic_indicators table")
        
        # Verify the change
        result = db.fetch_dataframe("""
        SELECT column_name, is_nullable, column_default
        FROM information_schema.columns 
        WHERE table_schema = 'source' AND table_name = 'economic_indicators'
        AND column_name = 'source_run_id';
        """)
        
        if not result.empty:
            row = result.iloc[0]
            print(f"✅ Verified: source_run_id is now nullable={row['is_nullable']}")
        
        db.commit()
        print("✅ Changes committed successfully")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        if 'db' in locals():
            try:
                db.connection.rollback()
            except:
                pass
        return False
    
    finally:
        if 'db' in locals():
            db.close()
    
    return True

if __name__ == "__main__":
    if fix_economic_indicators_schema():
        print("\n🎉 Economic indicators schema is now fixed!")
        print("   - source_run_id is now nullable")
        print("   - Constraint conflicts resolved")
    else:
        print("\n💥 Failed to fix economic indicators schema")
        exit(1)
