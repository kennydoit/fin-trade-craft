#!/usr/bin/env python3
"""
Create proper compatibility views for schema differences
"""

from db.postgres_database_manager import PostgresDatabaseManager

def main():
    print("🔗 Creating proper compatibility views...")
    
    with PostgresDatabaseManager() as db:
        cursor = db.connection.cursor()
        
        # Drop any existing problematic views first
        try:
            cursor.execute("DROP VIEW IF EXISTS listing_status CASCADE;")
            cursor.execute("DROP VIEW IF EXISTS overview CASCADE;")
            print("   ✅ Dropped existing conflicting views")
        except Exception as e:
            print(f"   ⚠️  Could not drop existing views: {e}")
        
        db.connection.commit()
        
        # Create fresh compatibility views
        try:
            cursor.execute("""
                CREATE VIEW listing_status AS 
                SELECT * FROM source.listing_status;
            """)
            print("   ✅ Created fresh listing_status view -> source.listing_status")
        except Exception as e:
            print(f"   ⚠️  Could not create listing_status view: {e}")
        
        # For overview, we need to match the expected column structure
        try:
            # Check what columns exist in source schema tables
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'source' 
                AND table_name LIKE '%overview%' 
                OR table_name LIKE '%company%'
                ORDER BY table_name, ordinal_position
            """)
            source_columns = cursor.fetchall()
            print(f"   Source overview-related columns: {source_columns}")
            
            # Try creating overview view pointing to the right table
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'source' 
                AND (table_name LIKE '%overview%' OR table_name LIKE '%company%')
            """)
            overview_tables = cursor.fetchall()
            print(f"   Available overview tables in source: {overview_tables}")
            
            if overview_tables:
                table_name = overview_tables[0][0]  # Use first match
                cursor.execute(f"""
                    CREATE VIEW overview AS 
                    SELECT * FROM source.{table_name};
                """)
                print(f"   ✅ Created overview view -> source.{table_name}")
            
        except Exception as e:
            print(f"   ⚠️  Could not create overview view: {e}")
        
        db.connection.commit()
        
        # Test the views
        try:
            cursor.execute("SELECT COUNT(*) FROM listing_status LIMIT 1;")
            count = cursor.fetchone()[0]
            print(f"   ✅ listing_status view working - {count} records")
        except Exception as e:
            print(f"   ❌ listing_status view test failed: {e}")
            
        try:
            cursor.execute("SELECT COUNT(*) FROM overview LIMIT 1;")
            count = cursor.fetchone()[0]
            print(f"   ✅ overview view working - {count} records")
        except Exception as e:
            print(f"   ❌ overview view test failed: {e}")

if __name__ == "__main__":
    main()
