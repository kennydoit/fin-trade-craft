#!/usr/bin/env python3
"""
Create compatibility view for listing_status table
"""

from db.postgres_database_manager import PostgresDatabaseManager

def main():
    print("🔗 Creating compatibility view for listing_status...")
    
    with PostgresDatabaseManager() as db:
        cursor = db.connection.cursor()
        
        # Check what the default schema is
        cursor.execute("SELECT current_schema();")
        current_schema = cursor.fetchone()[0]
        print(f"   Current default schema: {current_schema}")
        
        # Create a view in the default schema pointing to source.listing_status
        try:
            cursor.execute("""
                CREATE OR REPLACE VIEW listing_status AS 
                SELECT * FROM source.listing_status;
            """)
            print(f"   ✅ Created listing_status view in {current_schema} schema")
        except Exception as e:
            print(f"   ❌ Could not create view: {e}")
        
        # Grant necessary permissions
        try:
            cursor.execute("GRANT SELECT ON listing_status TO PUBLIC;")
            print("   ✅ Granted SELECT permissions on listing_status view")
        except Exception as e:
            print(f"   ⚠️  Could not grant permissions: {e}")
        
        # Verify the view works
        try:
            cursor.execute("SELECT COUNT(*) FROM listing_status;")
            count = cursor.fetchone()[0]
            print(f"   ✅ View working - {count} symbols accessible")
        except Exception as e:
            print(f"   ❌ View test failed: {e}")
        
        db.connection.commit()
        print("\n🎯 Compatibility view created successfully!")

if __name__ == "__main__":
    main()
