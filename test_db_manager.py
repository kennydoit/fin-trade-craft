#!/usr/bin/env python3
"""
Test the PostgreSQL database manager with the migrated database
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'db'))

from postgres_database_manager import PostgresDatabaseManager

def test_database_manager():
    """Test the PostgreSQL database manager"""
    
    print("🧪 Testing PostgreSQL Database Manager...")
    
    try:
        # Initialize the database manager
        db_manager = PostgresDatabaseManager()
        
        # Test connection
        db_manager.connect()
        print("✅ Database manager connected successfully!")
        
        # Test a simple query
        query = "SELECT table_name, COUNT(*) as row_count FROM (SELECT 'overview' as table_name UNION SELECT 'commodities' as table_name) t GROUP BY table_name"
        
        # Test query execution
        cursor = db_manager.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM overview;")
        overview_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM commodities;")
        commodities_count = cursor.fetchone()[0]
        
        print(f"✅ Query test successful!")
        print(f"   - Overview table: {overview_count:,} rows")
        print(f"   - Commodities table: {commodities_count:,} rows")
        
        # Test connection close
        db_manager.close()
        print("✅ Database manager disconnected successfully!")
        
        print("\n🎉 PostgreSQL Database Manager is working perfectly!")
        return True
        
    except Exception as e:
        print(f"❌ Database manager test failed: {e}")
        return False

if __name__ == "__main__":
    test_database_manager()
