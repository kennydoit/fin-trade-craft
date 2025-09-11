"""
Simple database connection test.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from db.postgres_database_manager import PostgresDatabaseManager


def test_connection():
    """Test database connection."""
    try:
        db = PostgresDatabaseManager()
        db.connect()  # Explicitly connect
        
        # Test simple query
        result = db.execute_query("SELECT version();")
        print("✓ Database connection successful")
        print(f"PostgreSQL version: {result[0][0] if result else 'Unknown'}")
        
        # Test listing_status table access
        count_query = "SELECT COUNT(*) FROM extracted.listing_status;"
        count_result = db.execute_query(count_query)
        print(f"✓ listing_status table accessible with {count_result[0][0]} rows")
        
        db.close()
        return True
        
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        return False


if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)
