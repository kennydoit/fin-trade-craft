#!/usr/bin/env python3
"""
Get PostgreSQL database information for migration.
"""

from db.postgres_database_manager import PostgresDatabaseManager

def get_database_info():
    """Get PostgreSQL database location and connection info."""
    print("Getting PostgreSQL database information...")
    
    try:
        db_manager = PostgresDatabaseManager()
        with db_manager as db:
            # Get data directory
            result = db.fetch_query("SHOW data_directory;")
            data_dir = result[0][0] if result else "Unknown"
            print(f"Data Directory: {data_dir}")
            
            # Get database name
            result = db.fetch_query("SELECT current_database();")
            db_name = result[0][0] if result else "Unknown"
            print(f"Database Name: {db_name}")
            
            # Get PostgreSQL version
            result = db.fetch_query("SELECT version();")
            version = result[0][0] if result else "Unknown"
            print(f"PostgreSQL Version: {version}")
            
            # Get connection info
            result = db.fetch_query("SELECT inet_server_addr(), inet_server_port();")
            if result and result[0][0]:
                server_addr = result[0][0]
                server_port = result[0][1]
                print(f"Server Address: {server_addr}")
                print(f"Server Port: {server_port}")
            else:
                print("Server Address: localhost (local connection)")
                result = db.fetch_query("SHOW port;")
                port = result[0][0] if result else "5432"
                print(f"Server Port: {port}")
            
            # Get database size
            result = db.fetch_query(f"SELECT pg_size_pretty(pg_database_size('{db_name}'));")
            db_size = result[0][0] if result else "Unknown"
            print(f"Database Size: {db_size}")
            
            # List all tables
            result = db.fetch_query("""
                SELECT table_name, 
                       pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) as size
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                ORDER BY pg_total_relation_size(quote_ident(table_name)) DESC;
            """)
            
            print("\nTables in database:")
            for row in result:
                print(f"  {row[0]}: {row[1]}")
            
    except Exception as e:
        print(f"Error getting database info: {e}")

if __name__ == "__main__":
    get_database_info()
