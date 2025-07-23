#!/usr/bin/env python3
"""
Fixed PostgreSQL Migration Script for fin-trade-craft
Handles database existence checks more robustly
"""
import os
import subprocess
import sys
from pathlib import Path

def run_psql_command(command, database="postgres", password=None):
    """Run a psql command with better error handling"""
    psql_path = r"C:\Program Files\PostgreSQL\17\bin\psql.exe"
    
    cmd = [
        psql_path,
        "-h", "localhost",
        "-U", "postgres",
        "-d", database,
        "-c", command
    ]
    
    env = os.environ.copy()
    if password:
        env['PGPASSWORD'] = password
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=env,
            check=True
        )
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        return False, e.stderr

def database_exists(database_name, password):
    """Check if database exists"""
    success, output = run_psql_command(
        f"SELECT 1 FROM pg_database WHERE datname = '{database_name}';",
        password=password
    )
    
    if success:
        # Check if there's a result (not just headers)
        lines = output.strip().split('\n')
        return len(lines) > 2 and any('1' in line for line in lines)
    return False

def drop_database_safely(database_name, password):
    """Safely drop database with force"""
    # First, terminate any connections to the database
    terminate_cmd = f"""
    SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity
    WHERE datname = '{database_name}'
    AND pid <> pg_backend_pid();
    """
    
    run_psql_command(terminate_cmd, password=password)
    
    # Now drop the database
    return run_psql_command(f"DROP DATABASE IF EXISTS {database_name};", password=password)

def create_database(database_name, password):
    """Create database"""
    return run_psql_command(f"CREATE DATABASE {database_name};", password=password)

def restore_database(backup_file, database, password=None):
    """Restore database from backup file"""
    psql_path = r"C:\Program Files\PostgreSQL\17\bin\psql.exe"
    
    cmd = [
        psql_path,
        "-h", "localhost",
        "-U", "postgres",
        "-d", database,
        "-f", backup_file
    ]
    
    env = os.environ.copy()
    if password:
        env['PGPASSWORD'] = password
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=env,
            check=True
        )
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        return False, e.stderr

def main():
    print("🚀 Fixed PostgreSQL Migration for fin-trade-craft")
    print("=" * 60)
    
    # Use password from .env file
    password = "Glaeken01."
    database_name = "fin_trade_craft"
    
    # Check if backup file exists
    backup_file = Path("database_backups/fin_trade_craft_backup_20250722_095155.sql")
    if not backup_file.exists():
        print(f"❌ Backup file not found: {backup_file}")
        return False
    
    print(f"✅ Found backup file: {backup_file}")
    
    # Test connection
    print("\n🔍 Testing PostgreSQL connection...")
    success, output = run_psql_command("SELECT version();", password=password)
    
    if not success:
        print(f"❌ Connection failed: {output}")
        return False
    
    print("✅ PostgreSQL connection successful!")
    
    # Check if database exists
    print(f"\n🔍 Checking if {database_name} database exists...")
    db_exists = database_exists(database_name, password)
    
    if db_exists:
        print(f"⚠️  Database '{database_name}' exists!")
        print("🗑️  Dropping existing database with force...")
        
        success, output = drop_database_safely(database_name, password)
        if not success and "does not exist" not in output:
            print(f"❌ Failed to drop database: {output}")
            return False
        print("✅ Database dropped successfully")
    else:
        print(f"ℹ️  Database '{database_name}' does not exist")
    
    # Create database
    print(f"\n🏗️  Creating {database_name} database...")
    success, output = create_database(database_name, password)
    
    if not success:
        if "already exists" in output:
            print("ℹ️  Database already exists, continuing...")
        else:
            print(f"❌ Failed to create database: {output}")
            return False
    else:
        print("✅ Database created successfully!")
    
    # Restore from backup
    print(f"\n📂 Restoring database from {backup_file}...")
    success, output = restore_database(
        str(backup_file.absolute()),
        database_name,
        password=password
    )
    
    if not success:
        print(f"❌ Database restore failed: {output}")
        # Show first few lines of error for debugging
        error_lines = output.split('\n')[:10]
        print("First few error lines:")
        for line in error_lines:
            if line.strip():
                print(f"  {line}")
        return False
    
    print("✅ Database restored successfully!")
    
    # Verify restoration
    print("\n🔍 Verifying database restoration...")
    success, output = run_psql_command(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';",
        database=database_name,
        password=password
    )
    
    if success:
        lines = output.strip().split('\n')
        for line in lines:
            if line.strip().isdigit():
                table_count = line.strip()
                break
        else:
            table_count = "unknown"
            
        print(f"✅ Found {table_count} tables in the restored database")
        
        # List some tables
        success, output = run_psql_command(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name LIMIT 10;",
            database=database_name,
            password=password
        )
        
        if success:
            print("\nSample tables:")
            lines = output.strip().split('\n')
            for line in lines:
                if line.strip() and not line.startswith('-') and 'table_name' not in line and line.strip() != '':
                    clean_line = line.strip()
                    if clean_line and clean_line != '(0 rows)':
                        print(f"  - {clean_line}")
    
    print("\n🎉 Migration completed successfully!")
    print("\nNext steps:")
    print("1. Test your application with the new PostgreSQL database")
    print("2. Verify all data extraction scripts work correctly") 
    print("3. Remove old SQLite database files if everything works")
    
    return True

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n❌ Migration cancelled by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
