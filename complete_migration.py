#!/usr/bin/env python3
"""
Complete PostgreSQL Migration Script for fin-trade-craft
This script will:
1. Create the database if it doesn't exist
2. Restore from the backup file
3. Verify the migration
"""
import os
import subprocess
import sys
from pathlib import Path

def run_psql_command(command, database="postgres", password=None):
    """Run a psql command"""
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
    print("🚀 Starting PostgreSQL Migration for fin-trade-craft")
    print("=" * 60)
    
    # Check if backup file exists
    backup_file = Path("database_backups/fin_trade_craft_backup_20250722_095155.sql")
    if not backup_file.exists():
        print(f"❌ Backup file not found: {backup_file}")
        return False
    
    print(f"✅ Found backup file: {backup_file}")
    
    # Get password from user
    password = input("Enter PostgreSQL password for user 'postgres': ").strip()
    
    if not password:
        print("❌ Password is required")
        return False
    
    # Test connection
    print("\n🔍 Testing PostgreSQL connection...")
    success, output = run_psql_command("SELECT version();", password=password)
    
    if not success:
        print(f"❌ Connection failed: {output}")
        print("Please check your PostgreSQL password and try again.")
        return False
    
    print("✅ PostgreSQL connection successful!")
    print(f"Version: {output.strip()}")
    
    # Check if database exists
    print("\n🔍 Checking if fin_trade_craft database exists...")
    success, output = run_psql_command(
        "SELECT 1 FROM pg_database WHERE datname = 'fin_trade_craft';",
        password=password
    )
    
    if success and output.strip():
        print("⚠️  Database 'fin_trade_craft' already exists!")
        response = input("Do you want to drop and recreate it? (y/N): ").strip().lower()
        
        if response == 'y':
            print("🗑️  Dropping existing database...")
            success, output = run_psql_command(
                "DROP DATABASE fin_trade_craft;",
                password=password
            )
            if not success:
                print(f"❌ Failed to drop database: {output}")
                return False
            print("✅ Database dropped successfully")
        else:
            print("Migration cancelled by user")
            return False
    
    # Create database
    print("\n🏗️  Creating fin_trade_craft database...")
    success, output = run_psql_command(
        "CREATE DATABASE fin_trade_craft;",
        password=password
    )
    
    if not success:
        print(f"❌ Failed to create database: {output}")
        return False
    
    print("✅ Database created successfully!")
    
    # Restore from backup
    print(f"\n📂 Restoring database from {backup_file}...")
    success, output = restore_database(
        str(backup_file.absolute()),
        "fin_trade_craft",
        password=password
    )
    
    if not success:
        print(f"❌ Database restore failed: {output}")
        return False
    
    print("✅ Database restored successfully!")
    
    # Verify restoration
    print("\n🔍 Verifying database restoration...")
    success, output = run_psql_command(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';",
        database="fin_trade_craft",
        password=password
    )
    
    if success:
        table_count = output.strip().split('\n')[2].strip()  # Get the count from output
        print(f"✅ Found {table_count} tables in the restored database")
        
        # List some tables
        success, output = run_psql_command(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name LIMIT 10;",
            database="fin_trade_craft",
            password=password
        )
        
        if success:
            print("\nSample tables:")
            lines = output.strip().split('\n')
            for line in lines[2:-2]:  # Skip header and footer
                if line.strip() and line.strip() != '-' * len(line.strip()):
                    print(f"  - {line.strip()}")
    
    print("\n🎉 Migration completed successfully!")
    print("\nNext steps:")
    print("1. Update your .env file with the correct password if needed")
    print("2. Test your application with the new PostgreSQL database")
    print("3. Remove the old SQLite database files if everything works correctly")
    
    return True

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n❌ Migration cancelled by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)
