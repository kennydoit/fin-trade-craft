#!/usr/bin/env python3
"""
Restore PostgreSQL database on new desktop.
Run this script on your new desktop after installing PostgreSQL.
"""

import subprocess
import sys
from pathlib import Path


def check_postgresql():
    """Check if PostgreSQL is installed and accessible."""
    print("Checking PostgreSQL installation...")

    # Try to find psql
    psql_paths = [
        "psql",  # In PATH
        "C:\\Program Files\\PostgreSQL\\17\\bin\\psql.exe",
        "C:\\Program Files\\PostgreSQL\\16\\bin\\psql.exe",
        "C:\\Program Files\\PostgreSQL\\15\\bin\\psql.exe"
    ]

    for psql_path in psql_paths:
        try:
            result = subprocess.run([psql_path, "--version"],
                                  check=False, capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                print(f"✅ Found PostgreSQL: {result.stdout.strip()}")
                return psql_path
        except (subprocess.TimeoutExpired, FileNotFoundError, subprocess.SubprocessError):
            continue

    print("❌ PostgreSQL not found!")
    print("\nPlease install PostgreSQL first:")
    print("1. Download from https://www.postgresql.org/download/windows/")
    print("2. Install PostgreSQL 17.x (or compatible version)")
    print("3. Add bin directory to PATH or run this script again")
    return None

def find_backup_file():
    """Find the database backup file."""
    backup_dir = Path("database_backups")

    if not backup_dir.exists():
        print(f"❌ Backup directory not found: {backup_dir}")
        print("Make sure you copied the entire project folder to this desktop.")
        return None

    # Find the most recent backup file
    sql_files = list(backup_dir.glob("*.sql"))
    if not sql_files:
        print(f"❌ No backup files found in {backup_dir}")
        return None

    # Sort by modification time, newest first
    backup_file = max(sql_files, key=lambda x: x.stat().st_mtime)
    print(f"✅ Found backup file: {backup_file}")
    print(f"📏 File size: {backup_file.stat().st_size / (1024*1024*1024):.2f} GB")

    return backup_file

def restore_database(psql_path, backup_file):
    """Restore the database from backup file."""
    print("\nRestoring database...")

    # Step 1: Create database
    print("1. Creating database 'fin_trade_craft'...")
    create_db_cmd = [psql_path, "-U", "postgres", "-c", "CREATE DATABASE fin_trade_craft;"]

    try:
        result = subprocess.run(create_db_cmd, check=False, capture_output=True, text=True)
        if result.returncode == 0 or "already exists" in result.stderr:
            print("✅ Database created or already exists")
        else:
            print(f"❌ Failed to create database: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Error creating database: {e}")
        return False

    # Step 2: Restore from backup
    print("2. Restoring from backup (this may take several minutes)...")
    restore_cmd = [psql_path, "-U", "postgres", "-d", "fin_trade_craft", "-f", str(backup_file)]

    try:
        print(f"Running: {' '.join(restore_cmd)}")
        result = subprocess.run(restore_cmd, check=False, capture_output=True, text=True)

        if result.returncode == 0:
            print("✅ Database restored successfully!")
            return True
        print(f"❌ Restore failed: {result.stderr}")
        return False

    except Exception as e:
        print(f"❌ Error during restore: {e}")
        return False

def test_connection():
    """Test the database connection using Python."""
    print("\n3. Testing Python database connection...")

    try:
        from db.postgres_database_manager import PostgresDatabaseManager

        db_manager = PostgresDatabaseManager()
        with db_manager as db:
            result = db.fetch_query("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';")
            table_count = result[0][0] if result else 0

            print("✅ Connection successful!")
            print(f"📊 Found {table_count} tables in the database")

        return True

    except ImportError:
        print("❌ Cannot import database manager. Make sure you're in the project directory.")
        return False
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("\nTroubleshooting:")
        print("1. Check if PostgreSQL service is running")
        print("2. Verify username/password in .env file")
        print("3. Check database connection settings")
        return False

def main():
    """Main restore process."""
    print("=" * 60)
    print("POSTGRESQL DATABASE RESTORE")
    print("=" * 60)
    print("This script will restore your fin-trade-craft database on the new desktop.")

    # Check if we're in the right directory
    if not Path("pyproject.toml").exists():
        print("❌ Not in project directory!")
        print("Please run this script from the fin-trade-craft directory.")
        sys.exit(1)

    # Check PostgreSQL installation
    psql_path = check_postgresql()
    if not psql_path:
        sys.exit(1)

    # Find backup file
    backup_file = find_backup_file()
    if not backup_file:
        sys.exit(1)

    # Confirm restore
    print("\nReady to restore:")
    print("  Database: fin_trade_craft")
    print(f"  Backup: {backup_file}")
    print(f"  Size: {backup_file.stat().st_size / (1024*1024*1024):.2f} GB")

    response = input("\nProceed with restore? (y/N): ")
    if response.lower() != 'y':
        print("Restore cancelled.")
        sys.exit(0)

    # Perform restore
    if restore_database(psql_path, backup_file):
        if test_connection():
            print("\n🎉 Database migration completed successfully!")
            print("\nNext steps:")
            print("1. Update .env file if needed")
            print("2. Test your applications")
            print("3. Run: python get_db_info.py")
        else:
            print("\n⚠️  Database restored but connection test failed.")
            print("Check your connection settings.")
    else:
        print("\n❌ Database restore failed.")
        sys.exit(1)

if __name__ == "__main__":
    main()
