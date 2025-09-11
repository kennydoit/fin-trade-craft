#!/usr/bin/env python3
"""
Create PostgreSQL database backup for migration to new desktop.
"""

import subprocess
from datetime import datetime
from pathlib import Path


def create_database_backup():
    """Create a full database backup using pg_dump."""

    # Create backup directory
    backup_dir = Path("database_backups")
    backup_dir.mkdir(exist_ok=True)

    # Generate backup filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = backup_dir / f"fin_trade_craft_backup_{timestamp}.sql"

    print("Creating PostgreSQL database backup...")
    print(f"Backup location: {backup_file.absolute()}")
    print("Database size: 11 GB - this may take several minutes...")

    # pg_dump command
    # You may need to adjust the path to pg_dump if it's not in PATH
    pg_dump_cmd = [
        "pg_dump",
        "-h", "localhost",
        "-p", "5432",
        "-U", "postgres",  # You may need to change this username
        "-d", "fin_trade_craft",
        "-f", str(backup_file),
        "--verbose",
        "--no-password"  # Remove this if you need password prompt
    ]

    try:
        print("Running pg_dump command...")
        print(f"Command: {' '.join(pg_dump_cmd)}")

        result = subprocess.run(pg_dump_cmd, check=False, capture_output=True, text=True)

        if result.returncode == 0:
            print("✅ Backup created successfully!")
            print(f"📁 Backup file: {backup_file.absolute()}")
            print(f"📏 File size: {backup_file.stat().st_size / (1024*1024*1024):.2f} GB")

            # Also create a compressed backup
            print("\nCreating compressed backup...")
            compressed_cmd = [
                "pg_dump",
                "-h", "localhost",
                "-p", "5432",
                "-U", "postgres",
                "-d", "fin_trade_craft",
                "-f", str(backup_file.with_suffix('.sql.gz')),
                "--compress=9",
                "--verbose",
                "--no-password"
            ]

            result2 = subprocess.run(compressed_cmd, check=False, capture_output=True, text=True)
            if result2.returncode == 0:
                compressed_file = backup_file.with_suffix('.sql.gz')
                print("✅ Compressed backup created!")
                print(f"📁 Compressed file: {compressed_file.absolute()}")
                print(f"📏 Compressed size: {compressed_file.stat().st_size / (1024*1024*1024):.2f} GB")
        else:
            print("❌ Backup failed!")
            print(f"Error: {result.stderr}")
            print("\nTroubleshooting:")
            print("1. Make sure pg_dump is in your PATH")
            print("2. Check if PostgreSQL bin directory is: C:\\Program Files\\PostgreSQL\\17\\bin")
            print("3. You may need to set PGPASSWORD environment variable")

    except FileNotFoundError:
        print("❌ pg_dump not found!")
        print("\nTo fix this:")
        print("1. Add PostgreSQL bin directory to PATH:")
        print("   C:\\Program Files\\PostgreSQL\\17\\bin")
        print("2. Or use full path to pg_dump.exe")

        # Try with full path
        full_path_cmd = pg_dump_cmd.copy()
        full_path_cmd[0] = "C:\\Program Files\\PostgreSQL\\17\\bin\\pg_dump.exe"

        print(f"\nTrying with full path: {full_path_cmd[0]}")
        try:
            result = subprocess.run(full_path_cmd, check=False, capture_output=True, text=True)
            if result.returncode == 0:
                print("✅ Backup created successfully with full path!")
            else:
                print(f"❌ Still failed: {result.stderr}")
        except Exception as e:
            print(f"❌ Full path also failed: {e}")

    except Exception as e:
        print(f"❌ Unexpected error: {e}")

def show_restore_instructions():
    """Show instructions for restoring on new desktop."""
    print("\n" + "="*60)
    print("RESTORE INSTRUCTIONS FOR NEW DESKTOP")
    print("="*60)
    print("\n1. Install PostgreSQL on new desktop (same version: 17.x)")
    print("   Download from: https://www.postgresql.org/download/windows/")
    print("\n2. Create database:")
    print("   psql -U postgres -c \"CREATE DATABASE fin_trade_craft;\"")
    print("\n3. Restore from backup:")
    print("   psql -U postgres -d fin_trade_craft -f fin_trade_craft_backup_YYYYMMDD_HHMMSS.sql")
    print("\n4. Or restore from compressed backup:")
    print("   gunzip -c fin_trade_craft_backup_YYYYMMDD_HHMMSS.sql.gz | psql -U postgres -d fin_trade_craft")
    print("\n5. Update connection settings in your .env file on new desktop")
    print("\n6. Test connection with: python get_db_info.py")

if __name__ == "__main__":
    create_database_backup()
    show_restore_instructions()
