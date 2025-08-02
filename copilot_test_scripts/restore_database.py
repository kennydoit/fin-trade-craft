#!/usr/bin/env python3
"""
Restore database from backup
"""

import os
import subprocess
from dotenv import load_dotenv

def main():
    # Load environment variables
    load_dotenv()
    
    # Database connection parameters
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_user = os.getenv("POSTGRES_USER", "postgres")
    db_password = os.getenv("POSTGRES_PASSWORD")
    db_name = os.getenv("POSTGRES_DATABASE", "fin_trade_craft")
    
    if not db_password:
        raise ValueError("POSTGRES_PASSWORD not found in environment variables")
    
    # Backup file path
    backup_file = r"E:\Backups\database_backups\fin_trade_craft_backup_20250722_095155.sql"
    
    if not os.path.exists(backup_file):
        print(f"❌ Backup file not found: {backup_file}")
        return
    
    print(f"🔄 Restoring database from backup: {backup_file}")
    print(f"Target database: {db_name} at {db_host}:{db_port}")
    
    # Set PGPASSWORD environment variable for authentication
    env = os.environ.copy()
    env['PGPASSWORD'] = db_password
    
    # Construct psql command with full path
    psql_path = r"C:\Program Files\PostgreSQL\17\bin\psql.exe"
    psql_cmd = [
        psql_path,
        f'-h', db_host,
        f'-p', db_port,
        f'-U', db_user,
        f'-d', db_name,
        f'-f', backup_file
    ]
    
    try:
        print("🚀 Starting restore operation...")
        result = subprocess.run(
            psql_cmd,
            env=env,
            capture_output=True,
            text=True,
            check=True
        )
        
        print("✅ Database restore completed successfully!")
        if result.stdout:
            print("📋 Restore output:")
            print(result.stdout)
            
    except subprocess.CalledProcessError as e:
        print(f"❌ Restore failed with exit code {e.returncode}")
        print("Error output:")
        print(e.stderr)
        print("Standard output:")
        print(e.stdout)
        return False
    except FileNotFoundError:
        print("❌ psql command not found. Make sure PostgreSQL client tools are installed and in PATH.")
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    if success:
        print("\n🎉 Restore operation completed! Run check_table_counts.py to verify the data.")
    else:
        print("\n💥 Restore operation failed. Please check the errors above.")
