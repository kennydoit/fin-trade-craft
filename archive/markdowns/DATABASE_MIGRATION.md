# PostgreSQL Database Migration Guide

## Current Database Status
- **Database Name**: fin_trade_craft
- **Current Size**: 11 GB
- **PostgreSQL Version**: 17.5
- **Data Directory**: C:/Program Files/PostgreSQL/17/data
- **Backup Created**: ✅ 9.5 GB SQL dump

## Migration Options

### Option 1: Database Dump/Restore (Recommended) ✅

**Advantages:**
- Platform independent
- Includes all data, indexes, and constraints
- Can upgrade PostgreSQL version during migration
- Most reliable method

**Files to Transfer:**
- `database_backups/fin_trade_craft_backup_20250722_095155.sql` (9.5 GB)
- Your entire project folder: `fin-trade-craft/`
- Your `.env` file with database credentials

### Option 2: Physical File Copy (Alternative)

**Location of Database Files:**
```
C:/Program Files/PostgreSQL/17/data/
├── base/           # Database files (including your fin_trade_craft DB)
├── pg_wal/         # Write-ahead log files
├── postgresql.conf # Configuration
└── pg_hba.conf     # Authentication config
```

**⚠️ Warning**: This method requires identical PostgreSQL versions and proper shutdown.

## Migration Steps for New Desktop

### Step 1: Install PostgreSQL 17.x
1. Download from https://www.postgresql.org/download/windows/
2. Install with same version (17.x)
3. Set same password for postgres user
4. Note the installation directory

### Step 2: Transfer Files
```bash
# Copy these to new desktop:
- fin-trade-craft/ folder (entire project)
- database_backups/fin_trade_craft_backup_20250722_095155.sql
```

### Step 3: Restore Database
```bash
# Method 1: Using psql command line
psql -U postgres -c "CREATE DATABASE fin_trade_craft;"
psql -U postgres -d fin_trade_craft -f fin_trade_craft_backup_20250722_095155.sql

# Method 2: Using pgAdmin (GUI)
# - Open pgAdmin
# - Right-click Databases → Create → Database (name: fin_trade_craft)
# - Right-click fin_trade_craft → Restore
# - Select the .sql backup file
```

### Step 4: Update Configuration
1. Update `.env` file if database connection details changed
2. Test connection: `python get_db_info.py`

### Step 5: Verify Data
```bash
python -c "
from utils.database_monitor import DatabaseMonitor
monitor = DatabaseMonitor()
monitor.quick_status()
"
```

## Alternative: Cloud Database Migration

Consider migrating to cloud PostgreSQL:
- **AWS RDS PostgreSQL**
- **Azure Database for PostgreSQL** 
- **Google Cloud SQL**
- **Supabase** (PostgreSQL-as-a-Service)

This eliminates the need to manage PostgreSQL installations.

## Backup Schedule

Create regular backups:
```bash
# Weekly backup (add to scheduled task)
python backup_database.py
```

## Troubleshooting

### Common Issues:
1. **pg_dump not found**: Add `C:\Program Files\PostgreSQL\17\bin` to PATH
2. **Permission denied**: Run as administrator
3. **Password authentication**: Set PGPASSWORD environment variable
4. **Different PostgreSQL version**: Use pg_upgrade or dump/restore

### Test Commands:
```bash
# Test PostgreSQL connection
psql -U postgres -c "SELECT version();"

# Test Python connection
python -c "from db.postgres_database_manager import PostgresDatabaseManager; PostgresDatabaseManager()"
```

## File Locations Summary

**Current Desktop:**
- Database files: `C:/Program Files/PostgreSQL/17/data/`
- Project: `C:/Users/Kenrm/repositories/fin-trade-craft/`
- Backup: `C:/Users/Kenrm/repositories/fin-trade-craft/database_backups/`

**Transfer to New Desktop:**
- Project folder (entire `fin-trade-craft/` directory)
- Backup file (`fin_trade_craft_backup_20250722_095155.sql`)
- Environment file (`.env`)

## Next Steps

1. ✅ Backup created successfully
2. ⏳ Install PostgreSQL on new desktop
3. ⏳ Transfer files
4. ⏳ Restore database
5. ⏳ Test connection
6. ⏳ Resume development
