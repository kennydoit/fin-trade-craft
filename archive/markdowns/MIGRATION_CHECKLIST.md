# 📋 PostgreSQL Migration Checklist

## ✅ Current Desktop (Source) - COMPLETED
- [x] Database backup created (9.5 GB)
- [x] Database info gathered
- [x] Migration scripts prepared
- [x] Backup location: `database_backups/fin_trade_craft_backup_20250722_095155.sql`

## ⏳ New Desktop (Target) - TODO

### 1. Install PostgreSQL
- [ ] Download PostgreSQL 17.x from https://www.postgresql.org/download/windows/
- [ ] Install with same password for `postgres` user
- [ ] Verify installation: Run `psql --version` in command prompt

### 2. Transfer Files
Copy these folders/files to new desktop:
- [ ] Entire `fin-trade-craft/` project folder 
- [ ] `.env` file (with database credentials)
- [ ] Backup file: `fin_trade_craft_backup_20250722_095155.sql` (9.5 GB)

### 3. Restore Database
- [ ] Open command prompt in project folder
- [ ] Run: `python restore_database.py`
- [ ] Follow the prompts (it will create database and restore data)

### 4. Verify Setup
- [ ] Run: `python get_db_info.py` 
- [ ] Check that all tables are present:
  - earnings_call_transcripts (~5.8 GB)
  - time_series_daily_adjusted (~3.9 GB) 
  - insider_transactions (~1.7 GB)
  - balance_sheet, income_statement, cash_flow
  - economic_indicators, commodities, overview
  - listing_status, historical_options

### 5. Test Applications
- [ ] Test database connections in your applications
- [ ] Run: `python -c "from utils.database_monitor import DatabaseMonitor; DatabaseMonitor().quick_status()"`
- [ ] Verify earnings call transcripts extractor: `python data_pipeline/extract/extract_earnings_call_transcripts.py`

## 🔧 If Issues Occur

### PostgreSQL Not Found
```bash
# Add to PATH environment variable:
C:\Program Files\PostgreSQL\17\bin
```

### Permission Issues
- Run command prompt as Administrator
- Or set PGPASSWORD environment variable

### Connection Issues
- Check PostgreSQL service is running
- Verify .env file database settings
- Check firewall/antivirus blocking connections

## 📁 Files Created for Migration

| File | Purpose |
|------|---------|
| `get_db_info.py` | Get database information |
| `backup_database.py` | Create database backup |
| `restore_database.py` | Restore database on new desktop |
| `DATABASE_MIGRATION.md` | Detailed migration guide |
| `MIGRATION_CHECKLIST.md` | This checklist |

## 🎯 Expected Results

After successful migration:
- Database size: ~11 GB
- Tables: 11 tables with all data intact
- Python connections working
- All extractors functional

## ⏰ Estimated Time

- PostgreSQL installation: 15-30 minutes
- File transfer: 30-60 minutes (depends on transfer method)
- Database restore: 15-45 minutes (depends on disk speed)
- Testing: 10-15 minutes

**Total: 1-2.5 hours**

## 📞 Support

If you encounter issues:
1. Check the detailed `DATABASE_MIGRATION.md` guide
2. Run `python get_db_info.py` to diagnose connection issues
3. Review PostgreSQL logs in installation directory

---

**Status**: Ready for migration! All files prepared. ✅
