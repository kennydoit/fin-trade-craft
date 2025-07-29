# Database Safety Guidelines

## Overview
This document outlines critical safety measures to prevent accidental data loss like the CASCADE incident that occurred with the listing_status table.

## 🚨 Critical Safety Rules

### 1. **NEVER USE TRUNCATE CASCADE**
- ❌ **DON'T**: `TRUNCATE TABLE listing_status CASCADE;`
- ✅ **DO**: `DELETE FROM listing_status;`

**Why**: TRUNCATE CASCADE automatically deletes ALL dependent data across ALL related tables without warning.

### 2. **Use Safe DELETE Operations**
```sql
-- Safe approach for replacing data
DELETE FROM listing_status;
-- Then insert new data
INSERT INTO listing_status (...) VALUES (...);
```

### 3. **Implement Backup-Before-Replace Pattern**
```python
def safe_replace_data(self, table_name, new_data):
    """Safely replace table data with backup"""
    # 1. Create backup table
    backup_table = f"{table_name}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    self.db.execute_query(f"CREATE TABLE {backup_table} AS SELECT * FROM {table_name}")
    
    try:
        # 2. Clear existing data
        self.db.execute_query(f"DELETE FROM {table_name}")
        
        # 3. Insert new data
        self.load_data(table_name, new_data)
        
        # 4. Verify success, then drop backup
        if self.verify_data_integrity(table_name):
            self.db.execute_query(f"DROP TABLE {backup_table}")
        else:
            # Restore from backup if something went wrong
            self.restore_from_backup(table_name, backup_table)
            
    except Exception as e:
        # Restore from backup on any error
        self.restore_from_backup(table_name, backup_table)
        raise e
```

## 🛡️ Foreign Key Safety

### Current Foreign Key Constraints
All financial tables reference `listing_status.symbol_id` with `ON DELETE CASCADE`:
- `time_series_daily_adjusted`
- `balance_sheet`
- `cash_flow`
- `income_statement`
- `earnings_call_transcripts`
- `insider_transactions`
- `overview`

### Recommended Changes

#### Option 1: Use RESTRICT Instead of CASCADE
```sql
-- Change foreign key constraints to prevent accidental deletion
ALTER TABLE time_series_daily_adjusted 
DROP CONSTRAINT IF EXISTS time_series_daily_adjusted_symbol_id_fkey,
ADD CONSTRAINT time_series_daily_adjusted_symbol_id_fkey 
FOREIGN KEY (symbol_id) REFERENCES listing_status(symbol_id) ON DELETE RESTRICT;
```

#### Option 2: Use SET NULL for Non-Critical References
```sql
-- For some tables, setting NULL might be acceptable
ALTER TABLE some_table 
ADD CONSTRAINT some_table_symbol_id_fkey 
FOREIGN KEY (symbol_id) REFERENCES listing_status(symbol_id) ON DELETE SET NULL;
```

## 🔄 Safe Data Replacement Workflow

### For Listing Status Updates:
1. **Backup**: Create automatic backup before any changes
2. **Validate**: Check new data quality and completeness
3. **Incremental Update**: Use UPSERT instead of full replacement when possible
4. **Verify**: Confirm dependent data integrity after changes

### Example Safe Implementation:
```python
def safe_update_listing_status(self, new_symbols):
    """Safely update listing status without losing dependent data"""
    
    # 1. Backup current data
    backup_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    self.create_backup_table('listing_status', backup_timestamp)
    
    # 2. Use UPSERT instead of replace
    for symbol_data in new_symbols:
        self.upsert_symbol(symbol_data)
    
    # 3. Handle deactivated symbols (don't delete, mark as inactive)
    self.mark_inactive_symbols(new_symbols)
    
    # 4. Verify integrity
    if not self.verify_foreign_key_integrity():
        self.restore_from_backup('listing_status', backup_timestamp)
        raise Exception("Foreign key integrity check failed")
```

## 🗄️ Automated Backup Strategy

### 1. **Pre-Operation Backups**
Create automatic backups before any destructive operations:

```python
def backup_before_operation(self, table_name, operation_name):
    """Create backup before potentially destructive operation"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_name = f"{table_name}_{operation_name}_{timestamp}"
    
    self.db.execute_query(f"""
        CREATE TABLE backup_{backup_name} AS 
        SELECT * FROM {table_name}
    """)
    
    return backup_name
```

### 2. **Daily Automated Backups**
Set up automated daily backups using Windows Task Scheduler:

```batch
@echo off
set BACKUP_DIR=E:\Backups\database_backups
set TIMESTAMP=%date:~-4,4%%date:~-10,2%%date:~-7,2%_%time:~0,2%%time:~3,2%%time:~6,2%
"C:\Program Files\PostgreSQL\17\bin\pg_dump.exe" -h localhost -U postgres -d fin_trade_craft > "%BACKUP_DIR%\fin_trade_craft_backup_%TIMESTAMP%.sql"
```

## 🧪 Testing Strategy

### 1. **Development Database Testing**
- Always test destructive operations on a copy first
- Use a separate development database for testing

### 2. **Dry Run Mode**
Add dry-run capabilities to extraction scripts:

```python
def run_etl(self, dry_run=False):
    """Run ETL with optional dry-run mode"""
    if dry_run:
        print("DRY RUN MODE - No actual changes will be made")
        # Simulate operations and show what would happen
        return self.simulate_changes()
    else:
        return self.execute_changes()
```

## 📋 Pre-Deployment Checklist

Before running any data replacement operation:

- [ ] **Backup Created**: Automated backup exists and is verified
- [ ] **Operation Reviewed**: Code reviewed for CASCADE operations
- [ ] **Dry Run Executed**: Test run completed successfully
- [ ] **Rollback Plan**: Clear rollback procedure documented
- [ ] **Monitoring Ready**: Progress monitoring and error handling in place
- [ ] **Off-Hours Execution**: Run during low-usage periods
- [ ] **Verification Plan**: Data integrity checks defined

## 🚨 Emergency Response Plan

### If Data Loss Occurs:

1. **STOP**: Immediately halt all ETL operations
2. **ASSESS**: Determine scope of data loss
3. **RESTORE**: Use most recent backup
4. **VERIFY**: Confirm restoration success
5. **INVESTIGATE**: Analyze root cause
6. **DOCUMENT**: Record incident and lessons learned

### Emergency Contacts:
- Database Admin: [Your contact info]
- Backup Location: `E:\Backups\database_backups`
- Recovery Scripts: `restore_database.py`, `targeted_recovery.py`

## 📈 Monitoring and Alerting

### Implement Table Count Monitoring:
```python
def monitor_table_counts(self):
    """Monitor for unexpected table count changes"""
    expected_counts = {
        'listing_status': (20000, 25000),  # min, max expected
        'time_series_daily_adjusted': (19000000, 25000000),
        # ... other tables
    }
    
    for table, (min_count, max_count) in expected_counts.items():
        current_count = self.get_table_count(table)
        if not (min_count <= current_count <= max_count):
            self.send_alert(f"Table {table} count anomaly: {current_count}")
```

## 🔧 Recommended Code Changes

### 1. Update Extract Scripts
Add safety checks to all extraction scripts:

```python
class SafeExtractor:
    def __init__(self):
        self.enable_safety_checks = True
        self.backup_before_replace = True
    
    def safe_replace_table_data(self, table_name, new_data):
        if self.backup_before_replace:
            self.create_backup(table_name)
        
        # Use DELETE instead of TRUNCATE
        self.db.execute_query(f"DELETE FROM {table_name}")
        self.load_data(table_name, new_data)
        
        if self.enable_safety_checks:
            self.verify_integrity()
```

### 2. Modify Foreign Key Constraints
Consider changing CASCADE to RESTRICT:

```sql
-- Review and potentially modify foreign key constraints
ALTER TABLE time_series_daily_adjusted 
DROP CONSTRAINT time_series_daily_adjusted_symbol_id_fkey,
ADD CONSTRAINT time_series_daily_adjusted_symbol_id_fkey 
FOREIGN KEY (symbol_id) REFERENCES listing_status(symbol_id) ON DELETE RESTRICT;
```

## 🎯 Action Items

### Immediate (This Week):
1. ✅ Update `extract_listing_status.py` to use DELETE instead of TRUNCATE CASCADE
2. [ ] Add backup creation to all extraction scripts
3. [ ] Implement table count monitoring
4. [ ] Create development database for testing

### Short Term (Next Month):
1. [ ] Review and update foreign key constraints
2. [ ] Implement automated daily backups
3. [ ] Add dry-run mode to all ETL scripts
4. [ ] Create emergency response procedures

### Long Term (Next Quarter):
1. [ ] Implement comprehensive monitoring system
2. [ ] Set up alerting for anomalies
3. [ ] Create automated testing pipeline
4. [ ] Document all procedures

---

**Remember**: It's better to be slow and safe than fast and sorry. Always prioritize data integrity over speed.
