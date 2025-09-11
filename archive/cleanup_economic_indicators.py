"""
Cleanup and rebuild script for source.economic_indicators table.
The table is currently 14GB due to storing massive JSON responses and duplicates.
This script will rebuild it properly with only essential business data.
"""

import sys
import uuid
from datetime import datetime
from pathlib import Path

# Add the parent directories to the path
sys.path.append(str(Path(__file__).parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager
from utils.incremental_etl import ContentHasher

def backup_current_data(db_manager):
    """Create a backup of the current source table before cleanup."""
    backup_table = f"economic_indicators_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    print(f"Creating backup table: source.{backup_table}")
    
    backup_sql = f"""
        CREATE TABLE source.{backup_table} AS 
        SELECT * FROM source.economic_indicators
    """
    
    db_manager.execute_query(backup_sql)
    
    # Get count
    count_result = db_manager.fetch_query(f"SELECT COUNT(*) FROM source.{backup_table}")
    count = count_result[0][0] if count_result else 0
    
    print(f"✓ Backed up {count:,} records to source.{backup_table}")
    return backup_table

def drop_and_recreate_table(db_manager):
    """Drop the bloated table and recreate with proper schema."""
    print("Dropping the bloated source.economic_indicators table...")
    
    # Drop the table
    db_manager.execute_query("DROP TABLE IF EXISTS source.economic_indicators CASCADE")
    
    # Recreate with proper schema (matching modern pattern)
    create_sql = """
        CREATE TABLE source.economic_indicators (
            economic_indicator_id   SERIAL PRIMARY KEY,
            indicator_name          VARCHAR(100) NOT NULL,
            function_name           VARCHAR(50),
            maturity                VARCHAR(20),
            date                    DATE NOT NULL,
            interval                VARCHAR(15),
            unit                    VARCHAR(50),
            value                   NUMERIC,
            name                    VARCHAR(255),
            content_hash            VARCHAR(32) NOT NULL,
            api_response_status     VARCHAR(20) DEFAULT 'pass',
            source_run_id          VARCHAR(36) NOT NULL,
            fetched_at             TIMESTAMP DEFAULT NOW(),
            created_at             TIMESTAMP DEFAULT NOW(),
            updated_at             TIMESTAMP DEFAULT NOW(),
            UNIQUE(indicator_name, date, content_hash)
        );
        
        -- Create indexes
        CREATE INDEX IF NOT EXISTS idx_economic_indicators_indicator ON source.economic_indicators(indicator_name);
        CREATE INDEX IF NOT EXISTS idx_economic_indicators_date ON source.economic_indicators(date);
        CREATE INDEX IF NOT EXISTS idx_economic_indicators_content_hash ON source.economic_indicators(content_hash);
        CREATE INDEX IF NOT EXISTS idx_economic_indicators_run_id ON source.economic_indicators(source_run_id);
    """
    
    # Create trigger for updated_at
    trigger_sql = """
        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_trigger 
                WHERE tgname = 'update_economic_indicators_updated_at'
                AND tgrelid = 'source.economic_indicators'::regclass
            ) THEN
                CREATE TRIGGER update_economic_indicators_updated_at 
                BEFORE UPDATE ON source.economic_indicators 
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
            END IF;
        END $$;
    """
    
    db_manager.execute_query(create_sql)
    db_manager.execute_query(trigger_sql)
    
    print("✓ Recreated source.economic_indicators with proper schema")

def migrate_from_extracted(db_manager):
    """Migrate clean data from extracted.economic_indicators."""
    print("Migrating data from extracted.economic_indicators...")
    
    migration_run_id = str(uuid.uuid4())
    current_timestamp = datetime.now()
    
    # Get total count from extracted
    total_result = db_manager.fetch_query("SELECT COUNT(*) FROM extracted.economic_indicators")
    total_records = total_result[0][0] if total_result else 0
    
    print(f"Migrating {total_records:,} records from extracted table...")
    
    # Migrate in batches
    batch_size = 1000
    migrated_count = 0
    offset = 0
    
    while offset < total_records:
        print(f"  Processing batch {offset + 1:,} to {min(offset + batch_size, total_records):,}...")
        
        # Select batch from extracted table
        select_query = """
            SELECT 
                economic_indicator_name,  -- Note: different column name in extracted
                function_name,
                maturity,
                date,
                interval,
                unit,
                value,
                name,
                api_response_status,
                created_at,
                updated_at
            FROM extracted.economic_indicators
            ORDER BY economic_indicator_id
            LIMIT %s OFFSET %s
        """
        
        batch_records = db_manager.fetch_query(select_query, (batch_size, offset))
        
        if not batch_records:
            break
        
        # Transform records for source table
        transformed_records = []
        for record in batch_records:
            (indicator_name, function_name, maturity, date, interval, 
             unit, value, name, api_response_status, created_at, updated_at) = record
            
            # Calculate content hash for business data
            business_data = {
                'indicator_name': indicator_name,
                'function_name': function_name,
                'maturity': maturity,
                'date': str(date) if date else None,
                'interval': interval,
                'unit': unit,
                'value': str(value) if value else None,
                'name': name
            }
            content_hash = ContentHasher.calculate_business_content_hash(business_data)
            
            transformed_record = (
                indicator_name,           # indicator_name
                function_name,            # function_name
                maturity,                 # maturity
                date,                     # date
                interval,                 # interval
                unit,                     # unit
                value,                    # value
                name,                     # name
                content_hash,             # content_hash
                api_response_status,      # api_response_status
                migration_run_id,         # source_run_id
                current_timestamp,        # fetched_at
                created_at or current_timestamp,  # created_at
                updated_at or current_timestamp   # updated_at
            )
            transformed_records.append(transformed_record)
        
        # Insert into source table
        insert_query = """
            INSERT INTO source.economic_indicators (
                indicator_name, function_name, maturity, date, interval,
                unit, value, name, content_hash, api_response_status,
                source_run_id, fetched_at, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (indicator_name, date, content_hash) 
            DO UPDATE SET
                function_name = EXCLUDED.function_name,
                maturity = EXCLUDED.maturity,
                interval = EXCLUDED.interval,
                unit = EXCLUDED.unit,
                value = EXCLUDED.value,
                name = EXCLUDED.name,
                api_response_status = EXCLUDED.api_response_status,
                source_run_id = EXCLUDED.source_run_id,
                fetched_at = EXCLUDED.fetched_at,
                updated_at = EXCLUDED.updated_at
        """
        
        # Execute batch insert
        rows_affected = db_manager.execute_many(insert_query, transformed_records)
        migrated_count += rows_affected
        offset += batch_size
        
        print(f"    ✓ Migrated {rows_affected} records (Total: {migrated_count:,})")
    
    print(f"✓ Migration completed: {migrated_count:,} records migrated")
    return migrated_count

def verify_cleanup(db_manager):
    """Verify the cleanup was successful."""
    print("\n📊 Verifying cleanup...")
    
    # Check new table size
    size_result = db_manager.fetch_query("""
        SELECT pg_size_pretty(pg_total_relation_size('source.economic_indicators'))
    """)
    new_size = size_result[0][0] if size_result else "Unknown"
    
    # Check record count
    count_result = db_manager.fetch_query("SELECT COUNT(*) FROM source.economic_indicators")
    new_count = count_result[0][0] if count_result else 0
    
    # Check for duplicates
    dup_result = db_manager.fetch_query("""
        SELECT COUNT(*) - COUNT(DISTINCT (indicator_name, date, content_hash))
        FROM source.economic_indicators
    """)
    duplicates = dup_result[0][0] if dup_result else 0
    
    print(f"  New table size: {new_size}")
    print(f"  Record count: {new_count:,}")
    print(f"  Duplicates: {duplicates}")
    
    if duplicates == 0:
        print("  ✅ No duplicates found")
    else:
        print(f"  ⚠️  {duplicates} duplicates found")
    
    return new_count, duplicates == 0

def main():
    """Main cleanup function."""
    print("🧹 Starting Economic Indicators Table Cleanup")
    print("   Problem: 14GB table with JSON bloat and duplicates")
    print("   Solution: Rebuild with clean data from extracted table")
    print()
    
    try:
        db_manager = PostgresDatabaseManager()
        
        with db_manager as db:
            # Confirm the cleanup
            response = input("This will DROP the current source.economic_indicators table. Continue? (y/N): ")
            if response.lower() != 'y':
                print("❌ Cleanup cancelled by user")
                return
            
            print()
            
            # Step 1: Backup current data
            backup_table = backup_current_data(db)
            
            # Step 2: Drop and recreate table
            drop_and_recreate_table(db)
            
            # Step 3: Migrate clean data
            migrated_count = migrate_from_extracted(db)
            
            # Step 4: Verify cleanup
            new_count, no_duplicates = verify_cleanup(db)
            
            print()
            print("🎯 Cleanup Summary:")
            print(f"   Backup table: source.{backup_table}")
            print(f"   Records migrated: {migrated_count:,}")
            print(f"   Final record count: {new_count:,}")
            print(f"   Table rebuilt successfully: {'✅' if no_duplicates else '⚠️'}")
            
            if migrated_count > 0 and no_duplicates:
                print()
                print("✅ Cleanup completed successfully!")
                print()
                print("📝 Next steps:")
                print("   1. Monitor the new table size (should be ~40MB)")
                print("   2. Update economic indicators extractor to use new schema")
                print("   3. Consider dropping backup table after verification")
                print(f"   4. Backup table: source.{backup_table}")
            else:
                print()
                print("❌ Cleanup had issues - please review before proceeding")
                
    except Exception as e:
        print(f"❌ Cleanup failed: {e}")
        raise

if __name__ == "__main__":
    main()
