"""
Migrate listing_status table from extracted to source schema.
This ensures all master reference data is in the source schema.
"""

import sys
from pathlib import Path

# Add the parent directories to the path
sys.path.append(str(Path(__file__).parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager

def migrate_listing_status():
    """Migrate listing_status from extracted to source schema."""
    print("📦 Migrating listing_status table from extracted to source schema...")
    
    with PostgresDatabaseManager() as db:
        # First, check current state
        extracted_count = db.fetch_query("SELECT COUNT(*) FROM extracted.listing_status")[0][0]
        print(f"📊 Current extracted.listing_status records: {extracted_count:,}")
        
        # Check if source.listing_status already exists
        source_exists = db.fetch_query("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'source' AND table_name = 'listing_status'
        """)[0][0]
        
        if source_exists:
            source_count = db.fetch_query("SELECT COUNT(*) FROM source.listing_status")[0][0]
            print(f"⚠️ source.listing_status already exists with {source_count:,} records")
            response = input("Do you want to replace it? (y/N): ")
            if response.lower() != 'y':
                print("❌ Migration cancelled")
                return
        
        try:
            # Step 1: Create source.listing_status with same structure
            print("📋 Creating source.listing_status table...")
            create_sql = """
                -- Drop existing table if it exists
                DROP TABLE IF EXISTS source.listing_status CASCADE;
                
                -- Create new table in source schema
                CREATE TABLE source.listing_status (
                    symbol_id BIGINT PRIMARY KEY,
                    symbol VARCHAR(20) UNIQUE NOT NULL,
                    name TEXT,
                    exchange VARCHAR(50),
                    asset_type VARCHAR(20),
                    ipo_date DATE,
                    delisting_date DATE,
                    status VARCHAR(20),
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
                
                -- Create indexes
                CREATE INDEX IF NOT EXISTS idx_source_listing_symbol ON source.listing_status(symbol);
                CREATE INDEX IF NOT EXISTS idx_source_listing_exchange ON source.listing_status(exchange);
                CREATE INDEX IF NOT EXISTS idx_source_listing_asset_type ON source.listing_status(asset_type);
                CREATE INDEX IF NOT EXISTS idx_source_listing_status ON source.listing_status(status);
                
                COMMENT ON TABLE source.listing_status IS 'Master symbol reference table - migrated from extracted schema';
            """
            
            db.execute_script(create_sql)
            print("✅ Created source.listing_status table")
            
            # Step 2: Copy data from extracted to source
            print("📤 Copying data from extracted.listing_status...")
            copy_sql = """
                INSERT INTO source.listing_status (
                    symbol_id, symbol, name, exchange, asset_type, 
                    ipo_date, delisting_date, status, created_at, updated_at
                )
                SELECT 
                    symbol_id, symbol, name, exchange, asset_type,
                    ipo_date, delisting_date, status, created_at, updated_at
                FROM extracted.listing_status
                ORDER BY symbol_id;
            """
            
            db.execute_query(copy_sql)
            
            # Verify copy
            new_count = db.fetch_query("SELECT COUNT(*) FROM source.listing_status")[0][0]
            print(f"✅ Copied {new_count:,} records to source.listing_status")
            
            if new_count != extracted_count:
                print(f"⚠️ Warning: Record count mismatch! Expected {extracted_count:,}, got {new_count:,}")
            
            # Step 3: Update foreign key constraints to point to source schema
            print("🔗 Updating foreign key constraints...")
            
            # Find all foreign keys pointing to extracted.listing_status
            fk_query = """
                SELECT 
                    tc.table_schema,
                    tc.table_name,
                    tc.constraint_name,
                    kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
                  AND ccu.table_schema = 'extracted'
                  AND ccu.table_name = 'listing_status';
            """
            
            foreign_keys = db.fetch_query(fk_query)
            
            for fk in foreign_keys:
                table_schema, table_name, constraint_name, column_name = fk
                print(f"  Updating FK: {table_schema}.{table_name}.{constraint_name}")
                
                # Drop old constraint
                drop_fk_sql = f"""
                    ALTER TABLE {table_schema}.{table_name} 
                    DROP CONSTRAINT IF EXISTS {constraint_name}
                """
                db.execute_query(drop_fk_sql)
                
                # Add new constraint pointing to source schema
                add_fk_sql = f"""
                    ALTER TABLE {table_schema}.{table_name}
                    ADD CONSTRAINT {constraint_name}
                    FOREIGN KEY ({column_name}) 
                    REFERENCES source.listing_status(symbol_id) 
                    ON DELETE CASCADE
                """
                db.execute_query(add_fk_sql)
            
            print(f"✅ Updated {len(foreign_keys)} foreign key constraints")
            
            # Step 4: Rename the original table and create a view for backward compatibility
            print("🔄 Creating backward compatibility view...")
            view_sql = """
                -- Rename original table to backup
                ALTER TABLE extracted.listing_status RENAME TO listing_status_backup;
                
                -- Create view in extracted schema for backward compatibility
                CREATE VIEW extracted.listing_status AS 
                SELECT * FROM source.listing_status;
                
                COMMENT ON VIEW extracted.listing_status IS 'Backward compatibility view - data now lives in source.listing_status';
            """
            
            db.execute_script(view_sql)
            print("✅ Created backward compatibility view")
            
            print("\n🎯 Migration Summary:")
            print(f"  ✅ Migrated {new_count:,} records from extracted.listing_status to source.listing_status")
            print(f"  ✅ Updated {len(foreign_keys)} foreign key constraints")
            print("  ✅ Created backward compatibility view")
            print("\n📝 Next steps:")
            print("  1. Update extractors to reference source.listing_status")
            print("  2. Test all functionality")
            print("  3. Consider dropping extracted.listing_status table after verification")
            
        except Exception as e:
            print(f"❌ Migration failed: {e}")
            print("🔄 Rolling back changes...")
            
            # Rollback - drop the source table if it was created
            try:
                db.execute_query("DROP TABLE IF EXISTS source.listing_status CASCADE")
                print("✅ Rollback completed")
            except:
                print("⚠️ Rollback failed - manual cleanup may be needed")
            
            raise

if __name__ == "__main__":
    migrate_listing_status()
