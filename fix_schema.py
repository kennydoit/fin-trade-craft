#!/usr/bin/env python3
"""
Fix database schema issues identified in diagnosis
"""

from db.postgres_database_manager import PostgresDatabaseManager

def main():
    print("🔧 Fixing database schema issues...")
    
    with PostgresDatabaseManager() as db:
        cursor = db.connection.cursor()
        
        # 1. Fix the listing_status table/view conflict
        print("\n1. Fixing listing_status table/view conflict...")
        
        # Drop the conflicting view in extracted schema
        try:
            cursor.execute("DROP VIEW IF EXISTS extracted.listing_status CASCADE;")
            print("   ✅ Dropped conflicting view extracted.listing_status")
        except Exception as e:
            print(f"   ⚠️  Could not drop view: {e}")
        
        # Find which schema has the listing_status table
        cursor.execute("""
            SELECT table_schema 
            FROM information_schema.tables 
            WHERE table_name = 'listing_status' 
            AND table_type = 'BASE TABLE'
        """)
        table_schemas = [row[0] for row in cursor.fetchall()]
        print(f"   listing_status table found in schemas: {table_schemas}")
        
        # Create index on the correct schema
        for schema in table_schemas:
            try:
                cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_listing_status_symbol ON {schema}.listing_status(symbol);")
                print(f"   ✅ Ensured index on {schema}.listing_status.symbol")
                break
            except Exception as e:
                print(f"   ⚠️  Could not create index on {schema}: {e}")
        
        # Commit the first transaction before starting the next
        db.connection.commit()
        
        # 2. Fix the run_id column issue in economic_indicators
        print("\n2. Fixing run_id column in economic_indicators...")
        
        # Check current column names
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'economic_indicators' 
            AND table_schema = 'source'
            AND column_name IN ('run_id', 'source_run_id')
        """)
        existing_columns = [row[0] for row in cursor.fetchall()]
        print(f"   Current run_id related columns: {existing_columns}")
        
        if 'source_run_id' in existing_columns and 'run_id' not in existing_columns:
            try:
                # Add run_id as an alias/copy of source_run_id
                cursor.execute("""
                    ALTER TABLE source.economic_indicators 
                    ADD COLUMN IF NOT EXISTS run_id VARCHAR(255);
                """)
                
                # Copy data from source_run_id to run_id
                cursor.execute("""
                    UPDATE source.economic_indicators 
                    SET run_id = source_run_id 
                    WHERE run_id IS NULL AND source_run_id IS NOT NULL;
                """)
                print("   ✅ Added run_id column and copied data from source_run_id")
            except Exception as e:
                print(f"   ⚠️  Could not add run_id column: {e}")
        
        # Commit this transaction too
        db.connection.commit()
        
        # 3. Check and fix extraction_watermarks table structure
        print("\n3. Checking extraction_watermarks table...")
        
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'extraction_watermarks' 
            AND table_schema = 'source'
        """)
        watermark_columns = [row[0] for row in cursor.fetchall()]
        print(f"   Current watermarks columns: {watermark_columns}")
        
        # Ensure all required columns exist
        required_columns = [
            ('table_name', 'VARCHAR(255)'),
            ('symbol_id', 'BIGINT'),
            ('last_fiscal_date', 'DATE'),
            ('last_successful_run', 'TIMESTAMP WITH TIME ZONE'),
            ('consecutive_failures', 'INTEGER DEFAULT 0'),
            ('created_at', 'TIMESTAMP WITH TIME ZONE DEFAULT NOW()'),
            ('updated_at', 'TIMESTAMP WITH TIME ZONE DEFAULT NOW()')
        ]
        
        for col_name, col_type in required_columns:
            if col_name not in watermark_columns:
                try:
                    cursor.execute(f"""
                        ALTER TABLE source.extraction_watermarks 
                        ADD COLUMN {col_name} {col_type};
                    """)
                    print(f"   ✅ Added missing column: {col_name}")
                except Exception as e:
                    print(f"   ⚠️  Could not add column {col_name}: {e}")
        
        # Commit after column additions
        db.connection.commit()
        
        # 4. Ensure primary key and indexes are correct
        print("\n4. Ensuring proper indexes and constraints...")
        
        # Add composite primary key to extraction_watermarks if missing
        try:
            cursor.execute("""
                ALTER TABLE source.extraction_watermarks 
                ADD CONSTRAINT pk_extraction_watermarks 
                PRIMARY KEY (table_name, symbol_id);
            """)
            print("   ✅ Added primary key to extraction_watermarks")
        except Exception as e:
            if "already exists" in str(e).lower() or "multiple primary keys" in str(e).lower():
                print("   ✅ Primary key already exists on extraction_watermarks")
            else:
                print(f"   ⚠️  Could not add primary key: {e}")
        
        # Commit after constraint changes
        db.connection.commit()
        
        # 5. Verify all source schema tables exist
        print("\n5. Verifying source schema tables...")
        
        required_tables = [
            'balance_sheet',
            'cash_flow', 
            'income_statement',
            'time_series_daily_adjusted',
            'earnings_call_transcripts',
            'company_overview',
            'commodities',
            'economic_indicators',
            'extraction_watermarks'
        ]
        
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'source'
        """)
        existing_tables = [row[0] for row in cursor.fetchall()]
        
        missing_tables = [table for table in required_tables if table not in existing_tables]
        if missing_tables:
            print(f"   ⚠️  Missing tables in source schema: {missing_tables}")
        else:
            print("   ✅ All required source schema tables exist")
        
        # Commit all remaining changes
        db.connection.commit()
        print("\n🎯 Schema fixes completed successfully!")
        
        # 6. Final verification
        print("\n6. Final verification...")
        
        # Test listing_status index creation
        try:
            cursor.execute("CREATE INDEX IF NOT EXISTS test_idx ON source.listing_status(symbol);")
            cursor.execute("DROP INDEX IF EXISTS test_idx;")
            print("   ✅ listing_status index creation works")
        except Exception as e:
            print(f"   ❌ listing_status still has issues: {e}")
        
        # Test run_id column access
        try:
            cursor.execute("SELECT run_id FROM source.economic_indicators LIMIT 1;")
            print("   ✅ run_id column accessible")
        except Exception as e:
            print(f"   ❌ run_id column still missing: {e}")
        
        # Final commit
        db.connection.commit()

if __name__ == "__main__":
    main()
