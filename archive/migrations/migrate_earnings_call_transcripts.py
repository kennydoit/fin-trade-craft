"""
Migration script to safely move earnings call transcripts data 
from extracted.earnings_call_transcripts to source.earnings_call_transcripts.

This script preserves all existing data while adding new required fields.
"""

import sys
import uuid
from datetime import datetime
from pathlib import Path

# Add the parent directories to the path so we can import from db
sys.path.append(str(Path(__file__).parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager
from utils.incremental_etl import ContentHasher

def check_source_table_exists(db_manager):
    """Check if source.earnings_call_transcripts table exists."""
    query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'source' AND table_name = 'earnings_call_transcripts'
    """
    result = db_manager.fetch_query(query)
    return len(result) > 0

def check_extracted_table_exists(db_manager):
    """Check if extracted.earnings_call_transcripts table exists."""
    query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'extracted' AND table_name = 'earnings_call_transcripts'
    """
    result = db_manager.fetch_query(query)
    return len(result) > 0

def get_extracted_table_schema(db_manager):
    """Get the schema of the extracted.earnings_call_transcripts table."""
    query = """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns 
        WHERE table_schema = 'extracted' 
        AND table_name = 'earnings_call_transcripts'
        ORDER BY ordinal_position
    """
    
    result = db_manager.fetch_query(query)
    return {row[0]: {'data_type': row[1], 'is_nullable': row[2], 'default': row[3]} 
            for row in result}

def get_record_counts(db_manager):
    """Get record counts from both tables."""
    counts = {}
    
    # Count extracted records
    extracted_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'extracted' AND table_name = 'earnings_call_transcripts'
    """
    if db_manager.fetch_query(extracted_query):
        result = db_manager.fetch_query("SELECT COUNT(*) FROM extracted.earnings_call_transcripts")
        counts['extracted'] = result[0][0] if result else 0
    else:
        counts['extracted'] = 0
    
    # Count source records
    source_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'source' AND table_name = 'earnings_call_transcripts'
    """
    if db_manager.fetch_query(source_query):
        result = db_manager.fetch_query("SELECT COUNT(*) FROM source.earnings_call_transcripts")
        counts['source'] = result[0][0] if result else 0
    else:
        counts['source'] = 0
    
    return counts

def create_source_table(db_manager):
    """Create the source.earnings_call_transcripts table."""
    create_table_sql = """
        CREATE SCHEMA IF NOT EXISTS source;
        
        CREATE TABLE IF NOT EXISTS source.earnings_call_transcripts (
            transcript_id       SERIAL PRIMARY KEY,
            symbol_id           INTEGER NOT NULL,
            symbol              VARCHAR(20) NOT NULL,
            quarter             VARCHAR(10) NOT NULL,  -- Format: YYYYQM (e.g., 2024Q1)
            speaker             VARCHAR(255) NOT NULL,
            title               VARCHAR(255),
            content             TEXT NOT NULL,
            content_hash        VARCHAR(32) NOT NULL,  -- MD5 hash of content for uniqueness
            sentiment           DECIMAL(5,3),  -- Sentiment score (e.g., 0.6, 0.7)
            api_response_status VARCHAR(20) DEFAULT 'pass',
            source_run_id       VARCHAR(36) NOT NULL,
            fetched_at          TIMESTAMP DEFAULT NOW(),
            created_at          TIMESTAMP DEFAULT NOW(),
            updated_at          TIMESTAMP DEFAULT NOW(),
            FOREIGN KEY (symbol_id) REFERENCES extracted.listing_status(symbol_id) ON DELETE CASCADE,
            UNIQUE(symbol_id, quarter, speaker, content_hash)  -- Use hash instead of full content
        );

        -- Create indexes for earnings call transcripts
        CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_symbol_id ON source.earnings_call_transcripts(symbol_id);
        CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_symbol ON source.earnings_call_transcripts(symbol);
        CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_quarter ON source.earnings_call_transcripts(quarter);
        CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_speaker ON source.earnings_call_transcripts(speaker);
        CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_sentiment ON source.earnings_call_transcripts(sentiment);
        CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_content_hash ON source.earnings_call_transcripts(content_hash);
        CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_run_id ON source.earnings_call_transcripts(source_run_id);
    """
    
    # Create trigger for updated_at
    trigger_sql = """
        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_trigger 
                WHERE tgname = 'update_earnings_call_transcripts_updated_at'
                AND tgrelid = 'source.earnings_call_transcripts'::regclass
            ) THEN
                CREATE TRIGGER update_earnings_call_transcripts_updated_at 
                BEFORE UPDATE ON source.earnings_call_transcripts 
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
            END IF;
        END $$;
    """
    
    db_manager.execute_query(create_table_sql)
    db_manager.execute_query(trigger_sql)
    print("✓ Created source.earnings_call_transcripts table with indexes and triggers")

def migrate_data_in_batches(db_manager, batch_size=1000):
    """Migrate data from extracted to source table in batches."""
    migration_run_id = str(uuid.uuid4())
    current_timestamp = datetime.now()
    
    # Get total count of records with valid symbol_ids
    total_result = db_manager.fetch_query("""
        SELECT COUNT(*) 
        FROM extracted.earnings_call_transcripts e
        INNER JOIN extracted.listing_status ls ON e.symbol_id = ls.symbol_id
    """)
    total_records = total_result[0][0] if total_result else 0
    
    print(f"Migrating {total_records:,} records in batches of {batch_size:,}...")
    
    migrated_count = 0
    offset = 0
    
    while offset < total_records:
        print(f"  Processing batch {offset + 1:,} to {min(offset + batch_size, total_records):,}...")
        
        # Select batch from extracted table with valid symbol_ids only
        select_query = """
            SELECT 
                e.symbol_id, e.symbol, e.quarter, e.speaker, e.title, e.content,
                e.content_hash, e.sentiment, e.api_response_status, 
                e.created_at, e.updated_at
            FROM extracted.earnings_call_transcripts e
            INNER JOIN extracted.listing_status ls ON e.symbol_id = ls.symbol_id
            ORDER BY e.transcript_id
            LIMIT %s OFFSET %s
        """
        
        batch_records = db_manager.fetch_query(select_query, (batch_size, offset))
        
        if not batch_records:
            break
        
        # Transform records for source table
        transformed_records = []
        for record in batch_records:
            symbol_id, symbol, quarter, speaker, title, content, content_hash, sentiment, api_response_status, created_at, updated_at = record
            
            # Recalculate content hash if it seems wrong or missing
            if not content_hash or len(content_hash) != 32:
                business_data = {
                    'symbol_id': symbol_id,
                    'quarter': quarter,
                    'speaker': speaker,
                    'title': title,
                    'content': content,
                    'sentiment': sentiment
                }
                content_hash = ContentHasher.calculate_business_content_hash(business_data)
            
            transformed_record = (
                symbol_id,
                symbol,
                quarter,
                speaker,
                title,
                content,
                content_hash,
                sentiment,
                api_response_status,
                migration_run_id,  # source_run_id
                current_timestamp,  # fetched_at
                created_at or current_timestamp,  # created_at
                updated_at or current_timestamp   # updated_at
            )
            transformed_records.append(transformed_record)
        
        # Insert into source table with conflict handling
        insert_query = """
            INSERT INTO source.earnings_call_transcripts (
                symbol_id, symbol, quarter, speaker, title, content,
                content_hash, sentiment, api_response_status, source_run_id,
                fetched_at, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol_id, quarter, speaker, content_hash) 
            DO UPDATE SET
                title = EXCLUDED.title,
                content = EXCLUDED.content,
                sentiment = EXCLUDED.sentiment,
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

def verify_migration(db_manager):
    """Verify the migration was successful."""
    print("\n📊 Verifying migration...")
    
    # Get counts
    counts = get_record_counts(db_manager)
    
    print(f"  Extracted table: {counts['extracted']:,} records")
    print(f"  Source table: {counts['source']:,} records")
    
    # Check for any data mismatches
    if counts['extracted'] > 0:
        # Compare sample records
        sample_query = """
            SELECT COUNT(*) FROM (
                SELECT e.symbol_id, e.quarter, e.speaker, e.content_hash
                FROM extracted.earnings_call_transcripts e
                WHERE EXISTS (
                    SELECT 1 FROM source.earnings_call_transcripts s
                    WHERE s.symbol_id = e.symbol_id 
                    AND s.quarter = e.quarter 
                    AND s.speaker = e.speaker 
                    AND s.content_hash = e.content_hash
                )
            ) matched
        """
        
        matched_result = db_manager.fetch_query(sample_query)
        matched_count = matched_result[0][0] if matched_result else 0
        
        print(f"  Matched records: {matched_count:,}")
        
        if matched_count == counts['extracted']:
            print("  ✅ All records successfully migrated!")
        else:
            print(f"  ⚠️  {counts['extracted'] - matched_count:,} records may need review")
    
    # Check unique constraints
    duplicate_query = """
        SELECT COUNT(*) - COUNT(DISTINCT (symbol_id, quarter, speaker, content_hash))
        FROM source.earnings_call_transcripts
    """
    
    duplicate_result = db_manager.fetch_query(duplicate_query)
    duplicate_count = duplicate_result[0][0] if duplicate_result else 0
    
    if duplicate_count == 0:
        print("  ✅ No duplicate records found")
    else:
        print(f"  ⚠️  {duplicate_count} potential duplicate records found")
    
    return counts

def main():
    """Main migration function."""
    print("🚀 Starting Earnings Call Transcripts Migration")
    print("   From: extracted.earnings_call_transcripts")
    print("   To:   source.earnings_call_transcripts")
    print()
    
    try:
        # Initialize database manager
        db_manager = PostgresDatabaseManager()
        
        with db_manager as db:
            # Check if extracted table exists
            if not check_extracted_table_exists(db):
                print("❌ Error: extracted.earnings_call_transcripts table does not exist")
                print("   No data to migrate.")
                return
            
            # Get initial counts
            initial_counts = get_record_counts(db)
            print(f"📊 Initial state:")
            print(f"   Extracted table: {initial_counts['extracted']:,} records")
            print(f"   Source table: {initial_counts['source']:,} records")
            print()
            
            if initial_counts['extracted'] == 0:
                print("ℹ️  No records found in extracted table to migrate")
                return
            
            # Show extracted table schema
            print("📋 Extracted table schema:")
            extracted_schema = get_extracted_table_schema(db)
            for column, info in extracted_schema.items():
                print(f"   {column}: {info['data_type']}")
            print()
            
            # Create source table if it doesn't exist
            if not check_source_table_exists(db):
                print("Creating source.earnings_call_transcripts table...")
                create_source_table(db)
                print()
            else:
                print("✓ Source table already exists")
                print()
            
            # Confirm migration
            response = input(f"Proceed with migrating {initial_counts['extracted']:,} records? (y/N): ")
            if response.lower() != 'y':
                print("❌ Migration cancelled by user")
                return
            
            print()
            print("🔄 Starting data migration...")
            
            # Migrate data
            migrated_count = migrate_data_in_batches(db, batch_size=1000)
            
            # Verify migration
            final_counts = verify_migration(db)
            
            print()
            print("🎯 Migration Summary:")
            print(f"   Records in extracted table: {final_counts['extracted']:,}")
            print(f"   Records in source table: {final_counts['source']:,}")
            print(f"   Records migrated: {migrated_count:,}")
            
            if migrated_count > 0:
                print()
                print("✅ Migration completed successfully!")
                print()
                print("📝 Next steps:")
                print("   1. Verify data integrity in source table")
                print("   2. Update applications to use source.earnings_call_transcripts")
                print("   3. Consider archiving extracted.earnings_call_transcripts")
                print("   4. Use extract_earnings_call_transcripts.py for new extractions")
            else:
                print("ℹ️  No new records were migrated (possibly already migrated)")
            
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        raise


if __name__ == "__main__":
    main()
