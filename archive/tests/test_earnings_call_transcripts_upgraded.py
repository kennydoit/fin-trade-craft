"""
Test and verification script for the upgraded earnings call transcripts extractor.
Validates the new watermark system, source schema, and data integrity.
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add the parent directories to the path
sys.path.append(str(Path(__file__).parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager
from data_pipeline.extract.extract_earnings_call_transcripts import EarningsCallTranscriptsExtractor
from utils.incremental_etl import WatermarkManager

def test_database_connection():
    """Test database connection and required tables."""
    print("🔍 Testing database connection and tables...")
    
    try:
        db_manager = PostgresDatabaseManager()
        with db_manager as db:
            # Test connection
            result = db.fetch_query("SELECT 1")
            if result and result[0][0] == 1:
                print("  ✅ Database connection successful")
            else:
                print("  ❌ Database connection failed")
                return False
            
            # Check required tables using direct query since table_exists may have issues
            query = """
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE (table_schema = 'extracted' AND table_name = 'listing_status')
            """
            result = db.fetch_query(query)
            
            if result:
                print(f"  ✅ Table extracted.listing_status exists")
            else:
                print(f"  ❌ Table extracted.listing_status missing")
                return False
            
            # Check if source schema exists (watermarks table will be created by extractor)
            schema_query = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'source'"
            schema_result = db.fetch_query(schema_query)
            if schema_result:
                print("  ✅ Source schema exists")
            else:
                print("  ℹ️  Source schema will be created by extractor")
            
            return True
            
    except Exception as e:
        print(f"  ❌ Database test failed: {e}")
        return False

def test_extractor_initialization():
    """Test extractor initialization."""
    print("\n🔍 Testing extractor initialization...")
    
    try:
        db_manager = PostgresDatabaseManager()
        with db_manager as db:
            extractor = EarningsCallTranscriptsExtractor(db)
            
            print(f"  ✅ Extractor initialized successfully")
            print(f"  ✅ Run ID: {extractor.run_id}")
            print(f"  ✅ Generated {len(extractor.quarters)} quarters")
            print(f"  ✅ Quarters range: {extractor.quarters[0]} to {extractor.quarters[-1]}")
            
            # Test source table creation using direct query
            source_query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'source' AND table_name = 'earnings_call_transcripts'
            """
            source_result = db.fetch_query(source_query)
            
            if source_result:
                print("  ✅ Source table exists")
            else:
                print("  ❌ Source table not created")
                return False
            
            return True
            
    except Exception as e:
        print(f"  ❌ Extractor initialization failed: {e}")
        return False

def test_watermark_system():
    """Test watermark management system."""
    print("\n🔍 Testing watermark system...")
    
    try:
        db_manager = PostgresDatabaseManager()
        with db_manager as db:
            watermark_manager = WatermarkManager(db)
            
            # Test getting watermark for non-existent symbol
            watermark = watermark_manager.get_watermark('earnings_call_transcripts', 99999)
            if watermark is None:
                print("  ✅ Returns None for non-existent watermark")
            else:
                print("  ❌ Should return None for non-existent watermark")
                return False
            
            # Test updating watermark (use a real symbol ID from extracted.listing_status)
            symbol_result = db.fetch_query("SELECT symbol_id FROM extracted.listing_status LIMIT 1")
            if not symbol_result:
                print("  ⚠️  No symbols in listing_status table for testing")
                return True  # Skip test if no data
            
            test_symbol_id = symbol_result[0][0]
            watermark_manager.update_watermark('earnings_call_transcripts', test_symbol_id, success=True)
            print("  ✅ Watermark update successful")
            
            # Test retrieving updated watermark
            watermark = watermark_manager.get_watermark('earnings_call_transcripts', test_symbol_id)
            if watermark and watermark['consecutive_failures'] == 0:
                print("  ✅ Watermark retrieval successful")
            else:
                print("  ❌ Watermark retrieval failed")
                return False
            
            return True
            
    except Exception as e:
        print(f"  ❌ Watermark system test failed: {e}")
        return False

def test_symbol_processing_logic():
    """Test symbol processing logic without API calls."""
    print("\n🔍 Testing symbol processing logic...")
    
    try:
        db_manager = PostgresDatabaseManager()
        with db_manager as db:
            extractor = EarningsCallTranscriptsExtractor(db)
            
            # Get a few symbols for testing
            symbols = extractor.get_symbols_needing_processing(limit=3)
            
            if symbols:
                print(f"  ✅ Found {len(symbols)} symbols needing processing")
                
                # Test with first symbol
                test_symbol = symbols[0]
                print(f"  🧪 Testing with symbol: {test_symbol['symbol']} (ID: {test_symbol['symbol_id']})")
                
                # Test quarters generation
                quarters = extractor.get_quarters_for_symbol(test_symbol)
                print(f"  ✅ Generated {len(quarters)} quarters for symbol")
                
                # Test existing quarters check
                existing = extractor.get_existing_quarters(test_symbol['symbol_id'])
                print(f"  ✅ Found {len(existing)} existing quarters")
                
                # Test transform with mock data
                mock_api_data = {
                    'transcript': [
                        {
                            'speaker': 'Test Speaker',
                            'title': 'Test Title',
                            'content': 'Test content for verification',
                            'sentiment': '0.75'
                        }
                    ]
                }
                
                records = extractor.transform_transcript_data(
                    test_symbol, '2024Q1', mock_api_data, 'success'
                )
                
                if records and len(records) == 1:
                    print("  ✅ Data transformation successful")
                    print(f"  ✅ Generated content hash: {records[0]['content_hash']}")
                else:
                    print("  ❌ Data transformation failed")
                    return False
                
            else:
                print("  ℹ️  No symbols need processing (all up to date)")
            
            return True
            
    except Exception as e:
        print(f"  ❌ Symbol processing logic test failed: {e}")
        return False

def test_dry_run_functionality():
    """Test dry run functionality."""
    print("\n🔍 Testing dry run functionality...")
    
    try:
        db_manager = PostgresDatabaseManager()
        with db_manager as db:
            extractor = EarningsCallTranscriptsExtractor(db)
            
            # Run dry run with small limit
            results = extractor.run_extraction(limit=5, dry_run=True)
            
            if results and results.get('dry_run'):
                print("  ✅ Dry run completed successfully")
                print(f"  ✅ Found {results.get('symbols_found', 0)} symbols")
                print(f"  ✅ Estimated API calls: {results.get('estimated_api_calls', 0):,}")
                print(f"  ✅ Estimated time: {results.get('estimated_time_minutes', 0):.1f} minutes")
            else:
                print("  ❌ Dry run failed")
                return False
            
            return True
            
    except Exception as e:
        print(f"  ❌ Dry run test failed: {e}")
        return False

def test_data_integrity():
    """Test data integrity in source table."""
    print("\n🔍 Testing data integrity...")
    
    try:
        db_manager = PostgresDatabaseManager()
        with db_manager as db:
            # Check if source table has data
            count_result = db.fetch_query("SELECT COUNT(*) FROM source.earnings_call_transcripts")
            record_count = count_result[0][0] if count_result else 0
            
            print(f"  📊 Source table has {record_count:,} records")
            
            if record_count > 0:
                # Check for required fields
                required_fields_query = """
                    SELECT 
                        COUNT(*) as total,
                        COUNT(symbol_id) as has_symbol_id,
                        COUNT(quarter) as has_quarter,
                        COUNT(content_hash) as has_content_hash,
                        COUNT(source_run_id) as has_run_id
                    FROM source.earnings_call_transcripts
                """
                
                integrity_result = db.fetch_query(required_fields_query)
                if integrity_result:
                    total, has_symbol_id, has_quarter, has_content_hash, has_run_id = integrity_result[0]
                    
                    if total == has_symbol_id == has_quarter == has_content_hash == has_run_id:
                        print("  ✅ All required fields populated")
                    else:
                        print("  ⚠️  Some required fields missing data")
                
                # Check for valid content hashes
                hash_check_query = """
                    SELECT COUNT(*) FROM source.earnings_call_transcripts 
                    WHERE content_hash IS NULL OR LENGTH(content_hash) != 32
                """
                
                invalid_hash_result = db.fetch_query(hash_check_query)
                invalid_hashes = invalid_hash_result[0][0] if invalid_hash_result else 0
                
                if invalid_hashes == 0:
                    print("  ✅ All content hashes valid")
                else:
                    print(f"  ⚠️  {invalid_hashes} invalid content hashes found")
                
                # Check for duplicates
                duplicate_query = """
                    SELECT COUNT(*) - COUNT(DISTINCT (symbol_id, quarter, speaker, content_hash))
                    FROM source.earnings_call_transcripts
                """
                
                duplicate_result = db.fetch_query(duplicate_query)
                duplicates = duplicate_result[0][0] if duplicate_result else 0
                
                if duplicates == 0:
                    print("  ✅ No duplicate records found")
                else:
                    print(f"  ⚠️  {duplicates} potential duplicate records found")
            
            return True
            
    except Exception as e:
        print(f"  ❌ Data integrity test failed: {e}")
        return False

def test_migration_status():
    """Check migration status between extracted and source tables."""
    print("\n🔍 Checking migration status...")
    
    try:
        db_manager = PostgresDatabaseManager()
        with db_manager as db:
            # Check if extracted table exists using direct query
            extracted_query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'extracted' AND table_name = 'earnings_call_transcripts'
            """
            extracted_result = db.fetch_query(extracted_query)
            
            if not extracted_result:
                print("  ℹ️  No extracted table found - fresh installation")
                return True
            
            # Get counts from both tables
            extracted_count_result = db.fetch_query("SELECT COUNT(*) FROM extracted.earnings_call_transcripts")
            extracted_count = extracted_count_result[0][0] if extracted_count_result else 0
            
            source_count_result = db.fetch_query("SELECT COUNT(*) FROM source.earnings_call_transcripts")
            source_count = source_count_result[0][0] if source_count_result else 0
            
            print(f"  📊 Extracted table: {extracted_count:,} records")
            print(f"  📊 Source table: {source_count:,} records")
            
            if extracted_count == 0:
                print("  ℹ️  No data in extracted table")
            elif source_count == 0:
                print("  ⚠️  Data exists in extracted but not source - migration needed")
                print("  💡 Run: python scripts/migrate_earnings_call_transcripts.py")
            elif source_count >= extracted_count:
                print("  ✅ Migration appears complete")
            else:
                print("  ⚠️  Partial migration detected")
            
            return True
            
    except Exception as e:
        print(f"  ❌ Migration status check failed: {e}")
        return False

def run_all_tests():
    """Run all tests and provide summary."""
    print("🧪 Running Earnings Call Transcripts Extractor Tests")
    print("=" * 60)
    
    tests = [
        ("Database Connection", test_database_connection),
        ("Extractor Initialization", test_extractor_initialization),
        ("Watermark System", test_watermark_system),
        ("Symbol Processing Logic", test_symbol_processing_logic),
        ("Dry Run Functionality", test_dry_run_functionality),
        ("Data Integrity", test_data_integrity),
        ("Migration Status", test_migration_status),
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"\n❌ {test_name} test crashed: {e}")
            failed += 1
    
    print("\n" + "=" * 60)
    print("🎯 Test Summary")
    print(f"   Passed: {passed}")
    print(f"   Failed: {failed}")
    print(f"   Total:  {passed + failed}")
    
    if failed == 0:
        print("\n✅ All tests passed! The upgraded extractor is ready to use.")
        print("\n📝 Next steps:")
        print("   1. If migration needed, run: python scripts/migrate_earnings_call_transcripts.py")
        print("   2. Test with small extraction: python data_pipeline/extract/extract_earnings_call_transcripts.py --dry-run --limit 5")
        print("   3. Run actual extraction: python data_pipeline/extract/extract_earnings_call_transcripts.py --limit 10")
    else:
        print(f"\n❌ {failed} test(s) failed. Please review and fix issues before proceeding.")
    
    print("=" * 60)
    
    return failed == 0

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
