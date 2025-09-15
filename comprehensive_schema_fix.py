#!/usr/bin/env python3
"""
Comprehensive database schema fixes for column mismatches and view conflicts
"""

from db.postgres_database_manager import PostgresDatabaseManager

def diagnose_column_mismatches():
    """Diagnose column name mismatches in key tables."""
    print("🔍 Diagnosing column mismatches...")
    
    with PostgresDatabaseManager() as db:
        cursor = db.connection.cursor()
        
        # 1. Check economic_indicators table columns
        print("\n1. Economic Indicators table columns:")
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'economic_indicators' 
            AND table_schema = 'source'
            ORDER BY ordinal_position
        """)
        econ_cols = [row[0] for row in cursor.fetchall()]
        print(f"   Actual columns: {econ_cols}")
        
        # Check what columns the code expects by examining recent error
        expected_cols = ['api_response', 'api_response_status', 'run_id', 'content_hash']
        missing_cols = [col for col in expected_cols if col not in econ_cols]
        if missing_cols:
            print(f"   Missing expected columns: {missing_cols}")
        
        # 2. Check overview table columns
        print("\n2. Overview table columns:")
        cursor.execute("""
            SELECT table_schema, column_name 
            FROM information_schema.columns 
            WHERE table_name = 'overview'
            ORDER BY table_schema, ordinal_position
        """)
        overview_info = cursor.fetchall()
        print(f"   Overview table locations and columns: {overview_info}")
        
        return econ_cols, overview_info

def diagnose_view_conflicts():
    """Diagnose view conflicts with table creation."""
    print("\n🔍 Diagnosing view conflicts...")
    
    with PostgresDatabaseManager() as db:
        cursor = db.connection.cursor()
        
        # Find all views that might conflict
        cursor.execute("""
            SELECT schemaname, viewname, definition 
            FROM pg_views 
            WHERE schemaname IN ('public', 'extracted', 'source')
            ORDER BY schemaname, viewname
        """)
        views = cursor.fetchall()
        
        print(f"   Found {len(views)} views:")
        for schema, view, definition in views:
            print(f"     {schema}.{view}")
        
        # Find tables with same names as views
        table_view_conflicts = []
        for schema, view, definition in views:
            cursor.execute("""
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_name = %s
                AND table_schema != %s
            """, (view, schema))
            conflicts = cursor.fetchall()
            if conflicts:
                table_view_conflicts.append((schema, view, conflicts))
        
        print(f"   Table-View conflicts: {table_view_conflicts}")
        return views, table_view_conflicts

def fix_column_mismatches(econ_cols):
    """Fix column name mismatches."""
    print("\n🔧 Fixing column mismatches...")
    
    with PostgresDatabaseManager() as db:
        cursor = db.connection.cursor()
        
        # Add missing columns to economic_indicators
        missing_columns = []
        if 'api_response' not in econ_cols:
            missing_columns.append(('api_response', 'TEXT'))
        
        for col_name, col_type in missing_columns:
            try:
                cursor.execute(f"""
                    ALTER TABLE source.economic_indicators 
                    ADD COLUMN {col_name} {col_type};
                """)
                print(f"   ✅ Added missing column: {col_name}")
            except Exception as e:
                print(f"   ⚠️  Could not add column {col_name}: {e}")
        
        # Check if we need to rename any columns
        # Sometimes source_run_id vs run_id, etc.
        if 'source_run_id' in econ_cols and 'run_id' not in econ_cols:
            try:
                cursor.execute("""
                    ALTER TABLE source.economic_indicators 
                    ADD COLUMN run_id VARCHAR(255);
                """)
                cursor.execute("""
                    UPDATE source.economic_indicators 
                    SET run_id = source_run_id 
                    WHERE run_id IS NULL;
                """)
                print("   ✅ Added run_id column and synced with source_run_id")
            except Exception as e:
                print(f"   ⚠️  Could not sync run_id: {e}")
        
        db.connection.commit()

def fix_view_conflicts(table_view_conflicts):
    """Fix view conflicts by dropping problematic views or renaming them."""
    print("\n🔧 Fixing view conflicts...")
    
    with PostgresDatabaseManager() as db:
        cursor = db.connection.cursor()
        
        for view_schema, view_name, table_conflicts in table_view_conflicts:
            print(f"   Fixing conflict: {view_schema}.{view_name} conflicts with tables: {table_conflicts}")
            
            # Strategy: Rename the conflicting view with a suffix
            try:
                cursor.execute(f"""
                    ALTER VIEW {view_schema}.{view_name} 
                    RENAME TO {view_name}_view;
                """)
                print(f"   ✅ Renamed view {view_schema}.{view_name} to {view_name}_view")
            except Exception as e:
                # If renaming fails, try dropping the view (safer approach)
                try:
                    cursor.execute(f"DROP VIEW IF EXISTS {view_schema}.{view_name} CASCADE;")
                    print(f"   ✅ Dropped conflicting view {view_schema}.{view_name}")
                except Exception as e2:
                    print(f"   ❌ Could not fix view conflict for {view_name}: {e2}")
        
        db.connection.commit()

def create_compatibility_views():
    """Create proper compatibility views for schema differences."""
    print("\n🔗 Creating compatibility views...")
    
    with PostgresDatabaseManager() as db:
        cursor = db.connection.cursor()
        
        # Create overview compatibility view if needed
        try:
            cursor.execute("""
                CREATE OR REPLACE VIEW overview AS 
                SELECT * FROM source.company_overview;
            """)
            print("   ✅ Created overview -> source.company_overview compatibility view")
        except Exception as e:
            print(f"   ⚠️  Could not create overview view: {e}")
        
        # Ensure listing_status view points to correct table
        try:
            cursor.execute("""
                CREATE OR REPLACE VIEW listing_status AS 
                SELECT * FROM source.listing_status;
            """)
            print("   ✅ Ensured listing_status -> source.listing_status compatibility view")
        except Exception as e:
            print(f"   ⚠️  Could not create listing_status view: {e}")
        
        db.connection.commit()

def update_schema_file():
    """Update schema file to avoid view conflicts."""
    print("\n📝 Updating schema file...")
    
    # The issue is schema initialization tries to create indexes on views
    # We need to either:
    # 1. Skip index creation on views
    # 2. Create indexes on the underlying tables
    
    schema_fixes = [
        ("CREATE INDEX IF NOT EXISTS idx_listing_status_symbol ON listing_status(symbol);",
         "-- CREATE INDEX IF NOT EXISTS idx_listing_status_symbol ON listing_status(symbol); -- Skip: conflicts with view"),
        ("CREATE INDEX IF NOT EXISTS idx_overview_symbol_id ON overview(symbol_id);",
         "-- CREATE INDEX IF NOT EXISTS idx_overview_symbol_id ON overview(symbol_id); -- Skip: conflicts with view"),
    ]
    
    print("   Schema file fixes recommended:")
    for old_line, new_line in schema_fixes:
        print(f"     Replace: {old_line}")
        print(f"     With: {new_line}")

def main():
    print("🔧 Comprehensive Database Schema Fixes")
    print("=" * 50)
    
    # Step 1: Diagnose issues
    econ_cols, overview_info = diagnose_column_mismatches()
    views, table_view_conflicts = diagnose_view_conflicts()
    
    # Step 2: Fix column mismatches
    fix_column_mismatches(econ_cols)
    
    # Step 3: Fix view conflicts
    fix_view_conflicts(table_view_conflicts)
    
    # Step 4: Create proper compatibility views
    create_compatibility_views()
    
    # Step 5: Provide schema file recommendations
    update_schema_file()
    
    print("\n🎯 Database schema fixes completed!")
    print("   Next steps:")
    print("   1. Test extractors to verify fixes")
    print("   2. Update schema file if index errors persist")
    print("   3. Consider running schema initialization separately")

if __name__ == "__main__":
    main()
