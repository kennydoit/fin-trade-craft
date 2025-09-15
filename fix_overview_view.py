#!/usr/bin/env python3
"""
Fix overview view column mapping
"""

from db.postgres_database_manager import PostgresDatabaseManager

def main():
    print("🔧 Fixing overview view column mapping...")
    
    with PostgresDatabaseManager() as db:
        cursor = db.connection.cursor()
        
        # Check current columns in source.company_overview
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'company_overview' 
            AND table_schema = 'source'
            ORDER BY ordinal_position
        """)
        source_cols = [row[0] for row in cursor.fetchall()]
        print(f"   Source company_overview columns: {source_cols[:10]}...")  # First 10
        
        # Expected columns by overview extractor (from transform_overview_data)
        expected_cols = [
            'symbol_id', 'symbol', 'assettype', 'name', 'description', 'cik', 
            'exchange', 'currency', 'country', 'sector', 'industry', 'address', 
            'officialsite', 'fiscalyearend', 'overview_id', 'status'
        ]
        
        # Create a better mapping view
        try:
            cursor.execute("DROP VIEW IF EXISTS overview CASCADE;")
            
            # Map source columns to expected names
            column_mapping = {
                'asset_type': 'assettype',
                'company_overview_id': 'overview_id',
                'fiscal_year_end': 'fiscalyearend',
                # Add a default status column since it doesn't exist in source
                # Handle officialsite - may not exist in source, so use a default
            }
            
            # Build select statement with column aliases and defaults
            select_cols = []
            for col in source_cols:
                if col in column_mapping:
                    select_cols.append(f"{col} AS {column_mapping[col]}")
                else:
                    select_cols.append(col)
            
            # Add missing columns with defaults
            missing_cols = []
            if 'officialsite' not in [c.split(' AS ')[-1] for c in select_cols]:
                missing_cols.append("NULL AS officialsite")
            if 'status' not in [c.split(' AS ')[-1] for c in select_cols]:
                missing_cols.append("'active' AS status")
            
            all_cols = select_cols + missing_cols
            select_statement = ", ".join(all_cols)
            
            create_view_sql = f"""
                CREATE VIEW overview AS 
                SELECT {select_statement}
                FROM source.company_overview;
            """
            
            cursor.execute(create_view_sql)
            print("   ✅ Created overview view with comprehensive column mapping")
            
            # Test the view with more columns
            cursor.execute("SELECT assettype, overview_id, officialsite, fiscalyearend, status FROM overview LIMIT 1;")
            test_row = cursor.fetchone()
            print(f"   ✅ View test successful: assettype={test_row[0]}, overview_id={test_row[1]}, officialsite={test_row[2]}")
            
        except Exception as e:
            print(f"   ❌ Could not create mapped view: {e}")
            # Show more details for debugging
            print(f"      Available source columns: {source_cols}")
            print(f"      Expected columns: {expected_cols}")
        
        db.connection.commit()

if __name__ == "__main__":
    main()
