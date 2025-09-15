#!/usr/bin/env python3

"""
Fix Overview View - Source Schema Migration
Creates updatable overview view in public schema pointing to source.company_overview
Drops deprecated extracted schema view
"""

from db.postgres_database_manager import PostgresDatabaseManager

def fix_overview_view():
    """Create updatable overview view pointing to source schema"""
    
    try:
        # Connect using proper database manager
        db = PostgresDatabaseManager()
        db.connect()
        
        print("✅ Connected to database")
        
        # Drop existing views (both default and extracted schemas)
        db.execute_query("DROP VIEW IF EXISTS overview CASCADE;")
        print("✅ Dropped overview view if it existed")
        
        db.execute_query("DROP VIEW IF EXISTS extracted.overview CASCADE;")
        print("✅ Dropped deprecated extracted.overview view")
        
        # Create updatable view in default schema pointing to source
        create_view_sql = """
        CREATE VIEW overview AS
        SELECT 
            symbol_id,
            symbol,
            asset_type AS assettype,
            name,
            description,
            cik,
            exchange,
            currency,
            country,
            sector,
            industry,
            address,
            NULL AS officialsite,  -- Not in source table, default to NULL
            fiscal_year_end AS fiscalyearend,
            company_overview_id AS overview_id,
            'active' AS status,  -- Default status
            created_at,
            updated_at
        FROM source.company_overview;
        """
        
        db.execute_query(create_view_sql)
        print("✅ Created overview view pointing to source.company_overview")
        
        # Create INSTEAD OF INSERT trigger function
        trigger_function_sql = """
        CREATE OR REPLACE FUNCTION overview_insert_trigger()
        RETURNS TRIGGER AS $$
        BEGIN
            INSERT INTO source.company_overview (
                symbol_id, symbol, asset_type, name, description,
                cik, exchange, currency, country, sector, industry, address,
                fiscal_year_end, content_hash
            ) VALUES (
                NEW.symbol_id, NEW.symbol, NEW.assettype, NEW.name, NEW.description,
                NEW.cik, NEW.exchange, NEW.currency, NEW.country, NEW.sector, 
                NEW.industry, NEW.address, NEW.fiscalyearend,
                -- Generate a simple content hash from the symbol and current timestamp
                md5(NEW.symbol || '|' || EXTRACT(EPOCH FROM NOW())::text)
            )
            ON CONFLICT (symbol) DO UPDATE SET
                symbol_id = EXCLUDED.symbol_id,
                asset_type = EXCLUDED.asset_type,
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                cik = EXCLUDED.cik,
                exchange = EXCLUDED.exchange,
                currency = EXCLUDED.currency,
                country = EXCLUDED.country,
                sector = EXCLUDED.sector,
                industry = EXCLUDED.industry,
                address = EXCLUDED.address,
                fiscal_year_end = EXCLUDED.fiscal_year_end,
                content_hash = md5(EXCLUDED.symbol || '|' || EXTRACT(EPOCH FROM NOW())::text),
                updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
        
        db.execute_query(trigger_function_sql)
        print("✅ Created trigger function")
        
        # Create INSTEAD OF trigger
        trigger_sql = """
        CREATE TRIGGER overview_insert_trigger
            INSTEAD OF INSERT ON overview
            FOR EACH ROW EXECUTE FUNCTION overview_insert_trigger();
        """
        
        db.execute_query(trigger_sql)
        print("✅ Created INSTEAD OF INSERT trigger")
        
        # Test the view
        result = db.fetch_dataframe("""
        SELECT symbol_id, symbol, assettype, name, officialsite, fiscalyearend, overview_id
        FROM overview 
        LIMIT 1;
        """)
        
        if not result.empty:
            row = result.iloc[0]
            print(f"✅ View test successful: symbol_id={row['symbol_id']}, symbol={row['symbol']}, assettype={row['assettype']}")
            print(f"   officialsite={row['officialsite']}, fiscalyearend={row['fiscalyearend']}, overview_id={row['overview_id']}")
        
        # Test insert capability (using a valid symbol_id from the existing data)
        test_result = db.fetch_dataframe("SELECT symbol_id FROM source.listing_status LIMIT 1;")
        if not test_result.empty:
            test_symbol_id = int(test_result.iloc[0]['symbol_id'])  # Convert numpy.int64 to Python int
            test_symbol = f'TEST_SYMBOL_{test_symbol_id}'
            
            db.execute_query("""
            INSERT INTO overview (
                symbol_id, symbol, assettype, name, description, exchange, overview_id
            ) VALUES (
                %s, %s, 'Common Stock', 'Test Company', 'Test Description', 'NASDAQ', %s
            );
            """, (test_symbol_id, test_symbol, 999999999))
            
            print("✅ Insert test completed successfully")
            
            # Clean up test data
            db.execute_query("DELETE FROM source.company_overview WHERE symbol = %s;", (test_symbol,))
        else:
            print("⚠️ Skipping insert test - no valid symbol_id found")
        
        db.connection.commit()
        print("✅ All changes committed successfully")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        if 'db' in locals():
            try:
                db.connection.rollback()
            except:
                pass
        return False
    
    finally:
        if 'db' in locals():
            db.close()
    
    return True

if __name__ == "__main__":
    if fix_overview_view():
        print("\n🎉 Overview view is now properly configured in default schema pointing to source!")
        print("   - Deprecated extracted.overview view removed")
        print("   - New overview view created with proper column mappings")
        print("   - INSTEAD OF trigger enables INSERT operations")
    else:
        print("\n💥 Failed to fix overview view")
        exit(1)
