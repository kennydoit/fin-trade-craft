#!/usr/bin/env python3

"""
Fix Overview View - Final Solution
Creates updatable overview view with INSTEAD OF trigger for inserts
"""

import psycopg2
import os
import sys

def fix_overview_view():
    """Create updatable overview view with proper INSERT handling"""
    
    try:
        # Connect to database
        conn = psycopg2.connect(
            host='localhost',
            database='fin_trade_craft',
            user='postgres',
            password='postgres'
        )
        cursor = conn.cursor()
        
        print("✅ Connected to database")
        
        # Drop existing view
        cursor.execute("DROP VIEW IF EXISTS overview CASCADE;")
        print("✅ Dropped existing overview view")
        
        # Create updatable view
        create_view_sql = """
        CREATE VIEW overview AS
        SELECT 
            calculated_symbol_id AS symbol_id,
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
            official_site AS officialsite,  -- Use actual column if exists
            fiscal_year_end AS fiscalyearend,  -- Use actual column if exists
            company_overview_id AS overview_id,
            status
        FROM source.company_overview;
        """
        
        cursor.execute(create_view_sql)
        print("✅ Created overview view")
        
        # Create INSTEAD OF INSERT trigger function
        trigger_function_sql = """
        CREATE OR REPLACE FUNCTION overview_insert_trigger()
        RETURNS TRIGGER AS $$
        BEGIN
            INSERT INTO source.company_overview (
                calculated_symbol_id, symbol, asset_type, name, description,
                cik, exchange, currency, country, sector, industry, address,
                company_overview_id, status
            ) VALUES (
                NEW.symbol_id, NEW.symbol, NEW.assettype, NEW.name, NEW.description,
                NEW.cik, NEW.exchange, NEW.currency, NEW.country, NEW.sector, 
                NEW.industry, NEW.address, NEW.overview_id, NEW.status
            )
            ON CONFLICT (calculated_symbol_id) DO UPDATE SET
                symbol = EXCLUDED.symbol,
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
                status = EXCLUDED.status;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
        
        cursor.execute(trigger_function_sql)
        print("✅ Created trigger function")
        
        # Create INSTEAD OF trigger
        trigger_sql = """
        CREATE TRIGGER overview_insert_trigger
            INSTEAD OF INSERT ON overview
            FOR EACH ROW EXECUTE FUNCTION overview_insert_trigger();
        """
        
        cursor.execute(trigger_sql)
        print("✅ Created INSTEAD OF INSERT trigger")
        
        # Test the view
        cursor.execute("""
        SELECT symbol_id, symbol, assettype, name, officialsite, fiscalyearend, overview_id
        FROM overview 
        LIMIT 1;
        """)
        result = cursor.fetchone()
        
        if result:
            print(f"✅ View test successful: symbol_id={result[0]}, symbol={result[1]}, assettype={result[2]}")
            print(f"   officialsite={result[4]}, fiscalyearend={result[5]}, overview_id={result[6]}")
        
        # Test insert capability
        cursor.execute("""
        INSERT INTO overview (
            symbol_id, symbol, assettype, name, description, exchange, overview_id, status
        ) VALUES (
            999999999, 'TEST', 'Common Stock', 'Test Company', 'Test Description', 'NASDAQ', 999999999, 'active'
        ) ON CONFLICT (symbol_id) DO NOTHING;
        """)
        
        rows_affected = cursor.rowcount
        print(f"✅ Insert test completed: {rows_affected} row(s) affected")
        
        # Clean up test data
        cursor.execute("DELETE FROM source.company_overview WHERE calculated_symbol_id = 999999999;")
        
        conn.commit()
        print("✅ All changes committed successfully")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        if 'conn' in locals():
            conn.rollback()
        return False
    
    finally:
        if 'conn' in locals():
            conn.close()
    
    return True

if __name__ == "__main__":
    if fix_overview_view():
        print("\n🎉 Overview view is now fully updatable!")
    else:
        print("\n💥 Failed to fix overview view")
        sys.exit(1)
