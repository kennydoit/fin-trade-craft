"""
Migrate overview table from extracted to source schema as company_overview.
This ensures all company reference data is in the source schema.
"""

import sys
from pathlib import Path

# Add the parent directories to the path
sys.path.append(str(Path(__file__).parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager

def migrate_company_overview():
    """Migrate overview table from extracted to source schema as company_overview."""
    print("📦 Migrating overview table from extracted to source.company_overview...")
    
    with PostgresDatabaseManager() as db:
        # First, check current state
        extracted_count = db.fetch_query("SELECT COUNT(*) FROM extracted.overview")[0][0]
        print(f"📊 Current extracted.overview records: {extracted_count:,}")
        
        # Get table structure
        columns_query = """
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM information_schema.columns 
            WHERE table_schema = 'extracted' AND table_name = 'overview'
            ORDER BY ordinal_position
        """
        columns_info = db.fetch_query(columns_query)
        print(f"📋 Table has {len(columns_info)} columns")
        
        # Check if source.company_overview already exists
        source_exists = db.fetch_query("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'source' AND table_name = 'company_overview'
        """)[0][0]
        
        if source_exists:
            source_count = db.fetch_query("SELECT COUNT(*) FROM source.company_overview")[0][0]
            print(f"⚠️ source.company_overview already exists with {source_count:,} records")
            response = input("Do you want to replace it? (y/N): ")
            if response.lower() != 'y':
                print("❌ Migration cancelled")
                return
        
        try:
            # Step 1: Create source.company_overview with proper schema
            print("📋 Creating source.company_overview table...")
            create_sql = """
                -- Drop existing table if it exists
                DROP TABLE IF EXISTS source.company_overview CASCADE;
                
                -- Create company_overview table in source schema
                CREATE TABLE source.company_overview (
                    company_overview_id SERIAL PRIMARY KEY,
                    symbol_id BIGINT,  -- Will be populated via join with listing_status
                    symbol VARCHAR(20) NOT NULL,
                    asset_type VARCHAR(20),
                    name TEXT,
                    description TEXT,
                    cik VARCHAR(20),
                    exchange VARCHAR(50),
                    currency VARCHAR(10),
                    country VARCHAR(50),
                    sector VARCHAR(100),
                    industry VARCHAR(200),
                    address TEXT,
                    fiscal_year_end VARCHAR(10),
                    latest_quarter DATE,
                    market_capitalization BIGINT,
                    ebitda BIGINT,
                    pe_ratio DECIMAL(10,2),
                    peg_ratio DECIMAL(10,4),
                    book_value DECIMAL(15,4),
                    dividend_per_share DECIMAL(10,4),
                    dividend_yield DECIMAL(10,6),
                    eps DECIMAL(10,4),
                    revenue_per_share_ttm DECIMAL(15,4),
                    profit_margin DECIMAL(10,6),
                    operating_margin_ttm DECIMAL(10,6),
                    return_on_assets_ttm DECIMAL(10,6),
                    return_on_equity_ttm DECIMAL(10,6),
                    revenue_ttm BIGINT,
                    gross_profit_ttm BIGINT,
                    diluted_eps_ttm DECIMAL(10,4),
                    quarterly_earnings_growth_yoy DECIMAL(10,6),
                    quarterly_revenue_growth_yoy DECIMAL(10,6),
                    analyst_target_price DECIMAL(10,2),
                    trailing_pe DECIMAL(10,2),
                    forward_pe DECIMAL(10,2),
                    price_to_sales_ratio_ttm DECIMAL(10,4),
                    price_to_book_ratio DECIMAL(10,4),
                    ev_to_revenue DECIMAL(10,4),
                    ev_to_ebitda DECIMAL(10,4),
                    beta DECIMAL(10,6),
                    week_52_high DECIMAL(10,2),
                    week_52_low DECIMAL(10,2),
                    day_50_moving_average DECIMAL(10,2),
                    day_200_moving_average DECIMAL(10,2),
                    shares_outstanding BIGINT,
                    dividend_date DATE,
                    ex_dividend_date DATE,
                    
                    -- ETL metadata
                    content_hash VARCHAR(32) NOT NULL,
                    source_run_id UUID,
                    fetched_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    
                    -- Natural key constraint
                    UNIQUE(symbol),
                    
                    -- Foreign key to listing_status
                    FOREIGN KEY (symbol_id) REFERENCES source.listing_status(symbol_id) ON DELETE CASCADE
                );
                
                -- Create indexes
                CREATE INDEX IF NOT EXISTS idx_source_company_overview_symbol ON source.company_overview(symbol);
                CREATE INDEX IF NOT EXISTS idx_source_company_overview_symbol_id ON source.company_overview(symbol_id);
                CREATE INDEX IF NOT EXISTS idx_source_company_overview_sector ON source.company_overview(sector);
                CREATE INDEX IF NOT EXISTS idx_source_company_overview_industry ON source.company_overview(industry);
                CREATE INDEX IF NOT EXISTS idx_source_company_overview_exchange ON source.company_overview(exchange);
                
                COMMENT ON TABLE source.company_overview IS 'Company overview and fundamental data - migrated from extracted.overview';
            """
            
            db.execute_script(create_sql)
            print("✅ Created source.company_overview table")
            
            # Step 2: Copy data from extracted.overview (simpler structure)
            print("📤 Copying data from extracted.overview...")
            copy_sql = """
                INSERT INTO source.company_overview (
                    symbol_id, symbol, asset_type, name, description, cik, exchange, currency, country,
                    sector, industry, address, content_hash, source_run_id, fetched_at, created_at, updated_at
                )
                SELECT 
                    symbol_id,
                    symbol,
                    assettype,
                    name,
                    description,
                    cik,
                    exchange,
                    currency,
                    country,
                    sector,
                    industry,
                    address,
                    COALESCE(MD5(symbol || COALESCE(name, '') || COALESCE(description, '')), MD5(symbol)) as content_hash,
                    NULL::UUID as source_run_id,  -- No source_run_id in original
                    NOW() as fetched_at,           -- Use current timestamp
                    COALESCE(created_at, NOW()),
                    COALESCE(updated_at, NOW())
                FROM (
                    SELECT o.*,
                           ROW_NUMBER() OVER (PARTITION BY o.symbol ORDER BY o.updated_at DESC, o.overview_id DESC) as rn
                    FROM extracted.overview o
                    INNER JOIN source.listing_status ls ON o.symbol_id = ls.symbol_id
                ) ranked
                WHERE ranked.rn = 1
                ORDER BY symbol;
            """
            
            db.execute_query(copy_sql)
            
            # Verify copy
            new_count = db.fetch_query("SELECT COUNT(*) FROM source.company_overview")[0][0]
            print(f"✅ Copied {new_count:,} records to source.company_overview")
            
            if new_count < extracted_count:
                missing = extracted_count - new_count
                print(f"⚠️ {missing:,} records not migrated (missing symbol_id in listing_status)")
            
            # Step 3: Create a view in extracted schema for backward compatibility
            print("🔄 Creating backward compatibility view...")
            view_sql = """
                -- Rename original table to backup
                ALTER TABLE extracted.overview RENAME TO overview_backup;
                
                -- Create view in extracted schema for backward compatibility
                CREATE VIEW extracted.overview AS 
                SELECT 
                    company_overview_id as overview_id,
                    symbol,
                    asset_type as assettype,
                    name,
                    description,
                    cik,
                    exchange,
                    currency,
                    country,
                    sector,
                    industry,
                    address,
                    '' as officialsite,  -- Not stored in new schema
                    '' as fiscalyearend, -- Not stored in new schema  
                    'Active' as status,  -- Default value
                    created_at,
                    updated_at,
                    symbol_id
                FROM source.company_overview;
                
                COMMENT ON VIEW extracted.overview IS 'Backward compatibility view - data now lives in source.company_overview';
            """
            
            db.execute_script(view_sql)
            print("✅ Created backward compatibility view")
            
            print("\n🎯 Migration Summary:")
            print(f"  ✅ Migrated {new_count:,} records from extracted.overview to source.company_overview")
            print("  ✅ Created proper data types and foreign key constraints")
            print("  ✅ Created backward compatibility view")
            print("\n📝 Next steps:")
            print("  1. Update extractors to reference source.company_overview")
            print("  2. Test all functionality")
            print("  3. Consider dropping extracted.overview_backup table after verification")
            
        except Exception as e:
            print(f"❌ Migration failed: {e}")
            print("🔄 Rolling back changes...")
            
            # Rollback - drop the source table if it was created
            try:
                db.execute_query("DROP TABLE IF EXISTS source.company_overview CASCADE")
                print("✅ Rollback completed")
            except:
                print("⚠️ Rollback failed - manual cleanup may be needed")
            
            raise

if __name__ == "__main__":
    migrate_company_overview()
