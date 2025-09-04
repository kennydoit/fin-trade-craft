"""
Balance Sheet Data Migration Script
==================================

One-time migration of balance sheet data from extracted schema to source schema.
This script migrates the 2,489 symbols that have meaningful data in extracted.balance_sheet
but are missing from source.balance_sheet.

WARNING: This is a one-time migration script. Do not include this code in production extractors.
"""

import sys
from pathlib import Path
from datetime import datetime
import uuid

# Add the parent directories to the path
sys.path.append(str(Path(__file__).parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager
from utils.incremental_etl import ContentHasher, RunIdGenerator

class BalanceSheetMigrator:
    """One-time migration of balance sheet data from extracted to source schema."""
    
    def __init__(self):
        """Initialize the migrator."""
        self.table_name = "balance_sheet"
        self.migration_run_id = RunIdGenerator.generate()
        
    def analyze_migration_scope(self):
        """Analyze what needs to be migrated."""
        print("🔍 ANALYZING MIGRATION SCOPE...")
        print("=" * 60)
        
        db_manager = PostgresDatabaseManager()
        with db_manager as db:
            # Count symbols with meaningful data missing from source (that also exist in listing_status)
            query = """
                SELECT COUNT(DISTINCT eb.symbol_id) as missing_symbols
                FROM extracted.balance_sheet eb
                JOIN extracted.listing_status ls ON eb.symbol_id = ls.symbol_id
                WHERE eb.total_assets IS NOT NULL AND eb.total_assets > 0
                  AND eb.symbol_id NOT IN (
                      SELECT DISTINCT symbol_id 
                      FROM source.balance_sheet 
                      WHERE symbol_id IS NOT NULL
                  )
            """
            result = db.fetch_query(query)
            missing_symbols = result[0][0]
            
            # Count total records to migrate (only for symbols that exist in listing_status)
            query = """
                SELECT COUNT(*) as total_records
                FROM extracted.balance_sheet eb
                JOIN extracted.listing_status ls ON eb.symbol_id = ls.symbol_id
                WHERE eb.total_assets IS NOT NULL AND eb.total_assets > 0
                  AND eb.symbol_id NOT IN (
                      SELECT DISTINCT symbol_id 
                      FROM source.balance_sheet 
                      WHERE symbol_id IS NOT NULL
                  )
            """
            result = db.fetch_query(query)
            total_records = result[0][0]
            
            print(f"📊 MIGRATION SCOPE:")
            print(f"   • Symbols to migrate: {missing_symbols:,}")
            print(f"   • Records to migrate: {total_records:,}")
            print(f"   • Estimated time: ~2-3 minutes")
            print()
            
            # Show sample symbols (only those that exist in listing_status)
            query = """
                SELECT ls.symbol, COUNT(*) as record_count,
                       MIN(eb.fiscal_date_ending) as earliest_date,
                       MAX(eb.fiscal_date_ending) as latest_date
                FROM extracted.balance_sheet eb
                JOIN extracted.listing_status ls ON eb.symbol_id = ls.symbol_id
                WHERE eb.total_assets IS NOT NULL AND eb.total_assets > 0
                  AND eb.symbol_id NOT IN (
                      SELECT DISTINCT symbol_id 
                      FROM source.balance_sheet 
                      WHERE symbol_id IS NOT NULL
                  )
                GROUP BY ls.symbol, eb.symbol_id
                ORDER BY COUNT(*) DESC
                LIMIT 10
            """
            results = db.fetch_query(query)
            
            print(f"📋 SAMPLE SYMBOLS TO MIGRATE:")
            for row in results:
                print(f"   • {row[0]}: {row[1]} records ({row[2]} to {row[3]})")
            
            return missing_symbols, total_records
    
    def migrate_balance_sheet_data(self):
        """Perform the actual migration."""
        print("\n🚀 STARTING BALANCE SHEET MIGRATION...")
        print("=" * 60)
        
        db_manager = PostgresDatabaseManager()
        with db_manager as db:
            # Build the migration query
            migration_query = """
                INSERT INTO source.balance_sheet (
                    symbol_id, symbol, fiscal_date_ending, report_type,
                    total_assets, total_current_assets, cash_and_cash_equivalents_at_carrying_value,
                    cash_and_short_term_investments, inventory, current_net_receivables,
                    total_non_current_assets, property_plant_equipment, accumulated_depreciation_amortization_ppe,
                    intangible_assets, intangible_assets_excluding_goodwill, goodwill,
                    investments, long_term_investments, short_term_investments,
                    other_current_assets, other_non_current_assets,
                    total_liabilities, total_current_liabilities, current_accounts_payable,
                    deferred_revenue, current_debt, short_term_debt,
                    total_non_current_liabilities, capital_lease_obligations, long_term_debt,
                    current_long_term_debt, long_term_debt_noncurrent, short_long_term_debt_total,
                    other_current_liabilities, other_non_current_liabilities,
                    total_shareholder_equity, treasury_stock, retained_earnings,
                    common_stock, common_stock_shares_outstanding, reported_currency,
                    api_response_status, content_hash, source_run_id, fetched_at,
                    created_at, updated_at
                )
                SELECT 
                    eb.symbol_id, eb.symbol, eb.fiscal_date_ending, eb.report_type,
                    eb.total_assets, eb.total_current_assets, eb.cash_and_cash_equivalents_at_carrying_value,
                    eb.cash_and_short_term_investments, eb.inventory, eb.current_net_receivables,
                    eb.total_non_current_assets, eb.property_plant_equipment, eb.accumulated_depreciation_amortization_ppe,
                    eb.intangible_assets, eb.intangible_assets_excluding_goodwill, eb.goodwill,
                    eb.investments, eb.long_term_investments, eb.short_term_investments,
                    eb.other_current_assets, eb.other_non_current_assets,
                    eb.total_liabilities, eb.total_current_liabilities, eb.current_accounts_payable,
                    eb.deferred_revenue, eb.current_debt, eb.short_term_debt,
                    eb.total_non_current_liabilities, eb.capital_lease_obligations, eb.long_term_debt,
                    eb.current_long_term_debt, eb.long_term_debt_noncurrent, eb.short_long_term_debt_total,
                    eb.other_current_liabilities, eb.other_non_current_liabilities,
                    eb.total_shareholder_equity, eb.treasury_stock, eb.retained_earnings,
                    eb.common_stock, eb.common_stock_shares_outstanding, eb.reported_currency,
                    'migrated' as api_response_status,
                    %s as content_hash,
                    %s as source_run_id,
                    NOW() as fetched_at,
                    NOW() as created_at,
                    NOW() as updated_at
                FROM extracted.balance_sheet eb
                JOIN extracted.listing_status ls ON eb.symbol_id = ls.symbol_id
                WHERE eb.total_assets IS NOT NULL AND eb.total_assets > 0
                  AND eb.symbol_id NOT IN (
                      SELECT DISTINCT symbol_id 
                      FROM source.balance_sheet 
                      WHERE symbol_id IS NOT NULL
                  )
                ON CONFLICT (symbol_id, fiscal_date_ending, report_type) 
                DO UPDATE SET 
                    total_assets = EXCLUDED.total_assets,
                    updated_at = NOW()
            """
            
            # Generate a migration content hash
            migration_hash = ContentHasher.calculate_api_response_hash({"migration": self.migration_run_id})
            
            print("⏳ Executing migration query...")
            start_time = datetime.now()
            
            # Execute the migration
            rows_affected = db.execute_query(migration_query, (migration_hash, self.migration_run_id))
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            print(f"✅ MIGRATION COMPLETED!")
            print(f"   • Records migrated: {rows_affected:,}")
            print(f"   • Duration: {duration:.1f} seconds")
            print(f"   • Migration run ID: {self.migration_run_id}")
            
            return rows_affected
    
    def update_watermarks(self):
        """Update watermarks for migrated symbols."""
        print("\n🔄 UPDATING WATERMARKS...")
        print("=" * 30)
        
        db_manager = PostgresDatabaseManager()
        with db_manager as db:
            # Update watermarks for migrated symbols
            watermark_query = """
                INSERT INTO source.extraction_watermarks 
                (table_name, symbol_id, last_fiscal_date, last_successful_run, 
                 consecutive_failures, created_at, updated_at)
                SELECT 
                    %s as table_name,
                    sb.symbol_id,
                    MAX(sb.fiscal_date_ending) as last_fiscal_date,
                    NOW() as last_successful_run,
                    0 as consecutive_failures,
                    NOW() as created_at,
                    NOW() as updated_at
                FROM source.balance_sheet sb
                WHERE sb.source_run_id = %s
                GROUP BY sb.symbol_id
                ON CONFLICT (table_name, symbol_id) 
                DO UPDATE SET 
                    last_fiscal_date = GREATEST(
                        source.extraction_watermarks.last_fiscal_date, 
                        EXCLUDED.last_fiscal_date
                    ),
                    last_successful_run = NOW(),
                    consecutive_failures = 0,
                    updated_at = NOW()
            """
            
            rows_affected = db.execute_query(watermark_query, (self.table_name, self.migration_run_id))
            
            print(f"✅ Watermarks updated: {rows_affected:,} symbols")
            
            return rows_affected
    
    def verify_migration(self):
        """Verify the migration was successful."""
        print("\n🔍 VERIFYING MIGRATION...")
        print("=" * 30)
        
        db_manager = PostgresDatabaseManager()
        with db_manager as db:
            # Check total symbols now in source
            result = db.fetch_query("SELECT COUNT(DISTINCT symbol_id) FROM source.balance_sheet")
            total_symbols = result[0][0]
            
            # Check migrated records
            result = db.fetch_query("SELECT COUNT(*) FROM source.balance_sheet WHERE source_run_id = %s", (self.migration_run_id,))
            migrated_records = result[0][0]
            
            # Check if we now have the expected ~5,969 symbols
            result = db.fetch_query("SELECT COUNT(DISTINCT symbol_id) FROM extracted.balance_sheet WHERE total_assets > 0")
            expected_symbols = result[0][0]
            
            print(f"📊 MIGRATION VERIFICATION:")
            print(f"   • Total symbols in source: {total_symbols:,}")
            print(f"   • Records from this migration: {migrated_records:,}")
            print(f"   • Expected total symbols: {expected_symbols:,}")
            print(f"   • Migration success: {total_symbols >= expected_symbols}")
            
            if total_symbols >= expected_symbols:
                print("\n🎉 MIGRATION SUCCESSFUL!")
                print("   All meaningful balance sheet data has been migrated to source schema.")
            else:
                print(f"\n⚠️  MIGRATION INCOMPLETE:")
                print(f"   Missing: {expected_symbols - total_symbols:,} symbols")

def main():
    """Main migration execution."""
    print("🚀 BALANCE SHEET DATA MIGRATION")
    print("=" * 60)
    print("This script migrates balance sheet data from extracted to source schema.")
    print("This is a ONE-TIME operation for data consolidation.")
    print()
    
    try:
        migrator = BalanceSheetMigrator()
        
        # Step 1: Analyze scope
        missing_symbols, total_records = migrator.analyze_migration_scope()
        
        if missing_symbols == 0:
            print("✅ No migration needed - all data already in source schema!")
            return
        
        # Step 2: Confirm migration
        print(f"⚠️  About to migrate {missing_symbols:,} symbols ({total_records:,} records)")
        confirm = input("🤔 Continue with migration? (yes/no): ").lower().strip()
        
        if confirm not in ['yes', 'y']:
            print("❌ Migration cancelled by user.")
            return
        
        # Step 3: Perform migration
        rows_migrated = migrator.migrate_balance_sheet_data()
        
        # Step 4: Update watermarks
        watermarks_updated = migrator.update_watermarks()
        
        # Step 5: Verify migration
        migrator.verify_migration()
        
        print("\n🎯 MIGRATION COMPLETE!")
        print("   Future balance sheet extractions will now focus on new/updated data only.")
        print("   The extracted schema data has been successfully consolidated into source schema.")
        
    except Exception as e:
        print(f"❌ MIGRATION FAILED: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
