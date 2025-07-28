#!/usr/bin/env python3
"""
Transform Company Master Table
Creates the first table in the transformed schema by combining overview and listing_status
with data availability counts from other tables.

Follows the specifications in prompts/transform_company_master.md
"""
import os
import sys
from pathlib import Path
import pandas as pd
from datetime import datetime

# Add the parent directories to the path so we can import from db
sys.path.append(str(Path(__file__).parent.parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager

class CompanyMasterTransformer:
    """Transform overview and listing_status into company_master table"""
    
    def __init__(self):
        self.db_manager = PostgresDatabaseManager()
    
    def step1_join_master_tables(self, db):
        """
        Step 1: Join overview and listing_status tables
        Args:
            db: Database connection instance
        Returns: DataFrame with combined overview and listing_status data
        """
        print("📊 Step 1: Joining overview and listing_status tables...")
        
        # Get overview data
        overview_query = """
            SELECT * FROM extracted.overview
            WHERE symbol IS NOT NULL
            ORDER BY symbol
        """
        overview_data = db.fetch_query(overview_query)
        
        # Get column names for overview
        overview_columns_query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'extracted' AND table_name = 'overview'
            ORDER BY ordinal_position
        """
        overview_columns = [row[0] for row in db.fetch_query(overview_columns_query)]
        
        # Get listing_status data
        listing_query = """
            SELECT * FROM extracted.listing_status
            WHERE symbol IS NOT NULL
            ORDER BY symbol
        """
        listing_data = db.fetch_query(listing_query)
        
        # Get column names for listing_status
        listing_columns_query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'extracted' AND table_name = 'listing_status'
            ORDER BY ordinal_position
        """
        listing_columns = [row[0] for row in db.fetch_query(listing_columns_query)]
        
        # Create DataFrames
        overview_df = pd.DataFrame(overview_data, columns=overview_columns)
        listing_df = pd.DataFrame(listing_data, columns=listing_columns)
        
        print(f"  📈 Overview data: {len(overview_df)} rows")
        print(f"  📋 Listing status data: {len(listing_df)} rows")
        
        # Join on symbol and exchange, handling name column conflicts
        # First, let's see what columns we have for joining
        print(f"  🔗 Overview columns: {overview_df.columns.tolist()}")
        print(f"  🔗 Listing columns: {listing_df.columns.tolist()}")
        
        # Handle the name column conflict - prefer overview name, use listing name as fallback
        # Also handle status column conflict - prefer listing status as it's more recent
        # Rename columns to avoid conflicts during merge
        overview_df_renamed = overview_df.rename(columns={'name': 'name_overview', 'status': 'status_overview'})
        listing_df_renamed = listing_df.rename(columns={'name': 'name_listing', 'status': 'status_listing'})
        
        # Join on symbol and exchange if both tables have exchange
        if 'exchange' in overview_df.columns and 'exchange' in listing_df.columns:
            overview_listing_status = pd.merge(
                overview_df_renamed, 
                listing_df_renamed, 
                on=['symbol', 'exchange'], 
                how='outer',
                suffixes=('_overview', '_listing')
            )
            print(f"  ✅ Joined on symbol and exchange")
        else:
            overview_listing_status = pd.merge(
                overview_df_renamed, 
                listing_df_renamed, 
                on='symbol', 
                how='outer',
                suffixes=('_overview', '_listing')
            )
            print(f"  ✅ Joined on symbol only")
        
        # Create a single 'name' column using overview first, listing as fallback
        overview_listing_status['name'] = overview_listing_status['name_overview'].fillna(
            overview_listing_status['name_listing']
        )
        
        # Create a single 'status' column using listing first (more current), overview as fallback
        overview_listing_status['status'] = overview_listing_status['status_listing'].fillna(
            overview_listing_status['status_overview']
        )
        
        # Drop the separate name and status columns
        overview_listing_status = overview_listing_status.drop(
            columns=['name_overview', 'name_listing', 'status_overview', 'status_listing'], 
            errors='ignore'
        )
        
        print(f"  📊 Combined data: {len(overview_listing_status)} rows")
        print(f"  ✅ Resolved name and status column conflicts")
        
        return overview_listing_status
    
    def step2_create_table_flags(self, db):
        """
        Step 2: Create data availability counts for each table
        Args:
            db: Database connection instance
        Returns: Dictionary of DataFrames with counts per symbol
        """
        print("\n📊 Step 2: Creating table availability flags...")
        
        tables_to_count = {
            'cash_flow': 'cash_flow_count',
            'income_statement': 'income_statement_count', 
            'insider_transactions': 'insider_transactions_count',
            'balance_sheet': 'balance_sheet_count',
            'earnings_call_transcripts': 'earnings_call_transcripts_count',
            'time_series_daily_adjusted': 'time_series_daily_adjusted_count'
        }
        
        count_dataframes = {}
        
        for table_name, count_column in tables_to_count.items():
            print(f"  📊 Counting records in {table_name}...")
            
            count_query = f"""
                SELECT symbol, COUNT(*) as record_count
                FROM extracted.{table_name}
                WHERE symbol IS NOT NULL
                GROUP BY symbol
                ORDER BY symbol
            """
            
            try:
                count_data = db.fetch_query(count_query)
                count_df = pd.DataFrame(count_data, columns=['symbol', count_column])
                count_dataframes[table_name] = count_df
                
                print(f"    ✅ {table_name}: {len(count_df)} symbols with data")
                
            except Exception as e:
                print(f"    ❌ Error counting {table_name}: {str(e)}")
                # Create empty DataFrame as fallback
                count_dataframes[table_name] = pd.DataFrame(columns=['symbol', count_column])
        
        return count_dataframes
    
    def step3_create_output_table(self, overview_listing_status, count_dataframes):
        """
        Step 3: Join all count DataFrames to the master table and create final output
        """
        print("\n📊 Step 3: Creating final company_master table...")
        
        # Start with the overview_listing_status DataFrame
        company_master = overview_listing_status.copy()
        
        # Join each count DataFrame
        for table_name, count_df in count_dataframes.items():
            if not count_df.empty:
                company_master = pd.merge(
                    company_master,
                    count_df,
                    on='symbol',
                    how='left'
                )
                
                count_column = count_df.columns[1]  # Second column is the count
                # Fill NaN values with 0 for counts
                company_master[count_column] = company_master[count_column].fillna(0).astype(int)
                
                print(f"  ✅ Joined {count_column}")
        
        # Add metadata columns
        company_master['created_at'] = datetime.now()
        company_master['updated_at'] = datetime.now()
        
        print(f"  📊 Final company_master table: {len(company_master)} rows, {len(company_master.columns)} columns")
        
        return company_master
    
    def create_company_master_table_schema(self, db, company_master_df):
        """Create the company_master table in the transformed schema using actual DataFrame columns"""
        print("\n🏗️  Creating company_master table schema...")
        
        # Drop dependent views first, then table (for clean recreation)
        try:
            db.execute_query("DROP VIEW IF EXISTS transformed.analysis_ready_stocks CASCADE;")
            print("  ✅ Dropped analysis_ready_stocks view")
        except Exception as e:
            print(f"  ⚠️  Note: {str(e)[:50]}...")
        
        try:
            db.execute_query("DROP VIEW IF EXISTS transformed.active_stocks CASCADE;")
            print("  ✅ Dropped active_stocks view")
        except Exception as e:
            print(f"  ⚠️  Note: {str(e)[:50]}...")
        
        # Now drop the table
        db.execute_query("DROP TABLE IF EXISTS transformed.company_master CASCADE;")
        
        # Create table based on actual DataFrame columns
        print(f"  📊 Creating table with {len(company_master_df.columns)} columns from DataFrame")
        
        # Build CREATE TABLE statement dynamically with explicit column type mapping
        column_definitions = []
        
        # Add primary key
        column_definitions.append("company_master_id SERIAL PRIMARY KEY")
        
        # Define explicit column type mappings to avoid data type issues
        column_type_map = {
            # ID columns
            'overview_id': 'INTEGER',
            'symbol_id': 'INTEGER', 
            
            # String/VARCHAR columns  
            'symbol': 'VARCHAR(20) NOT NULL',
            'name': 'VARCHAR(255)',
            'assettype': 'VARCHAR(50)',
            'asset_type': 'VARCHAR(50)',
            'exchange': 'VARCHAR(50)',
            'currency': 'VARCHAR(10)',
            'country': 'VARCHAR(100)',
            'sector': 'VARCHAR(100)',
            'industry': 'VARCHAR(100)',
            'status': 'VARCHAR(50)',
            'status_overview': 'VARCHAR(50)',
            'status_listing': 'VARCHAR(50)',
            'cik': 'VARCHAR(20)',
            'officialsite': 'VARCHAR(255)',
            'fiscalyearend': 'VARCHAR(20)',
            
            # TEXT columns for long content
            'description': 'TEXT',
            'address': 'TEXT',
            
            # Date columns
            'ipo_date': 'DATE',
            'delisting_date': 'DATE',
            
            # Timestamp columns
            'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
            'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
        }
        
        # Process each column from DataFrame
        for col in company_master_df.columns:
            if col in column_type_map:
                column_definitions.append(f"{col} {column_type_map[col]}")
            elif 'count' in col.lower():
                # Handle count columns (from our data availability counts)
                column_definitions.append(f"{col} INTEGER DEFAULT 0")
            else:
                # Default fallback for any unmapped columns
                column_definitions.append(f"{col} VARCHAR(255)")
        
        # Add unique constraint on symbol
        column_definitions.append("UNIQUE(symbol)")
        
        create_table_query = f"""
            CREATE TABLE transformed.company_master (
                {','.join(column_definitions)}
            );
        """
        
        db.execute_query(create_table_query)
        print("  ✅ Created transformed.company_master table with dynamic schema")
        
        # Create indexes for better performance
        indexes = [
            "CREATE INDEX idx_company_master_symbol ON transformed.company_master(symbol);",
            "CREATE INDEX idx_company_master_exchange ON transformed.company_master(exchange);",
            "CREATE INDEX idx_company_master_sector ON transformed.company_master(sector);",
            "CREATE INDEX idx_company_master_industry ON transformed.company_master(industry);",
            "CREATE INDEX idx_company_master_market_cap ON transformed.company_master(market_capitalization);"
        ]
        
        for index_sql in indexes:
            try:
                db.execute_query(index_sql)
            except Exception as e:
                print(f"    ⚠️  Index creation note: {str(e)[:50]}...")
        
        print("  ✅ Created indexes for company_master table")
    
    def recreate_dependent_views(self, db):
        """Recreate the views that depend on company_master table"""
        print("\n🔄 Recreating dependent views...")
        
        # Recreate active_stocks view
        active_stocks_sql = """
            CREATE VIEW transformed.active_stocks AS
            SELECT 
                company_master_id,
                symbol,
                name,
                description,
                cik,
                exchange,
                currency,
                country,
                sector,
                industry,
                address,
                ipo_date,
                status,
                -- Data availability counts
                cash_flow_count,
                income_statement_count,
                insider_transactions_count,
                balance_sheet_count,
                earnings_call_transcripts_count,
                time_series_daily_adjusted_count,
                -- Metadata
                created_at,
                updated_at
            FROM transformed.company_master
            WHERE 
                -- Focus on stocks only (exclude ETFs, REITs, etc.)
                (asset_type = 'Common Stock' OR asset_type = 'Stock' OR asset_type IS NULL)
                -- Focus on major US exchanges
                AND exchange IN ('NYSE', 'NASDAQ', 'NASDAQ Global Select', 'NASDAQ Global Market', 'NASDAQ Capital Market')
                -- Exclude delisted companies
                AND (status IS NULL OR status != 'Delisted')
                -- Must have a valid symbol
                AND symbol IS NOT NULL
                AND symbol != ''
            ORDER BY symbol;
        """
        
        try:
            db.execute_query(active_stocks_sql)
            print("  ✅ Recreated transformed.active_stocks view")
        except Exception as e:
            print(f"  ⚠️  Error recreating active_stocks view: {str(e)[:50]}...")
        
        # Recreate analysis_ready_stocks view
        analysis_ready_sql = """
            CREATE VIEW transformed.analysis_ready_stocks AS
            SELECT *
            FROM transformed.active_stocks
            WHERE 
                -- Must have financial data
                cash_flow_count > 0 
                AND income_statement_count > 0
                AND balance_sheet_count > 0
                -- Must have price data
                AND time_series_daily_adjusted_count > 0
                -- Must have basic company info
                AND name IS NOT NULL
                AND sector IS NOT NULL
            ORDER BY symbol;
        """
        
        try:
            db.execute_query(analysis_ready_sql)
            print("  ✅ Recreated transformed.analysis_ready_stocks view")
        except Exception as e:
            print(f"  ⚠️  Error recreating analysis_ready_stocks view: {str(e)[:50]}...")
    
    def insert_company_master_data(self, company_master_df, db):
        """Insert the company master data into the database"""
        print("\n💾 Inserting company_master data...")
        
        # Use all columns from DataFrame (excluding auto-generated columns like SERIAL)
        insert_columns = [col for col in company_master_df.columns 
                         if col not in ['company_master_id']]  # Skip auto-generated columns
        
        print(f"  📊 Inserting {len(insert_columns)} columns: {insert_columns[:5]}...")
        
        # Create insert query with actual DataFrame columns
        placeholders = ', '.join(['%s'] * len(insert_columns))
        columns_str = ', '.join(insert_columns)
        
        insert_query = f"""
            INSERT INTO transformed.company_master ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (symbol) DO UPDATE SET
                updated_at = EXCLUDED.updated_at
        """
        
        # Prepare data for batch insert
        data_to_insert = []
        for _, row in company_master_df.iterrows():
            # Get values in the same order as insert_columns
            row_values = [row[col] for col in insert_columns]
            # Convert None values to NULL and handle data types
            processed_values = []
            for val in row_values:
                if pd.isna(val) or val is None:
                    processed_values.append(None)
                else:
                    processed_values.append(val)
            data_to_insert.append(processed_values)
        
        try:
            # Execute batch insert using execute_many
            row_count = db.execute_many(insert_query, data_to_insert)
            print(f"  ✅ Successfully inserted {len(data_to_insert)} company records")
            return len(data_to_insert)
            
        except Exception as e:
            print(f"  ❌ Error inserting data: {str(e)}")
            # Debug: check first few rows
            print(f"  🔍 First row data: {data_to_insert[0][:5] if data_to_insert else 'No data'}")
            print(f"  🔍 Columns: {insert_columns[:5]}")
            return 0
    
    def run_transformation(self):
        """Run the complete company_master transformation"""
        print("🚀 Starting Company Master Transformation")
        print("=" * 60)
        
        try:
            # Use a single database connection for the entire transformation
            with self.db_manager as db:
                # Step 1: Join master tables
                overview_listing_status = self.step1_join_master_tables(db)
                
                # Step 2: Create table flags
                count_dataframes = self.step2_create_table_flags(db)
                
                # Step 3: Create output table
                company_master_df = self.step3_create_output_table(overview_listing_status, count_dataframes)
                
                # Create table schema
                self.create_company_master_table_schema(db, company_master_df)
                
                # Insert data
                inserted_count = self.insert_company_master_data(company_master_df, db)
                
                # Recreate dependent views
                self.recreate_dependent_views(db)
            
            print("\n" + "=" * 60)
            print("🎉 Company Master Transformation Completed Successfully!")
            print(f"✅ Created transformed.company_master table")
            print(f"✅ Processed {inserted_count} company records")
            print(f"✅ Combined overview + listing_status + 6 data availability counts")
            print("\nTable: fin_trade_craft.transformed.company_master")
            print("Schema: All overview columns + all listing columns + 6 count indicators")
            print("=" * 60)
            
            return True
            
        except Exception as e:
            print(f"\n❌ Transformation failed: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

def main():
    """Main function to run the transformation"""
    transformer = CompanyMasterTransformer()
    
    print("This will create the first table in the transformed schema:")
    print("- Combine extracted.overview and extracted.listing_status")
    print("- Add data availability counts from 6 other tables")
    print("- Create transformed.company_master table")
    
    response = input("\nProceed with company_master transformation? (y/N): ").strip().lower()
    
    if response == 'y':
        success = transformer.run_transformation()
        if success:
            print("\n✅ Company master table ready for analytics!")
        else:
            print("\n❌ Transformation failed. Please check errors above.")
    else:
        print("Transformation cancelled.")

if __name__ == "__main__":
    main()
