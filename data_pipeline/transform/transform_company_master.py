#!/usr/bin/env python3
"""
Transform Company Master Table
Creates the first table in the transformed schema by combining overview and listing_status
with data availability counts from other tables.

Follows the specifications in prompts/transform_company_master.md

```
Edits Needed:
remove company_master from the database and rebuild it with the following specification:

[from extracted.listing_status]
- symbol_id
- symbol
- name
- exchange
- asset_type
- status
- ipo_date
- delisting_date
[from extracted.overview]
- overview_id
- description
- currency
- country
- sector
- industry
- address
- officialsite
- fiscalyearend
- status_overview (append _overview to overview.status)
[from create_table_flag aggregations]
- cash_flow_count
- income_statement_count
- balance_sheet_count
- insider_transactions_count
- earnings_call_transcript_count
- time_series_daily_adjusted_count
"""

import sys
import traceback
from datetime import datetime
from pathlib import Path

import pandas as pd

# Add the parent directories to the path so we can import from db
sys.path.append(str(Path(__file__).parent.parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager


class CompanyMasterTransformer:
    """Transform overview and listing_status into company_master table"""

    def __init__(self):
        self.db_manager = PostgresDatabaseManager()

    def step1_join_master_tables(self, db):
        """
        Step 1: Join overview and listing_status tables with specific column selection
        Args:
            db: Database connection instance
        Returns: DataFrame with combined overview and listing_status data
        """
        print("📊 Step 1: Joining overview and listing_status tables...")

        # Get specific columns from overview data
        # Required columns: overview_id, description, currency, country, sector, industry, address, officialsite, fiscalyearend, status_overview
        overview_query = """
            SELECT
                overview_id,
                symbol,
                exchange,
                description,
                currency,
                country,
                sector,
                industry,
                address,
                officialsite,
                fiscalyearend,
                status as status_overview
            FROM extracted.overview
            WHERE symbol IS NOT NULL
            ORDER BY symbol
        """
        overview_data = db.fetch_query(overview_query)
        overview_columns = ['overview_id', 'symbol', 'exchange', 'description', 'currency', 'country',
                           'sector', 'industry', 'address', 'officialsite', 'fiscalyearend', 'status_overview']

        # Get specific columns from listing_status data
        # Required columns: symbol_id, symbol, name, exchange, asset_type, status, ipo_date, delisting_date
        listing_query = """
            SELECT
                symbol_id,
                symbol,
                name,
                exchange,
                asset_type,
                status,
                ipo_date,
                delisting_date
            FROM extracted.listing_status
            WHERE symbol IS NOT NULL
            ORDER BY symbol
        """
        listing_data = db.fetch_query(listing_query)
        listing_columns = ['symbol_id', 'symbol', 'name', 'exchange', 'asset_type', 'status', 'ipo_date', 'delisting_date']

        # Create DataFrames
        overview_df = pd.DataFrame(overview_data, columns=overview_columns)
        listing_df = pd.DataFrame(listing_data, columns=listing_columns)

        print(f"  📈 Overview data: {len(overview_df)} rows")
        print(f"  📋 Listing status data: {len(listing_df)} rows")
        print(f"  🔗 Overview columns: {overview_df.columns.tolist()}")
        print(f"  🔗 Listing columns: {listing_df.columns.tolist()}")

        # Join on symbol and exchange
        # Use outer join to include all companies from both tables
        overview_listing_status = pd.merge(
            listing_df,  # Start with listing_status as base
            overview_df,
            on=['symbol', 'exchange'],
            how='outer',
            suffixes=('', '_dup')  # Avoid duplicating column names
        )

        print("  ✅ Joined on symbol and exchange")
        print(f"  📊 Combined data: {len(overview_listing_status)} rows")

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
            'earnings_call_transcripts': 'earnings_call_transcript_count',  # Note: table is plural, column is singular
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

        # Define explicit column type mappings for the new specific columns
        column_type_map = {
            # ID columns from listing_status and overview
            'symbol_id': 'INTEGER',
            'overview_id': 'INTEGER',

            # String/VARCHAR columns from listing_status
            'symbol': 'VARCHAR(20) NOT NULL',
            'name': 'VARCHAR(255)',
            'exchange': 'VARCHAR(50)',
            'asset_type': 'VARCHAR(50)',
            'status': 'VARCHAR(50)',

            # Date columns from listing_status
            'ipo_date': 'DATE',
            'delisting_date': 'DATE',

            # String/VARCHAR columns from overview
            'currency': 'VARCHAR(10)',
            'country': 'VARCHAR(100)',
            'sector': 'VARCHAR(100)',
            'industry': 'VARCHAR(100)',
            'officialsite': 'VARCHAR(255)',
            'fiscalyearend': 'VARCHAR(20)',
            'status_overview': 'VARCHAR(50)',

            # TEXT columns for long content from overview
            'description': 'TEXT',
            'address': 'TEXT',

            # Count columns
            'cash_flow_count': 'INTEGER DEFAULT 0',
            'income_statement_count': 'INTEGER DEFAULT 0',
            'balance_sheet_count': 'INTEGER DEFAULT 0',
            'insider_transactions_count': 'INTEGER DEFAULT 0',
            'earnings_call_transcript_count': 'INTEGER DEFAULT 0',
            'time_series_daily_adjusted_count': 'INTEGER DEFAULT 0',

            # Timestamp columns
            'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
            'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
        }

        # Process each column from DataFrame
        for col in company_master_df.columns:
            if col in column_type_map:
                column_definitions.append(f"{col} {column_type_map[col]}")
            elif 'count' in col.lower():
                # Handle any additional count columns
                column_definitions.append(f"{col} INTEGER DEFAULT 0")
            else:
                # Default fallback for any unmapped columns
                print(f"  ⚠️  Unmapped column '{col}' using VARCHAR(255)")
                column_definitions.append(f"{col} VARCHAR(255)")

        # Add unique constraint on symbol
        column_definitions.append("UNIQUE(symbol)")

        create_table_query = f"""
            CREATE TABLE transformed.company_master (
                {','.join(column_definitions)}
            );
        """

        db.execute_query(create_table_query)
        print("  ✅ Created transformed.company_master table with new column specification")

        # Create indexes for better performance
        indexes = [
            "CREATE INDEX idx_company_master_symbol ON transformed.company_master(symbol);",
            "CREATE INDEX idx_company_master_exchange ON transformed.company_master(exchange);",
            "CREATE INDEX idx_company_master_sector ON transformed.company_master(sector);",
            "CREATE INDEX idx_company_master_industry ON transformed.company_master(industry);"
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

        # Recreate active_stocks view with new column structure
        active_stocks_sql = """
            CREATE VIEW transformed.active_stocks AS
            SELECT 
                company_master_id,
                symbol_id,
                symbol,
                name,
                exchange,
                asset_type,
                status,
                ipo_date,
                delisting_date,
                overview_id,
                description,
                currency,
                country,
                sector,
                industry,
                address,
                officialsite,
                fiscalyearend,
                status_overview,
                -- Data availability counts
                cash_flow_count,
                income_statement_count,
                balance_sheet_count,
                insider_transactions_count,
                earnings_call_transcript_count,
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
            print("✅ Created transformed.company_master table")
            print(f"✅ Processed {inserted_count} company records")
            print("✅ Combined specific columns from overview + listing_status + 6 data availability counts")
            print("\nTable: fin_trade_craft.transformed.company_master")
            print("Schema: Selected columns from listing_status + overview + 6 count indicators")
            print("Columns from listing_status: symbol_id, symbol, name, exchange, asset_type, status, ipo_date, delisting_date")
            print("Columns from overview: overview_id, description, currency, country, sector, industry, address, officialsite, fiscalyearend, status_overview")
            print("Count columns: cash_flow_count, income_statement_count, balance_sheet_count, insider_transactions_count, earnings_call_transcript_count, time_series_daily_adjusted_count")
            print("=" * 60)

            return True

        except Exception as e:
            print(f"\n❌ Transformation failed: {str(e)}")
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
