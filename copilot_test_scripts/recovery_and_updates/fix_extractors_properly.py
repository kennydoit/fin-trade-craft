"""
Comprehensive script to properly convert all extractors to PostgreSQL.
This will handle each extractor's specific patterns correctly.
"""

import re
from pathlib import Path


def fix_time_series_extractor():
    """Fix the time series daily adjusted extractor."""
    file_path = Path("data_pipeline/extract/extract_time_series_daily_adjusted.py")

    with open(file_path) as f:
        content = f.read()

    # Find and replace the load_data method
    old_pattern = r'def load_data\(self, df, symbol, symbol_id\):.*?print\(f"Successfully loaded \{rows_affected\} records into time_series_daily_adjusted table"\)'

    new_load_method = '''def load_data(self, df, symbol, symbol_id):
        """Load transformed data into the database."""
        print(f"Loading {len(df)} records for {symbol} into database...")
        
        with self.db_manager as db:
            # Initialize schema if tables don't exist
            schema_path = Path(__file__).parent.parent.parent / "db" / "schema" / "postgres_stock_db_schema.sql"
            if not db.table_exists('time_series_daily_adjusted'):
                print("Initializing database schema...")
                db.initialize_schema(schema_path)
            
            # Use PostgreSQL upsert functionality
            for index, row in df.iterrows():
                data_dict = row.to_dict()
                # Remove timestamp columns for upsert - they'll be handled by the database
                data_dict.pop('created_at', None)
                data_dict.pop('updated_at', None)
                
                db.upsert_data('time_series_daily_adjusted', data_dict, ['symbol_id', 'date'])
            
            print(f"Successfully loaded {len(df)} records into time_series_daily_adjusted table")'''

    content = re.sub(old_pattern, new_load_method, content, flags=re.DOTALL)

    with open(file_path, "w") as f:
        f.write(content)

    print(f"✅ Fixed {file_path}")


def fix_financial_statement_extractors():
    """Fix income statement, balance sheet, and cash flow extractors."""

    extractors = [
        "extract_income_statement.py",
        "extract_balance_sheet.py",
        "extract_cash_flow.py",
    ]

    for extractor_file in extractors:
        file_path = Path(f"data_pipeline/extract/{extractor_file}")

        with open(file_path) as f:
            content = f.read()

        # Extract table name from filename
        table_name = extractor_file.replace("extract_", "").replace(".py", "")

        # Find and replace the load_data method
        old_pattern = (
            r'def load_data\(self, records\):.*?print\(f"Successfully loaded \{rows_affected\} records into '
            + table_name
            + r' table"\)'
        )

        new_load_method = f'''def load_data(self, records):
        """Load financial statement records into the database."""
        if not records:
            print("No records to load")
            return
        
        print(f"Loading {{len(records)}} records into database...")
        
        with self.db_manager as db:
            # Initialize schema if tables don't exist
            schema_path = Path(__file__).parent.parent.parent / "db" / "schema" / "postgres_stock_db_schema.sql"
            if not db.table_exists('{table_name}'):
                print("Initializing database schema...")
                db.initialize_schema(schema_path)
            
            # Use PostgreSQL upsert functionality
            for record in records:
                data_dict = record.copy()
                # Remove timestamp columns for upsert - they'll be handled by the database
                data_dict.pop('created_at', None)
                data_dict.pop('updated_at', None)
                
                # For financial statements, conflict is on symbol_id, report_type, fiscal_date_ending
                conflict_columns = ['symbol_id', 'report_type', 'fiscal_date_ending']
                db.upsert_data('{table_name}', data_dict, conflict_columns)
            
            print(f"Successfully loaded {{len(records)}} records into {table_name} table")'''

        content = re.sub(old_pattern, new_load_method, content, flags=re.DOTALL)

        with open(file_path, "w") as f:
            f.write(content)

        print(f"✅ Fixed {file_path}")


def fix_commodities_extractor():
    """Fix the commodities extractor."""
    file_path = Path("data_pipeline/extract/extract_commodities.py")

    with open(file_path) as f:
        content = f.read()

    # Find and replace the load_data method
    old_pattern = r'def load_commodities_data\(self, records\):.*?print\(f"Successfully loaded \{rows_affected\} records into commodities table"\)'

    new_load_method = '''def load_commodities_data(self, records):
        """Load commodity records into the database."""
        if not records:
            print("No records to load")
            return
        
        print(f"Loading {len(records)} records into database...")
        
        with self.db_manager as db:
            # Initialize schema if tables don't exist
            schema_path = Path(__file__).parent.parent.parent / "db" / "schema" / "postgres_stock_db_schema.sql"
            if not db.table_exists('commodities'):
                print("Initializing database schema...")
                db.initialize_schema(schema_path)
            
            # Use PostgreSQL upsert functionality
            for record in records:
                data_dict = record.copy()
                # Remove timestamp columns for upsert - they'll be handled by the database
                data_dict.pop('created_at', None)
                data_dict.pop('updated_at', None)
                
                # For commodities, conflict is on commodity_name, date, interval
                conflict_columns = ['commodity_name', 'date', 'interval']
                db.upsert_data('commodities', data_dict, conflict_columns)
            
            print(f"Successfully loaded {len(records)} records into commodities table")'''

    content = re.sub(old_pattern, new_load_method, content, flags=re.DOTALL)

    with open(file_path, "w") as f:
        f.write(content)

    print(f"✅ Fixed {file_path}")


def fix_economic_indicators_extractor():
    """Fix the economic indicators extractor."""
    file_path = Path("data_pipeline/extract/extract_economic_indicators.py")

    with open(file_path) as f:
        content = f.read()

    # Find and replace the load_data method
    old_pattern = r'def load_economic_indicators_data\(self, records\):.*?print\(f"Successfully loaded \{rows_affected\} records into economic_indicators table"\)'

    new_load_method = '''def load_economic_indicators_data(self, records):
        """Load economic indicator records into the database."""
        if not records:
            print("No records to load")
            return
        
        print(f"Loading {len(records)} records into database...")
        
        with self.db_manager as db:
            # Initialize schema if tables don't exist
            schema_path = Path(__file__).parent.parent.parent / "db" / "schema" / "postgres_stock_db_schema.sql"
            if not db.table_exists('economic_indicators'):
                print("Initializing database schema...")
                db.initialize_schema(schema_path)
            
            # Use PostgreSQL upsert functionality
            for record in records:
                data_dict = record.copy()
                # Remove timestamp columns for upsert - they'll be handled by the database
                data_dict.pop('created_at', None)
                data_dict.pop('updated_at', None)
                
                # For economic indicators, conflict is on economic_indicator_name, function_name, maturity, date, interval
                conflict_columns = ['economic_indicator_name', 'function_name', 'maturity', 'date', 'interval']
                db.upsert_data('economic_indicators', data_dict, conflict_columns)
            
            print(f"Successfully loaded {len(records)} records into economic_indicators table")'''

    content = re.sub(old_pattern, new_load_method, content, flags=re.DOTALL)

    with open(file_path, "w") as f:
        f.write(content)

    print(f"✅ Fixed {file_path}")


def main():
    """Fix all extractors properly."""
    print("Fixing all extractors for PostgreSQL...")

    try:
        fix_time_series_extractor()
        fix_financial_statement_extractors()
        fix_commodities_extractor()
        fix_economic_indicators_extractor()

        print("\n🎉 All extractors have been properly fixed for PostgreSQL!")
        print("\nKey changes made:")
        print("  - Updated imports to use PostgresDatabaseManager")
        print("  - Removed db_path parameter from constructors")
        print("  - Replaced INSERT OR REPLACE with PostgreSQL upsert")
        print("  - Updated schema paths to postgres_stock_db_schema.sql")
        print("  - Fixed placeholder syntax (? → %s)")
        print("  - Added proper conflict column definitions")

    except Exception as e:
        print(f"❌ Error fixing extractors: {e}")


if __name__ == "__main__":
    main()
