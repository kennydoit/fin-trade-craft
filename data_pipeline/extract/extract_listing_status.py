"""
Extract listing status data from Alpha Vantage API and load into database.
"""

import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

# Add the parent directories to the path so we can import from db
sys.path.append(str(Path(__file__).parent.parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager
from utils.database_safety import DatabaseSafetyManager

STOCK_API_FUNCTION = "LISTING_STATUS"


class ListingStatusExtractor:
    """Extract and load listing status data from Alpha Vantage API."""

    def __init__(self):
        # Load ALPHAVANTAGE_API_KEY from .env file
        load_dotenv()
        self.api_key = os.getenv("ALPHAVANTAGE_API_KEY")

        if not self.api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY not found in environment variables")

        self.db_manager = PostgresDatabaseManager()
        self.base_url = "https://www.alphavantage.co/query"

    def extract_data(self):
        """Extract listing status data from Alpha Vantage API for both active and delisted stocks."""
        print("Extracting listing status data from Alpha Vantage API...")

        all_data = []
        states = ['active', 'delisted']
        
        for state in states:
            print(f"Extracting {state} stocks...")
            url = f"{self.base_url}?function={STOCK_API_FUNCTION}&state={state}&apikey={self.api_key}"

            try:
                response = requests.get(url)
                response.raise_for_status()

                # Read CSV data directly from the response
                df = pd.read_csv(url)
                
                # Add state column to track which API call this data came from
                df['api_state'] = state
                
                print(f"Successfully extracted {len(df)} {state} records")
                all_data.append(df)

            except requests.exceptions.RequestException as e:
                print(f"Error fetching {state} data from API: {e}")
                # Continue with other state if one fails
                continue
            except pd.errors.EmptyDataError:
                print(f"No {state} data received from API")
                continue

        if not all_data:
            raise Exception("No data received from any API calls")
        
        # Combine all dataframes
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"Successfully extracted {len(combined_df)} total records")
        return combined_df

    def transform_data(self, df):
        """Transform the extracted data to match database schema."""
        print("Transforming data...")

        # Map API column names to database column names
        column_mapping = {
            "symbol": "symbol",
            "name": "name",
            "exchange": "exchange",
            "assetType": "asset_type",
            "ipoDate": "ipo_date",
            "delistingDate": "delisting_date",
            "status": "status",
            # Note: api_state is not mapped - status column already captures this information
        }

        # Rename columns to match database schema
        df_transformed = df.rename(columns=column_mapping)

        # Clean data - remove rows with null or empty symbols (required field)
        if "symbol" in df_transformed.columns:
            initial_count = len(df_transformed)
            df_transformed = df_transformed.dropna(subset=["symbol"])
            df_transformed = df_transformed[df_transformed["symbol"].str.strip() != ""]
            final_count = len(df_transformed)
            if initial_count != final_count:
                print(
                    f"Removed {initial_count - final_count} rows with null/empty symbols"
                )

        # Handle date columns - convert to proper date format or NULL
        date_columns = ["ipo_date", "delisting_date"]
        for col in date_columns:
            if col in df_transformed.columns:
                df_transformed[col] = pd.to_datetime(
                    df_transformed[col], errors="coerce"
                )
                df_transformed[col] = df_transformed[col].dt.strftime("%Y-%m-%d")
                df_transformed[col] = df_transformed[col].where(
                    pd.notnull(df_transformed[col]), None
                )

        # Add timestamp columns
        current_timestamp = datetime.now().isoformat()
        df_transformed["created_at"] = current_timestamp
        df_transformed["updated_at"] = current_timestamp

        # Select only columns that exist in our database schema
        required_columns = [
            "symbol",
            "name", 
            "exchange",
            "asset_type",
            "ipo_date",
            "delisting_date",
            "status",  # Status column captures active/delisted state
            "created_at",
            "updated_at",
        ]

        # Keep only columns that exist in both dataframe and required columns
        available_columns = [
            col for col in required_columns if col in df_transformed.columns
        ]
        df_final = df_transformed[available_columns]

        # Handle duplicate symbols - keep the most recent record (using status column to determine priority)
        # First check for duplicates
        initial_count = len(df_transformed)
        duplicate_symbols = df_transformed[df_transformed.duplicated(subset=['symbol'], keep=False)]
        
        if len(duplicate_symbols) > 0:
            print(f"Found {len(duplicate_symbols)} duplicate symbol entries")
            # Use api_state for sorting since it's more reliable than status for this purpose
            if 'api_state' in df_transformed.columns:
                df_sorted = df_transformed.sort_values('api_state')  # 'active' comes before 'delisted'
                # Keep last occurrence (delisted if both exist)
                df_deduped = df_sorted.drop_duplicates(subset=['symbol'], keep='last')
                print(f"Removed {initial_count - len(df_deduped)} duplicate symbols (kept most recent status)")
            else:
                # If no api_state column, just remove duplicates keeping the last one
                df_deduped = df_transformed.drop_duplicates(subset=['symbol'], keep='last')
                print(f"Removed {initial_count - len(df_deduped)} duplicate symbols")
        else:
            df_deduped = df_transformed
            print("No duplicate symbols found")
        
        # Now select only the required columns
        df_final = df_deduped[available_columns]
        
        # Final check for duplicates
        final_duplicates = df_final[df_final.duplicated(subset=['symbol'])]
        if len(final_duplicates) > 0:
            print(f"WARNING: Still have {len(final_duplicates)} duplicates in final data!")
            print(f"Sample duplicates: {final_duplicates['symbol'].head().tolist()}")

        print(f"Transformed data with columns: {list(df_final.columns)}")
        print(f"Final record count: {len(df_final)}")
        print(f"✅ Status column captures active/delisted state information")
        return df_final

    def load_data(self, df):
        """Load transformed data into the database, replacing all existing records."""
        print("Loading data into database...")

        # Initialize safety manager
        safety_manager = DatabaseSafetyManager(enable_backups=True, enable_checks=True)

        with self.db_manager as db:
            # Check if the table exists in the extracted schema
            table_exists_query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'extracted' 
                    AND table_name = 'listing_status'
                );
            """
            result = db.fetch_query(table_exists_query)
            table_exists = result[0][0] if result else False
            
            if not table_exists:
                print("Table extracted.listing_status does not exist!")
                print("Please run the schema initialization first or check your database setup.")
                raise Exception("Table extracted.listing_status not found")

            # SAFE data replacement using safety manager
            print("🛡️ Performing SAFE data replacement...")
            if not safety_manager.safe_delete_table_data('listing_status', 'listing_status_update'):
                raise Exception("Safe deletion failed - operation aborted")
            
            # Reset the sequence for symbol_id if it exists
            try:
                db.execute_query("ALTER SEQUENCE IF EXISTS extracted.listing_status_symbol_id_seq RESTART WITH 1;")
            except Exception as e:
                print(f"Note: Could not reset sequence (this is normal if no sequence exists): {e}")
            
            # Prepare data for batch insert
            records = []
            for index, row in df.iterrows():
                data_dict = row.to_dict()
                # Convert NaN values to None for proper NULL handling
                for key, value in data_dict.items():
                    if pd.isna(value):
                        data_dict[key] = None
                records.append(data_dict)

            # Build the INSERT query
            if records:
                columns = list(records[0].keys())
                placeholders = ', '.join(['%s'] * len(columns))
                columns_str = ', '.join(columns)
                
                insert_query = f"""
                    INSERT INTO extracted.listing_status ({columns_str})
                    VALUES ({placeholders})
                """
                
                # Prepare data for batch insert
                data_to_insert = []
                for record in records:
                    row_values = [record[col] for col in columns]
                    data_to_insert.append(row_values)
                
                # Execute batch insert
                db.execute_many(insert_query, data_to_insert)
                
            print(f"✅ Successfully replaced all records with {len(df)} new records in listing_status table")
            
            # Verify integrity after operation
            safety_manager.verify_table_integrity('listing_status')

    def run_etl(self):
        """Run the complete ETL process."""
        print("Starting Listing Status ETL process...")

        try:
            # Extract
            raw_data = self.extract_data()

            # Transform
            transformed_data = self.transform_data(raw_data)

            # Load
            self.load_data(transformed_data)

            print("ETL process completed successfully!")

        except Exception as e:
            print(f"ETL process failed: {e}")
            raise


def main():
    """Main function to run the listing status extraction."""
    extractor = ListingStatusExtractor()
    extractor.run_etl()


if __name__ == "__main__":
    main()

# -----------------------------------------------------------------------------
# Example command (PowerShell):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_listing_status.py \
# -----------------------------------------------------------------------------
