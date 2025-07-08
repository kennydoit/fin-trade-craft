"""
Extract listing status data from Alpha Vantage API and load into database.
"""

import requests
import pandas as pd
import os
import sys
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

# Add the parent directories to the path so we can import from db
sys.path.append(str(Path(__file__).parent.parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager

STOCK_API_FUNCTION = "LISTING_STATUS"

class ListingStatusExtractor:
    """Extract and load listing status data from Alpha Vantage API."""
    
    def __init__(self):
        # Load ALPHAVANTAGE_API_KEY from .env file
        load_dotenv()
        self.api_key = os.getenv('ALPHAVANTAGE_API_KEY')
        
        if not self.api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY not found in environment variables")
        
        self.db_manager = PostgresDatabaseManager()
        self.base_url = "https://www.alphavantage.co/query"
    
    def extract_data(self):
        """Extract listing status data from Alpha Vantage API."""
        print("Extracting listing status data from Alpha Vantage API...")
        
        url = f'{self.base_url}?function={STOCK_API_FUNCTION}&apikey={self.api_key}'
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            
            # Read CSV data directly from the response
            df = pd.read_csv(url)
            
            print(f"Successfully extracted {len(df)} records")
            return df
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from API: {e}")
            raise
        except pd.errors.EmptyDataError:
            print("No data received from API")
            raise
    
    def transform_data(self, df):
        """Transform the extracted data to match database schema."""
        print("Transforming data...")
        
        # Map API column names to database column names
        column_mapping = {
            'symbol': 'symbol',
            'name': 'name',
            'exchange': 'exchange',
            'assetType': 'asset_type',
            'ipoDate': 'ipo_date',
            'delistingDate': 'delisting_date',
            'status': 'status'
        }
        
        # Rename columns to match database schema
        df_transformed = df.rename(columns=column_mapping)
        
        # Clean data - remove rows with null or empty symbols (required field)
        if 'symbol' in df_transformed.columns:
            initial_count = len(df_transformed)
            df_transformed = df_transformed.dropna(subset=['symbol'])
            df_transformed = df_transformed[df_transformed['symbol'].str.strip() != '']
            final_count = len(df_transformed)
            if initial_count != final_count:
                print(f"Removed {initial_count - final_count} rows with null/empty symbols")
        
        # Handle date columns - convert to proper date format or NULL
        date_columns = ['ipo_date', 'delisting_date']
        for col in date_columns:
            if col in df_transformed.columns:
                df_transformed[col] = pd.to_datetime(df_transformed[col], errors='coerce')
                df_transformed[col] = df_transformed[col].dt.strftime('%Y-%m-%d')
                df_transformed[col] = df_transformed[col].where(pd.notnull(df_transformed[col]), None)
        
        # Add timestamp columns
        current_timestamp = datetime.now().isoformat()
        df_transformed['created_at'] = current_timestamp
        df_transformed['updated_at'] = current_timestamp
        
        # Select only columns that exist in our database schema
        required_columns = ['symbol', 'name', 'exchange', 'asset_type', 'ipo_date', 
                           'delisting_date', 'status', 'created_at', 'updated_at']
        
        # Keep only columns that exist in both dataframe and required columns
        available_columns = [col for col in required_columns if col in df_transformed.columns]
        df_final = df_transformed[available_columns]
        
        print(f"Transformed data with columns: {list(df_final.columns)}")
        print(f"Final record count: {len(df_final)}")
        return df_final
    
    def load_data(self, df):
        """Load transformed data into the database."""
        print("Loading data into database...")
        
        with self.db_manager as db:
            # Initialize schema if tables don't exist
            schema_path = Path(__file__).parent.parent.parent / "db" / "schema" / "postgres_stock_db_schema.sql"
            if not db.table_exists('listing_status'):
                print("Initializing database schema...")
                db.initialize_schema(schema_path)
            
            # Use PostgreSQL upsert functionality
            # Convert dataframe to list of records for upsert
            for index, row in df.iterrows():
                data_dict = row.to_dict()
                # Remove timestamp columns for upsert - they'll be handled by the database
                data_dict.pop('created_at', None)
                data_dict.pop('updated_at', None)
                
                db.upsert_data('listing_status', data_dict, ['symbol'])
            
            print(f"Successfully loaded {len(df)} records into listing_status table")
    
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


