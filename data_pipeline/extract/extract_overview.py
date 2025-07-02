"""
Extract company overview data from Alpha Vantage API and load into database.
"""

import requests
import pandas as pd
import os
import sys
import time
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

# Add the parent directories to the path so we can import from db
sys.path.append(str(Path(__file__).parent.parent.parent))
from db.database_manager import DatabaseManager

API_FUNCTION = "OVERVIEW"

class OverviewExtractor:
    """Extract and load company overview data from Alpha Vantage API."""
    
    def __init__(self, db_path="db/stock_db.db"):
        # Load ALPHAVANTAGE_API_KEY from .env file
        load_dotenv()
        self.api_key = os.getenv('ALPHAVANTAGE_API_KEY')
        
        if not self.api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY not found in environment variables")
        
        self.db_manager = DatabaseManager(db_path)
        self.base_url = "https://www.alphavantage.co/query"
        
        # Rate limiting: 75 requests per minute for Alpha Vantage
        self.rate_limit_delay = 1  # seconds between requests
    
    def load_valid_symbols(self, exchange_filter=None, limit=None):
        """Load valid stock symbols from the database with their symbol_ids."""
        with self.db_manager as db:
            base_query = "SELECT symbol_id, symbol FROM listing_status WHERE asset_type = 'Stock'"
            params = []
            
            if exchange_filter:
                if isinstance(exchange_filter, list):
                    placeholders = ','.join(['?' for _ in exchange_filter])
                    base_query += f" AND exchange IN ({placeholders})"
                    params.extend(exchange_filter)
                else:
                    base_query += " AND exchange = ?"
                    params.append(exchange_filter)
            
            if limit:
                base_query += " LIMIT ?"
                params.append(limit)
            
            result = db.fetch_query(base_query, params)
            # Return dict mapping symbol -> symbol_id
            return {row[1]: row[0] for row in result}
    
    def extract_single_overview(self, symbol):
        """Extract overview data for a single symbol."""
        print(f"Processing TICKER: {symbol}")
        
        url = f'{self.base_url}?function={API_FUNCTION}&symbol={symbol}&apikey={self.api_key}'
        print(f"Fetching data from: {url}")
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            
            print(f"Response status: {response.status_code}")
            data = response.json()
            
            # Check if we got valid data or an empty response
            if not data or data == {} or 'Symbol' not in data:
                print(f"Empty or invalid response for {symbol}: {data}")
                return None, 'fail'
            
            print(f"Successfully fetched data for {symbol}")
            return data, 'pass'
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {symbol}: {e}")
            return None, 'fail'
        except Exception as e:
            print(f"Unexpected error for {symbol}: {e}")
            return None, 'fail'
    
    def transform_overview_data(self, symbol, symbol_id, data, status):
        """Transform overview data to match database schema."""
        current_timestamp = datetime.now().isoformat()
        
        if status == 'fail' or data is None:
            # Create a minimal record for failed fetches
            return {
                'symbol_id': symbol_id,
                'symbol': symbol,
                'assettype': None,
                'name': None,
                'description': None,
                'cik': None,
                'exchange': None,
                'currency': None,
                'country': None,
                'sector': None,
                'industry': None,
                'address': None,
                'officialsite': None,
                'fiscalyearend': None,
                'status': 'fail',
                'created_at': current_timestamp,
                'updated_at': current_timestamp
            }
        
        # Map API fields to database columns
        transformed = {
            'symbol_id': symbol_id,
            'symbol': data.get('Symbol', symbol),
            'assettype': data.get('AssetType'),
            'name': data.get('Name'),
            'description': data.get('Description'),
            'cik': data.get('CIK'),
            'exchange': data.get('Exchange'),
            'currency': data.get('Currency'),
            'country': data.get('Country'),
            'sector': data.get('Sector'),
            'industry': data.get('Industry'),
            'address': data.get('Address'),
            'officialsite': data.get('OfficialSite'),
            'fiscalyearend': data.get('FiscalYearEnd'),
            'status': 'pass',
            'created_at': current_timestamp,
            'updated_at': current_timestamp
        }
        
        return transformed
    
    def load_overview_data(self, records):
        """Load overview records into the database."""
        if not records:
            print("No records to load")
            return
        
        print(f"Loading {len(records)} records into database...")
        
        # Use a fresh database manager instance for loading
        with DatabaseManager(self.db_manager.db_path) as db:
            # Initialize schema if tables don't exist
            schema_path = Path(__file__).parent.parent.parent / "db" / "schema" / "stock_db_schema.sql"
            if not db.table_exists('overview'):
                print("Initializing database schema...")
                db.initialize_schema(schema_path)
            
            # Prepare insert query - use INSERT OR REPLACE based on symbol_id
            columns = list(records[0].keys())
            placeholders = ', '.join(['?' for _ in columns])
            insert_query = f"""
                INSERT OR REPLACE INTO overview ({', '.join(columns)}) 
                VALUES ({placeholders})
            """
            
            # Convert records to list of tuples
            record_tuples = [tuple(record[col] for col in columns) for record in records]
            
            # Execute bulk insert
            rows_affected = db.execute_many(insert_query, record_tuples)
            print(f"Successfully loaded {rows_affected} records into overview table")
    
    def load_unprocessed_symbols(self, exchange_filter=None, limit=None):
        """Load symbols that haven't been processed yet (not in overview table)."""
        with self.db_manager as db:
            base_query = """
                SELECT ls.symbol_id, ls.symbol 
                FROM listing_status ls 
                LEFT JOIN overview ov ON ls.symbol_id = ov.symbol_id 
                WHERE ls.asset_type = 'Stock' AND ov.symbol_id IS NULL
            """
            params = []
            
            if exchange_filter:
                if isinstance(exchange_filter, list):
                    placeholders = ','.join(['?' for _ in exchange_filter])
                    base_query += f" AND ls.exchange IN ({placeholders})"
                    params.extend(exchange_filter)
                else:
                    base_query += " AND ls.exchange = ?"
                    params.append(exchange_filter)
            
            if limit:
                base_query += " LIMIT ?"
                params.append(limit)
            
            result = db.fetch_query(base_query, params)
            return {row[1]: row[0] for row in result}
    
    def run_etl(self, exchange_filter=None, limit=None):
        """Run the complete ETL process for overview data."""
        print("Starting Overview ETL process...")
        
        try:
            # Load symbols to process (now returns dict: symbol -> symbol_id)
            symbol_mapping = self.load_valid_symbols(exchange_filter, limit)
            symbols = list(symbol_mapping.keys())
            print(f"Found {len(symbols)} symbols to process")
            
            if not symbols:
                print("No symbols found to process")
                return
            
            records = []
            
            for i, symbol in enumerate(symbols):
                symbol_id = symbol_mapping[symbol]
                
                # Extract data for this symbol
                data, status = self.extract_single_overview(symbol)
                
                # Transform data
                transformed_record = self.transform_overview_data(symbol, symbol_id, data, status)
                records.append(transformed_record)
                
                print(f"Processed {symbol} (ID: {symbol_id}) with status: {status}")
                
                # Rate limiting - wait between requests
                if i < len(symbols) - 1:  # Don't wait after the last request
                    time.sleep(self.rate_limit_delay)
            
            # Load all records
            self.load_overview_data(records)
            
            # Print summary
            pass_count = sum(1 for r in records if r['status'] == 'pass')
            fail_count = sum(1 for r in records if r['status'] == 'fail')
            print(f"\nETL Summary:")
            print(f"  Total processed: {len(records)}")
            print(f"  Successful: {pass_count}")
            print(f"  Failed: {fail_count}")
            
            print("Overview ETL process completed successfully!")
            
        except Exception as e:
            print(f"Overview ETL process failed: {e}")
            raise

    def run_etl_incremental(self, exchange_filter=None, limit=None):
        """Run ETL only for symbols not yet processed."""
        print("Starting Incremental Overview ETL process...")
        
        try:
            # Load only unprocessed symbols
            symbol_mapping = self.load_unprocessed_symbols(exchange_filter, limit)
            symbols = list(symbol_mapping.keys())
            print(f"Found {len(symbols)} unprocessed symbols")
            
            if not symbols:
                print("No unprocessed symbols found")
                return
            
            records = []
            
            for i, symbol in enumerate(symbols):
                symbol_id = symbol_mapping[symbol]
                
                # Extract data for this symbol
                data, status = self.extract_single_overview(symbol)
                
                # Transform data
                transformed_record = self.transform_overview_data(symbol, symbol_id, data, status)
                records.append(transformed_record)
                
                print(f"Processed {symbol} (ID: {symbol_id}) with status: {status} [{i+1}/{len(symbols)}]")
                
                # Rate limiting - wait between requests
                if i < len(symbols) - 1:
                    time.sleep(self.rate_limit_delay)
            
            # Load all records
            self.load_overview_data(records)
            
            # Print summary
            pass_count = sum(1 for r in records if r['status'] == 'pass')
            fail_count = sum(1 for r in records if r['status'] == 'fail')
            print(f"\nIncremental ETL Summary:")
            print(f"  Total processed: {len(records)}")
            print(f"  Successful: {pass_count}")
            print(f"  Failed: {fail_count}")
            
            print("Incremental Overview ETL process completed successfully!")
            
        except Exception as e:
            print(f"Incremental Overview ETL process failed: {e}")
            raise

def main():
    """Main function to run the overview extraction."""
    extractor = OverviewExtractor()
    
    # Configuration options:
    # Option 1: Small test batch
    # extractor.run_etl(exchange_filter='NASDAQ', limit=5)
    
    # Option 2: Process by exchange in batches
    # extractor.run_etl(exchange_filter='NASDAQ', limit=100)
    # extractor.run_etl(exchange_filter='NYSE', limit=100)
    
    # Option 3: Process only symbols not yet in overview table
    extractor.run_etl_incremental(exchange_filter='NYSE', limit=695)

    # Add this to check remaining symbols before running
    with DatabaseManager('db/stock_db.db') as db:
        remaining = db.fetch_query("""
            SELECT COUNT(*) 
            FROM listing_status ls 
            LEFT JOIN overview ov ON ls.symbol_id = ov.symbol_id 
            WHERE ls.asset_type = 'Stock' AND ov.symbol_id IS NULL 
            AND ls.exchange = 'NYSE'
        """)[0][0]
        print(f"Remaining NYSE symbols to process: {remaining}")

if __name__ == "__main__":
    main()