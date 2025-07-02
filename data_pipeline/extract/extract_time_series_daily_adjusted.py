

"""
Extract time series daily adjusted data from Alpha Vantage API and load into database.
"""

import requests
import pandas as pd
import os
import sys
import time
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
from io import StringIO

# Add the parent directories to the path so we can import from db
sys.path.append(str(Path(__file__).parent.parent.parent))
from db.database_manager import DatabaseManager

STOCK_API_FUNCTION = "TIME_SERIES_DAILY_ADJUSTED"
DATATYPE = "csv"
OUTPUTSIZE = "full"  # Get full historical data

class TimeSeriesExtractor:
    """Extract and load time series daily adjusted data from Alpha Vantage API."""
    
    def __init__(self, db_path="db/stock_db.db", output_size="full"):
        # Load ALPHAVANTAGE_API_KEY from .env file
        load_dotenv()
        self.api_key = os.getenv('ALPHAVANTAGE_API_KEY')
        
        if not self.api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY not found in environment variables")
        
        self.db_manager = DatabaseManager(db_path)
        self.base_url = "https://www.alphavantage.co/query"
        self.output_size = output_size  # "full" for historical, "compact" for recent
        
        # Rate limiting: 75 requests per minute for Alpha Vantage Premium
        self.rate_limit_delay = 0.8  # seconds between requests (75/min = 0.8s delay)
    
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
            return {row[1]: row[0] for row in result}
    
    def load_unprocessed_symbols(self, exchange_filter=None, limit=None):
        """Load symbols that haven't been processed yet (not in time_series_daily_adjusted table)."""
        with self.db_manager as db:
            # First ensure the table exists, or create the schema
            if not db.table_exists('time_series_daily_adjusted'):
                # Initialize schema to create the table
                schema_path = Path(__file__).parent.parent.parent / "db" / "schema" / "stock_db_schema.sql"
                db.initialize_schema(schema_path)
            
            # Now we can safely query with LEFT JOIN
            base_query = """
                SELECT ls.symbol_id, ls.symbol 
                FROM listing_status ls 
                LEFT JOIN time_series_daily_adjusted ts ON ls.symbol_id = ts.symbol_id 
                WHERE ls.asset_type = 'Stock' AND ts.symbol_id IS NULL
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
            
            base_query += " GROUP BY ls.symbol_id, ls.symbol"
            
            if limit:
                base_query += " LIMIT ?"
                params.append(limit)
            
            result = db.fetch_query(base_query, params)
            return {row[1]: row[0] for row in result}
    
    def extract_single_time_series(self, symbol):
        """Extract time series data for a single symbol."""
        print(f"Processing TICKER: {symbol}")
        
        url = f'{self.base_url}?function={STOCK_API_FUNCTION}&datatype={DATATYPE}&outputsize={self.output_size}&symbol={symbol}&apikey={self.api_key}'
        print(f"Fetching data from: {url}")
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            
            print(f"Response status: {response.status_code}")
            
            # Check if we got CSV data
            if response.headers.get('content-type', '').startswith('text/csv') or 'timestamp' in response.text.lower():
                # Read CSV data
                df = pd.read_csv(StringIO(response.text))
                
                if df.empty or 'timestamp' not in df.columns:
                    print(f"Empty or invalid CSV response for {symbol}")
                    return None, 'fail'
                
                print(f"Successfully fetched {len(df)} records for {symbol}")
                return df, 'pass'
            else:
                # Might be JSON error response
                try:
                    data = response.json()
                    print(f"JSON response for {symbol}: {data}")
                    return None, 'fail'
                except:
                    print(f"Invalid response format for {symbol}")
                    return None, 'fail'
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {symbol}: {e}")
            return None, 'fail'
        except Exception as e:
            print(f"Unexpected error for {symbol}: {e}")
            return None, 'fail'
    
    def transform_time_series_data(self, symbol, symbol_id, df, status, start_date=None, end_date=None):
        """Transform time series data to match database schema."""
        current_timestamp = datetime.now().isoformat()
        
        if status == 'fail' or df is None or df.empty:
            # Return empty list for failed fetches
            return []
        
        try:
            # Rename columns to match database schema
            column_mapping = {
                'timestamp': 'date',
                'open': 'open',
                'high': 'high', 
                'low': 'low',
                'close': 'close',
                'adjusted_close': 'adjusted_close',
                'volume': 'volume',
                'dividend_amount': 'dividend_amount',
                'split_coefficient': 'split_coefficient'
            }
            
            df_transformed = df.rename(columns=column_mapping)
            
            # Convert date column
            df_transformed['date'] = pd.to_datetime(df_transformed['date'])
            
            # Apply date range filtering if specified
            if start_date:
                start_date_parsed = pd.to_datetime(start_date)
                df_transformed = df_transformed[df_transformed['date'] >= start_date_parsed]
                print(f"Filtered data from {start_date} onwards: {len(df_transformed)} records")
            
            if end_date:
                end_date_parsed = pd.to_datetime(end_date)
                df_transformed = df_transformed[df_transformed['date'] <= end_date_parsed]
                print(f"Filtered data until {end_date}: {len(df_transformed)} records")
            
            # Convert date back to string format for database
            df_transformed['date'] = df_transformed['date'].dt.strftime('%Y-%m-%d')
            
            # Add symbol information
            df_transformed['symbol_id'] = symbol_id
            df_transformed['symbol'] = symbol
            
            # Add timestamps
            df_transformed['created_at'] = current_timestamp
            df_transformed['updated_at'] = current_timestamp
            
            # Select columns in the correct order for database
            db_columns = ['symbol_id', 'symbol', 'date', 'open', 'high', 'low', 'close', 
                         'adjusted_close', 'volume', 'dividend_amount', 'split_coefficient',
                         'created_at', 'updated_at']
            
            # Keep only columns that exist
            available_columns = [col for col in db_columns if col in df_transformed.columns]
            df_final = df_transformed[available_columns]
            
            # Convert to list of dictionaries
            records = df_final.to_dict('records')
            
            print(f"Transformed {len(records)} records for {symbol}")
            return records
            
        except Exception as e:
            print(f"Error transforming data for {symbol}: {e}")
            return []
    
    def load_time_series_data(self, records):
        """Load time series records into the database."""
        if not records:
            print("No records to load")
            return
        
        print(f"Loading {len(records)} records into database...")
        
        with DatabaseManager(self.db_manager.db_path) as db:
            # Initialize schema if tables don't exist
            schema_path = Path(__file__).parent.parent.parent / "db" / "schema" / "stock_db_schema.sql"
            if not db.table_exists('time_series_daily_adjusted'):
                print("Initializing database schema...")
                db.initialize_schema(schema_path)
            
            # Prepare insert query
            columns = list(records[0].keys())
            placeholders = ', '.join(['?' for _ in columns])
            insert_query = f"""
                INSERT OR REPLACE INTO time_series_daily_adjusted ({', '.join(columns)}) 
                VALUES ({placeholders})
            """
            
            # Convert records to list of tuples
            record_tuples = [tuple(record[col] for col in columns) for record in records]
            
            # Execute bulk insert
            rows_affected = db.execute_many(insert_query, record_tuples)
            print(f"Successfully loaded {rows_affected} records into time_series_daily_adjusted table")
    
    def run_etl_incremental(self, exchange_filter=None, limit=None, start_date=None, end_date=None):
        """Run ETL only for symbols not yet processed.
        
        Args:
            exchange_filter: Filter by exchange (e.g., 'NASDAQ', 'NYSE')
            limit: Maximum number of symbols to process (for chunking)
            start_date: Start date filter (YYYY-MM-DD format)
            end_date: End date filter (YYYY-MM-DD format)
        """
        print("Starting Incremental Time Series ETL process...")
        print(f"Configuration: exchange={exchange_filter}, limit={limit}, output_size={self.output_size}")
        if start_date or end_date:
            print(f"Date range: {start_date or 'beginning'} to {end_date or 'end'}")
        
        try:
            # Load only unprocessed symbols
            symbol_mapping = self.load_unprocessed_symbols(exchange_filter, limit)
            symbols = list(symbol_mapping.keys())
            print(f"Found {len(symbols)} unprocessed symbols")
            
            if not symbols:
                print("No unprocessed symbols found")
                return
            
            total_records = 0
            success_count = 0
            fail_count = 0
            
            for i, symbol in enumerate(symbols):
                symbol_id = symbol_mapping[symbol]
                
                try:
                    # Extract data for this symbol
                    df, status = self.extract_single_time_series(symbol)
                    
                    # Transform data with date filtering
                    records = self.transform_time_series_data(symbol, symbol_id, df, status, start_date, end_date)
                    
                    if records:
                        # Load records for this symbol
                        self.load_time_series_data(records)
                        total_records += len(records)
                        success_count += 1
                        print(f"✓ Processed {symbol} (ID: {symbol_id}) - {len(records)} records [{i+1}/{len(symbols)}]")
                    else:
                        fail_count += 1
                        print(f"✗ Processed {symbol} (ID: {symbol_id}) - 0 records [{i+1}/{len(symbols)}]")
                    
                except Exception as e:
                    fail_count += 1
                    print(f"✗ Error processing {symbol} (ID: {symbol_id}): {e} [{i+1}/{len(symbols)}]")
                    # Continue processing other symbols even if one fails
                    continue
                
                # Rate limiting - wait between requests
                if i < len(symbols) - 1:
                    time.sleep(self.rate_limit_delay)
            
            # Get remaining symbols count for summary
            with DatabaseManager(self.db_manager.db_path) as db:
                # Count remaining unprocessed symbols
                base_query = """
                    SELECT COUNT(DISTINCT ls.symbol_id)
                    FROM listing_status ls 
                    LEFT JOIN time_series_daily_adjusted ts ON ls.symbol_id = ts.symbol_id 
                    WHERE ls.asset_type = 'Stock' AND ts.symbol_id IS NULL
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
                
                remaining_count = db.fetch_query(base_query, params)[0][0]
            
            # Print summary
            print(f"\n" + "="*50)
            print(f"Incremental Time Series ETL Summary:")
            print(f"  Exchange: {exchange_filter or 'All exchanges'}")
            print(f"  Total symbols processed: {len(symbols)}")
            print(f"  Successful symbols: {success_count}")
            print(f"  Failed symbols: {fail_count}")
            print(f"  Remaining symbols: {remaining_count}")
            print(f"  Total records loaded: {total_records:,}")
            print(f"  Average records per symbol: {total_records/success_count if success_count > 0 else 0:.1f}")
            print(f"="*50)
            
            print("Incremental Time Series ETL process completed successfully!")
            
        except Exception as e:
            print(f"Incremental Time Series ETL process failed: {e}")
            raise

def main():
    """Main function to run the time series extraction."""
    
    # Configuration options for different use cases:
    
    # Option 1: Initial full historical data collection (recommended for first run)
    extractor_full = TimeSeriesExtractor(output_size="full")
    extractor_full.run_etl_incremental(exchange_filter='NYSE MKT', limit=266)  # Increased for premium tier
    
    # Option 2: Daily updates with compact data (for ongoing collection)
    # extractor_compact = TimeSeriesExtractor(output_size="compact")
    # extractor_compact.run_etl_incremental(exchange_filter='NASDAQ', limit=10)
    
    # Option 3: Historical data with date range filtering
    # extractor_filtered = TimeSeriesExtractor(output_size="full")
    # extractor_filtered.run_etl_incremental(
    #     exchange_filter='NASDAQ', 
    #     limit=5, 
    #     start_date='2020-01-01',  # Only data from 2020 onwards
    #     end_date='2023-12-31'     # Only data until end of 2023
    # )
    
    # Option 4: Large batch processing (manage memory with chunks)
    # extractor_batch = TimeSeriesExtractor(output_size="full")
    # extractor_batch.run_etl_incremental(exchange_filter='NASDAQ', limit=50)

if __name__ == "__main__":
    main()    