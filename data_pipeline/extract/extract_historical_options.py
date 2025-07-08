"""
Extract historical options data from Alpha Vantage API and load into database.
"""

import requests
import pandas as pd
import os
import sys
import time
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path
from io import StringIO

# Add the parent directories to the path so we can import from db
sys.path.append(str(Path(__file__).parent.parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager

OPTIONS_API_FUNCTION = "HISTORICAL_OPTIONS"
DATATYPE = "json"  # Options data comes as JSON

class HistoricalOptionsExtractor:
    """Extract and load historical options data from Alpha Vantage API."""
    
    def __init__(self, db_path="db/stock_db.db"):
        # Load ALPHAVANTAGE_API_KEY from .env file
        load_dotenv()
        self.api_key = os.getenv('ALPHAVANTAGE_API_KEY')
        
        if not self.api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY not found in environment variables")
        
        self.db_manager = PostgresDatabaseManager()
        self.base_url = "https://www.alphavantage.co/query"
        
        # Rate limiting: 75 requests per minute for Alpha Vantage Premium
        self.rate_limit_delay = 0.8  # seconds between requests (75/min = 0.8s delay)
    
    def load_valid_symbols(self, exchange_filter=None, limit=None):
        """Load valid stock symbols from the database with their symbol_ids."""
        with self.db_manager as db:
            base_query = "SELECT symbol_id, symbol FROM listing_status WHERE asset_type = 'Stock'"
            params = []
            
            if exchange_filter:
                if isinstance(exchange_filter, list):
                    placeholders = ','.join(['%s' for _ in exchange_filter])
                    base_query += f" AND exchange IN ({placeholders})"
                    params.extend(exchange_filter)
                else:
                    base_query += " AND exchange = %s"
                    params.append(exchange_filter)
            
            if limit:
                base_query += " LIMIT %s"
                params.append(limit)
            
            result = db.fetch_query(base_query, params)
            return {row[1]: row[0] for row in result}
    
    def load_unprocessed_symbols_for_date(self, target_date, exchange_filter=None, limit=None):
        """Load symbols that haven't been processed for a specific date."""
        with self.db_manager as db:
            # First ensure the table exists - create only if needed
            if not db.table_exists('historical_options'):
                try:
                    # Try to create just the historical_options table
                    create_table_sql = """
                    CREATE TABLE IF NOT EXISTS historical_options (
                        option_id           SERIAL PRIMARY KEY,
                        symbol_id           INTEGER NOT NULL,
                        symbol              VARCHAR(20) NOT NULL,
                        contract_name       VARCHAR(50) NOT NULL,
                        option_type         VARCHAR(4) NOT NULL CHECK (option_type IN ('call', 'put')),
                        strike              DECIMAL(12,4) NOT NULL,
                        expiration          DATE NOT NULL,
                        last_trade_date     DATE NOT NULL,
                        last_price          DECIMAL(12,4),
                        mark                DECIMAL(12,4),
                        bid                 DECIMAL(12,4),
                        bid_size            INTEGER,
                        ask                 DECIMAL(12,4),
                        ask_size            INTEGER,
                        volume              BIGINT,
                        open_interest       BIGINT,
                        implied_volatility  DECIMAL(8,6),
                        delta               DECIMAL(8,6),
                        gamma               DECIMAL(8,6),
                        theta               DECIMAL(8,6),
                        vega                DECIMAL(8,6),
                        rho                 DECIMAL(8,6),
                        intrinsic_value     DECIMAL(12,4),
                        extrinsic_value     DECIMAL(12,4),
                        updated_unix        BIGINT,
                        time_value          DECIMAL(12,4),
                        created_at          TIMESTAMP DEFAULT NOW(),
                        updated_at          TIMESTAMP DEFAULT NOW(),
                        FOREIGN KEY (symbol_id) REFERENCES listing_status(symbol_id) ON DELETE CASCADE,
                        UNIQUE(symbol_id, contract_name, last_trade_date)
                    )
                    """
                    db.execute_query(create_table_sql)
                    print("Created historical_options table")
                    
                    # Create indexes
                    indexes_sql = [
                        "CREATE INDEX IF NOT EXISTS idx_historical_options_symbol_id ON historical_options(symbol_id)",
                        "CREATE INDEX IF NOT EXISTS idx_historical_options_symbol ON historical_options(symbol)",
                        "CREATE INDEX IF NOT EXISTS idx_historical_options_date ON historical_options(last_trade_date)",
                        "CREATE INDEX IF NOT EXISTS idx_historical_options_expiration ON historical_options(expiration)",
                        "CREATE INDEX IF NOT EXISTS idx_historical_options_type_strike ON historical_options(option_type, strike)",
                        "CREATE INDEX IF NOT EXISTS idx_historical_options_contract ON historical_options(contract_name)"
                    ]
                    for index_sql in indexes_sql:
                        db.execute_query(index_sql)
                    print("Created indexes for historical_options table")
                    
                except Exception as e:
                    print(f"Could not create historical_options table: {e}")
                    print("Please ensure the database schema is properly initialized")
                    raise
            
            # Query for symbols not processed for this specific date
            base_query = """
                SELECT ls.symbol_id, ls.symbol 
                FROM listing_status ls 
                LEFT JOIN historical_options ho ON ls.symbol_id = ho.symbol_id AND ho.last_trade_date = %s
                WHERE ls.asset_type = 'Stock' AND ho.symbol_id IS NULL
            """
            params = [target_date]
            
            if exchange_filter:
                if isinstance(exchange_filter, list):
                    placeholders = ','.join(['%s' for _ in exchange_filter])
                    base_query += f" AND ls.exchange IN ({placeholders})"
                    params.extend(exchange_filter)
                else:
                    base_query += " AND ls.exchange = %s"
                    params.append(exchange_filter)
            
            base_query += " GROUP BY ls.symbol_id, ls.symbol"
            
            if limit:
                base_query += " LIMIT %s"
                params.append(limit)
            
            result = db.fetch_query(base_query, params)
            return {row[1]: row[0] for row in result}
    
    def extract_single_historical_options(self, symbol, date=None):
        """Extract historical options data for a single symbol on a specific date."""
        print(f"Processing HISTORICAL OPTIONS for: {symbol} on {date or 'latest'}")
        
        # Build URL with optional date parameter
        url = f'{self.base_url}?function={OPTIONS_API_FUNCTION}&symbol={symbol}&apikey={self.api_key}'
        if date:
            url += f'&date={date}'
        
        print(f"Fetching data from: {url}")
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            
            print(f"Response status: {response.status_code}")
            
            # Parse JSON response
            data = response.json()
            
            # Check for API errors
            if 'Error Message' in data:
                print(f"API Error for {symbol}: {data['Error Message']}")
                return None, 'fail'
            
            if 'Note' in data:
                print(f"API Note for {symbol}: {data['Note']}")
                return None, 'rate_limit'
            
            if 'data' not in data:
                print(f"No options data found for {symbol}")
                return None, 'no_data'
            
            options_data = data['data']
            if not options_data:
                print(f"Empty options data for {symbol}")
                return None, 'no_data'
            
            print(f"Successfully fetched {len(options_data)} option contracts for {symbol}")
            return options_data, 'pass'
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {symbol}: {e}")
            return None, 'fail'
        except json.JSONDecodeError as e:
            print(f"JSON decode error for {symbol}: {e}")
            return None, 'fail'
        except Exception as e:
            print(f"Unexpected error for {symbol}: {e}")
            return None, 'fail'
    
    def transform_historical_options_data(self, symbol, symbol_id, options_data, status, target_date):
        """Transform historical options data to match database schema."""
        current_timestamp = datetime.now().isoformat()
        
        if status != 'pass' or not options_data:
            return []
        
        try:
            records = []
            
            for option in options_data:
                # Extract and validate required fields using actual API field names
                contract_name = option.get('contractID', '')
                if not contract_name:
                    continue
                
                # Get option type directly from API field
                option_type = option.get('type', '')
                if option_type not in ['call', 'put']:
                    continue
                
                # Parse numeric values safely
                def safe_float(value, default=None):
                    if value is None or value == '':
                        return default
                    try:
                        return float(value)
                    except (ValueError, TypeError):
                        return default
                
                def safe_int(value, default=None):
                    if value is None or value == '':
                        return default
                    try:
                        return int(value)
                    except (ValueError, TypeError):
                        return default
                
                # Create record using actual API field names
                record = {
                    'symbol_id': symbol_id,
                    'symbol': symbol,
                    'contract_name': contract_name,
                    'option_type': option_type,
                    'strike': safe_float(option.get('strike')),
                    'expiration': option.get('expiration'),
                    'last_trade_date': target_date,
                    'last_price': safe_float(option.get('last')),
                    'mark': safe_float(option.get('mark')),
                    'bid': safe_float(option.get('bid')),
                    'bid_size': safe_int(option.get('bid_size')),
                    'ask': safe_float(option.get('ask')),
                    'ask_size': safe_int(option.get('ask_size')),
                    'volume': safe_int(option.get('volume')),
                    'open_interest': safe_int(option.get('open_interest')),
                    'implied_volatility': safe_float(option.get('implied_volatility')),
                    'delta': safe_float(option.get('delta')),
                    'gamma': safe_float(option.get('gamma')),
                    'theta': safe_float(option.get('theta')),
                    'vega': safe_float(option.get('vega')),
                    'rho': safe_float(option.get('rho')),
                    'intrinsic_value': None,  # Not provided by this API endpoint
                    'extrinsic_value': None,  # Not provided by this API endpoint
                    'updated_unix': None,     # Not provided by this API endpoint
                    'time_value': None,       # Not provided by this API endpoint
                    'created_at': current_timestamp,
                    'updated_at': current_timestamp
                }
                
                records.append(record)
            
            print(f"Transformed {len(records)} option contracts for {symbol}")
            return records
            
        except Exception as e:
            print(f"Error transforming options data for {symbol}: {e}")
            return []
    
    def load_historical_options_data(self, records, db_connection=None):
        """Load historical options records into the database."""
        if not records:
            print("No records to load")
            return
        
        print(f"Loading {len(records)} option records into database...")
        
        # Use provided connection or create new one
        if db_connection:
            # Use existing connection
            db = db_connection
            # The table should already exist from load_unprocessed_symbols_for_date
            if not db.table_exists('historical_options'):
                raise Exception("historical_options table does not exist. Please check database schema.")
            
            # Prepare insert query with ON CONFLICT handling
            columns = list(records[0].keys())
            placeholders = ', '.join(['%s' for _ in columns])
            insert_query = f"""
                INSERT INTO historical_options ({', '.join(columns)}) 
                VALUES ({placeholders})
                ON CONFLICT (symbol_id, contract_name, last_trade_date) 
                DO UPDATE SET
                    last_price = EXCLUDED.last_price,
                    mark = EXCLUDED.mark,
                    bid = EXCLUDED.bid,
                    bid_size = EXCLUDED.bid_size,
                    ask = EXCLUDED.ask,
                    ask_size = EXCLUDED.ask_size,
                    volume = EXCLUDED.volume,
                    open_interest = EXCLUDED.open_interest,
                    implied_volatility = EXCLUDED.implied_volatility,
                    delta = EXCLUDED.delta,
                    gamma = EXCLUDED.gamma,
                    theta = EXCLUDED.theta,
                    vega = EXCLUDED.vega,
                    rho = EXCLUDED.rho,
                    intrinsic_value = EXCLUDED.intrinsic_value,
                    extrinsic_value = EXCLUDED.extrinsic_value,
                    updated_unix = EXCLUDED.updated_unix,
                    time_value = EXCLUDED.time_value,
                    updated_at = EXCLUDED.updated_at
            """
            
            # Convert records to list of tuples
            record_tuples = [tuple(record[col] for col in columns) for record in records]
            
            # Execute bulk insert
            rows_affected = db.execute_many(insert_query, record_tuples)
            print(f"Successfully loaded/updated {rows_affected} records into historical_options table")
        else:
            # Create new connection (fallback)
            with self.db_manager as db:
                self.load_historical_options_data(records, db)
    
    def run_etl_historical_date(self, target_date, exchange_filter=None, limit=None):
        """Run ETL for historical options data for a specific date.
        
        Args:
            target_date: Date to collect options data for (YYYY-MM-DD format)
            exchange_filter: Filter by exchange (e.g., 'NASDAQ', 'NYSE')
            limit: Maximum number of symbols to process (for chunking)
        """
        print("Starting Historical Options ETL process...")
        print(f"Configuration: date={target_date}, exchange={exchange_filter}, limit={limit}")
        
        try:
            # Use a single database connection throughout the entire process
            with self.db_manager as db:
                # Ensure the table exists first
                if not db.table_exists('historical_options'):
                    try:
                        # Try to create just the historical_options table
                        create_table_sql = """
                        CREATE TABLE IF NOT EXISTS historical_options (
                            option_id           SERIAL PRIMARY KEY,
                            symbol_id           INTEGER NOT NULL,
                            symbol              VARCHAR(20) NOT NULL,
                            contract_name       VARCHAR(50) NOT NULL,
                            option_type         VARCHAR(4) NOT NULL CHECK (option_type IN ('call', 'put')),
                            strike              DECIMAL(12,4) NOT NULL,
                            expiration          DATE NOT NULL,
                            last_trade_date     DATE NOT NULL,
                            last_price          DECIMAL(12,4),
                            mark                DECIMAL(12,4),
                            bid                 DECIMAL(12,4),
                            bid_size            INTEGER,
                            ask                 DECIMAL(12,4),
                            ask_size            INTEGER,
                            volume              BIGINT,
                            open_interest       BIGINT,
                            implied_volatility  DECIMAL(8,6),
                            delta               DECIMAL(8,6),
                            gamma               DECIMAL(8,6),
                            theta               DECIMAL(8,6),
                            vega                DECIMAL(8,6),
                            rho                 DECIMAL(8,6),
                            intrinsic_value     DECIMAL(12,4),
                            extrinsic_value     DECIMAL(12,4),
                            updated_unix        BIGINT,
                            time_value          DECIMAL(12,4),
                            created_at          TIMESTAMP DEFAULT NOW(),
                            updated_at          TIMESTAMP DEFAULT NOW(),
                            FOREIGN KEY (symbol_id) REFERENCES listing_status(symbol_id) ON DELETE CASCADE,
                            UNIQUE(symbol_id, contract_name, last_trade_date)
                        )
                        """
                        db.execute_query(create_table_sql)
                        print("Created historical_options table")
                        
                        # Create indexes
                        indexes_sql = [
                            "CREATE INDEX IF NOT EXISTS idx_historical_options_symbol_id ON historical_options(symbol_id)",
                            "CREATE INDEX IF NOT EXISTS idx_historical_options_symbol ON historical_options(symbol)",
                            "CREATE INDEX IF NOT EXISTS idx_historical_options_date ON historical_options(last_trade_date)",
                            "CREATE INDEX IF NOT EXISTS idx_historical_options_expiration ON historical_options(expiration)",
                            "CREATE INDEX IF NOT EXISTS idx_historical_options_type_strike ON historical_options(option_type, strike)",
                            "CREATE INDEX IF NOT EXISTS idx_historical_options_contract ON historical_options(contract_name)"
                        ]
                        for index_sql in indexes_sql:
                            db.execute_query(index_sql)
                        print("Created indexes for historical_options table")
                        
                    except Exception as e:
                        print(f"Could not create historical_options table: {e}")
                        print("Please ensure the database schema is properly initialized")
                        raise
                
                # Load symbols not yet processed for this date
                base_query = """
                    SELECT ls.symbol_id, ls.symbol 
                    FROM listing_status ls 
                    LEFT JOIN historical_options ho ON ls.symbol_id = ho.symbol_id AND ho.last_trade_date = %s
                    WHERE ls.asset_type = 'Stock' AND ho.symbol_id IS NULL
                """
                params = [target_date]
                
                if exchange_filter:
                    if isinstance(exchange_filter, list):
                        placeholders = ','.join(['%s' for _ in exchange_filter])
                        base_query += f" AND ls.exchange IN ({placeholders})"
                        params.extend(exchange_filter)
                    else:
                        base_query += " AND ls.exchange = %s"
                        params.append(exchange_filter)
                
                base_query += " GROUP BY ls.symbol_id, ls.symbol"
                
                if limit:
                    base_query += " LIMIT %s"
                    params.append(limit)
                
                result = db.fetch_query(base_query, params)
                symbol_mapping = {row[1]: row[0] for row in result}
                
                symbols = list(symbol_mapping.keys())
                print(f"Found {len(symbols)} symbols to process for {target_date}")
                
                if not symbols:
                    print(f"No unprocessed symbols found for {target_date}")
                    return
                
                total_records = 0
                success_count = 0
                fail_count = 0
                rate_limit_count = 0
                
                for i, symbol in enumerate(symbols):
                    symbol_id = symbol_mapping[symbol]
                    
                    try:
                        # Extract options data for this symbol and date
                        options_data, status = self.extract_single_historical_options(symbol, target_date)
                        
                        if status == 'rate_limit':
                            rate_limit_count += 1
                            print(f"⚠ Rate limited on {symbol} - waiting 60 seconds [{i+1}/{len(symbols)}]")
                            time.sleep(60)  # Wait a minute for rate limit
                            continue
                        
                        # Transform data
                        records = self.transform_historical_options_data(symbol, symbol_id, options_data, status, target_date)
                        
                        if records:
                            # Load records for this symbol using the same database connection
                            self.load_historical_options_data(records, db)
                            total_records += len(records)
                            success_count += 1
                            print(f"✓ Processed {symbol} (ID: {symbol_id}) - {len(records)} contracts [{i+1}/{len(symbols)}]")
                        else:
                            fail_count += 1
                            print(f"✗ Processed {symbol} (ID: {symbol_id}) - 0 contracts [{i+1}/{len(symbols)}]")
                        
                    except Exception as e:
                        fail_count += 1
                        print(f"✗ Error processing {symbol} (ID: {symbol_id}): {e} [{i+1}/{len(symbols)}]")
                        # Continue processing other symbols even if one fails
                        continue
                    
                    # Rate limiting - wait between requests
                    if i < len(symbols) - 1:
                        time.sleep(self.rate_limit_delay)
            
            # Print summary
            print(f"\n" + "="*60)
            print(f"Historical Options ETL Summary:")
            print(f"  Date: {target_date}")
            print(f"  Exchange: {exchange_filter or 'All exchanges'}")
            print(f"  Total symbols processed: {len(symbols)}")
            print(f"  Successful symbols: {success_count}")
            print(f"  Failed symbols: {fail_count}")
            print(f"  Rate limited symbols: {rate_limit_count}")
            print(f"  Total option contracts loaded: {total_records:,}")
            print(f"  Average contracts per symbol: {total_records/success_count if success_count > 0 else 0:.1f}")
            print(f"="*60)
            
            print("Historical Options ETL process completed successfully!")
            
        except Exception as e:
            print(f"Historical Options ETL process failed: {e}")
            raise
    
    def run_etl_date_range(self, start_date, end_date, exchange_filter=None, limit=None):
        """Run ETL for a range of historical dates.
        
        Args:
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format) 
            exchange_filter: Filter by exchange
            limit: Maximum number of symbols to process per date
        """
        from datetime import datetime, timedelta
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        current_dt = start_dt
        while current_dt <= end_dt:
            # Skip weekends (options markets closed)
            if current_dt.weekday() < 5:  # Monday=0, Friday=4
                target_date = current_dt.strftime('%Y-%m-%d')
                print(f"\n{'='*60}")
                print(f"Processing date: {target_date}")
                print(f"{'='*60}")
                
                try:
                    self.run_etl_historical_date(target_date, exchange_filter, limit)
                except Exception as e:
                    print(f"Error processing {target_date}: {e}")
                    # Continue with next date
            
            current_dt += timedelta(days=1)

def main():
    """Main function to run the historical options extraction."""
    
    # Configuration options for different use cases:
    
    # Option 1: Single date historical options collection
    extractor = HistoricalOptionsExtractor()
    
    # Get options data for a specific recent trading day
    target_date = "2024-12-31"  # Last trading day of 2024
    extractor.run_etl_historical_date(
        target_date=target_date,
        exchange_filter='NASDAQ',  # Start with NASDAQ
        limit=10  # Test with a small number first
    )
    
    # Option 2: Date range collection (uncomment to use)
    # extractor.run_etl_date_range(
    #     start_date="2024-12-20",
    #     end_date="2024-12-31", 
    #     exchange_filter='NASDAQ',
    #     limit=5
    # )
    
    # Option 3: Large batch for a single date (uncomment to use)
    # extractor.run_etl_historical_date(
    #     target_date="2024-12-31",
    #     exchange_filter=['NASDAQ', 'NYSE'],  # Multiple exchanges
    #     limit=50
    # )
    
    # Option 4: Weekly options data collection (uncomment to use)
    # recent_fridays = ["2024-12-27", "2024-12-20", "2024-12-13", "2024-12-06"]
    # for friday in recent_fridays:
    #     print(f"\nProcessing weekly options for {friday}")
    #     extractor.run_etl_historical_date(
    #         target_date=friday,
    #         exchange_filter='NASDAQ',
    #         limit=20
    #     )

if __name__ == "__main__":
    main()
