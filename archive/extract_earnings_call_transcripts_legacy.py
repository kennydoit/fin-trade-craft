"""
Extract earnings call transcripts data from Alpha Vantage API and load into database.
"""

import argparse
import hashlib
import os
import sys
import time
from datetime import datetime, date
from pathlib import Path

import requests
from dotenv import load_dotenv

# Add the parent directories to the path so we can import from db
sys.path.append(str(Path(__file__).parent.parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager

# -----------------------------------------------------------------------------
# Example commands (PowerShell):
# 
# Incremental update (default) - processes only missing quarters:
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_earnings_call_transcripts.py
# 
# Process specific exchange:
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_earnings_call_transcripts.py --exchange NASDAQ --limit 100
# 
# Dry run to estimate API calls:
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_earnings_call_transcripts.py --dry-run --exchange NYSE --limit 50
#
# Full replacement (re-processes all symbols):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_earnings_call_transcripts.py --mode replace --limit 10
# -----------------------------------------------------------------------------

STOCK_API_FUNCTION = "EARNINGS_CALL_TRANSCRIPT"

class EarningsCallTranscriptsExtractor:
    """Extract and load earnings call transcripts data from Alpha Vantage API."""

    def __init__(self, mode="update", retry_failed=False):
        """Initialize the extractor with specified mode and retry options.
        
        Args:
            mode (str): Operation mode - "update" for incremental updates (default), "replace" for full replacement
            retry_failed (bool): Whether to retry previously failed symbols (default: False)
        """
        if mode not in ["replace", "update"]:
            raise ValueError("Mode must be 'replace' or 'update'")
        
        self.mode = mode
        self.retry_failed = retry_failed
        
        # Load ALPHAVANTAGE_API_KEY from .env file
        load_dotenv()
        self.api_key = os.getenv('ALPHAVANTAGE_API_KEY')

        if not self.api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY not found in environment variables")

        self.db_manager = PostgresDatabaseManager()
        self.base_url = "https://www.alphavantage.co/query"

        # Rate limiting: 75 requests per minute for Alpha Vantage Premium
        self.rate_limit_delay = 0.8  # seconds between requests (75/min = 0.8s delay)

        # Precompute quarters from current quarter back to 2010Q1
        self.quarters = []
        current = datetime.now()
        current_year = current.year
        current_quarter = (current.month - 1) // 3 + 1
        for year in range(current_year, 2009, -1):
            if year == current_year:
                quarters_range = range(current_quarter, 0, -1)
            else:
                quarters_range = range(4, 0, -1)
            for quarter in quarters_range:
                self.quarters.append(f"{year}Q{quarter}")

        if self.quarters:
            print(
                f"Initialized with {len(self.quarters)} quarters from {self.quarters[0]} to {self.quarters[-1]}"
            )

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

    def load_unprocessed_symbols_with_db(self, db, exchange_filter=None, limit=None):
        """Load unprocessed symbols with IPO and listing status information."""
        # First ensure the table exists, or create just the table
        if not db.table_exists('extracted.earnings_call_transcripts'):
            # Create just the earnings_call_transcripts table in extracted schema
            create_table_sql = """
                CREATE TABLE IF NOT EXISTS extracted.earnings_call_transcripts (
                    transcript_id       SERIAL PRIMARY KEY,
                    symbol_id           INTEGER NOT NULL,
                    symbol              VARCHAR(20) NOT NULL,
                    quarter             VARCHAR(10) NOT NULL,
                    speaker             VARCHAR(255) NOT NULL,
                    title               VARCHAR(255),
                    content             TEXT NOT NULL,
                    content_hash        VARCHAR(32) NOT NULL,
                    sentiment           DECIMAL(5,3),
                    api_response_status VARCHAR(20) DEFAULT 'pass',
                    created_at          TIMESTAMP DEFAULT NOW(),
                    updated_at          TIMESTAMP DEFAULT NOW(),
                    FOREIGN KEY (symbol_id) REFERENCES listing_status(symbol_id) ON DELETE CASCADE,
                    UNIQUE(symbol_id, quarter, speaker, content_hash)
                );
                
                CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_symbol_id ON extracted.earnings_call_transcripts(symbol_id);
                CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_symbol ON extracted.earnings_call_transcripts(symbol);
                CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_quarter ON extracted.earnings_call_transcripts(quarter);
                CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_speaker ON extracted.earnings_call_transcripts(speaker);
                CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_sentiment ON extracted.earnings_call_transcripts(sentiment);
            """

            # Create trigger separately if the update function exists
            trigger_sql = """
                DO $$
                BEGIN
                    IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'update_updated_at_column') THEN
                        CREATE TRIGGER update_earnings_call_transcripts_updated_at 
                        BEFORE UPDATE ON extracted.earnings_call_transcripts 
                        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
                    END IF;
                EXCEPTION WHEN others THEN
                    -- Trigger may already exist, ignore error
                END $$;
            """

            try:
                db.execute_query(create_table_sql)
                print("Created extracted.earnings_call_transcripts table")
                # Try to create trigger separately
                try:
                    db.execute_query(trigger_sql)
                    print("Created extracted.earnings_call_transcripts trigger")
                except Exception as te:
                    print(f"Note: Could not create trigger (may already exist): {te}")
            except Exception as e:
                print(f"Warning: Could not create extracted.earnings_call_transcripts table: {e}")

        # Now query for unprocessed symbols with their IPO dates and status info
        base_query = """
            SELECT ls.symbol_id, ls.symbol, ls.ipo_date, ls.status, ls.delisting_date
            FROM listing_status ls
            LEFT JOIN extracted.earnings_call_transcripts ect ON ls.symbol_id = ect.symbol_id
            WHERE ls.asset_type = 'Stock' AND ect.symbol_id IS NULL
        """
        params = []

        if exchange_filter:
            if isinstance(exchange_filter, list):
                placeholders = ','.join(['%s' for _ in exchange_filter])
                base_query += f" AND ls.exchange IN ({placeholders})"
                params.extend(exchange_filter)
            else:
                base_query += " AND ls.exchange = %s"
                params.append(exchange_filter)

        base_query += " GROUP BY ls.symbol_id, ls.symbol, ls.ipo_date, ls.status, ls.delisting_date"

        if limit:
            base_query += " LIMIT %s"
            params.append(limit)

        result = db.fetch_query(base_query, params)
        # Return dictionary with symbol as key and tuple of (symbol_id, ipo_date, status, delisting_date)
        return {row[1]: (row[0], row[2], row[3], row[4]) for row in result}

    def get_failed_symbols(self, db, exchange_filter=None):
        """Get symbols that have failed extractions (4+ consecutive no_data or error quarters)."""
        # A symbol is considered "failed" if it has 4+ consecutive quarters with no_data/error status
        # and no successful quarters after the failures
        query = """
        WITH symbol_quarters AS (
            SELECT 
                symbol_id,
                symbol,
                quarter,
                api_response_status,
                ROW_NUMBER() OVER (PARTITION BY symbol_id ORDER BY 
                    CAST(SUBSTRING(quarter, 1, 4) AS INTEGER) DESC,
                    CAST(SUBSTRING(quarter, 6, 1) AS INTEGER) DESC
                ) as quarter_rank
            FROM extracted.earnings_call_transcripts ect
            WHERE api_response_status IN ('no_data', 'error')
        ),
        consecutive_failures AS (
            SELECT 
                symbol_id,
                symbol,
                COUNT(*) as consecutive_fails
            FROM symbol_quarters
            WHERE quarter_rank <= 4  -- Check most recent 4 quarters
            GROUP BY symbol_id, symbol
            HAVING COUNT(*) >= 4  -- 4 or more consecutive failures
        ),
        symbols_with_success AS (
            SELECT DISTINCT symbol_id
            FROM extracted.earnings_call_transcripts
            WHERE api_response_status = 'pass'
        )
        SELECT cf.symbol_id, cf.symbol
        FROM consecutive_failures cf
        LEFT JOIN symbols_with_success sws ON cf.symbol_id = sws.symbol_id
        WHERE sws.symbol_id IS NULL  -- No successful quarters
        """
        
        if exchange_filter:
            query += """
            AND cf.symbol_id IN (
                SELECT symbol_id FROM extracted.listing_status 
                WHERE asset_type = 'Stock' 
            """
            
            if isinstance(exchange_filter, list):
                placeholders = ','.join(['%s' for _ in exchange_filter])
                query += f" AND exchange IN ({placeholders})"
                result = db.fetch_query(query + ")", exchange_filter)
            else:
                query += " AND exchange = %s"
                result = db.fetch_query(query + ")", [exchange_filter])
        else:
            result = db.fetch_query(query)
        
        return set(row[0] for row in result) if result else set()

    def load_symbols_for_processing(self, db, exchange_filter=None, limit=None):
        """Load symbols for processing based on mode and retry_failed setting."""
        if self.mode == "replace":
            symbol_mapping = self.load_valid_symbols_for_replacement(db, exchange_filter, limit)
        else:
            symbol_mapping = self.load_unprocessed_symbols_with_db(db, exchange_filter, limit)
        
        # Apply retry_failed filtering
        if not self.retry_failed:
            # Exclude failed symbols
            failed_symbol_ids = self.get_failed_symbols(db, exchange_filter)
            if failed_symbol_ids:
                print(f"🚫 Excluding {len(failed_symbol_ids)} previously failed symbols (use --retry-failed to include them)")
                # Filter out failed symbols
                symbol_mapping = {
                    symbol: data for symbol, data in symbol_mapping.items() 
                    if data[0] not in failed_symbol_ids  # data[0] is symbol_id
                }
        else:
            failed_symbol_ids = self.get_failed_symbols(db, exchange_filter)
            if failed_symbol_ids:
                print(f"🔄 Including {len(failed_symbol_ids)} previously failed symbols for retry")
        
        return symbol_mapping

    def load_valid_symbols_for_replacement(self, db, exchange_filter=None, limit=None):
        """Load all valid symbols for replacement mode (ignores existing data)."""
        base_query = "SELECT symbol_id, symbol, ipo_date, status, delisting_date FROM extracted.listing_status WHERE asset_type = 'Stock'"
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
        return {row[1]: (row[0], row[2], row[3], row[4]) for row in result}

    def get_existing_quarters_for_symbol(self, db, symbol_id):
        """Get quarters that already have data for a symbol."""
        if self.mode == "replace":
            return set()  # In replace mode, ignore existing data
            
        query = """
        SELECT DISTINCT quarter 
        FROM extracted.earnings_call_transcripts 
        WHERE symbol_id = %s AND api_response_status IN ('pass', 'no_data', 'error')
        """
        
        result = db.fetch_query(query, (symbol_id,))
        return set(row[0] for row in result) if result else set()

    def calculate_api_calls_needed(self, db, exchange_filter=None, limit=None):
        """Calculate how many API calls would be needed for the current run."""
        print("🧮 Calculating API calls needed...")
        
        # Load symbols that would be processed
        symbol_mapping = self.load_symbols_for_processing(db, exchange_filter, limit)
        symbols = list(symbol_mapping.keys())
        
        if not symbols:
            print("No symbols found for processing")
            return 0, {}
        
        total_calls = 0
        symbol_breakdown = {}
        
        for symbol in symbols:
            symbol_id, ipo_date, status, delisting_date = symbol_mapping[symbol]
            
            # Get quarters for this symbol
            symbol_quarters = self.get_quarters_for_symbol(ipo_date, delisting_date)
            
            # Get existing quarters (empty set for replace mode)
            existing_quarters = self.get_existing_quarters_for_symbol(db, symbol_id)
            
            # Calculate missing quarters
            missing_quarters = [q for q in symbol_quarters if q not in existing_quarters]
            
            calls_for_symbol = len(missing_quarters)
            total_calls += calls_for_symbol
            
            if calls_for_symbol > 0:
                symbol_breakdown[symbol] = {
                    'total_quarters': len(symbol_quarters),
                    'existing_quarters': len(existing_quarters),
                    'missing_quarters': calls_for_symbol,
                    'quarters_list': missing_quarters[:5] + (['...'] if len(missing_quarters) > 5 else [])
                }
        
        return total_calls, symbol_breakdown
        """Get count of remaining unprocessed symbols using provided database connection."""
        base_query = """
            SELECT COUNT(DISTINCT ls.symbol_id)
            FROM listing_status ls 
            LEFT JOIN extracted.earnings_call_transcripts ect ON ls.symbol_id = ect.symbol_id 
            WHERE ls.asset_type = 'Stock' AND ect.symbol_id IS NULL
        """
        params = []

        if exchange_filter:
            if isinstance(exchange_filter, list):
                placeholders = ','.join(['%s' for _ in exchange_filter])
                base_query += f" AND ls.exchange IN ({placeholders})"
                params.extend(exchange_filter)
            else:
                base_query += " AND ls.exchange = %s"
                params.append(exchange_filter)

        return db.fetch_query(base_query, params)[0][0]

    def extract_single_earnings_call_transcript(self, symbol, quarter):
        """Extract earnings call transcript data for a single symbol and quarter."""
        print(f"Processing TICKER: {symbol}, QUARTER: {quarter}")

        url = f'{self.base_url}?function={STOCK_API_FUNCTION}&symbol={symbol}&quarter={quarter}&apikey={self.api_key}'
        print(f"Fetching data from: {url}")

        try:
            response = requests.get(url)
            response.raise_for_status()

            print(f"Response status: {response.status_code}")
            data = response.json()

            # Check for API errors
            if 'Error Message' in data:
                print(f"API Error for {symbol} {quarter}: {data['Error Message']}")
                return None, 'fail'

            if 'Note' in data:
                print(f"API Note for {symbol} {quarter}: {data['Note']}")
                return None, 'fail'

            # Check if we have transcript data
            if 'transcript' not in data or not data['transcript']:
                print(f"No earnings call transcript data found for {symbol} {quarter}")
                return None, 'no_data'

            print(f"Successfully fetched {len(data['transcript'])} transcript entries for {symbol} {quarter}")
            return data, 'pass'

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {symbol} {quarter}: {e}")
            return None, 'fail'
        except Exception as e:
            print(f"Unexpected error for {symbol} {quarter}: {e}")
            return None, 'fail'

    def transform_earnings_call_transcript_data(self, symbol, symbol_id, quarter, data, status):
        """Transform earnings call transcript data to match database schema."""
        current_timestamp = datetime.now().isoformat()

        if status == 'no_data':
            # Create no data record
            return [{
                'symbol_id': symbol_id,
                'symbol': symbol,
                'quarter': quarter,
                'speaker': 'NO_DATA',
                'title': None,
                'content': 'No transcript data available',
                'content_hash': hashlib.md5(b'No transcript data available').hexdigest(),
                'sentiment': None,
                'api_response_status': 'no_data',
                'created_at': current_timestamp,
                'updated_at': current_timestamp
            }]

        if status == 'fail' or data is None:
            # Create error record
            return [{
                'symbol_id': symbol_id,
                'symbol': symbol,
                'quarter': quarter,
                'speaker': 'ERROR',
                'title': None,
                'content': 'API Error',
                'content_hash': hashlib.md5(b'API Error').hexdigest(),
                'sentiment': None,
                'api_response_status': 'error',
                'created_at': current_timestamp,
                'updated_at': current_timestamp
            }]

        try:
            records = []

            for transcript_entry in data['transcript']:
                # Helper function to convert sentiment values
                def convert_sentiment(value):
                    if value is None or value == '' or value == 'None':
                        return None
                    try:
                        return float(value)
                    except (ValueError, TypeError):
                        return None

                record = {
                    'symbol_id': symbol_id,
                    'symbol': symbol,
                    'quarter': quarter,
                    'speaker': transcript_entry.get('speaker', 'Unknown'),
                    'title': transcript_entry.get('title'),
                    'content': transcript_entry.get('content', ''),
                    'content_hash': hashlib.md5(transcript_entry.get('content', '').encode()).hexdigest(),
                    'sentiment': convert_sentiment(transcript_entry.get('sentiment')),
                    'api_response_status': 'pass',
                    'created_at': current_timestamp,
                    'updated_at': current_timestamp
                }

                records.append(record)

            print(f"Transformed {len(records)} transcript records for {symbol} {quarter}")
            return records

        except Exception as e:
            print(f"Error transforming data for {symbol} {quarter}: {e}")
            return []

    def load_earnings_call_transcript_data_with_db(self, db_manager, records):
        """Load earnings call transcript records into the database using provided database manager."""
        if not records:
            print("No records to load")
            return

        print(f"Loading {len(records)} records into database...")

        # The table should already exist from load_unprocessed_symbols_with_db
        if not db_manager.table_exists('extracted.earnings_call_transcripts'):
            print("Warning: extracted.earnings_call_transcripts table doesn't exist, creating it...")
            # Create just the earnings_call_transcripts table in extracted schema
            create_table_sql = """
                CREATE TABLE IF NOT EXISTS extracted.earnings_call_transcripts (
                    transcript_id       SERIAL PRIMARY KEY,
                    symbol_id           INTEGER NOT NULL,
                    symbol              VARCHAR(20) NOT NULL,
                    quarter             VARCHAR(10) NOT NULL,
                    speaker             VARCHAR(255) NOT NULL,
                    title               VARCHAR(255),
                    content             TEXT NOT NULL,
                    content_hash        VARCHAR(32) NOT NULL,
                    sentiment           DECIMAL(5,3),
                    api_response_status VARCHAR(20) DEFAULT 'pass',
                    created_at          TIMESTAMP DEFAULT NOW(),
                    updated_at          TIMESTAMP DEFAULT NOW(),
                    FOREIGN KEY (symbol_id) REFERENCES listing_status(symbol_id) ON DELETE CASCADE,
                    UNIQUE(symbol_id, quarter, speaker, content_hash)
                );
            """
            db_manager.execute_query(create_table_sql)

        # Prepare insert query using PostgreSQL syntax
        columns = list(records[0].keys())
        placeholders = ', '.join(['%s' for _ in columns])
        insert_query = f"""
            INSERT INTO extracted.earnings_call_transcripts ({', '.join(columns)}) 
            VALUES ({placeholders})
            ON CONFLICT (symbol_id, quarter, speaker, content_hash) 
            DO UPDATE SET
                title = EXCLUDED.title,
                content = EXCLUDED.content,
                sentiment = EXCLUDED.sentiment,
                api_response_status = EXCLUDED.api_response_status,
                updated_at = EXCLUDED.updated_at
        """

        # Convert records to list of tuples
        record_tuples = [tuple(record[col] for col in columns) for record in records]

        # Execute bulk insert
        rows_affected = db_manager.execute_many(insert_query, record_tuples)
        print(f"Successfully loaded {rows_affected} records into extracted.earnings_call_transcripts table")

    def run_etl_incremental(self, exchange_filter=None, limit=None, dry_run=False):
        """Run ETL for earnings call transcripts.
        
        Args:
            exchange_filter: Filter by exchange (e.g., 'NASDAQ', 'NYSE')
            limit: Maximum number of symbols to process (for chunking)
            dry_run: If True, only calculate and display API call estimates
        """
        mode_desc = "REPLACE (re-process all)" if self.mode == "replace" else "UPDATE (process missing only)"
        retry_desc = "INCLUDE failed" if self.retry_failed else "EXCLUDE failed"
        print(f"Starting Earnings Call Transcripts ETL process in {mode_desc} mode...")
        print(f"Failed symbol handling: {retry_desc}")
        print(f"Configuration: exchange={exchange_filter}, limit={limit}, dry_run={dry_run}")

        try:
            # Use a fresh database manager for this ETL run
            db_manager = PostgresDatabaseManager()
            with db_manager as db:
                # Ensure table exists first
                if not db.table_exists('extracted.earnings_call_transcripts'):
                    self.create_earnings_call_transcripts_table(db)
                
                # Calculate API calls needed
                total_api_calls, symbol_breakdown = self.calculate_api_calls_needed(db, exchange_filter, limit)
                
                # Display dry-run results
                if dry_run:
                    self.display_dry_run_results(total_api_calls, symbol_breakdown, exchange_filter, limit)
                    return
                
                # Load symbols for processing
                symbol_mapping = self.load_symbols_for_processing(db, exchange_filter, limit)
                symbols = list(symbol_mapping.keys())
                print(f"Found {len(symbols)} symbols for processing")

                if not symbols:
                    print("No symbols found for processing")
                    return

                total_records = 0
                success_count = 0
                fail_count = 0
                actual_api_calls = 0

                for i, symbol in enumerate(symbols):
                    symbol_id, ipo_date, status, delisting_date = symbol_mapping[symbol]
                    symbol_records = []

                    print(f"\n--- Processing symbol {symbol} (ID: {symbol_id}) [{i+1}/{len(symbols)}] ---")

                    # Get quarters for this symbol
                    symbol_quarters = self.get_quarters_for_symbol(ipo_date, delisting_date)
                    
                    # Get existing quarters to avoid duplicates
                    existing_quarters = self.get_existing_quarters_for_symbol(db, symbol_id)
                    
                    # Filter out already processed quarters
                    missing_quarters = [q for q in symbol_quarters if q not in existing_quarters]
                    
                    if not missing_quarters:
                        print(f"  All quarters already processed for {symbol}, skipping...")
                        continue
                    
                    print(f"  Processing {len(missing_quarters)} missing quarters (out of {len(symbol_quarters)} total)")
                    if existing_quarters:
                        print(f"  Skipping {len(existing_quarters)} existing quarters")

                    consecutive_no_data = 0

                    # Process only missing quarters for this symbol
                    for quarter in missing_quarters:
                        actual_api_calls += 1

                        try:
                            data, status_code = self.extract_single_earnings_call_transcript(symbol, quarter)

                            records = self.transform_earnings_call_transcript_data(
                                symbol, symbol_id, quarter, data, status_code
                            )

                            if records:
                                symbol_records.extend(records)
                                if status_code == 'pass':
                                    print(f"✓ Processed {symbol} {quarter} (ID: {symbol_id}) - {len(records)} records [API: {actual_api_calls}/{total_api_calls}]")
                                elif status_code == 'no_data':
                                    print(f"○ Processed {symbol} {quarter} (ID: {symbol_id}) - No data available [API: {actual_api_calls}/{total_api_calls}]")
                                else:
                                    print(f"! Processed {symbol} {quarter} (ID: {symbol_id}) - {status_code} [API: {actual_api_calls}/{total_api_calls}]")
                            else:
                                print(f"✗ Processed {symbol} {quarter} (ID: {symbol_id}) - Transform failed [API: {actual_api_calls}/{total_api_calls}]")

                            # Track consecutive missing quarters
                            if status_code == 'no_data':
                                consecutive_no_data += 1
                            else:
                                consecutive_no_data = 0

                            # Early stopping optimization
                            if consecutive_no_data >= 4:
                                print(f"  Reached 4 consecutive missing quarters for {symbol}, stopping early")
                                break

                        except Exception as e:
                            print(f"✗ Error processing {symbol} {quarter} (ID: {symbol_id}): {e} [API: {actual_api_calls}/{total_api_calls}]")
                            continue

                        # Rate limiting between requests
                        if actual_api_calls < total_api_calls:
                            time.sleep(self.rate_limit_delay)

                    # Load all records for this symbol at once
                    if symbol_records:
                        self.load_earnings_call_transcript_data_with_db(db, symbol_records)
                        total_records += len(symbol_records)
                        success_count += 1
                        print(f"✓ Completed symbol {symbol} - {len(symbol_records)} total records")
                    else:
                        if missing_quarters:  # Only count as fail if there were quarters to process
                            fail_count += 1
                            print(f"✗ Completed symbol {symbol} - 0 total records")

                # Get remaining symbols count for summary
                remaining_count = self.get_remaining_symbols_count_with_db(db, exchange_filter)

            # Print summary
            print("\n" + "="*60)
            print("Earnings Call Transcripts ETL Summary:")
            print(f"  Mode: {mode_desc}")
            print(f"  Exchange: {exchange_filter or 'All exchanges'}")
            print(f"  Total symbols processed: {len(symbols)}")
            print(f"  Successful symbols: {success_count}")
            print(f"  Failed symbols: {fail_count}")
            print(f"  Remaining symbols: {remaining_count}")
            print(f"  Estimated API calls: {total_api_calls}")
            print(f"  Actual API calls made: {actual_api_calls}")
            print(f"  API call efficiency: {(actual_api_calls/total_api_calls*100):.1f}%" if total_api_calls > 0 else "N/A")
            print(f"  Total records loaded: {total_records:,}")
            print(f"  Average records per symbol: {total_records/success_count if success_count > 0 else 0:.1f}")
            print("="*60)

            print("Earnings Call Transcripts ETL process completed successfully!")

        except Exception as e:
            print(f"Earnings Call Transcripts ETL process failed: {e}")
            raise

    def display_dry_run_results(self, total_api_calls, symbol_breakdown, exchange_filter, limit):
        """Display dry-run results showing estimated API calls."""
        mode_desc = "REPLACE (re-process all)" if self.mode == "replace" else "UPDATE (process missing only)"
        retry_desc = "INCLUDE failed" if self.retry_failed else "EXCLUDE failed"
        
        print("\n" + "="*70)
        print("🧮 DRY RUN RESULTS - API CALL ESTIMATION")
        print("="*70)
        print(f"Mode: {mode_desc}")
        print(f"Failed symbol handling: {retry_desc}")
        print(f"Exchange filter: {exchange_filter or 'All exchanges'}")
        print(f"Symbol limit: {limit or 'No limit'}")
        print(f"📊 Total estimated API calls: {total_api_calls:,}")
        
        if total_api_calls > 0:
            # Calculate time estimates
            estimated_time_minutes = (total_api_calls * self.rate_limit_delay) / 60
            estimated_time_hours = estimated_time_minutes / 60
            
            print(f"⏱️  Estimated time at {self.rate_limit_delay}s per call:")
            print(f"   - {estimated_time_minutes:.1f} minutes")
            print(f"   - {estimated_time_hours:.1f} hours")
            
            # Cost estimates (if applicable)
            if total_api_calls > 500:  # Free tier limit
                premium_calls = total_api_calls - 500
                print(f"💰 API usage:")
                print(f"   - Free tier: 500 calls")
                print(f"   - Premium calls needed: {premium_calls:,}")
        
        print(f"\n📋 Symbol breakdown (showing symbols needing processing):")
        
        symbols_needing_processing = {k: v for k, v in symbol_breakdown.items() if v['missing_quarters'] > 0}
        
        if not symbols_needing_processing:
            if self.retry_failed:
                print("   ✅ No symbols need processing - all data is current (including previously failed)!")
            else:
                print("   ✅ No symbols need processing - all data is current!")
                print("   💡 Use --retry-failed to include previously failed symbols")
        else:
            # Show top 10 symbols with most missing quarters
            sorted_symbols = sorted(symbols_needing_processing.items(), 
                                   key=lambda x: x[1]['missing_quarters'], reverse=True)
            
            print(f"   Showing top {min(10, len(sorted_symbols))} symbols with most missing quarters:")
            print(f"   {'Symbol':<8} {'Missing':<8} {'Existing':<9} {'Total':<7} {'Sample Quarters'}")
            print(f"   {'-'*8} {'-'*8} {'-'*9} {'-'*7} {'-'*20}")
            
            for symbol, info in sorted_symbols[:10]:
                quarters_sample = ', '.join(map(str, info['quarters_list']))
                print(f"   {symbol:<8} {info['missing_quarters']:<8} {info['existing_quarters']:<9} {info['total_quarters']:<7} {quarters_sample}")
            
            if len(sorted_symbols) > 10:
                print(f"   ... and {len(sorted_symbols) - 10} more symbols")
        
        print("="*70)
        print("💡 To proceed with actual extraction, run without --dry-run flag")
        print("="*70)

    def create_earnings_call_transcripts_table(self, db):
        """Create the earnings_call_transcripts table if it doesn't exist."""
        create_table_sql = """
            CREATE SCHEMA IF NOT EXISTS extracted;
            
            CREATE TABLE IF NOT EXISTS extracted.earnings_call_transcripts (
                transcript_id       SERIAL PRIMARY KEY,
                symbol_id           INTEGER NOT NULL,
                symbol              VARCHAR(20) NOT NULL,
                quarter             VARCHAR(10) NOT NULL,
                speaker             VARCHAR(255) NOT NULL,
                title               VARCHAR(255),
                content             TEXT NOT NULL,
                content_hash        VARCHAR(32) NOT NULL,
                sentiment           DECIMAL(5,3),
                api_response_status VARCHAR(20) DEFAULT 'pass',
                created_at          TIMESTAMP DEFAULT NOW(),
                updated_at          TIMESTAMP DEFAULT NOW(),
                FOREIGN KEY (symbol_id) REFERENCES extracted.listing_status(symbol_id) ON DELETE CASCADE,
                UNIQUE(symbol_id, quarter, speaker, content_hash)
            );
            
            CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_symbol_id ON extracted.earnings_call_transcripts(symbol_id);
            CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_symbol ON extracted.earnings_call_transcripts(symbol);
            CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_quarter ON extracted.earnings_call_transcripts(quarter);
            CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_speaker ON extracted.earnings_call_transcripts(speaker);
            CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_sentiment ON extracted.earnings_call_transcripts(sentiment);
        """
        
        db.execute_query(create_table_sql)
        print("Created extracted.earnings_call_transcripts table with indexes")

    def get_remaining_symbols_count_with_db(self, db, exchange_filter=None):
        """Get count of remaining unprocessed symbols using provided database connection."""
        if self.mode == "replace":
            # In replace mode, count all symbols (since we re-process everything)
            base_query = """
                SELECT COUNT(DISTINCT symbol_id)
                FROM extracted.listing_status 
                WHERE asset_type = 'Stock'
            """
        else:
            # In update mode, count only symbols with no data
            base_query = """
                SELECT COUNT(DISTINCT ls.symbol_id)
                FROM extracted.listing_status ls 
                LEFT JOIN extracted.earnings_call_transcripts ect ON ls.symbol_id = ect.symbol_id 
                WHERE ls.asset_type = 'Stock' AND ect.symbol_id IS NULL
            """
        
        params = []

        if exchange_filter:
            if isinstance(exchange_filter, list):
                placeholders = ','.join(['%s' for _ in exchange_filter])
                base_query += f" AND ls.exchange IN ({placeholders})" if "ls.exchange" in base_query else f" AND exchange IN ({placeholders})"
                params.extend(exchange_filter)
            else:
                base_query += f" AND ls.exchange = %s" if "ls.exchange" in base_query else f" AND exchange = %s"
                params.append(exchange_filter)

        return db.fetch_query(base_query, params)[0][0]

    def get_quarters_for_symbol(self, ipo_date, delisting_date=None):
        """Generate quarters for a symbol in reverse chronological order.

        Starts from the most recent quarter (or delisting quarter if provided)
        and moves backwards to either the IPO quarter or 2010Q1, whichever is later.
        """

        def parse_date(d):
            if not d:
                return None
            if isinstance(d, str):
                try:
                    return datetime.strptime(d, "%Y-%m-%d").date()
                except ValueError:
                    return None
            if isinstance(d, datetime):
                return d.date()
            return d

        def to_year_quarter(dt: date):
            return dt.year, (dt.month - 1) // 3 + 1

        ipo_dt = parse_date(ipo_date)
        delist_dt = parse_date(delisting_date)
        start_dt = delist_dt or datetime.now().date()
        earliest_dt = max(date(2010, 1, 1), ipo_dt) if ipo_dt else date(2010, 1, 1)

        start_year, start_quarter = to_year_quarter(start_dt)
        end_year, end_quarter = to_year_quarter(earliest_dt)

        quarters = []
        year, quarter = start_year, start_quarter
        while year > end_year or (year == end_year and quarter >= end_quarter):
            quarters.append(f"{year}Q{quarter}")
            quarter -= 1
            if quarter == 0:
                quarter = 4
                year -= 1

        if quarters:
            print(
                f"  Generated {len(quarters)} quarters: {quarters[0]} to {quarters[-1]}"
            )
        return quarters

def main():
    """Main function to run the earnings call transcripts extraction with CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Extract earnings call transcripts data from Alpha Vantage API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Incremental update (default) - process only missing quarters
  python extract_earnings_call_transcripts.py
  
  # Process specific exchange with limit
  python extract_earnings_call_transcripts.py --exchange NASDAQ --limit 100
  
  # Dry run to estimate API calls
  python extract_earnings_call_transcripts.py --dry-run --exchange NYSE --limit 50
  
  # Full replacement mode (re-process all symbols)
  python extract_earnings_call_transcripts.py --mode replace --limit 10
  
  # Include previously failed symbols for retry
  python extract_earnings_call_transcripts.py --retry-failed --limit 20
  
  # Dry run with failed symbols included
  python extract_earnings_call_transcripts.py --dry-run --retry-failed --exchange NASDAQ
        """
    )
    
    parser.add_argument(
        "--mode",
        choices=["update", "replace"],
        default="update",
        help="Operation mode: 'update' for incremental (default), 'replace' for full re-processing"
    )
    
    parser.add_argument(
        "--exchange",
        help="Filter by exchange (e.g., NASDAQ, NYSE, AMEX)"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of symbols to process"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Calculate and display API call estimates without making actual calls"
    )
    
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Include previously failed symbols in processing (default: exclude failed symbols)"
    )
    
    args = parser.parse_args()
    
    print(f"🚀 Starting Earnings Call Transcripts Extractor")
    print(f"   Mode: {args.mode.upper()}")
    print(f"   Exchange: {args.exchange or 'All'}")
    print(f"   Limit: {args.limit or 'No limit'}")
    print(f"   Dry run: {'Yes' if args.dry_run else 'No'}")
    print(f"   Retry failed: {'Yes' if args.retry_failed else 'No'}")
    
    try:
        extractor = EarningsCallTranscriptsExtractor(mode=args.mode, retry_failed=args.retry_failed)
        extractor.run_etl_incremental(
            exchange_filter=args.exchange,
            limit=args.limit,
            dry_run=args.dry_run
        )
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
