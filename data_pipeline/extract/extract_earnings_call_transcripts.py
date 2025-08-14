"""
Extract earnings call transcripts data from Alpha Vantage API and load into database.
"""

import hashlib
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

# Add the parent directories to the path so we can import from db
sys.path.append(str(Path(__file__).parent.parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager

STOCK_API_FUNCTION = "EARNINGS_CALL_TRANSCRIPT"

class EarningsCallTranscriptsExtractor:
    """Extract and load earnings call transcripts data from Alpha Vantage API."""

    def __init__(self):
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

    def get_remaining_symbols_count_with_db(self, db, exchange_filter=None):
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

    def run_etl_incremental(self, exchange_filter=None, limit=None, quarters_to_process=None):
        """Run ETL only for symbols not yet processed.
        
        Args:
            exchange_filter: Filter by exchange (e.g., 'NASDAQ', 'NYSE')
            limit: Maximum number of symbols to process (for chunking)
            quarters_to_process: List of quarters to process (if None, uses IPO-based quarters for each symbol)
        """
        print("Starting Incremental Earnings Call Transcripts ETL process...")
        print(f"Configuration: exchange={exchange_filter}, limit={limit}")

        use_ipo_based_quarters = quarters_to_process is None
        if use_ipo_based_quarters:
            print("Using IPO-based quarter calculation for each symbol")
        else:
            print(f"Using provided quarters: {len(quarters_to_process)} quarters from {quarters_to_process[0] if quarters_to_process else 'none'} to {quarters_to_process[-1] if quarters_to_process else 'none'}")

        try:
            # Use a fresh database manager for this ETL run
            db_manager = PostgresDatabaseManager()
            with db_manager as db:
                # Load unprocessed symbols with IPO and status information
                symbol_mapping = self.load_unprocessed_symbols_with_db(db, exchange_filter, limit)
                symbols = list(symbol_mapping.keys())
                print(f"Found {len(symbols)} unprocessed symbols")

                if not symbols:
                    print("No unprocessed symbols found")
                    return

                total_records = 0
                success_count = 0
                fail_count = 0
                total_calls = 0
                call_count = 0

                for i, symbol in enumerate(symbols):
                    symbol_id, ipo_date, status, delisting_date = symbol_mapping[symbol]
                    symbol_records = []

                    print(
                        f"\n--- Processing symbol {symbol} (ID: {symbol_id}) [{i+1}/{len(symbols)}] ---"
                    )

                    # Determine quarters to process for this symbol
                    if use_ipo_based_quarters:
                        symbol_quarters = self.get_quarters_for_symbol(
                            ipo_date, delisting_date
                        )
                    else:
                        symbol_quarters = quarters_to_process

                    total_calls += len(symbol_quarters)
                    consecutive_no_data = 0

                    # Process quarters for this symbol from most recent backwards
                    for quarter in symbol_quarters:
                        call_count += 1

                        try:
                            data, status_code = self.extract_single_earnings_call_transcript(
                                symbol, quarter
                            )

                            records = self.transform_earnings_call_transcript_data(
                                symbol, symbol_id, quarter, data, status_code
                            )

                            if records:
                                symbol_records.extend(records)
                                if status_code == 'pass':
                                    print(
                                        f"✓ Processed {symbol} {quarter} (ID: {symbol_id}) - {len(records)} records [{call_count}/{total_calls}]"
                                    )
                                elif status_code == 'no_data':
                                    print(
                                        f"○ Processed {symbol} {quarter} (ID: {symbol_id}) - No data available [{call_count}/{total_calls}]"
                                    )
                                else:
                                    print(
                                        f"! Processed {symbol} {quarter} (ID: {symbol_id}) - {status_code} [{call_count}/{total_calls}]"
                                    )
                            else:
                                print(
                                    f"✗ Processed {symbol} {quarter} (ID: {symbol_id}) - Transform failed [{call_count}/{total_calls}]"
                                )

                            # Track consecutive missing quarters
                            if status_code == 'no_data':
                                consecutive_no_data += 1
                            else:
                                consecutive_no_data = 0

                            if consecutive_no_data >= 4:
                                print(
                                    f"  Reached 4 consecutive missing quarters for {symbol}, stopping early"
                                )
                                break

                        except Exception as e:
                            print(
                                f"✗ Error processing {symbol} {quarter} (ID: {symbol_id}): {e} [{call_count}/{total_calls}]"
                            )
                            continue

                        if call_count < total_calls:
                            time.sleep(self.rate_limit_delay)

                    # Load all records for this symbol at once
                    if symbol_records:
                        self.load_earnings_call_transcript_data_with_db(db, symbol_records)
                        total_records += len(symbol_records)
                        success_count += 1
                        print(f"✓ Completed symbol {symbol} - {len(symbol_records)} total records")
                    else:
                        fail_count += 1
                        print(f"✗ Completed symbol {symbol} - 0 total records")

                # Get remaining symbols count for summary using the same connection
                remaining_count = self.get_remaining_symbols_count_with_db(db, exchange_filter)

            # Print summary
            print("\n" + "="*50)
            print("Incremental Earnings Call Transcripts ETL Summary:")
            print(f"  Exchange: {exchange_filter or 'All exchanges'}")
            print(f"  Quarter strategy: {'IPO-based' if use_ipo_based_quarters else 'Fixed quarters'}")
            print(f"  Total symbols processed: {len(symbols)}")
            print(f"  Successful symbols: {success_count}")
            print(f"  Failed symbols: {fail_count}")
            print(f"  Remaining symbols: {remaining_count}")
            print(f"  Total API calls made: {call_count}")
            print(f"  Total records loaded: {total_records:,}")
            print(f"  Average records per symbol: {total_records/success_count if success_count > 0 else 0:.1f}")
            print("="*50)

            print("Incremental Earnings Call Transcripts ETL process completed successfully!")

        except Exception as e:
            print(f"Incremental Earnings Call Transcripts ETL process failed: {e}")
            raise

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
    """Main function to run the earnings call transcripts extraction."""

    extractor = EarningsCallTranscriptsExtractor()

    # Configuration options for different use cases:

    # Option 1: Small test batch with IPO-based quarters (recommended)
    extractor.run_etl_incremental(exchange_filter='NASDAQ', limit=10000)  # Uses IPO dates automatically
    extractor.run_etl_incremental(exchange_filter='NYSE', limit=10000)  # Uses IPO dates automatically


if __name__ == "__main__":
    main()
