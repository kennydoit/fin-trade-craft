"""
Extract insider transactions data from Alpha Vantage API and load into database.
"""

import os
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

# Add the parent directories to the path so we can import from db
sys.path.append(str(Path(__file__).parent.parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager

STOCK_API_FUNCTION = "INSIDER_TRANSACTIONS"

# Schema-driven field mapping configuration
INSIDER_TRANSACTIONS_FIELDS = {
    'symbol_id': 'symbol_id',
    'symbol': 'symbol',
    'transaction_date': 'transaction_date',
    'executive': 'executive',
    'executive_title': 'executive_title',
    'security_type': 'security_type',
    'acquisition_or_disposal': 'acquisition_or_disposal',
    'shares': 'shares',
    'share_price': 'share_price',
    'api_response_status': 'api_response_status',
    'created_at': 'created_at',
    'updated_at': 'updated_at'
}


class InsiderTransactionsExtractor:
    """Extract and load insider transactions data from Alpha Vantage API."""

    def __init__(self):
        # Load ALPHAVANTAGE_API_KEY from .env file
        load_dotenv()
        self.api_key = os.getenv("ALPHAVANTAGE_API_KEY")

        if not self.api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY not found in environment variables")

        self.base_url = "https://www.alphavantage.co/query"
        # Rate limiting: 75 requests per minute for Alpha Vantage Premium
        self.rate_limit_delay = 0.8  # seconds between requests (75/min = 0.8s delay)

    
    def _get_db_manager(self):
        """Create a fresh database manager instance for each use."""
        return PostgresDatabaseManager()

    def load_symbols(self, limit=None):
        """Load symbols that haven't been processed yet (not in insider_transactions table)."""
        with self._get_db_manager() as db:
            # First ensure the table exists - create if needed (idempotent operation)
            self._create_insider_transactions_table(db)

            # Now we can safely query with LEFT JOIN to find unprocessed symbols
            base_query = """
                SELECT ls.symbol_id, ls.symbol 
                FROM listing_status ls 
                LEFT JOIN extracted.insider_transactions it ON ls.symbol_id = it.symbol_id 
                WHERE ls.asset_type = 'Stock' AND it.symbol_id IS NULL
                GROUP BY ls.symbol_id, ls.symbol
            """
            params = []

            if limit:
                base_query += " LIMIT %s"
                params.append(limit)

            result = db.fetch_query(base_query, params)
            return {row[1]: row[0] for row in result}

    def _extract_api_data(self, symbol):
        """Extract insider transactions data for a single symbol from Alpha Vantage API."""
        print(f"Processing TICKER: {symbol}")

        url = f"{self.base_url}?function={STOCK_API_FUNCTION}&symbol={symbol}&apikey={self.api_key}"
        print(f"Fetching data from: {url}")

        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            # Validate API response
            if not self._validate_api_response(data, symbol):
                return None, "fail"

            print(f"Successfully fetched {len(data['data'])} insider transactions for {symbol}")
            return data, "pass"

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {symbol}: {e}")
            return None, "fail"
        except Exception as e:
            print(f"Unexpected error for {symbol}: {e}")
            return None, "fail"

    def _validate_api_response(self, data, symbol):
        """Validate API response for errors and data availability."""
        # Check for API errors
        if "Error Message" in data:
            print(f"API Error for {symbol}: {data['Error Message']}")
            return False

        if "Note" in data:
            print(f"API Note for {symbol}: {data['Note']}")
            return False

        # Check if we have insider transactions data
        if "data" not in data or not data["data"]:
            print(f"No insider transactions data found for {symbol}")
            return False

        return True

    def _transform_data(self, symbol, symbol_id, data, status):
        """Transform insider transactions data to match database schema."""
        current_timestamp = datetime.now().isoformat()

        if status == "fail" or data is None:
            # Create error record
            return [self._create_base_record(symbol, symbol_id, "error", current_timestamp)]

        if "data" not in data or not data["data"]:
            # Create empty record for no data
            return [self._create_base_record(symbol, symbol_id, "empty", current_timestamp)]

        try:
            records = []

            for transaction in data["data"]:
                record = self._transform_single_transaction(
                    symbol, symbol_id, transaction, current_timestamp
                )
                if record:
                    records.append(record)

            print(f"Transformed {len(records)} insider transaction records for {symbol}")
            return records

        except Exception as e:
            print(f"Error transforming data for {symbol}: {e}")
            # Return error record
            return [self._create_base_record(symbol, symbol_id, f"TRANSFORM_ERROR: {str(e)[:200]}", current_timestamp)]

    def _create_base_record(self, symbol, symbol_id, api_status, timestamp, executive_name="NO_DATA"):
        """Create a base record with all required fields."""
        record = {}
        
        # Populate all fields with None initially
        for db_field in INSIDER_TRANSACTIONS_FIELDS.keys():
            record[db_field] = None
        
        # Set the known values
        record.update({
            "symbol_id": symbol_id,
            "symbol": symbol,
            "transaction_date": "1900-01-01",  # Placeholder date for NOT NULL constraint
            "executive": executive_name if api_status == "empty" else api_status,
            "api_response_status": api_status if api_status in ["pass", "empty", "error"] else "error",
            "created_at": timestamp,
            "updated_at": timestamp,
        })
        
        return record

    def _transform_single_transaction(self, symbol, symbol_id, transaction, timestamp):
        """Transform a single insider transaction using schema-driven field mapping."""
        try:
            # Start with a base record
            record = self._create_base_record(symbol, symbol_id, "pass", timestamp)
            
            # Helper function to convert values
            def convert_decimal(value):
                if value is None or value == "" or value == "None":
                    return None
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return None

            # Helper function to parse and clean date
            def parse_transaction_date(date_str):
                if not date_str or date_str == "None":
                    return "1900-01-01"  # Placeholder for missing dates
                
                # Clean up common date format issues
                date_str = str(date_str).strip()
                
                # Remove timezone info if present (e.g., "2013-12-11-05:00")
                if '-' in date_str and len(date_str) > 10:
                    # Split by '-' and take first 3 parts for YYYY-MM-DD
                    parts = date_str.split('-')
                    if len(parts) >= 3:
                        try:
                            year, month, day = parts[0], parts[1], parts[2]
                            # Validate basic format
                            if len(year) == 4 and len(month) <= 2 and len(day) <= 2:
                                clean_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                                # Validate the date can be parsed
                                datetime.strptime(clean_date, "%Y-%m-%d")
                                return clean_date
                        except (ValueError, IndexError):
                            pass
                
                # Check for XML/HTML fragments and skip them
                if '<' in date_str or '>' in date_str:
                    print(f"Skipping malformed date with XML: {date_str[:50]}...")
                    return "1900-01-01"
                
                # Try standard date formats
                for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"]:
                    try:
                        datetime.strptime(date_str[:10], fmt)  # Only take first 10 chars
                        return date_str[:10]
                    except ValueError:
                        continue
                
                # If all parsing fails, use placeholder
                print(f"Could not parse date '{date_str}' for {symbol}, using placeholder")
                return "1900-01-01"

            # Clean and validate acquisition_or_disposal (should be single character)
            def clean_acquisition_disposal(value):
                if not value or value == "None":
                    return None
                value_str = str(value).strip().upper()
                # Take only the first character if it's A or D
                if value_str and value_str[0] in ['A', 'D']:
                    return value_str[0]
                return None

            # Populate transaction-specific fields
            record.update({
                "transaction_date": parse_transaction_date(transaction.get("transaction_date")),
                "executive": str(transaction.get("executive", ""))[:255] if transaction.get("executive") else None,
                "executive_title": str(transaction.get("executive_title", ""))[:255] if transaction.get("executive_title") else None,
                "security_type": str(transaction.get("security_type", ""))[:100] if transaction.get("security_type") else None,
                "acquisition_or_disposal": clean_acquisition_disposal(transaction.get("acquisition_or_disposal")),
                "shares": convert_decimal(transaction.get("shares")),
                "share_price": convert_decimal(transaction.get("share_price")),
            })

            return record

        except Exception as e:
            print(f"Error transforming single transaction for {symbol}: {e}")
            return None

    def _create_insider_transactions_table(self, db):
        """Create the insider_transactions table if it doesn't exist."""
        try:
            create_table_sql = """
                CREATE TABLE IF NOT EXISTS extracted.insider_transactions (
                    transaction_id      SERIAL PRIMARY KEY,
                    symbol_id           INTEGER NOT NULL,
                    symbol              VARCHAR(20) NOT NULL,
                    transaction_date    DATE,
                    executive           VARCHAR(255),
                    executive_title     VARCHAR(255),
                    security_type       VARCHAR(100),
                    acquisition_or_disposal VARCHAR(1),
                    shares              DECIMAL(20,4),
                    share_price         DECIMAL(20,4),
                    api_response_status VARCHAR(20) DEFAULT 'pass',
                    created_at          TIMESTAMP DEFAULT NOW(),
                    updated_at          TIMESTAMP DEFAULT NOW(),
                    FOREIGN KEY (symbol_id) REFERENCES listing_status(symbol_id) ON DELETE CASCADE,
                    UNIQUE(symbol_id, transaction_date, executive, security_type, acquisition_or_disposal, shares, share_price)
                );
                
                CREATE INDEX IF NOT EXISTS idx_insider_transactions_symbol_id ON extracted.insider_transactions(symbol_id);
                CREATE INDEX IF NOT EXISTS idx_insider_transactions_symbol ON extracted.insider_transactions(symbol);
                CREATE INDEX IF NOT EXISTS idx_insider_transactions_date ON extracted.insider_transactions(transaction_date);
                CREATE INDEX IF NOT EXISTS idx_insider_transactions_executive ON extracted.insider_transactions(executive);
                CREATE INDEX IF NOT EXISTS idx_insider_transactions_type ON extracted.insider_transactions(acquisition_or_disposal);
            """
            db.execute_query(create_table_sql)
            print("Created extracted.insider_transactions table with indexes")
        except Exception as e:
            print(f"Warning: Table creation failed, but continuing: {e}")
            # Continue anyway - table might already exist

    def _load_data_to_db(self, records, db_connection=None):
        """Load insider transactions records into the database."""
        if not records:
            print("No records to load")
            return

        print(f"Loading {len(records)} records into database...")

        # Use provided connection or create new one
        if db_connection:
            db = db_connection

            # Prepare insert query using simple INSERT (remove problematic ON CONFLICT)
            columns = list(records[0].keys())
            placeholders = ", ".join(["%s" for _ in columns])
            insert_query = f"""
                INSERT INTO extracted.insider_transactions ({', '.join(columns)}) 
                VALUES ({placeholders})
            """

            # Convert records to list of tuples
            record_tuples = [tuple(record[col] for col in columns) for record in records]

            # Execute bulk insert
            rows_affected = db.execute_many(insert_query, record_tuples)
            print(f"Successfully loaded {rows_affected} records into extracted.insider_transactions table")
        else:
            # Create new connection (fallback)
            with self._get_db_manager() as db:
                self._load_data_to_db(records, db)

    def run_etl_incremental(self, limit=None):
        """Run ETL only for symbols not yet processed.

        Args:
            limit: Maximum number of symbols to process (for chunking)
        """
        print("Starting Incremental Insider Transactions ETL process...")
        print(f"Configuration: limit={limit}")

        try:
            # Use a single database connection throughout the entire process
            with self._get_db_manager() as db:
                # Ensure the table exists first - create if needed (idempotent operation)
                self._create_insider_transactions_table(db)

                # Load only unprocessed symbols using the simplified method
                symbol_mapping = self.load_symbols(limit)
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
                        data, status = self._extract_api_data(symbol)

                        # Transform data
                        records = self._transform_data(symbol, symbol_id, data, status)

                        if records:
                            # Load records for this symbol using the same database connection
                            self._load_data_to_db(records, db)
                            total_records += len(records)
                            success_count += 1
                            print(
                                f"✓ Processed {symbol} (ID: {symbol_id}) - {len(records)} records [{i+1}/{len(symbols)}]"
                            )
                        else:
                            fail_count += 1
                            print(
                                f"✗ Processed {symbol} (ID: {symbol_id}) - 0 records [{i+1}/{len(symbols)}]"
                            )

                    except Exception as e:
                        fail_count += 1
                        print(
                            f"✗ Error processing {symbol} (ID: {symbol_id}): {e} [{i+1}/{len(symbols)}]"
                        )
                        # Continue processing other symbols even if one fails
                        continue

                    # Rate limiting - wait between requests
                    if i < len(symbols) - 1:
                        time.sleep(self.rate_limit_delay)

                # Count remaining unprocessed symbols
                remaining_symbols = self.load_symbols()
                remaining_count = len(remaining_symbols)

            # Print summary
            print("\n" + "=" * 50)
            print("Incremental Insider Transactions ETL Summary:")
            print(f"  Total symbols processed: {len(symbols)}")
            print(f"  Successful symbols: {success_count}")
            print(f"  Failed symbols: {fail_count}")
            print(f"  Remaining symbols: {remaining_count}")
            print(f"  Total records loaded: {total_records:,}")
            print(
                f"  Average records per symbol: {total_records/success_count if success_count > 0 else 0:.1f}"
            )
            print("=" * 50)

            print("Incremental Insider Transactions ETL process completed successfully!")

        except Exception as e:
            print(f"Incremental Insider Transactions ETL process failed: {e}")
            raise


def main():
    """Main function to run the insider transactions extraction."""

    extractor = InsiderTransactionsExtractor()

    # Simplified configuration - no exchange filtering
    try:
        extractor.run_etl_incremental(limit=10)
    except Exception as e:
        print(f"Error in main execution: {e}")
        raise


if __name__ == "__main__":
    main()
