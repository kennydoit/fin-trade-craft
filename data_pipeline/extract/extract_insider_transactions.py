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


class InsiderTransactionsExtractor:
    """Extract and load insider transactions data from Alpha Vantage API."""

    def __init__(self):
        # Load ALPHAVANTAGE_API_KEY from .env file
        load_dotenv()
        self.api_key = os.getenv("ALPHAVANTAGE_API_KEY")

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
                    placeholders = ",".join(["%s" for _ in exchange_filter])
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
        """Load symbols that haven't been processed yet using provided database connection."""
        # First ensure the table exists, or create just the table
        if not db.table_exists("insider_transactions"):
            # Create just the insider_transactions table
            create_table_sql = """
                CREATE TABLE IF NOT EXISTS insider_transactions (
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
                
                CREATE INDEX IF NOT EXISTS idx_insider_transactions_symbol_id ON insider_transactions(symbol_id);
                CREATE INDEX IF NOT EXISTS idx_insider_transactions_symbol ON insider_transactions(symbol);
                CREATE INDEX IF NOT EXISTS idx_insider_transactions_date ON insider_transactions(transaction_date);
                CREATE INDEX IF NOT EXISTS idx_insider_transactions_executive ON insider_transactions(executive);
                CREATE INDEX IF NOT EXISTS idx_insider_transactions_type ON insider_transactions(acquisition_or_disposal);
            """

            # Create trigger separately if the update function exists
            trigger_sql = """
                DO $$
                BEGIN
                    IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'update_updated_at_column') THEN
                        CREATE TRIGGER update_insider_transactions_updated_at 
                        BEFORE UPDATE ON insider_transactions 
                        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
                    END IF;
                EXCEPTION WHEN others THEN
                    -- Trigger may already exist, ignore error
                END $$;
            """

            try:
                db.execute_query(create_table_sql)
                print("Created insider_transactions table")
                # Try to create trigger separately
                try:
                    db.execute_query(trigger_sql)
                    print("Created insider_transactions trigger")
                except Exception as te:
                    print(f"Note: Could not create trigger (may already exist): {te}")
            except Exception as e:
                print(f"Warning: Could not create insider_transactions table: {e}")

        # Now we can safely query with LEFT JOIN
        base_query = """
            SELECT ls.symbol_id, ls.symbol 
            FROM listing_status ls 
            LEFT JOIN insider_transactions it ON ls.symbol_id = it.symbol_id 
            WHERE ls.asset_type = 'Stock' AND it.symbol_id IS NULL
        """
        params = []

        if exchange_filter:
            if isinstance(exchange_filter, list):
                placeholders = ",".join(["%s" for _ in exchange_filter])
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

    def get_remaining_symbols_count_with_db(self, db, exchange_filter=None):
        """Get count of remaining unprocessed symbols using provided database connection."""
        base_query = """
            SELECT COUNT(DISTINCT ls.symbol_id)
            FROM listing_status ls 
            LEFT JOIN insider_transactions it ON ls.symbol_id = it.symbol_id 
            WHERE ls.asset_type = 'Stock' AND it.symbol_id IS NULL
        """
        params = []

        if exchange_filter:
            if isinstance(exchange_filter, list):
                placeholders = ",".join(["%s" for _ in exchange_filter])
                base_query += f" AND ls.exchange IN ({placeholders})"
                params.extend(exchange_filter)
            else:
                base_query += " AND ls.exchange = %s"
                params.append(exchange_filter)

        return db.fetch_query(base_query, params)[0][0]

    def extract_single_insider_transactions(self, symbol):
        """Extract insider transactions data for a single symbol."""
        print(f"Processing TICKER: {symbol}")

        url = f"{self.base_url}?function={STOCK_API_FUNCTION}&symbol={symbol}&apikey={self.api_key}"
        print(f"Fetching data from: {url}")

        try:
            response = requests.get(url)
            response.raise_for_status()

            print(f"Response status: {response.status_code}")
            data = response.json()

            # Check for API errors
            if "Error Message" in data:
                print(f"API Error for {symbol}: {data['Error Message']}")
                return None, "fail"

            if "Note" in data:
                print(f"API Note for {symbol}: {data['Note']}")
                return None, "fail"

            # Check if we have insider transactions data
            if "data" not in data or not data["data"]:
                print(f"No insider transactions data found for {symbol}")
                return None, "empty"

            print(
                f"Successfully fetched {len(data['data'])} insider transactions for {symbol}"
            )
            return data, "pass"

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {symbol}: {e}")
            return None, "fail"
        except Exception as e:
            print(f"Unexpected error for {symbol}: {e}")
            return None, "fail"

    def transform_insider_transactions_data(self, symbol, symbol_id, data, status):
        """Transform insider transactions data to match database schema."""
        current_timestamp = datetime.now().isoformat()

        if status == "fail" or data is None:
            # Create error record
            return [
                {
                    "symbol_id": symbol_id,
                    "symbol": symbol,
                    "transaction_date": None,
                    "executive": None,
                    "executive_title": None,
                    "security_type": None,
                    "acquisition_or_disposal": None,
                    "shares": None,
                    "share_price": None,
                    "api_response_status": "error",
                    "created_at": current_timestamp,
                    "updated_at": current_timestamp,
                }
            ]

        if status == "empty":
            # Create empty record
            return [
                {
                    "symbol_id": symbol_id,
                    "symbol": symbol,
                    "transaction_date": None,
                    "executive": None,
                    "executive_title": None,
                    "security_type": None,
                    "acquisition_or_disposal": None,
                    "shares": None,
                    "share_price": None,
                    "api_response_status": "empty",
                    "created_at": current_timestamp,
                    "updated_at": current_timestamp,
                }
            ]

        try:
            records = []

            for transaction in data["data"]:
                # Helper function to convert values
                def convert_decimal(value):
                    if value is None or value == "" or value == "None":
                        return None
                    try:
                        return float(value)
                    except (ValueError, TypeError):
                        return None

                record = {
                    "symbol_id": symbol_id,
                    "symbol": symbol,
                    "transaction_date": transaction.get("transaction_date"),
                    "executive": transaction.get("executive"),
                    "executive_title": transaction.get("executive_title"),
                    "security_type": transaction.get("security_type"),
                    "acquisition_or_disposal": transaction.get(
                        "acquisition_or_disposal"
                    ),
                    "shares": convert_decimal(transaction.get("shares")),
                    "share_price": convert_decimal(transaction.get("share_price")),
                    "api_response_status": "pass",
                    "created_at": current_timestamp,
                    "updated_at": current_timestamp,
                }

                records.append(record)

            print(
                f"Transformed {len(records)} insider transaction records for {symbol}"
            )
            return records

        except Exception as e:
            print(f"Error transforming data for {symbol}: {e}")
            return []

    def load_insider_transactions_data_with_db(self, db_manager, records):
        """Load insider transactions records into the database using provided database manager."""
        if not records:
            print("No records to load")
            return

        print(f"Loading {len(records)} records into database...")

        # The table should already exist from load_unprocessed_symbols_with_db
        if not db_manager.table_exists("insider_transactions"):
            print("Warning: insider_transactions table doesn't exist, creating it...")
            # Create just the insider_transactions table
            create_table_sql = """
                CREATE TABLE IF NOT EXISTS insider_transactions (
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
            """
            db_manager.execute_query(create_table_sql)

        # Prepare insert query using PostgreSQL syntax
        columns = list(records[0].keys())
        placeholders = ", ".join(["%s" for _ in columns])
        insert_query = f"""
            INSERT INTO insider_transactions ({', '.join(columns)}) 
            VALUES ({placeholders})
            ON CONFLICT (symbol_id, transaction_date, executive, security_type, acquisition_or_disposal, shares, share_price) 
            DO UPDATE SET
                executive_title = EXCLUDED.executive_title,
                api_response_status = EXCLUDED.api_response_status,
                updated_at = EXCLUDED.updated_at
        """

        # Convert records to list of tuples
        record_tuples = [tuple(record[col] for col in columns) for record in records]

        # Execute bulk insert
        rows_affected = db_manager.execute_many(insert_query, record_tuples)
        print(
            f"Successfully loaded {rows_affected} records into insider_transactions table"
        )

    def run_etl_incremental(self, exchange_filter=None, limit=None):
        """Run ETL only for symbols not yet processed.

        Args:
            exchange_filter: Filter by exchange (e.g., 'NASDAQ', 'NYSE')
            limit: Maximum number of symbols to process (for chunking)
        """
        print("Starting Incremental Insider Transactions ETL process...")
        print(f"Configuration: exchange={exchange_filter}, limit={limit}")

        try:
            # Use a fresh database manager for this ETL run
            db_manager = PostgresDatabaseManager()
            with db_manager as db:
                # Load only unprocessed symbols using the shared connection
                symbol_mapping = self.load_unprocessed_symbols_with_db(
                    db, exchange_filter, limit
                )
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
                        data, status = self.extract_single_insider_transactions(symbol)

                        # Transform data
                        records = self.transform_insider_transactions_data(
                            symbol, symbol_id, data, status
                        )

                        if records:
                            # Load records for this symbol using the shared connection
                            self.load_insider_transactions_data_with_db(db, records)
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

                # Get remaining symbols count for summary using the same connection
                remaining_count = self.get_remaining_symbols_count_with_db(
                    db, exchange_filter
                )

            # Print summary
            print("\n" + "=" * 50)
            print("Incremental Insider Transactions ETL Summary:")
            print(f"  Exchange: {exchange_filter or 'All exchanges'}")
            print(f"  Total symbols processed: {len(symbols)}")
            print(f"  Successful symbols: {success_count}")
            print(f"  Failed symbols: {fail_count}")
            print(f"  Remaining symbols: {remaining_count}")
            print(f"  Total records loaded: {total_records:,}")
            print(
                f"  Average records per symbol: {total_records/success_count if success_count > 0 else 0:.1f}"
            )
            print("=" * 50)

            print(
                "Incremental Insider Transactions ETL process completed successfully!"
            )

        except Exception as e:
            print(f"Incremental Insider Transactions ETL process failed: {e}")
            raise


def main():
    """Main function to run the insider transactions extraction."""

    extractor = InsiderTransactionsExtractor()

    # Configuration options for different use cases:

    # Option 1: Small test batch
    # extractor.run_etl_incremental(exchange_filter='NYSE', limit=3000)
    extractor.run_etl_incremental(exchange_filter="NASDAQ", limit=5000)

    # Option 2: Process by exchange in batches
    # extractor.run_etl_incremental(exchange_filter='NASDAQ', limit=10)
    # extractor.run_etl_incremental(exchange_filter='NYSE', limit=10)

    # Option 3: Large batch processing
    # extractor.run_etl_incremental(exchange_filter='NASDAQ', limit=100)


if __name__ == "__main__":
    main()
