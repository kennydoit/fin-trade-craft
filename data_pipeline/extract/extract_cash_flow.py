"""
Extract cash flow data from Alpha Vantage API and load into database.
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

STOCK_API_FUNCTION = "CASH_FLOW"


class CashFlowExtractor:
    """Extract and load cash flow data from Alpha Vantage API."""

    def __init__(self):
        # Load ALPHAVANTAGE_API_KEY from .env file
        load_dotenv()
        self.api_key = os.getenv("ALPHAVANTAGE_API_KEY")

        if not self.api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY not found in environment variables")

        # Don't create db_manager in init - create fresh for each operation
        self.base_url = "https://www.alphavantage.co/query"

        # Rate limiting: 75 requests per minute for Alpha Vantage Premium
        self.rate_limit_delay = 0.8  # seconds between requests (75/min = 0.8s delay)

    @property
    def db_manager(self):
        """Create a fresh database manager instance for each use."""
        return PostgresDatabaseManager()

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

    def load_unprocessed_symbols(self, exchange_filter=None, limit=None):
        """Load symbols that haven't been processed yet (not in cash_flow table)."""
        with self.db_manager as db:
            # First ensure the table exists - create if needed (idempotent operation)
            self.create_cash_flow_table(db)

            # Now we can safely query with LEFT JOIN
            base_query = """
                SELECT ls.symbol_id, ls.symbol 
                FROM listing_status ls 
                LEFT JOIN extracted.cash_flow cf ON ls.symbol_id = cf.symbol_id 
                WHERE ls.asset_type = 'Stock' AND cf.symbol_id IS NULL
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

    def extract_single_cash_flow(self, symbol):
        """Extract cash flow data for a single symbol."""
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

            # Check if we have cash flow data
            if "annualReports" not in data and "quarterlyReports" not in data:
                print(f"No cash flow data found for {symbol}")
                return None, "fail"

            print(f"Successfully fetched cash flow data for {symbol}")
            return data, "pass"

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {symbol}: {e}")
            return None, "fail"
        except Exception as e:
            print(f"Unexpected error for {symbol}: {e}")
            return None, "fail"

    def transform_cash_flow_data(self, symbol, symbol_id, data, status):
        """Transform cash flow data to match database schema."""
        current_timestamp = datetime.now().isoformat()

        if status == "fail" or data is None:
            # Create error records for both annual and quarterly
            records = []

            # Create annual error record
            annual_record = self._create_empty_record(
                symbol, symbol_id, "annual", "error", current_timestamp
            )
            records.append(annual_record)

            # Create quarterly error record
            quarterly_record = self._create_empty_record(
                symbol, symbol_id, "quarterly", "error", current_timestamp
            )
            records.append(quarterly_record)

            print(f"Created {len(records)} error records for {symbol}")
            return records

        try:
            records = []

            # Process annual reports
            if "annualReports" in data:
                if data["annualReports"]:  # Has data
                    for report in data["annualReports"]:
                        record = self._transform_single_report(
                            symbol, symbol_id, report, "annual", current_timestamp
                        )
                        if record:
                            records.append(record)
                else:  # Empty array
                    empty_record = self._create_empty_record(
                        symbol, symbol_id, "annual", "empty", current_timestamp
                    )
                    records.append(empty_record)

            # Process quarterly reports
            if "quarterlyReports" in data:
                if data["quarterlyReports"]:  # Has data
                    for report in data["quarterlyReports"]:
                        record = self._transform_single_report(
                            symbol, symbol_id, report, "quarterly", current_timestamp
                        )
                        if record:
                            records.append(record)
                else:  # Empty array
                    empty_record = self._create_empty_record(
                        symbol, symbol_id, "quarterly", "empty", current_timestamp
                    )
                    records.append(empty_record)

            print(f"Transformed {len(records)} cash flow records for {symbol}")
            return records

        except Exception as e:
            print(f"Error transforming data for {symbol}: {e}")
            return []

    def _create_empty_record(
        self, symbol, symbol_id, report_type, api_status, timestamp
    ):
        """Create an empty record for symbols with no data or errors."""
        return {
            "symbol_id": symbol_id,
            "symbol": symbol,
            "fiscal_date_ending": None,
            "report_type": report_type,
            "reported_currency": None,
            "operating_cashflow": None,
            "payments_for_operating_activities": None,
            "proceeds_from_operating_activities": None,
            "change_in_operating_liabilities": None,
            "change_in_operating_assets": None,
            "depreciation_depletion_and_amortization": None,
            "capital_expenditures": None,
            "change_in_receivables": None,
            "change_in_inventory": None,
            "profit_loss": None,
            "cashflow_from_investment": None,
            "cashflow_from_financing": None,
            "proceeds_from_repayments_of_short_term_debt": None,
            "payments_for_repurchase_of_common_stock": None,
            "payments_for_repurchase_of_equity": None,
            "payments_for_repurchase_of_preferred_stock": None,
            "dividend_payout": None,
            "dividend_payout_common_stock": None,
            "dividend_payout_preferred_stock": None,
            "proceeds_from_issuance_of_common_stock": None,
            "proceeds_from_issuance_of_long_term_debt_and_capital_securities_net": None,
            "proceeds_from_issuance_of_preferred_stock": None,
            "proceeds_from_repurchase_of_equity": None,
            "proceeds_from_sale_of_treasury_stock": None,
            "change_in_cash_and_cash_equivalents": None,
            "change_in_exchange_rate": None,
            "net_income": None,
            "api_response_status": api_status,
            "created_at": timestamp,
            "updated_at": timestamp,
        }

    def _transform_single_report(
        self, symbol, symbol_id, report, report_type, timestamp
    ):
        """Transform a single cash flow report."""
        try:
            # Helper function to convert API values to database format
            def convert_value(value):
                if value is None or value == "None" or value == "":
                    return None
                try:
                    return int(float(value))
                except (ValueError, TypeError):
                    return None

            record = {
                "symbol_id": symbol_id,
                "symbol": symbol,
                "fiscal_date_ending": report.get("fiscalDateEnding"),
                "report_type": report_type,
                "reported_currency": report.get("reportedCurrency"),
                "operating_cashflow": convert_value(report.get("operatingCashflow")),
                "payments_for_operating_activities": convert_value(
                    report.get("paymentsForOperatingActivities")
                ),
                "proceeds_from_operating_activities": convert_value(
                    report.get("proceedsFromOperatingActivities")
                ),
                "change_in_operating_liabilities": convert_value(
                    report.get("changeInOperatingLiabilities")
                ),
                "change_in_operating_assets": convert_value(
                    report.get("changeInOperatingAssets")
                ),
                "depreciation_depletion_and_amortization": convert_value(
                    report.get("depreciationDepletionAndAmortization")
                ),
                "capital_expenditures": convert_value(
                    report.get("capitalExpenditures")
                ),
                "change_in_receivables": convert_value(
                    report.get("changeInReceivables")
                ),
                "change_in_inventory": convert_value(report.get("changeInInventory")),
                "profit_loss": convert_value(report.get("profitLoss")),
                "cashflow_from_investment": convert_value(
                    report.get("cashflowFromInvestment")
                ),
                "cashflow_from_financing": convert_value(
                    report.get("cashflowFromFinancing")
                ),
                "proceeds_from_repayments_of_short_term_debt": convert_value(
                    report.get("proceedsFromRepaymentsOfShortTermDebt")
                ),
                "payments_for_repurchase_of_common_stock": convert_value(
                    report.get("paymentsForRepurchaseOfCommonStock")
                ),
                "payments_for_repurchase_of_equity": convert_value(
                    report.get("paymentsForRepurchaseOfEquity")
                ),
                "payments_for_repurchase_of_preferred_stock": convert_value(
                    report.get("paymentsForRepurchaseOfPreferredStock")
                ),
                "dividend_payout": convert_value(report.get("dividendPayout")),
                "dividend_payout_common_stock": convert_value(
                    report.get("dividendPayoutCommonStock")
                ),
                "dividend_payout_preferred_stock": convert_value(
                    report.get("dividendPayoutPreferredStock")
                ),
                "proceeds_from_issuance_of_common_stock": convert_value(
                    report.get("proceedsFromIssuanceOfCommonStock")
                ),
                "proceeds_from_issuance_of_long_term_debt_and_capital_securities_net": convert_value(
                    report.get(
                        "proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet"
                    )
                ),
                "proceeds_from_issuance_of_preferred_stock": convert_value(
                    report.get("proceedsFromIssuanceOfPreferredStock")
                ),
                "proceeds_from_repurchase_of_equity": convert_value(
                    report.get("proceedsFromRepurchaseOfEquity")
                ),
                "proceeds_from_sale_of_treasury_stock": convert_value(
                    report.get("proceedsFromSaleOfTreasuryStock")
                ),
                "change_in_cash_and_cash_equivalents": convert_value(
                    report.get("changeInCashAndCashEquivalents")
                ),
                "change_in_exchange_rate": convert_value(
                    report.get("changeInExchangeRate")
                ),
                "net_income": convert_value(report.get("netIncome")),
                "api_response_status": "pass",
                "created_at": timestamp,
                "updated_at": timestamp,
            }

            # Validate required fields (only for records with data)
            if (
                record["api_response_status"] == "pass"
                and not record["fiscal_date_ending"]
            ):
                print(f"Missing fiscal_date_ending for {symbol} {report_type} report")
                return None

            return record

        except Exception as e:
            print(f"Error transforming single report for {symbol}: {e}")
            return None

    def create_cash_flow_table(self, db):
        """Create the cash_flow table if it doesn't exist."""
        try:
            create_table_sql = """
                CREATE TABLE IF NOT EXISTS extracted.cash_flow (
                    symbol_id                               INTEGER NOT NULL,
                    symbol                                  VARCHAR(20) NOT NULL,
                    fiscal_date_ending                      DATE,  -- Allow NULL for empty/error records
                    report_type                             VARCHAR(10) NOT NULL CHECK (report_type IN ('annual', 'quarterly')),
                    reported_currency                       VARCHAR(10),
                    operating_cashflow                      BIGINT,
                    payments_for_operating_activities       BIGINT,
                    proceeds_from_operating_activities      BIGINT,
                    change_in_operating_liabilities         BIGINT,
                    change_in_operating_assets              BIGINT,
                    depreciation_depletion_and_amortization BIGINT,
                    capital_expenditures                    BIGINT,
                    change_in_receivables                   BIGINT,
                    change_in_inventory                     BIGINT,
                    profit_loss                             BIGINT,
                    cashflow_from_investment                BIGINT,
                    cashflow_from_financing                 BIGINT,
                    proceeds_from_repayments_of_short_term_debt BIGINT,
                    payments_for_repurchase_of_common_stock BIGINT,
                    payments_for_repurchase_of_equity       BIGINT,
                    payments_for_repurchase_of_preferred_stock BIGINT,
                    dividend_payout                         BIGINT,
                    dividend_payout_common_stock            BIGINT,
                    dividend_payout_preferred_stock         BIGINT,
                    proceeds_from_issuance_of_common_stock  BIGINT,
                    proceeds_from_issuance_of_long_term_debt_and_capital_securities_net BIGINT,
                    proceeds_from_issuance_of_preferred_stock BIGINT,
                    proceeds_from_repurchase_of_equity      BIGINT,
                    proceeds_from_sale_of_treasury_stock    BIGINT,
                    change_in_cash_and_cash_equivalents     BIGINT,
                    change_in_exchange_rate                 BIGINT,
                    net_income                              BIGINT,
                    api_response_status                     VARCHAR(20),
                    created_at                              TIMESTAMP DEFAULT NOW(),
                    updated_at                              TIMESTAMP DEFAULT NOW(),
                    FOREIGN KEY (symbol_id) REFERENCES listing_status(symbol_id) ON DELETE CASCADE
                );

                -- Create indexes for cash flow
                CREATE INDEX IF NOT EXISTS idx_cash_flow_symbol_id ON extracted.cash_flow(symbol_id);
                CREATE INDEX IF NOT EXISTS idx_cash_flow_fiscal_date ON extracted.cash_flow(fiscal_date_ending);
            """
            db.execute_query(create_table_sql)
            print("Created extracted.cash_flow table with indexes")
        except Exception as e:
            print(f"Warning: Table creation failed, but continuing: {e}")
            # Continue anyway - table might already exist

    def load_cash_flow_data(self, records, db_connection=None):
        """Load cash flow records into the database."""
        if not records:
            print("No records to load")
            return

        print(f"Loading {len(records)} records into database...")

        # Use provided connection or create new one
        if db_connection:
            # Use existing connection
            db = db_connection
            # Skip table existence check since we create it in run_etl_incremental if needed

            # Prepare insert query - use simple INSERT since there's no proper unique constraint
            columns = list(records[0].keys())
            placeholders = ", ".join(["%s" for _ in columns])
            insert_query = f"""
                INSERT INTO extracted.cash_flow ({', '.join(columns)}) 
                VALUES ({placeholders})
            """

            # Convert records to list of tuples
            record_tuples = [
                tuple(record[col] for col in columns) for record in records
            ]

            # Execute bulk insert
            rows_affected = db.execute_many(insert_query, record_tuples)
            print(f"Successfully loaded {rows_affected} records into extracted.cash_flow table")
        else:
            # Create new connection (fallback)
            with self.db_manager as db:
                self.load_cash_flow_data(records, db)

    def run_etl_incremental(self, exchange_filter=None, limit=None):
        """Run ETL only for symbols not yet processed.

        Args:
            exchange_filter: Filter by exchange (e.g., 'NASDAQ', 'NYSE')
            limit: Maximum number of symbols to process (for chunking)
        """
        print("Starting Incremental Cash Flow ETL process...")
        print(f"Configuration: exchange={exchange_filter}, limit={limit}")

        try:
            # Use a single database connection throughout the entire process
            with self.db_manager as db:
                # Ensure the table exists first - create if needed (idempotent operation)
                self.create_cash_flow_table(db)

                # Load only unprocessed symbols
                base_query = """
                    SELECT ls.symbol_id, ls.symbol 
                    FROM listing_status ls 
                    LEFT JOIN extracted.cash_flow cf ON ls.symbol_id = cf.symbol_id 
                    WHERE ls.asset_type = 'Stock' AND cf.symbol_id IS NULL
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
                symbol_mapping = {row[1]: row[0] for row in result}

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
                        data, status = self.extract_single_cash_flow(symbol)

                        # Transform data
                        records = self.transform_cash_flow_data(
                            symbol, symbol_id, data, status
                        )

                        if records:
                            # Load records for this symbol using the same database connection
                            self.load_cash_flow_data(records, db)
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
                base_query = """
                    SELECT COUNT(DISTINCT ls.symbol_id)
                    FROM listing_status ls 
                    LEFT JOIN extracted.cash_flow cf ON ls.symbol_id = cf.symbol_id 
                    WHERE ls.asset_type = 'Stock' AND cf.symbol_id IS NULL
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

                remaining_count = db.fetch_query(base_query, params)[0][0]

            # Print summary
            print("\n" + "=" * 50)
            print("Incremental Cash Flow ETL Summary:")
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

            print("Incremental Cash Flow ETL process completed successfully!")

        except Exception as e:
            print(f"Incremental Cash Flow ETL process failed: {e}")
            raise

def main():
    """Main function to run the cash flow extraction."""

    extractor = CashFlowExtractor()

    # Configuration options for different use cases:

    # === INITIAL DATA COLLECTION ===
    # Option 1: Initial cash flow data collection (recommended for first run)
    try:
        extractor.run_etl_incremental(exchange_filter='NYSE', limit=10000)
        print("\n" + "="*60)
        print("NYSE processing completed. Starting NASDAQ processing...")
        print("="*60 + "\n")
        extractor.run_etl_incremental(exchange_filter='NASDAQ', limit=10000)
    except Exception as e:
        print(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    main()
