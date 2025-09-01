"""
Extract balance sheet data from Alpha Vantage API and load into database.
Streamlined version with consolidated logic and reduced redundancy.
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

STOCK_API_FUNCTION = "BALANCE_SHEET"

# Schema configuration for balance sheet fields
BALANCE_SHEET_FIELDS = {
    'total_assets': 'totalAssets',
    'total_current_assets': 'totalCurrentAssets',
    'cash_and_cash_equivalents_at_carrying_value': 'cashAndCashEquivalentsAtCarryingValue',
    'cash_and_short_term_investments': 'cashAndShortTermInvestments',
    'inventory': 'inventory',
    'current_net_receivables': 'currentNetReceivables',
    'total_non_current_assets': 'totalNonCurrentAssets',
    'property_plant_equipment': 'propertyPlantEquipment',
    'accumulated_depreciation_amortization_ppe': 'accumulatedDepreciationAmortizationPPE',
    'intangible_assets': 'intangibleAssets',
    'intangible_assets_excluding_goodwill': 'intangibleAssetsExcludingGoodwill',
    'goodwill': 'goodwill',
    'investments': 'investments',
    'long_term_investments': 'longTermInvestments',
    'short_term_investments': 'shortTermInvestments',
    'other_current_assets': 'otherCurrentAssets',
    'other_non_current_assets': 'otherNonCurrentAssets',
    'total_liabilities': 'totalLiabilities',
    'total_current_liabilities': 'totalCurrentLiabilities',
    'current_accounts_payable': 'currentAccountsPayable',
    'deferred_revenue': 'deferredRevenue',
    'current_debt': 'currentDebt',
    'short_term_debt': 'shortTermDebt',
    'total_non_current_liabilities': 'totalNonCurrentLiabilities',
    'capital_lease_obligations': 'capitalLeaseObligations',
    'long_term_debt': 'longTermDebt',
    'current_long_term_debt': 'currentLongTermDebt',
    'long_term_debt_noncurrent': 'longTermDebtNoncurrent',
    'short_long_term_debt_total': 'shortLongTermDebtTotal',
    'other_current_liabilities': 'otherCurrentLiabilities',
    'other_non_current_liabilities': 'otherNonCurrentLiabilities',
    'total_shareholder_equity': 'totalShareholderEquity',
    'treasury_stock': 'treasuryStock',
    'retained_earnings': 'retainedEarnings',
    'common_stock': 'commonStock',
    'common_stock_shares_outstanding': 'commonStockSharesOutstanding'
}


class BalanceSheetExtractor:
    """Extract and load balance sheet data from Alpha Vantage API."""

    def __init__(self):
        load_dotenv()
        self.api_key = os.getenv("ALPHAVANTAGE_API_KEY")
        if not self.api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY not found in environment variables")
        
        self.base_url = "https://www.alphavantage.co/query"
        self.rate_limit_delay = 0.8  # 75 requests per minute
        
    def _get_db_manager(self):
        """Create a fresh database manager instance for each operation to avoid connection issues."""
        return PostgresDatabaseManager()

    def load_symbols(self, processed=False, limit=None, db_connection=None):
        """Unified symbol loading method - simplified without exchange filtering."""
        def _load_symbols_query(db):
            if processed:
                # Load all valid symbols
                base_query = "SELECT symbol_id, symbol FROM extracted.listing_status WHERE asset_type = 'Stock'"
                params = []
            else:
                # Load unprocessed symbols (not in balance_sheet table)
                if not db.table_exists("extracted.balance_sheet"):
                    self.create_balance_sheet_table(db)
                
                base_query = """
                    SELECT ls.symbol_id, ls.symbol
                    FROM extracted.listing_status ls
                    LEFT JOIN extracted.balance_sheet bs ON ls.symbol_id = bs.symbol_id
                    WHERE ls.asset_type = 'Stock' AND bs.symbol_id IS NULL
                """
                params = []

            if not processed:
                base_query += " GROUP BY ls.symbol_id, ls.symbol"
            
            if limit:
                base_query += " LIMIT %s"
                params.append(limit)

            result = db.fetch_query(base_query, params)
            return {row[1]: row[0] for row in result}
        
        # Use provided connection or create new one
        if db_connection:
            return _load_symbols_query(db_connection)
        else:
            db_manager = self._get_db_manager()
            with db_manager as db:
                return _load_symbols_query(db)

    def _validate_api_response(self, data, symbol):
        """Unified API response validation to reduce redundant error checking."""
        if "Error Message" in data:
            print(f"API Error for {symbol}: {data['Error Message']}")
            return False, "fail"
        
        if "Note" in data:
            print(f"API Note for {symbol}: {data['Note']}")
            return False, "fail"
        
        if "annualReports" not in data and "quarterlyReports" not in data:
            print(f"No balance sheet data found for {symbol}")
            return False, "fail"
        
        return True, "pass"

    def extract_single_balance_sheet(self, symbol):
        """Extract balance sheet data for a single symbol with unified error handling."""
        print(f"Processing TICKER: {symbol}")
        url = f"{self.base_url}?function={STOCK_API_FUNCTION}&symbol={symbol}&apikey={self.api_key}"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            # Use unified validation
            is_valid, status = self._validate_api_response(data, symbol)
            if not is_valid:
                return None, status
            
            print(f"Successfully fetched balance sheet data for {symbol}")
            return data, "pass"
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {symbol}: {e}")
            return None, "fail"
        except Exception as e:
            print(f"Unexpected error for {symbol}: {e}")
            return None, "fail"

    def _process_reports(self, data, symbol, symbol_id, timestamp):
        """Process both annual and quarterly reports with unified logic."""
        records = []
        
        for report_type in ["annualReports", "quarterlyReports"]:
            type_name = report_type.replace("Reports", "").lower()  # "annual" or "quarterly"
            
            if report_type in data:
                if data[report_type]:  # Has data
                    for report in data[report_type]:
                        record = self._transform_single_report(symbol, symbol_id, report, type_name, timestamp)
                        if record:
                            records.append(record)
                else:  # Empty array
                    empty_record = self._create_base_record(symbol, symbol_id, type_name, "empty", timestamp)
                    records.append(empty_record)
        
        return records

    def transform_balance_sheet_data(self, symbol, symbol_id, data, status):
        """Transform balance sheet data to match database schema - streamlined version."""
        current_timestamp = datetime.now().isoformat()

        if status == "fail" or data is None:
            # Create error records for both annual and quarterly
            records = [
                self._create_base_record(symbol, symbol_id, "annual", "error", current_timestamp),
                self._create_base_record(symbol, symbol_id, "quarterly", "error", current_timestamp)
            ]
            print(f"Created {len(records)} error records for {symbol}")
            return records

        try:
            records = self._process_reports(data, symbol, symbol_id, current_timestamp)
            print(f"Transformed {len(records)} balance sheet records for {symbol}")
            return records
        except Exception as e:
            print(f"Error transforming data for {symbol}: {e}")
            return []

    def _convert_value(self, value):
        """Convert API values to database format."""
        if value in (None, "None", ""):
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    def _create_base_record(self, symbol, symbol_id, report_type, api_status, timestamp):
        """Create base record template - consolidated record creation logic."""
        return {
            "symbol_id": symbol_id,
            "symbol": symbol,
            "fiscal_date_ending": None,
            "report_type": report_type,
            "reported_currency": None,
            "api_response_status": api_status,
            "created_at": timestamp,
            "updated_at": timestamp,
            **{field: None for field in BALANCE_SHEET_FIELDS.keys()}
        }

    def _transform_single_report(self, symbol, symbol_id, report, report_type, timestamp):
        """Transform a single balance sheet report using schema-driven approach."""
        try:
            # Start with base record
            record = self._create_base_record(symbol, symbol_id, report_type, "pass", timestamp)
            
            # Update with actual data
            record.update({
                "fiscal_date_ending": report.get("fiscalDateEnding"),
                "reported_currency": report.get("reportedCurrency"),
            })
            
            # Apply field mappings using schema configuration
            for db_field, api_field in BALANCE_SHEET_FIELDS.items():
                record[db_field] = self._convert_value(report.get(api_field))
            
            # Validate required fields
            if not record["fiscal_date_ending"]:
                print(f"Missing fiscal_date_ending for {symbol} {report_type} report")
                return None
            
            return record
            
        except Exception as e:
            print(f"Error transforming single report for {symbol}: {e}")
            return None

    def load_balance_sheet_data(self, records, db_connection=None):
        """Load balance sheet records into the database - simplified connection handling."""
        if not records:
            print("No records to load")
            return

        print(f"Loading {len(records)} records into database...")

        def _insert_records(db):
            if not db.table_exists("extracted.balance_sheet"):
                self.create_balance_sheet_table(db)

            columns = list(records[0].keys())
            placeholders = ", ".join(["%s" for _ in columns])
            insert_query = f"INSERT INTO extracted.balance_sheet ({', '.join(columns)}) VALUES ({placeholders})"
            
            record_tuples = [tuple(record[col] for col in columns) for record in records]
            rows_affected = db.execute_many(insert_query, record_tuples)
            print(f"Successfully loaded {rows_affected} records into extracted.balance_sheet table")

        # Always use the provided connection since we're in a context manager
        if db_connection:
            _insert_records(db_connection)
        else:
            # This shouldn't happen in our streamlined version but keep as fallback
            db_manager = self._get_db_manager()
            with db_manager as db:
                _insert_records(db)

    def create_balance_sheet_table(self, db):
        """Create the balance_sheet table using schema-driven approach."""
        # Generate column definitions from schema
        field_definitions = []
        for field in BALANCE_SHEET_FIELDS.keys():
            field_definitions.append(f"{field} BIGINT")
        
        create_table_sql = f"""
            CREATE SCHEMA IF NOT EXISTS extracted;
            
            CREATE TABLE IF NOT EXISTS extracted.balance_sheet (
                symbol_id INTEGER NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                fiscal_date_ending DATE,
                report_type VARCHAR(10) NOT NULL CHECK (report_type IN ('annual', 'quarterly')),
                reported_currency VARCHAR(10),
                {', '.join(field_definitions)},
                api_response_status VARCHAR(20),
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                FOREIGN KEY (symbol_id) REFERENCES extracted.listing_status(symbol_id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_balance_sheet_symbol_id ON extracted.balance_sheet(symbol_id);
            CREATE INDEX IF NOT EXISTS idx_balance_sheet_fiscal_date ON extracted.balance_sheet(fiscal_date_ending);
        """
        db.execute_query(create_table_sql)
        print("Created extracted.balance_sheet table with indexes")

    def _track_progress(self, symbol, symbol_id, records, index, total, success_count, fail_count):
        """Unified progress tracking to reduce redundant logging."""
        if records:
            success_count += 1
            print(f"✓ Processed {symbol} (ID: {symbol_id}) - {len(records)} records [{index+1}/{total}]")
        else:
            fail_count += 1
            print(f"✗ Processed {symbol} (ID: {symbol_id}) - 0 records [{index+1}/{total}]")
        return success_count, fail_count

    def run_etl_incremental(self, limit=None):
        """Run ETL only for symbols not yet processed - simplified without exchange filtering."""
        print("Starting Incremental Balance Sheet ETL process...")
        print(f"Configuration: limit={limit}")

        try:
            # Create a fresh database manager for this run to avoid connection issues
            db_manager = self._get_db_manager()
            with db_manager as db:
                # Load unprocessed symbols using unified method with same connection
                symbol_mapping = self.load_symbols(processed=False, limit=limit, db_connection=db)
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
                        # Extract and transform
                        data, status = self.extract_single_balance_sheet(symbol)
                        records = self.transform_balance_sheet_data(symbol, symbol_id, data, status)

                        # Load and track progress
                        if records:
                            self.load_balance_sheet_data(records, db)
                            total_records += len(records)
                        
                        success_count, fail_count = self._track_progress(
                            symbol, symbol_id, records, i, len(symbols), success_count, fail_count
                        )

                    except Exception as e:
                        fail_count += 1
                        print(f"✗ Error processing {symbol} (ID: {symbol_id}): {e} [{i+1}/{len(symbols)}]")
                        continue

                    # Rate limiting
                    if i < len(symbols) - 1:
                        time.sleep(self.rate_limit_delay)

                # Get remaining count - simplified query
                remaining_query = """
                    SELECT COUNT(DISTINCT ls.symbol_id)
                    FROM extracted.listing_status ls
                    LEFT JOIN extracted.balance_sheet bs ON ls.symbol_id = bs.symbol_id
                    WHERE ls.asset_type = 'Stock' AND bs.symbol_id IS NULL
                """
                remaining_count = db.fetch_query(remaining_query)[0][0]

                # Print summary
                self._print_summary(len(symbols), success_count, fail_count, remaining_count, total_records)
                print("Incremental Balance Sheet ETL process completed successfully!")

        except Exception as e:
            print(f"Incremental Balance Sheet ETL process failed: {e}")
            raise

    def _print_summary(self, total_processed, success_count, fail_count, remaining_count, total_records):
        """Centralized summary printing - simplified without exchange info."""
        print("\n" + "=" * 50)
        print("Incremental Balance Sheet ETL Summary:")
        print(f"  Total symbols processed: {total_processed}")
        print(f"  Successful symbols: {success_count}")
        print(f"  Failed symbols: {fail_count}")
        print(f"  Remaining symbols: {remaining_count}")
        print(f"  Total records loaded: {total_records:,}")
        print(f"  Average records per symbol: {total_records/success_count if success_count > 0 else 0:.1f}")
        print("=" * 50)


def main():
    """Main function to run the balance sheet extraction."""
    extractor = BalanceSheetExtractor()
    
    # Simple approach: process symbols without exchange filtering
    # Start with a small batch to test
    extractor.run_etl_incremental(limit=25000)


if __name__ == "__main__":
    main()
