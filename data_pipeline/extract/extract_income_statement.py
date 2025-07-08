"""
Extract income statement data from Alpha Vantage API and load into database.
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
from db.postgres_database_manager import PostgresDatabaseManager

STOCK_API_FUNCTION = "INCOME_STATEMENT"

class IncomeStatementExtractor:
    """Extract and load income statement data from Alpha Vantage API."""
    
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
    
    def load_unprocessed_symbols(self, exchange_filter=None, limit=None):
        """Load symbols that haven't been processed yet (not in income_statement table)."""
        with self.db_manager as db:
            # First ensure the table exists, or create the schema
            if not db.table_exists('income_statement'):
                # Initialize schema to create the table
                schema_path = Path(__file__).parent.parent.parent / "db" / "schema" / "postgres_stock_db_schema.sql"
                db.initialize_schema(schema_path)
            
            # Now we can safely query with LEFT JOIN
            base_query = """
                SELECT ls.symbol_id, ls.symbol 
                FROM listing_status ls 
                LEFT JOIN income_statement inc ON ls.symbol_id = inc.symbol_id 
                WHERE ls.asset_type = 'Stock' AND inc.symbol_id IS NULL
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
            
            base_query += " GROUP BY ls.symbol_id, ls.symbol"
            
            if limit:
                base_query += " LIMIT %s"
                params.append(limit)
            
            result = db.fetch_query(base_query, params)
            return {row[1]: row[0] for row in result}
    
    def extract_single_income_statement(self, symbol):
        """Extract income statement data for a single symbol."""
        print(f"Processing TICKER: {symbol}")
        
        url = f'{self.base_url}?function={STOCK_API_FUNCTION}&symbol={symbol}&apikey={self.api_key}'
        print(f"Fetching data from: {url}")
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            
            print(f"Response status: {response.status_code}")
            data = response.json()
            
            # Check for API errors
            if 'Error Message' in data:
                print(f"API Error for {symbol}: {data['Error Message']}")
                return None, 'fail'
            
            if 'Note' in data:
                print(f"API Note for {symbol}: {data['Note']}")
                return None, 'fail'
            
            # Check if we have income statement data
            if 'annualReports' not in data and 'quarterlyReports' not in data:
                print(f"No income statement data found for {symbol}")
                return None, 'fail'
            
            print(f"Successfully fetched income statement data for {symbol}")
            return data, 'pass'
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {symbol}: {e}")
            return None, 'fail'
        except Exception as e:
            print(f"Unexpected error for {symbol}: {e}")
            return None, 'fail'
    
    def transform_income_statement_data(self, symbol, symbol_id, data, status):
        """Transform income statement data to match database schema."""
        current_timestamp = datetime.now().isoformat()
        
        if status == 'fail' or data is None:
            # Create error records for both annual and quarterly
            records = []
            
            # Create annual error record
            annual_record = self._create_empty_record(
                symbol, symbol_id, 'annual', 'error', current_timestamp
            )
            records.append(annual_record)
            
            # Create quarterly error record
            quarterly_record = self._create_empty_record(
                symbol, symbol_id, 'quarterly', 'error', current_timestamp
            )
            records.append(quarterly_record)
            
            print(f"Created {len(records)} error records for {symbol}")
            return records
        
        try:
            records = []
            
            # Process annual reports
            if 'annualReports' in data:
                if data['annualReports']:  # Has data
                    for report in data['annualReports']:
                        record = self._transform_single_report(
                            symbol, symbol_id, report, 'annual', current_timestamp
                        )
                        if record:
                            records.append(record)
                else:  # Empty array
                    empty_record = self._create_empty_record(
                        symbol, symbol_id, 'annual', 'empty', current_timestamp
                    )
                    records.append(empty_record)
            
            # Process quarterly reports
            if 'quarterlyReports' in data:
                if data['quarterlyReports']:  # Has data
                    for report in data['quarterlyReports']:
                        record = self._transform_single_report(
                            symbol, symbol_id, report, 'quarterly', current_timestamp
                        )
                        if record:
                            records.append(record)
                else:  # Empty array
                    empty_record = self._create_empty_record(
                        symbol, symbol_id, 'quarterly', 'empty', current_timestamp
                    )
                    records.append(empty_record)
            
            print(f"Transformed {len(records)} income statement records for {symbol}")
            return records
            
        except Exception as e:
            print(f"Error transforming data for {symbol}: {e}")
            return []
    
    def _create_empty_record(self, symbol, symbol_id, report_type, api_status, timestamp):
        """Create an empty record for symbols with no data or errors."""
        return {
            'symbol_id': symbol_id,
            'symbol': symbol,
            'fiscal_date_ending': None,
            'report_type': report_type,
            'reported_currency': None,
            'gross_profit': None,
            'total_revenue': None,
            'cost_of_revenue': None,
            'cost_of_goods_and_services_sold': None,
            'operating_income': None,
            'selling_general_and_administrative': None,
            'research_and_development': None,
            'operating_expenses': None,
            'investment_income_net': None,
            'net_interest_income': None,
            'interest_income': None,
            'interest_expense': None,
            'non_interest_income': None,
            'other_non_operating_income': None,
            'depreciation': None,
            'depreciation_and_amortization': None,
            'income_before_tax': None,
            'income_tax_expense': None,
            'interest_and_debt_expense': None,
            'net_income_from_continuing_operations': None,
            'comprehensive_income_net_of_tax': None,
            'ebit': None,
            'ebitda': None,
            'net_income': None,
            'api_response_status': api_status,
            'created_at': timestamp,
            'updated_at': timestamp
        }
    
    def _transform_single_report(self, symbol, symbol_id, report, report_type, timestamp):
        """Transform a single income statement report."""
        try:
            # Helper function to convert API values to database format
            def convert_value(value):
                if value is None or value == 'None' or value == '':
                    return None
                try:
                    return int(float(value))
                except (ValueError, TypeError):
                    return None
            
            record = {
                'symbol_id': symbol_id,
                'symbol': symbol,
                'fiscal_date_ending': report.get('fiscalDateEnding'),
                'report_type': report_type,
                'reported_currency': report.get('reportedCurrency'),
                'gross_profit': convert_value(report.get('grossProfit')),
                'total_revenue': convert_value(report.get('totalRevenue')),
                'cost_of_revenue': convert_value(report.get('costOfRevenue')),
                'cost_of_goods_and_services_sold': convert_value(report.get('costofGoodsAndServicesSold')),
                'operating_income': convert_value(report.get('operatingIncome')),
                'selling_general_and_administrative': convert_value(report.get('sellingGeneralAndAdministrative')),
                'research_and_development': convert_value(report.get('researchAndDevelopment')),
                'operating_expenses': convert_value(report.get('operatingExpenses')),
                'investment_income_net': convert_value(report.get('investmentIncomeNet')),
                'net_interest_income': convert_value(report.get('netInterestIncome')),
                'interest_income': convert_value(report.get('interestIncome')),
                'interest_expense': convert_value(report.get('interestExpense')),
                'non_interest_income': convert_value(report.get('nonInterestIncome')),
                'other_non_operating_income': convert_value(report.get('otherNonOperatingIncome')),
                'depreciation': convert_value(report.get('depreciation')),
                'depreciation_and_amortization': convert_value(report.get('depreciationAndAmortization')),
                'income_before_tax': convert_value(report.get('incomeBeforeTax')),
                'income_tax_expense': convert_value(report.get('incomeTaxExpense')),
                'interest_and_debt_expense': convert_value(report.get('interestAndDebtExpense')),
                'net_income_from_continuing_operations': convert_value(report.get('netIncomeFromContinuingOperations')),
                'comprehensive_income_net_of_tax': convert_value(report.get('comprehensiveIncomeNetOfTax')),
                'ebit': convert_value(report.get('ebit')),
                'ebitda': convert_value(report.get('ebitda')),
                'net_income': convert_value(report.get('netIncome')),
                'api_response_status': 'pass',
                'created_at': timestamp,
                'updated_at': timestamp
            }
            
            # Validate required fields (only for records with data)
            if record['api_response_status'] == 'pass' and not record['fiscal_date_ending']:
                print(f"Missing fiscal_date_ending for {symbol} {report_type} report")
                return None
            
            return record
            
        except Exception as e:
            print(f"Error transforming single report for {symbol}: {e}")
            return None
    
    def load_income_statement_data(self, records):
        """Load income statement records into the database."""
        if not records:
            print("No records to load")
            return
        
        print(f"Loading {len(records)} records into database...")
        
        with DatabaseManager(self.db_manager.db_path) as db:
            # Initialize schema if tables don't exist
            schema_path = Path(__file__).parent.parent.parent / "db" / "schema" / "postgres_stock_db_schema.sql"
            if not db.table_exists('income_statement'):
                print("Initializing database schema...")
                db.initialize_schema(schema_path)
            
            # Prepare insert query
            columns = list(records[0].keys())
            placeholders = ', '.join(['%s' for _ in columns])
            insert_query = f"""
                INSERT OR REPLACE INTO income_statement ({', '.join(columns)}) 
                VALUES ({placeholders})
            """
            
            # Convert records to list of tuples
            record_tuples = [tuple(record[col] for col in columns) for record in records]
            
            # Execute bulk insert
            rows_affected = db.execute_many(insert_query, record_tuples)
            print(f"Successfully loaded {rows_affected} records into income_statement table")
    
    def run_etl_incremental(self, exchange_filter=None, limit=None):
        """Run ETL only for symbols not yet processed.
        
        Args:
            exchange_filter: Filter by exchange (e.g., 'NASDAQ', 'NYSE')
            limit: Maximum number of symbols to process (for chunking)
        """
        print("Starting Incremental Income Statement ETL process...")
        print(f"Configuration: exchange={exchange_filter}, limit={limit}")
        
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
                    data, status = self.extract_single_income_statement(symbol)
                    
                    # Transform data
                    records = self.transform_income_statement_data(symbol, symbol_id, data, status)
                    
                    if records:
                        # Load records for this symbol
                        self.load_income_statement_data(records)
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
                    LEFT JOIN income_statement inc ON ls.symbol_id = inc.symbol_id 
                    WHERE ls.asset_type = 'Stock' AND inc.symbol_id IS NULL
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
                
                remaining_count = db.fetch_query(base_query, params)[0][0]
            
            # Print summary
            print(f"\n" + "="*50)
            print(f"Incremental Income Statement ETL Summary:")
            print(f"  Exchange: {exchange_filter or 'All exchanges'}")
            print(f"  Total symbols processed: {len(symbols)}")
            print(f"  Successful symbols: {success_count}")
            print(f"  Failed symbols: {fail_count}")
            print(f"  Remaining symbols: {remaining_count}")
            print(f"  Total records loaded: {total_records:,}")
            print(f"  Average records per symbol: {total_records/success_count if success_count > 0 else 0:.1f}")
            print(f"="*50)
            
            print("Incremental Income Statement ETL process completed successfully!")
            
        except Exception as e:
            print(f"Incremental Income Statement ETL process failed: {e}")
            raise

    def load_symbols_for_update(self, exchange_filter=None, limit=None, min_age_days=90):
        """Load symbols that need data updates (existing symbols that haven't been updated recently).
        
        Args:
            exchange_filter: Filter by exchange (e.g., 'NASDAQ', 'NYSE')
            limit: Maximum number of symbols to process
            min_age_days: Minimum days since last update (default 90 days for quarterly updates)
        """
        with self.db_manager as db:
            # First ensure the table exists, or create the schema
            if not db.table_exists('income_statement'):
                # Initialize schema to create the table
                schema_path = Path(__file__).parent.parent.parent / "db" / "schema" / "postgres_stock_db_schema.sql"
                db.initialize_schema(schema_path)
                return {}  # No symbols to update if table was just created
            
            # Find symbols that haven't been updated recently
            base_query = """
                SELECT ls.symbol_id, ls.symbol, MAX(inc.updated_at) as last_updated
                FROM listing_status ls 
                INNER JOIN income_statement inc ON ls.symbol_id = inc.symbol_id 
                WHERE ls.asset_type = 'Stock' 
                  AND (inc.updated_at IS NULL OR 
                       datetime(inc.updated_at) < datetime('now', '-{} days'))
            """.format(min_age_days)
            
            params = []
            
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

    def run_etl_update(self, exchange_filter=None, limit=None, min_age_days=90):
        """Run ETL to update existing symbols with latest data.
        
        Args:
            exchange_filter: Filter by exchange (e.g., 'NASDAQ', 'NYSE')
            limit: Maximum number of symbols to process (for chunking)
            min_age_days: Minimum days since last update (default 90 days for quarterly updates)
        """
        print("Starting Income Statement UPDATE ETL process...")
        print(f"Configuration: exchange={exchange_filter}, limit={limit}, min_age_days={min_age_days}")
        
        try:
            # Load symbols that need updates
            symbol_mapping = self.load_symbols_for_update(exchange_filter, limit, min_age_days)
            symbols = list(symbol_mapping.keys())
            print(f"Found {len(symbols)} symbols needing updates")
            
            if not symbols:
                print("No symbols need updates")
                return
            
            total_records = 0
            success_count = 0
            fail_count = 0
            
            for i, symbol in enumerate(symbols):
                symbol_id = symbol_mapping[symbol]
                
                try:
                    # Extract fresh data for this symbol
                    data, status = self.extract_single_income_statement(symbol)
                    
                    # Transform data
                    records = self.transform_income_statement_data(symbol, symbol_id, data, status)
                    
                    if records:
                        # Delete existing records for this symbol before inserting fresh data
                        with DatabaseManager(self.db_manager.db_path) as db:
                            delete_query = "DELETE FROM income_statement WHERE symbol_id = %s"
                            db.execute_query(delete_query, (symbol_id,))
                            print(f"Deleted existing records for {symbol}")
                        
                        # Load fresh records for this symbol
                        self.load_income_statement_data(records)
                        total_records += len(records)
                        success_count += 1
                        print(f"✓ Updated {symbol} (ID: {symbol_id}) - {len(records)} records [{i+1}/{len(symbols)}]")
                    else:
                        fail_count += 1
                        print(f"✗ Updated {symbol} (ID: {symbol_id}) - 0 records [{i+1}/{len(symbols)}]")
                    
                except Exception as e:
                    fail_count += 1
                    print(f"✗ Error updating {symbol} (ID: {symbol_id}): {e} [{i+1}/{len(symbols)}]")
                    # Continue processing other symbols even if one fails
                    continue
                
                # Rate limiting - wait between requests
                if i < len(symbols) - 1:
                    time.sleep(self.rate_limit_delay)
            
            # Print summary
            print(f"\n" + "="*50)
            print(f"Income Statement UPDATE ETL Summary:")
            print(f"  Exchange: {exchange_filter or 'All exchanges'}")
            print(f"  Total symbols updated: {len(symbols)}")
            print(f"  Successful updates: {success_count}")
            print(f"  Failed updates: {fail_count}")
            print(f"  Total records loaded: {total_records:,}")
            print(f"  Average records per symbol: {total_records/success_count if success_count > 0 else 0:.1f}")
            print(f"="*50)
            
            print("Income Statement UPDATE ETL process completed successfully!")
            
        except Exception as e:
            print(f"Income Statement UPDATE ETL process failed: {e}")
            raise

    def run_etl_latest_periods(self, exchange_filter=None, limit=None, periods_back=4):
        """Run ETL to get only the latest X periods for existing symbols.
        This is more efficient than full updates when you only need recent data.
        
        Args:
            exchange_filter: Filter by exchange (e.g., 'NASDAQ', 'NYSE')
            limit: Maximum number of symbols to process
            periods_back: Number of latest periods to keep (default 4 = 1 year of quarters)
        """
        print("Starting Income Statement LATEST PERIODS ETL process...")
        print(f"Configuration: exchange={exchange_filter}, limit={limit}, periods_back={periods_back}")
        
        try:
            # Load symbols that already exist in the income_statement table
            with self.db_manager as db:
                if not db.table_exists('income_statement'):
                    print("Income statement table doesn't exist. Use run_etl_incremental first.")
                    return
                
                base_query = """
                    SELECT DISTINCT ls.symbol_id, ls.symbol 
                    FROM listing_status ls 
                    INNER JOIN income_statement inc ON ls.symbol_id = inc.symbol_id 
                    WHERE ls.asset_type = 'Stock'
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
                
                if limit:
                    base_query += " LIMIT %s"
                    params.append(limit)
                
                result = db.fetch_query(base_query, params)
                symbol_mapping = {row[1]: row[0] for row in result}
            
            symbols = list(symbol_mapping.keys())
            print(f"Found {len(symbols)} existing symbols to update with latest periods")
            
            if not symbols:
                print("No existing symbols found")
                return
            
            total_records = 0
            success_count = 0
            fail_count = 0
            
            for i, symbol in enumerate(symbols):
                symbol_id = symbol_mapping[symbol]
                
                try:
                    # Extract fresh data for this symbol
                    data, status = self.extract_single_income_statement(symbol)
                    
                    if status == 'pass' and data:
                        # Keep only the latest periods for each report type
                        filtered_data = {
                            'symbol': data.get('symbol'),
                        }
                        
                        # Keep latest annual reports
                        if 'annualReports' in data and data['annualReports']:
                            # Sort by fiscal date and take the latest periods_back
                            annual_reports = sorted(
                                data['annualReports'], 
                                key=lambda x: x.get('fiscalDateEnding', '1900-01-01'), 
                                reverse=True
                            )[:periods_back]
                            filtered_data['annualReports'] = annual_reports
                        else:
                            filtered_data['annualReports'] = data.get('annualReports', [])
                        
                        # Keep latest quarterly reports
                        if 'quarterlyReports' in data and data['quarterlyReports']:
                            # Sort by fiscal date and take the latest periods_back
                            quarterly_reports = sorted(
                                data['quarterlyReports'], 
                                key=lambda x: x.get('fiscalDateEnding', '1900-01-01'), 
                                reverse=True
                            )[:periods_back]
                            filtered_data['quarterlyReports'] = quarterly_reports
                        else:
                            filtered_data['quarterlyReports'] = data.get('quarterlyReports', [])
                        
                        # Transform filtered data
                        records = self.transform_income_statement_data(symbol, symbol_id, filtered_data, status)
                        
                        if records:
                            # Delete existing records for this symbol before inserting fresh data
                            with DatabaseManager(self.db_manager.db_path) as db:
                                delete_query = "DELETE FROM income_statement WHERE symbol_id = %s"
                                db.execute_query(delete_query, (symbol_id,))
                            
                            # Load latest period records for this symbol
                            self.load_income_statement_data(records)
                            total_records += len(records)
                            success_count += 1
                            print(f"✓ Updated {symbol} (ID: {symbol_id}) - {len(records)} latest records [{i+1}/{len(symbols)}]")
                        else:
                            fail_count += 1
                            print(f"✗ Updated {symbol} (ID: {symbol_id}) - 0 records [{i+1}/{len(symbols)}]")
                    else:
                        # Handle failed extractions
                        records = self.transform_income_statement_data(symbol, symbol_id, data, status)
                        if records:
                            # Delete and replace with error records
                            with DatabaseManager(self.db_manager.db_path) as db:
                                delete_query = "DELETE FROM income_statement WHERE symbol_id = %s"
                                db.execute_query(delete_query, (symbol_id,))
                            self.load_income_statement_data(records)
                            total_records += len(records)
                        fail_count += 1
                        print(f"✗ Failed to get data for {symbol} (ID: {symbol_id}) [{i+1}/{len(symbols)}]")
                    
                except Exception as e:
                    fail_count += 1
                    print(f"✗ Error processing {symbol} (ID: {symbol_id}): {e} [{i+1}/{len(symbols)}]")
                    # Continue processing other symbols even if one fails
                    continue
                
                # Rate limiting - wait between requests
                if i < len(symbols) - 1:
                    time.sleep(self.rate_limit_delay)
            
            # Print summary
            print(f"\n" + "="*50)
            print(f"Income Statement LATEST PERIODS ETL Summary:")
            print(f"  Exchange: {exchange_filter or 'All exchanges'}")
            print(f"  Total symbols processed: {len(symbols)}")
            print(f"  Successful updates: {success_count}")
            print(f"  Failed updates: {fail_count}")
            print(f"  Total records loaded: {total_records:,}")
            print(f"  Average records per symbol: {total_records/success_count if success_count > 0 else 0:.1f}")
            print(f"  Periods kept per symbol: {periods_back}")
            print(f"="*50)
            
            print("Income Statement LATEST PERIODS ETL process completed successfully!")
            
        except Exception as e:
            print(f"Income Statement LATEST PERIODS ETL process failed: {e}")
            raise
def main():
    """Main function to run the income statement extraction."""
    
    extractor = IncomeStatementExtractor()
    
    # Configuration options for different use cases:
    
    # === INITIAL DATA COLLECTION ===
    # Option 1: Initial income statement data collection (recommended for first run)
    extractor.run_etl_incremental(exchange_filter='NYSE', limit=5)
    
    # Option 2: Process NYSE symbols
    # extractor.run_etl_incremental(exchange_filter='NYSE', limit=10)
    
    # Option 3: Large batch processing
    # extractor.run_etl_incremental(exchange_filter='NASDAQ', limit=1000)
    
    # === QUARTERLY UPDATES ===
    # Option 4: Update existing symbols with latest quarterly data (90+ days old)
    # extractor.run_etl_update(exchange_filter='NASDAQ', limit=100, min_age_days=90)
    
    # Option 5: Update all symbols regardless of age
    # extractor.run_etl_update(exchange_filter='NASDAQ', limit=50, min_age_days=0)
    
    # === LATEST PERIODS ONLY ===
    # Option 6: Keep only latest 4 periods (1 year of quarters) - most efficient for recent data
    # extractor.run_etl_latest_periods(exchange_filter='NASDAQ', limit=50, periods_back=4)
    
    # Option 7: Keep only latest 8 periods (2 years of quarters)
    # extractor.run_etl_latest_periods(exchange_filter='NASDAQ', limit=50, periods_back=8)
    
    # === PRODUCTION SCHEDULE EXAMPLES ===
    # For quarterly updates, uncomment one of these:
    
    # Monthly refresh of NASDAQ symbols (catches new quarters)
    # extractor.run_etl_update(exchange_filter='NASDAQ', min_age_days=30)
    
    # Quarterly refresh keeping only recent data (storage efficient)
    # extractor.run_etl_latest_periods(periods_back=4)

if __name__ == "__main__":
    main()