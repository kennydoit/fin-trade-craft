"""
Extract economic indicators data from Alpha Vantage API and load into database.
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

# Economic indicator configurations: function_name -> (interval, display_name, maturity)
# For Treasury yields, maturity is specified; for others, it's None
ECONOMIC_INDICATOR_CONFIGS = {
    'REAL_GDP': ('quarterly', 'Real GDP', None),
    'REAL_GDP_PER_CAPITA': ('quarterly', 'Real GDP Per Capita', None),
    'TREASURY_YIELD_10YEAR': ('daily', 'Treasury Yield 10 Year', '10year'),
    'TREASURY_YIELD_3MONTH': ('daily', 'Treasury Yield 3 Month', '3month'),
    'TREASURY_YIELD_2YEAR': ('daily', 'Treasury Yield 2 Year', '2year'),
    'TREASURY_YIELD_5YEAR': ('daily', 'Treasury Yield 5 Year', '5year'),
    'TREASURY_YIELD_7YEAR': ('daily', 'Treasury Yield 7 Year', '7year'),
    'TREASURY_YIELD_30YEAR': ('daily', 'Treasury Yield 30 Year', '30year'),
    'FEDERAL_FUNDS_RATE': ('daily', 'Federal Funds Rate', None),
    'CPI': ('monthly', 'Consumer Price Index', None),
    'INFLATION': ('monthly', 'Inflation Rate', None),
    'RETAIL_SALES': ('monthly', 'Retail Sales', None),
    'DURABLES': ('monthly', 'Durable Goods Orders', None),
    'UNEMPLOYMENT': ('monthly', 'Unemployment Rate', None),
    'NONFARM_PAYROLL': ('monthly', 'Total Nonfarm Payroll', None)
}

class EconomicIndicatorsExtractor:
    """Extract and load economic indicators data from Alpha Vantage API."""
    
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
    
    def get_existing_data_dates(self, indicator_name, function_name, maturity, interval):
        """Get existing dates for an economic indicator to avoid duplicates."""
        with DatabaseManager(self.db_manager.db_path) as db:
            query = """
                SELECT date 
                FROM economic_indicators 
                WHERE economic_indicator_name = %s AND function_name = %s 
                  AND interval = %s AND api_response_status = 'data'
                  AND (maturity = %s OR (maturity IS NULL AND %s IS NULL))
                ORDER BY date DESC
            """
            result = db.fetch_query(query, (indicator_name, function_name, interval, maturity, maturity))
            return [row[0] for row in result] if result else []
    
    def get_last_update_date(self, indicator_name, function_name, maturity, interval):
        """Get the most recent date for an economic indicator."""
        with DatabaseManager(self.db_manager.db_path) as db:
            query = """
                SELECT MAX(date) 
                FROM economic_indicators 
                WHERE economic_indicator_name = %s AND function_name = %s 
                  AND interval = %s AND api_response_status = 'data'
                  AND (maturity = %s OR (maturity IS NULL AND %s IS NULL))
            """
            result = db.fetch_query(query, (indicator_name, function_name, interval, maturity, maturity))
            return result[0][0] if result and result[0] and result[0][0] else None
    
    def extract_economic_indicator_data(self, function_key):
        """Extract data for a single economic indicator from Alpha Vantage API."""
        interval, display_name, maturity = ECONOMIC_INDICATOR_CONFIGS[function_key]
        
        # For Treasury yields, we need to specify the function and maturity
        if function_key.startswith('TREASURY_YIELD_'):
            actual_function = 'TREASURY_YIELD'
            params = {
                'function': actual_function,
                'interval': interval,
                'maturity': maturity,
                'datatype': 'json',
                'apikey': self.api_key
            }
        else:
            # For other indicators, use the function key directly
            actual_function = function_key
            params = {
                'function': actual_function,
                'interval': interval,
                'datatype': 'json',
                'apikey': self.api_key
            }
        
        try:
            print(f"Extracting {display_name} data...")
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API error messages
            if 'Error Message' in data:
                print(f"API Error for {function_key}: {data['Error Message']}")
                return None, 'error', data.get('Error Message', 'Unknown API error')
            
            if 'Note' in data:
                print(f"API Note for {function_key}: {data['Note']}")
                return None, 'error', data.get('Note', 'API rate limit or other note')
                
            if 'Information' in data:
                print(f"API Information for {function_key}: {data['Information']}")
                return None, 'error', data.get('Information', 'API information message')
            
            # Check if we have data
            if 'data' not in data:
                print(f"No 'data' field found in response for {function_key}")
                return None, 'empty', 'No data field in API response'
            
            indicator_data = data['data']
            if not indicator_data:
                print(f"Empty data array for {function_key}")
                return None, 'empty', 'Empty data array'
            
            # Extract metadata
            name = data.get('name', display_name)
            unit = data.get('unit', '')
            
            # Convert to DataFrame
            df = pd.DataFrame(indicator_data)
            df['economic_indicator_name'] = display_name
            df['function_name'] = actual_function
            df['maturity'] = maturity
            df['interval'] = interval
            df['unit'] = unit
            df['name'] = name
            df['api_response_status'] = 'data'
            
            # Convert value to float
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            
            # Convert date
            df['date'] = pd.to_datetime(df['date']).dt.date
            
            print(f"Successfully extracted {len(df)} records for {display_name}")
            return df, 'data', f"Successfully extracted {len(df)} records"
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Request error for {function_key}: {str(e)}"
            print(error_msg)
            return None, 'error', error_msg
        except Exception as e:
            error_msg = f"Unexpected error for {function_key}: {str(e)}"
            print(error_msg)
            return None, 'error', error_msg
    
    def load_economic_indicator_data(self, df, indicator_name, function_name, maturity, interval):
        """Load economic indicator data into the database."""
        if df is None or df.empty:
            return 0
        
        # Get existing dates to avoid duplicates
        existing_dates = set(self.get_existing_data_dates(indicator_name, function_name, maturity, interval))
        
        # Filter out existing dates
        if existing_dates:
            df = df[~df['date'].isin(existing_dates)]
            if df.empty:
                print(f"All {len(existing_dates)} records for {indicator_name} already exist in database")
                return 0
        
        # Prepare data for insertion
        records = []
        for _, row in df.iterrows():
            record = (
                row['economic_indicator_name'],
                row['function_name'],
                row['maturity'],
                row['date'],
                row['interval'],
                row['unit'],
                row['value'],
                row['name'],
                row['api_response_status']
            )
            records.append(record)
        
        # Insert data
        with DatabaseManager(self.db_manager.db_path) as db:
            # Initialize schema if tables don't exist
            schema_path = Path(__file__).parent.parent.parent / "db" / "schema" / "postgres_stock_db_schema.sql"
            if not db.table_exists('economic_indicators'):
                print("Initializing database schema...")
                db.initialize_schema(schema_path)
                
            insert_query = """
                INSERT OR IGNORE INTO economic_indicators 
                (economic_indicator_name, function_name, maturity, date, interval, unit, value, name, api_response_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            inserted_count = db.execute_many(insert_query, records)
        
        print(f"Inserted {inserted_count} new records for {indicator_name}")
        return inserted_count
    
    def record_status(self, indicator_name, function_name, maturity, interval, status, message):
        """Record extraction status (empty/error/pass) in database."""
        with DatabaseManager(self.db_manager.db_path) as db:
            # Initialize schema if tables don't exist
            schema_path = Path(__file__).parent.parent.parent / "db" / "schema" / "postgres_stock_db_schema.sql"
            if not db.table_exists('economic_indicators'):
                print("Initializing database schema...")
                db.initialize_schema(schema_path)
                
            # Check if status record already exists
            check_query = """
                SELECT economic_indicator_id FROM economic_indicators 
                WHERE economic_indicator_name = %s AND function_name = %s 
                  AND interval = %s AND api_response_status = %s AND date IS NULL
                  AND (maturity = %s OR (maturity IS NULL AND %s IS NULL))
            """
            existing = db.fetch_query(check_query, (indicator_name, function_name, interval, status, maturity, maturity))
            
            if existing:
                print(f"Status record already exists for {indicator_name}: {status}")
                return
            
            # Insert status record
            insert_query = """
                INSERT INTO economic_indicators 
                (economic_indicator_name, function_name, maturity, date, interval, unit, value, name, api_response_status)
                VALUES (%s, %s, %s, NULL, %s, NULL, NULL, %s, %s)
            """
            
            db.execute_query(insert_query, (indicator_name, function_name, maturity, interval, message, status))
            print(f"Recorded {status} status for {indicator_name}")
    
    def extract_and_load_indicator(self, function_key, force_update=False):
        """Extract and load data for a single economic indicator."""
        interval, display_name, maturity = ECONOMIC_INDICATOR_CONFIGS[function_key]
        actual_function = 'TREASURY_YIELD' if function_key.startswith('TREASURY_YIELD_') else function_key
        
        # Check if we should skip (unless force_update)
        if not force_update:
            last_update = self.get_last_update_date(display_name, actual_function, maturity, interval)
            if last_update:
                print(f"Data exists for {display_name} (last update: {last_update}). Use force_update=True to refresh.")
                return 0, 'pass'
        
        # Extract data from API
        df, status, message = self.extract_economic_indicator_data(function_key)
        
        if status == 'data':
            # Load data into database
            inserted_count = self.load_economic_indicator_data(df, display_name, actual_function, maturity, interval)
            return inserted_count, status
        else:
            # Record status (empty/error)
            self.record_status(display_name, actual_function, maturity, interval, status, message)
            return 0, status
    
    def run_etl_batch(self, indicator_list=None, batch_size=5, force_update=False):
        """Run ETL for multiple economic indicators with batch processing and rate limiting."""
        if indicator_list is None:
            indicator_list = list(ECONOMIC_INDICATOR_CONFIGS.keys())
        
        print(f"Starting economic indicators ETL for {len(indicator_list)} indicators...")
        print(f"Batch size: {batch_size}, Force update: {force_update}")
        print("-" * 50)
        
        total_inserted = 0
        status_summary = {'data': 0, 'empty': 0, 'error': 0, 'pass': 0}
        
        for i, function_key in enumerate(indicator_list):
            if function_key not in ECONOMIC_INDICATOR_CONFIGS:
                print(f"Unknown economic indicator function: {function_key}")
                continue
                
            display_name = ECONOMIC_INDICATOR_CONFIGS[function_key][1]
            print(f"Processing {i+1}/{len(indicator_list)}: {display_name}")
            
            try:
                inserted_count, status = self.extract_and_load_indicator(function_key, force_update)
                total_inserted += inserted_count
                status_summary[status] += 1
                
                # Rate limiting between requests
                if i < len(indicator_list) - 1:  # Don't sleep after the last request
                    print(f"Rate limiting: sleeping for {self.rate_limit_delay} seconds...")
                    time.sleep(self.rate_limit_delay)
                
                # Batch processing pause
                if (i + 1) % batch_size == 0 and i < len(indicator_list) - 1:
                    print(f"Batch {(i + 1) // batch_size} completed. Pausing for 2 seconds...")
                    time.sleep(2)
                    
            except Exception as e:
                print(f"Error processing {display_name}: {str(e)}")
                status_summary['error'] += 1
                continue
            
            print("-" * 30)
        
        # Print summary
        print("\n" + "=" * 50)
        print("ECONOMIC INDICATORS ETL SUMMARY")
        print("=" * 50)
        print(f"Total indicators processed: {len(indicator_list)}")
        print(f"Total records inserted: {total_inserted}")
        print(f"Status breakdown:")
        for status, count in status_summary.items():
            print(f"  - {status}: {count}")
        print("=" * 50)
        
        return total_inserted, status_summary
    
    def run_etl_update(self, indicator_list=None, batch_size=5):
        """Run ETL update to refresh latest data for economic indicators."""
        print("Running ECONOMIC INDICATORS UPDATE ETL...")
        return self.run_etl_batch(indicator_list, batch_size, force_update=True)
    
    def get_indicators_needing_update(self, days_threshold=1):
        """Get list of economic indicators that need updates based on last update date."""
        from datetime import datetime, timedelta
        
        threshold_date = datetime.now().date() - timedelta(days=days_threshold)
        
        with DatabaseManager(self.db_manager.db_path) as db:
            query = """
                SELECT DISTINCT economic_indicator_name, function_name, maturity, MAX(date) as last_date
                FROM economic_indicators 
                WHERE api_response_status = 'data'
                GROUP BY economic_indicator_name, function_name, maturity
                HAVING last_date IS NULL OR last_date <= %s
            """
            result = db.fetch_query(query, (threshold_date,))
            
            if result:
                function_keys = []
                for row in result:
                    # Find the function key for this indicator
                    for func_key, (interval, display_name, maturity) in ECONOMIC_INDICATOR_CONFIGS.items():
                        if display_name == row[0] and (maturity == row[2] or (maturity is None and row[2] is None)):
                            function_keys.append(func_key)
                            break
                return function_keys
            return []
    
    def run_etl_latest_periods(self, days_threshold=1, batch_size=5):
        """Run ETL for economic indicators needing updates based on last update date."""
        indicator_functions = self.get_indicators_needing_update(days_threshold)
        
        if not indicator_functions:
            print(f"No economic indicators need updates (threshold: {days_threshold} days)")
            return 0, {'pass': len(ECONOMIC_INDICATOR_CONFIGS)}
        
        print(f"Found {len(indicator_functions)} economic indicators needing updates...")
        return self.run_etl_update(indicator_functions, batch_size)
    
    def get_database_summary(self):
        """Get summary of economic indicators data in the database."""
        with DatabaseManager(self.db_manager.db_path) as db:
            # Total records by status
            status_query = """
                SELECT api_response_status, COUNT(*) as count
                FROM economic_indicators 
                GROUP BY api_response_status
                ORDER BY api_response_status
            """
            status_results = db.fetch_query(status_query)
            
            # Data records by indicator
            indicator_query = """
                SELECT economic_indicator_name, interval, COUNT(*) as count, MIN(date) as earliest, MAX(date) as latest
                FROM economic_indicators 
                WHERE api_response_status = 'data'
                GROUP BY economic_indicator_name, interval, maturity
                ORDER BY economic_indicator_name, interval
            """
            indicator_results = db.fetch_query(indicator_query)
            
            # Latest values for each indicator
            latest_query = """
                SELECT e1.economic_indicator_name, e1.interval, e1.date, e1.value, e1.unit
                FROM economic_indicators e1
                INNER JOIN (
                    SELECT economic_indicator_name, function_name, maturity, interval, MAX(date) as max_date
                    FROM economic_indicators 
                    WHERE api_response_status = 'data'
                    GROUP BY economic_indicator_name, function_name, maturity, interval
                ) e2 ON e1.economic_indicator_name = e2.economic_indicator_name 
                     AND e1.function_name = e2.function_name
                     AND (e1.maturity = e2.maturity OR (e1.maturity IS NULL AND e2.maturity IS NULL))
                     AND e1.interval = e2.interval 
                     AND e1.date = e2.max_date
                ORDER BY e1.economic_indicator_name, e1.interval
            """
            latest_results = db.fetch_query(latest_query)
        
        print("\n" + "=" * 70)
        print("ECONOMIC INDICATORS DATABASE SUMMARY")
        print("=" * 70)
        
        print("\nRecords by Status:")
        if status_results:
            for status, count in status_results:
                print(f"  {status}: {count}")
        else:
            print("  No records found")
        
        print(f"\nData Records by Indicator:")
        if indicator_results:
            for indicator, interval, count, earliest, latest in indicator_results:
                print(f"  {indicator} ({interval}): {count} records from {earliest} to {latest}")
        else:
            print("  No data records found")
        
        print(f"\nLatest Values:")
        if latest_results:
            for indicator, interval, date, value, unit in latest_results:
                if value is not None:
                    if unit and '%' in unit:
                        print(f"  {indicator} ({interval}): {value:.2f}% on {date}")
                    elif unit and ('thousands' in unit or 'millions' in unit):
                        print(f"  {indicator} ({interval}): {value:,.0f} {unit} on {date}")
                    else:
                        print(f"  {indicator} ({interval}): {value:.2f} {unit} on {date}")
                else:
                    print(f"  {indicator} ({interval}): No value on {date}")
        else:
            print("  No latest values found")
        
        print("=" * 70)


def main():
    """Main function for economic indicators extraction."""
    
    extractor = EconomicIndicatorsExtractor()
    
    # Configuration options for different use cases:
    
    # === INITIAL DATA COLLECTION ===
    # Option 1: Test with a few indicators first (recommended for first run)
    test_indicators = ['REAL_GDP', 'FEDERAL_FUNDS_RATE', 'UNEMPLOYMENT']
    extractor.run_etl_batch(test_indicators, batch_size=2, force_update=False)
    
    # Option 2: Extract GDP and related indicators
    # gdp_indicators = ['REAL_GDP', 'REAL_GDP_PER_CAPITA']
    # extractor.run_etl_batch(gdp_indicators, batch_size=2, force_update=False)
    
    # Option 3: Extract Treasury yields (all maturities)
    # treasury_indicators = ['TREASURY_YIELD_3MONTH', 'TREASURY_YIELD_2YEAR', 'TREASURY_YIELD_5YEAR', 
    #                       'TREASURY_YIELD_7YEAR', 'TREASURY_YIELD_10YEAR', 'TREASURY_YIELD_30YEAR']
    # extractor.run_etl_batch(treasury_indicators, batch_size=3, force_update=False)
    
    # Option 4: Extract labor market indicators
    # labor_indicators = ['UNEMPLOYMENT', 'NONFARM_PAYROLL']
    # extractor.run_etl_batch(labor_indicators, batch_size=2, force_update=False)
    
    # Option 5: Extract inflation and price indicators
    # inflation_indicators = ['CPI', 'INFLATION']
    # extractor.run_etl_batch(inflation_indicators, batch_size=2, force_update=False)
    
    # Option 6: Extract all economic indicators (full dataset)
    # extractor.run_etl_batch(force_update=False)  # Uses all indicators by default
    
    # === DATA UPDATES ===
    # Option 7: Update all indicators with fresh data (use sparingly due to API limits)
    # extractor.run_etl_update(batch_size=3)
    
    # Option 8: Update only indicators that are older than 7 days
    # extractor.run_etl_latest_periods(days_threshold=7, batch_size=3)
    
    # Option 9: Force update specific indicators
    # priority_indicators = ['FEDERAL_FUNDS_RATE', 'TREASURY_YIELD_10YEAR', 'UNEMPLOYMENT']
    # extractor.run_etl_update(priority_indicators, batch_size=2)
    
    # === PRODUCTION SCHEDULE EXAMPLES ===
    # For daily updates of daily indicators:
    # daily_indicators = ['TREASURY_YIELD_10YEAR', 'TREASURY_YIELD_3MONTH', 'TREASURY_YIELD_2YEAR', 
    #                    'TREASURY_YIELD_5YEAR', 'TREASURY_YIELD_7YEAR', 'TREASURY_YIELD_30YEAR', 'FEDERAL_FUNDS_RATE']
    # extractor.run_etl_latest_periods(days_threshold=1, batch_size=3)
    
    # For monthly updates of monthly indicators:
    # monthly_indicators = ['CPI', 'INFLATION', 'RETAIL_SALES', 'DURABLES', 'UNEMPLOYMENT', 'NONFARM_PAYROLL']
    # extractor.run_etl_latest_periods(days_threshold=30, batch_size=3)
    
    # For quarterly updates of quarterly indicators:
    # quarterly_indicators = ['REAL_GDP', 'REAL_GDP_PER_CAPITA']
    # extractor.run_etl_latest_periods(days_threshold=90, batch_size=2)
    
    print("\nFinal Database Summary:")
    extractor.get_database_summary()


if __name__ == "__main__":
    main()
