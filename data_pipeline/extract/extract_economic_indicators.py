"""
Extract economic indicators data from Alpha Vantage API and load into database.
"""

import os
import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

# Add the parent directories to the path so we can import from db
sys.path.append(str(Path(__file__).parent.parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager

# Economic indicator configurations: function_name -> (interval, display_name, maturity)
# For Treasury yields, maturity is specified; for others, it's None
ECONOMIC_INDICATOR_CONFIGS = {
    "REAL_GDP": ("quarterly", "Real GDP", None),
    "REAL_GDP_PER_CAPITA": ("quarterly", "Real GDP Per Capita", None),
    "TREASURY_YIELD_10YEAR": ("daily", "Treasury Yield 10 Year", "10year"),
    "TREASURY_YIELD_3MONTH": ("daily", "Treasury Yield 3 Month", "3month"),
    "TREASURY_YIELD_2YEAR": ("daily", "Treasury Yield 2 Year", "2year"),
    "TREASURY_YIELD_5YEAR": ("daily", "Treasury Yield 5 Year", "5year"),
    "TREASURY_YIELD_7YEAR": ("daily", "Treasury Yield 7 Year", "7year"),
    "TREASURY_YIELD_30YEAR": ("daily", "Treasury Yield 30 Year", "30year"),
    "FEDERAL_FUNDS_RATE": ("daily", "Federal Funds Rate", None),
    "CPI": ("monthly", "Consumer Price Index", None),
    "INFLATION": ("monthly", "Inflation Rate", None),
    "RETAIL_SALES": ("monthly", "Retail Sales", None),
    "DURABLES": ("monthly", "Durable Goods Orders", None),
    "UNEMPLOYMENT": ("monthly", "Unemployment Rate", None),
    "NONFARM_PAYROLL": ("monthly", "Total Nonfarm Payroll", None),
}


class EconomicIndicatorsExtractor:
    """Extract and load economic indicators data from Alpha Vantage API."""

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

    def create_economic_indicators_table_if_not_exists(self, db):
        """Create the economic_indicators table in the extracted schema if it doesn't exist."""
        create_table_sql = """
            CREATE SCHEMA IF NOT EXISTS extracted;

            CREATE TABLE IF NOT EXISTS extracted.economic_indicators (
                economic_indicator_id SERIAL PRIMARY KEY,
                economic_indicator_name VARCHAR(100) NOT NULL,
                function_name VARCHAR(50) NOT NULL,
                maturity VARCHAR(20),
                date DATE,
                interval VARCHAR(15) NOT NULL CHECK (interval IN ('daily','monthly','quarterly')),
                unit VARCHAR(50),
                value NUMERIC(15,6),
                name VARCHAR(255),
                api_response_status VARCHAR(20),
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(economic_indicator_name, function_name, maturity, date, interval)
            );

            CREATE INDEX IF NOT EXISTS idx_economic_indicators_name ON extracted.economic_indicators(economic_indicator_name);
            CREATE INDEX IF NOT EXISTS idx_economic_indicators_date ON extracted.economic_indicators(date);
        """
        db.execute_query(create_table_sql)
        print("Created extracted.economic_indicators table with indexes")

    def extract_economic_indicator_data(self, function_key):
        """Extract data for a single economic indicator from Alpha Vantage API."""
        interval, display_name, maturity = ECONOMIC_INDICATOR_CONFIGS[function_key]

        # For Treasury yields, we need to specify the function and maturity
        if function_key.startswith("TREASURY_YIELD_"):
            actual_function = "TREASURY_YIELD"
            params = {
                "function": actual_function,
                "interval": interval,
                "maturity": maturity,
                "datatype": "json",
                "apikey": self.api_key,
            }
        else:
            # For other indicators, use the function key directly
            actual_function = function_key
            params = {
                "function": actual_function,
                "interval": interval,
                "datatype": "json",
                "apikey": self.api_key,
            }

        try:
            print(f"Extracting {display_name} data...")
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()

            data = response.json()

            # Check for API error messages
            if "Error Message" in data:
                print(f"API Error for {function_key}: {data['Error Message']}")
                return None, "error", data.get("Error Message", "Unknown API error")

            if "Note" in data:
                print(f"API Note for {function_key}: {data['Note']}")
                return None, "error", data.get("Note", "API rate limit or other note")

            if "Information" in data:
                print(f"API Information for {function_key}: {data['Information']}")
                return None, "error", data.get("Information", "API information message")

            # Check if we have data
            if "data" not in data:
                print(f"No 'data' field found in response for {function_key}")
                return None, "empty", "No data field in API response"

            indicator_data = data["data"]
            if not indicator_data:
                print(f"Empty data array for {function_key}")
                return None, "empty", "Empty data array"

            # Extract metadata
            name = data.get("name", display_name)
            unit = data.get("unit", "")

            # Convert to DataFrame
            df = pd.DataFrame(indicator_data)
            df["economic_indicator_name"] = display_name
            df["function_name"] = actual_function
            df["maturity"] = maturity
            df["interval"] = interval
            df["unit"] = unit
            df["name"] = name
            df["api_response_status"] = "data"

            # Convert value to float
            df["value"] = pd.to_numeric(df["value"], errors="coerce")

            # Convert date
            df["date"] = pd.to_datetime(df["date"]).dt.date

            print(f"Successfully extracted {len(df)} records for {display_name}")
            return df, "data", f"Successfully extracted {len(df)} records"

        except requests.exceptions.RequestException as e:
            error_msg = f"Request error for {function_key}: {str(e)}"
            print(error_msg)
            return None, "error", error_msg
        except Exception as e:
            error_msg = f"Unexpected error for {function_key}: {str(e)}"
            print(error_msg)
            return None, "error", error_msg


    def get_existing_data_dates_with_db(
        self, db, indicator_name, function_name, maturity, interval
    ):
        """Get existing dates for an economic indicator to avoid duplicates using provided database connection."""
        query = """
            SELECT date
            FROM extracted.economic_indicators
            WHERE economic_indicator_name = %s AND function_name = %s
              AND interval = %s AND api_response_status = 'data'
              AND (maturity = %s OR (maturity IS NULL AND %s IS NULL))
            ORDER BY date DESC
        """
        result = db.fetch_query(
            query, (indicator_name, function_name, interval, maturity, maturity)
        )
        return [row[0] for row in result] if result else []

    def load_indicator_data_with_db(
        self, db, df, indicator_name, function_name, maturity, interval
    ):
        """Load economic indicator data into the database using provided database connection."""
        if df is None or df.empty:
            return 0

        # Ensure the destination table exists before querying it for existing
        # records. Without this check, the first run of the ETL would attempt
        # to select from a non-existent table and abort the transaction.
        if not db.table_exists("economic_indicators", schema_name="extracted"):
            self.create_economic_indicators_table_if_not_exists(db)

        existing_dates = set(
            self.get_existing_data_dates_with_db(
                db, indicator_name, function_name, maturity, interval
            )
        )

        if existing_dates:
            df = df[~df["date"].isin(existing_dates)]
            if df.empty:
                print(
                    f"All {len(existing_dates)} records for {indicator_name} already exist in database"
                )
                return 0

        records = []
        for _, row in df.iterrows():
            record = (
                row["economic_indicator_name"],
                row["function_name"],
                row["maturity"],
                row["date"],
                row["interval"],
                row["unit"],
                row["value"],
                row["name"],
                row["api_response_status"],
            )
            records.append(record)

        if not db.table_exists("economic_indicators", schema_name="extracted"):
            self.create_economic_indicators_table_if_not_exists(db)

        insert_query = """
            INSERT INTO extracted.economic_indicators
            (economic_indicator_name, function_name, maturity, date, interval, unit, value, name, api_response_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (economic_indicator_name, function_name, maturity, date, interval) DO NOTHING
        """

        inserted_count = db.execute_many(insert_query, records)
        print(f"Inserted {inserted_count} new records for {indicator_name}")
        return inserted_count

    def record_status_with_db(
        self, db, indicator_name, function_name, maturity, interval, status, message
    ):
        """Record extraction status (empty/error/pass) in database using provided database connection."""
        if not db.table_exists("economic_indicators", schema_name="extracted"):
            self.create_economic_indicators_table_if_not_exists(db)

        check_query = """
            SELECT economic_indicator_id FROM extracted.economic_indicators
            WHERE economic_indicator_name = %s AND function_name = %s
              AND interval = %s AND api_response_status = %s AND date IS NULL
              AND (maturity = %s OR (maturity IS NULL AND %s IS NULL))
        """
        existing = db.fetch_query(
            check_query,
            (indicator_name, function_name, interval, status, maturity, maturity),
        )

        if existing:
            print(f"Status record already exists for {indicator_name}: {status}")
            return

        insert_query = """
            INSERT INTO extracted.economic_indicators
            (economic_indicator_name, function_name, maturity, date, interval, unit, value, name, api_response_status)
            VALUES (%s, %s, %s, NULL, %s, NULL, NULL, %s, %s)
        """

        db.execute_query(
            insert_query,
            (indicator_name, function_name, maturity, interval, message, status),
        )
        print(f"Recorded {status} status for {indicator_name}")

    def extract_and_load_indicator_with_db(self, db, function_key):
        """Extract and load data for a single economic indicator using provided database connection."""
        interval, display_name, maturity = ECONOMIC_INDICATOR_CONFIGS[function_key]
        actual_function = function_key

        df, status, message = self.extract_economic_indicator_data(function_key)

        if status == "data":
            inserted_count = self.load_indicator_data_with_db(
                db, df, display_name, actual_function, maturity, interval
            )
            return inserted_count, status

        self.record_status_with_db(
            db, display_name, actual_function, maturity, interval, status, message
        )
        return 0, status

    def run_etl_batch(self, indicator_list=None, batch_size=5):
        """Run ETL for multiple economic indicators with batch processing and rate limiting."""
        if indicator_list is None:
            indicator_list = list(ECONOMIC_INDICATOR_CONFIGS.keys())

        print(
            f"Starting economic indicators ETL for {len(indicator_list)} indicators..."
        )
        print(f"Batch size: {batch_size}")
        print("-" * 50)

        total_inserted = 0
        status_summary = {"data": 0, "empty": 0, "error": 0, "pass": 0}

        # Use a single database connection for the entire ETL batch
        with self.db_manager as db:
            for i, function_key in enumerate(indicator_list):
                if function_key not in ECONOMIC_INDICATOR_CONFIGS:
                    print(f"Unknown economic indicator function: {function_key}")
                    continue

                display_name = ECONOMIC_INDICATOR_CONFIGS[function_key][1]
                print(f"Processing {i+1}/{len(indicator_list)}: {display_name}")

                try:
                    inserted_count, status = self.extract_and_load_indicator_with_db(
                        db, function_key
                    )
                    total_inserted += inserted_count
                    status_summary[status] += 1

                    # Rate limiting between requests
                    if (
                        i < len(indicator_list) - 1
                    ):  # Don't sleep after the last request
                        print(
                            f"Rate limiting: sleeping for {self.rate_limit_delay} seconds..."
                        )
                        time.sleep(self.rate_limit_delay)

                    # Batch processing pause
                    if (i + 1) % batch_size == 0 and i < len(indicator_list) - 1:
                        print(
                            f"Batch {(i + 1) // batch_size} completed. Pausing for 0.8 seconds..."
                        )
                        time.sleep(0.8)

                except Exception as e:
                    print(f"Error processing {display_name}: {str(e)}")
                    status_summary["error"] += 1
                    continue

                print("-" * 30)

        # Print summary
        print("\n" + "=" * 50)
        print("ECONOMIC INDICATORS ETL SUMMARY")
        print("=" * 50)
        print(f"Total indicators processed: {len(indicator_list)}")
        print(f"Total records inserted: {total_inserted}")
        print("Status breakdown:")
        for status, count in status_summary.items():
            print(f"  - {status}: {count}")
        print("=" * 50)

        return total_inserted, status_summary

    def get_database_summary(self):
        """Get summary of economic indicators data in the database."""
        # Create a fresh database manager instance to avoid connection issues
        db_manager = PostgresDatabaseManager()

        with db_manager as db:
            # Total records by status
            status_query = """
                SELECT api_response_status, COUNT(*) as count
                FROM extracted.economic_indicators
                GROUP BY api_response_status
                ORDER BY api_response_status
            """
            status_results = db.fetch_query(status_query)

            # Data records by indicator
            indicator_query = """
                SELECT economic_indicator_name, interval, COUNT(*) as count, MIN(date) as earliest, MAX(date) as latest
                FROM extracted.economic_indicators
                WHERE api_response_status = 'data'
                GROUP BY economic_indicator_name, interval, maturity
                ORDER BY economic_indicator_name, interval
            """
            indicator_results = db.fetch_query(indicator_query)

            # Latest values for each indicator
            latest_query = """
                SELECT e1.economic_indicator_name, e1.interval, e1.date, e1.value, e1.unit
                FROM extracted.economic_indicators e1
                INNER JOIN (
                    SELECT economic_indicator_name, function_name, maturity, interval, MAX(date) as max_date
                    FROM extracted.economic_indicators
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

        print("\nData Records by Indicator:")
        if indicator_results:
            for indicator, interval, count, earliest, latest in indicator_results:
                print(
                    f"  {indicator} ({interval}): {count} records from {earliest} to {latest}"
                )
        else:
            print("  No data records found")

        print("\nLatest Values:")
        if latest_results:
            for indicator, interval, date, value, unit in latest_results:
                if value is not None:
                    if unit and "%" in unit:
                        print(f"  {indicator} ({interval}): {value:.2f}% on {date}")
                    elif unit and ("thousands" in unit or "millions" in unit):
                        print(
                            f"  {indicator} ({interval}): {value:,.0f} {unit} on {date}"
                        )
                    else:
                        print(
                            f"  {indicator} ({interval}): {value:.2f} {unit} on {date}"
                        )
                else:
                    print(f"  {indicator} ({interval}): No value on {date}")
        else:
            print("  No latest values found")

        print("=" * 70)

    def create_daily_table_if_not_exists(self, db):
        """Create the economic_indicators_daily table if it doesn't exist."""
        if not db.table_exists("economic_indicators_daily", schema_name="extracted"):
            create_table_sql = """
                CREATE TABLE IF NOT EXISTS extracted.economic_indicators_daily (
                    daily_indicator_id            SERIAL PRIMARY KEY,
                    economic_indicator_name       VARCHAR(100) NOT NULL,
                    function_name                 VARCHAR(50) NOT NULL,
                    maturity                      VARCHAR(20),
                    date                         DATE NOT NULL,
                    original_interval            VARCHAR(15) NOT NULL CHECK (original_interval IN ('daily', 'monthly', 'quarterly')),
                    updated_interval             VARCHAR(15) NOT NULL DEFAULT 'daily',
                    unit                         VARCHAR(50),
                    value                        NUMERIC(15,6),
                    name                         VARCHAR(255),
                    is_forward_filled            BOOLEAN DEFAULT FALSE,
                    original_date               DATE,  -- The original date from the source data
                    created_at                  TIMESTAMP DEFAULT NOW(),
                    updated_at                  TIMESTAMP DEFAULT NOW(),
                    UNIQUE(economic_indicator_name, function_name, maturity, date)
                );
                
                CREATE INDEX IF NOT EXISTS idx_economic_indicators_daily_name ON extracted.economic_indicators_daily(economic_indicator_name);
                CREATE INDEX IF NOT EXISTS idx_economic_indicators_daily_date ON extracted.economic_indicators_daily(date);
                CREATE INDEX IF NOT EXISTS idx_economic_indicators_daily_interval ON extracted.economic_indicators_daily(original_interval);
                CREATE INDEX IF NOT EXISTS idx_economic_indicators_daily_forward_filled ON extracted.economic_indicators_daily(is_forward_filled);
            """
            db.execute_query(create_table_sql)
            print("Created extracted.economic_indicators_daily table")

    def transform_to_daily_data(self, db):
        """Transform all economic indicators data to daily frequency using forward filling."""
        print("\nStarting daily transformation of economic indicators...")

        # Create the daily table if it doesn't exist
        self.create_daily_table_if_not_exists(db)

        # Get all data records from the main table
        query = """
            SELECT economic_indicator_name, function_name, maturity, date, interval as original_interval,
                   unit, value, name
            FROM extracted.economic_indicators 
            WHERE api_response_status = 'data' AND date IS NOT NULL
            ORDER BY economic_indicator_name, function_name, maturity, date
        """

        source_data = db.fetch_query(query)
        if not source_data:
            print("No source data found for transformation")
            return 0

        print(f"Found {len(source_data)} source records to transform")

        # Group data by indicator
        df = pd.DataFrame(source_data, columns=[
            'economic_indicator_name', 'function_name', 'maturity', 'date',
            'original_interval', 'unit', 'value', 'name'
        ])

        # Convert date column to datetime
        df['date'] = pd.to_datetime(df['date'])

        total_inserted = 0

        # Get all unique indicator combinations explicitly
        unique_combos = df[['economic_indicator_name', 'function_name', 'maturity']].drop_duplicates()

        print(f"Found {len(unique_combos)} unique indicator groups to process")

        for idx, (_, combo_row) in enumerate(unique_combos.iterrows()):
            indicator_name = combo_row['economic_indicator_name']
            function_name = combo_row['function_name']
            maturity = combo_row['maturity']

            # Get the group data for this combination
            if pd.notna(maturity):
                group = df[(df['economic_indicator_name'] == indicator_name) &
                          (df['function_name'] == function_name) &
                          (df['maturity'] == maturity)]
            else:
                group = df[(df['economic_indicator_name'] == indicator_name) &
                          (df['function_name'] == function_name) &
                          (df['maturity'].isna())]
            try:
                print(f"Processing {indicator_name} ({function_name}, maturity: {maturity})...")

                check_query = """
                    SELECT COUNT(*) FROM extracted.economic_indicators_daily
                    WHERE economic_indicator_name = %s AND function_name = %s
                    AND (maturity = %s OR (maturity IS NULL AND %s IS NULL))
                """
                existing_count = db.fetch_query(
                    check_query, (indicator_name, function_name, maturity, maturity)
                )[0][0]
                if existing_count > 0:
                    print(f"  Skipping {indicator_name} - already exists")
                    continue

                # Sort by date and prepare for transformation
                group = group.sort_values('date')
                original_interval = group['original_interval'].iloc[0]
                unit = group['unit'].iloc[0]
                name = group['name'].iloc[0]

                if original_interval == 'daily':
                    # Daily data: just copy as-is
                    records = []
                    for _, row in group.iterrows():
                        records.append((
                            indicator_name, function_name, maturity, row['date'].date(),
                            original_interval, 'daily', unit, row['value'], name,
                            False, row['date'].date()  # is_forward_filled=False, original_date=same
                        ))
                    print(f"  Daily data: {len(records)} records")

                elif original_interval in ['monthly', 'quarterly']:
                    # Monthly/Quarterly data: forward fill to current date
                    records = []

                    # Get date range from first date to current date
                    start_date = group['date'].min().date()
                    end_date = date.today()

                    print(f"  {original_interval.title()} data: Forward filling from {start_date} to {end_date}")

                    # Create daily date range
                    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

                    # Forward fill the values
                    current_value = None
                    current_original_date = None

                    for current_date in date_range:
                        current_date = current_date.date()

                        # Check if we have a new value for this date
                        matching_rows = group[group['date'].dt.date == current_date]
                        if not matching_rows.empty:
                            # Update current value with new data
                            current_value = matching_rows.iloc[0]['value']
                            current_original_date = current_date
                            is_forward_filled = False
                        else:
                            # Use forward-filled value
                            is_forward_filled = True if current_value is not None else False

                        # Only add records if we have a value (either original or forward-filled)
                        if current_value is not None:
                            records.append((
                                indicator_name, function_name, maturity, current_date,
                                original_interval, 'daily', unit, current_value, name,
                                is_forward_filled, current_original_date
                            ))

                    print(f"  {original_interval.title()} data: {len(records)} daily records (forward-filled from {len(group)} original records)")

                # Insert the transformed records
                if records:
                    insert_query = """
                        INSERT INTO extracted.economic_indicators_daily
                        (economic_indicator_name, function_name, maturity, date, original_interval,
                         updated_interval, unit, value, name, is_forward_filled, original_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (economic_indicator_name, function_name, maturity, date) DO NOTHING
                    """

                    inserted_count = db.execute_many(insert_query, records)
                    total_inserted += inserted_count
                    print(f"  Inserted {inserted_count} daily records")
                else:
                    print(f"  No records to insert for {indicator_name}")

            except Exception as e:
                print(f"  ERROR processing {indicator_name}: {str(e)}")
                continue  # Continue with next indicator

        print(f"\nDaily transformation completed: {total_inserted} total records inserted")
        return total_inserted

    def run_etl_with_daily_transform(self, indicator_list=None, batch_size=5, transform_to_daily=True):
        """Run ETL for economic indicators and optionally transform to daily data."""
        print("Starting economic indicators ETL with daily transformation...")

        daily_inserted = 0

        with self.db_manager as db:
            total_inserted, status_summary = self.run_etl_batch_with_db(db, indicator_list, batch_size)

            if transform_to_daily:
                daily_inserted = self.transform_to_daily_data(db)

        print("\nTotal ETL Summary:")
        print(f"  Original data inserted: {total_inserted}")
        print(f"  Daily transformed records: {daily_inserted}")

        return total_inserted, daily_inserted, status_summary

    def run_etl_batch_with_db(self, db, indicator_list=None, batch_size=5):
        """Run ETL for multiple economic indicators using provided database connection."""
        if indicator_list is None:
            indicator_list = list(ECONOMIC_INDICATOR_CONFIGS.keys())

        print(f"Starting economic indicators ETL for {len(indicator_list)} indicators...")
        print(f"Batch size: {batch_size}")
        print("-" * 50)

        total_inserted = 0
        status_summary = {"data": 0, "empty": 0, "error": 0, "pass": 0}

        for i, function_key in enumerate(indicator_list):
            if function_key not in ECONOMIC_INDICATOR_CONFIGS:
                print(f"Unknown economic indicator function: {function_key}")
                continue

            display_name = ECONOMIC_INDICATOR_CONFIGS[function_key][1]
            print(f"Processing {i+1}/{len(indicator_list)}: {display_name}")

            try:
                inserted_count, status = self.extract_and_load_indicator_with_db(
                    db, function_key
                )
                total_inserted += inserted_count
                status_summary[status] += 1

                # Rate limiting between requests
                if i < len(indicator_list) - 1:  # Don't sleep after the last request
                    print(f"Rate limiting: sleeping for {self.rate_limit_delay} seconds...")
                    time.sleep(self.rate_limit_delay)

                # Batch processing pause
                if (i + 1) % batch_size == 0 and i < len(indicator_list) - 1:
                    print(f"Batch {(i + 1) // batch_size} completed. Pausing for 0.8 seconds...")
                    time.sleep(0.8)

            except Exception as e:
                print(f"Error processing {display_name}: {str(e)}")
                status_summary["error"] += 1
                continue

            print("-" * 30)

        # Print summary
        print("\n" + "=" * 50)
        print("ECONOMIC INDICATORS ETL SUMMARY")
        print("=" * 50)
        print(f"Total indicators processed: {len(indicator_list)}")
        print(f"Total records inserted: {total_inserted}")
        print("Status breakdown:")
        for status, count in status_summary.items():
            print(f"  - {status}: {count}")
        print("=" * 50)

        return total_inserted, status_summary


def main():
    """Main function for economic indicators extraction."""

    extractor = EconomicIndicatorsExtractor()

    # Run extraction for all indicators using the master list defined in
    # ECONOMIC_INDICATOR_CONFIGS.  This pulls every available indicator regardless
    # of its update interval (daily, monthly, or quarterly).
    extractor.run_etl_with_daily_transform()

    print("\nFinal Database Summary:")
    # Small delay to ensure previous connection is fully closed
    time.sleep(0.1)
    extractor.get_database_summary()


if __name__ == "__main__":
    main()
