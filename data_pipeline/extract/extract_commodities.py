"""
Extract commodities data from Alpha Vantage API and load into database.
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

# Commodity configurations: function_name -> (interval, display_name)
COMMODITY_CONFIGS = {
    "WTI": ("daily", "Crude Oil WTI"),
    "BRENT": ("daily", "Crude Oil Brent"),
    "NATURAL_GAS": ("daily", "Natural Gas"),
    "COPPER": ("monthly", "Copper"),
    "ALUMINUM": ("monthly", "Aluminum"),
    "WHEAT": ("monthly", "Wheat"),
    "CORN": ("monthly", "Corn"),
    "COTTON": ("monthly", "Cotton"),
    "SUGAR": ("monthly", "Sugar"),
    "COFFEE": ("monthly", "Coffee"),
    "ALL_COMMODITIES": ("monthly", "Global Commodities Index"),
}


class CommoditiesExtractor:
    """Extract and load commodities data from Alpha Vantage API."""

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

    def create_commodities_table_if_not_exists(self, db):
        """Create the commodities table in the extracted schema if it doesn't exist."""
        create_table_sql = """
            CREATE SCHEMA IF NOT EXISTS extracted;

            CREATE TABLE IF NOT EXISTS extracted.commodities (
                commodity_id        SERIAL PRIMARY KEY,
                commodity_name      VARCHAR(100) NOT NULL,
                function_name       VARCHAR(50) NOT NULL,
                date                DATE,
                interval            VARCHAR(10) NOT NULL CHECK (interval IN ('daily','monthly')),
                unit                VARCHAR(50),
                value               NUMERIC(15,6),
                name                VARCHAR(255),
                api_response_status VARCHAR(20),
                created_at          TIMESTAMP DEFAULT NOW(),
                updated_at          TIMESTAMP DEFAULT NOW(),
                UNIQUE(commodity_name, date, interval)
            );

            CREATE INDEX IF NOT EXISTS idx_commodities_name ON extracted.commodities(commodity_name);
            CREATE INDEX IF NOT EXISTS idx_commodities_date ON extracted.commodities(date);
        """
        db.execute_query(create_table_sql)
        print("Created extracted.commodities table with indexes")

    def get_existing_data_dates_with_db(self, db, commodity_name, interval):
        """Get existing dates for a commodity to avoid duplicates using provided database connection."""
        query = """
            SELECT date
            FROM extracted.commodities
            WHERE commodity_name = %s AND interval = %s AND api_response_status = 'data'
            ORDER BY date DESC
        """
        result = db.fetch_query(query, (commodity_name, interval))
        return [row[0] for row in result] if result else []

    def extract_commodity_data(self, function_name):  # noqa: PLR0911
        """Extract data for a single commodity from Alpha Vantage API."""
        interval, display_name = COMMODITY_CONFIGS[function_name]

        params = {
            "function": function_name,
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
                print(f"API Error for {function_name}: {data['Error Message']}")
                return None, "error", data.get("Error Message", "Unknown API error")

            if "Note" in data:
                print(f"API Note for {function_name}: {data['Note']}")
                return None, "error", data.get("Note", "API rate limit or other note")

            if "Information" in data:
                print(f"API Information for {function_name}: {data['Information']}")
                return None, "error", data.get("Information", "API information message")

            # Check if we have data
            if "data" not in data:
                print(f"No 'data' field found in response for {function_name}")
                return None, "empty", "No data field in API response"

            commodity_data = data["data"]
            if not commodity_data:
                print(f"Empty data array for {function_name}")
                return None, "empty", "Empty data array"

            # Extract metadata
            name = data.get("name", display_name)
            unit = data.get("unit", "")

            # Convert to DataFrame
            df = pd.DataFrame(commodity_data)
            df["commodity_name"] = display_name
            df["function_name"] = function_name
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
            error_msg = f"Request error for {function_name}: {str(e)}"
            print(error_msg)
            return None, "error", error_msg
        except Exception as e:
            error_msg = f"Unexpected error for {function_name}: {str(e)}"
            print(error_msg)
            return None, "error", error_msg

    def load_commodity_data_with_db(
        self, db, df, commodity_name, _function_name, interval
    ):
        """Load commodity data into the database using provided database connection."""
        if df is None or df.empty:
            return 0

        # Get existing dates to avoid duplicates
        existing_dates = set(
            self.get_existing_data_dates_with_db(db, commodity_name, interval)
        )

        # Filter out existing dates
        if existing_dates:
            df = df[~df["date"].isin(existing_dates)]
            if df.empty:
                print(
                    f"All {len(existing_dates)} records for {commodity_name} already exist in database"
                )
                return 0

        # Prepare data for insertion
        records = []
        for _, row in df.iterrows():
            record = (
                row["commodity_name"],
                row["function_name"],
                row["date"],
                row["interval"],
                row["unit"],
                row["value"],
                row["name"],
                row["api_response_status"],
            )
            records.append(record)

        if not db.table_exists("extracted.commodities"):
            self.create_commodities_table_if_not_exists(db)

        insert_query = """
            INSERT INTO extracted.commodities
            (commodity_name, function_name, date, interval, unit, value, name, api_response_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (commodity_name, date, interval) DO NOTHING
        """

        inserted_count = db.execute_many(insert_query, records)
        print(f"Inserted {inserted_count} new records for {commodity_name}")
        return inserted_count


    def record_status_with_db(  # noqa: PLR0913
        self, db, commodity_name, function_name, interval, status, message
    ) -> None:
        """Record extraction status (empty/error/pass) in database using provided database connection."""
        if not db.table_exists("extracted.commodities"):
            self.create_commodities_table_if_not_exists(db)

        # Check if status record already exists
        check_query = """
            SELECT commodity_id FROM extracted.commodities
            WHERE commodity_name = %s AND function_name = %s AND interval = %s
              AND api_response_status = %s AND date IS NULL
        """
        existing = db.fetch_query(
            check_query, (commodity_name, function_name, interval, status)
        )

        if existing:
            print(f"Status record already exists for {commodity_name}: {status}")
            return

        insert_query = """
            INSERT INTO extracted.commodities
            (commodity_name, function_name, date, interval, unit, value, name, api_response_status)
            VALUES (%s, %s, NULL, %s, NULL, NULL, %s, %s)
        """

        db.execute_query(
            insert_query, (commodity_name, function_name, interval, message, status)
        )
        print(f"Recorded {status} status for {commodity_name}")

    def extract_and_load_commodity_with_db(self, db, function_name):
        """Extract and load data for a single commodity using provided database connection."""
        interval, display_name = COMMODITY_CONFIGS[function_name]

        # Extract data from API
        df, status, message = self.extract_commodity_data(function_name)

        if status == "data":
            inserted_count = self.load_commodity_data_with_db(
                db, df, display_name, function_name, interval
            )
            return inserted_count, status

        self.record_status_with_db(
            db, display_name, function_name, interval, status, message
        )
        return 0, status

    def run_etl_batch(self, commodity_list=None, batch_size=5):
        """Run ETL for multiple commodities with batch processing and rate limiting."""
        if commodity_list is None:
            commodity_list = list(COMMODITY_CONFIGS.keys())

        print(f"Starting commodities ETL for {len(commodity_list)} commodities...")
        print(f"Batch size: {batch_size}")
        print("-" * 50)

        total_inserted = 0
        status_summary = {"data": 0, "empty": 0, "error": 0, "pass": 0}

        # Use a single database connection for the entire ETL batch
        with self.db_manager as db:
            for i, function_name in enumerate(commodity_list):
                if function_name not in COMMODITY_CONFIGS:
                    print(f"Unknown commodity function: {function_name}")
                    continue

                display_name = COMMODITY_CONFIGS[function_name][1]
                print(f"Processing {i+1}/{len(commodity_list)}: {display_name}")

                try:
                    inserted_count, status = self.extract_and_load_commodity_with_db(
                        db, function_name
                    )
                    total_inserted += inserted_count
                    status_summary[status] += 1

                    # Rate limiting between requests
                    if (
                        i < len(commodity_list) - 1
                    ):  # Don't sleep after the last request
                        print(
                            f"Rate limiting: sleeping for {self.rate_limit_delay} seconds..."
                        )
                        time.sleep(self.rate_limit_delay)

                    # Batch processing pause
                    if (i + 1) % batch_size == 0 and i < len(commodity_list) - 1:
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
        print("COMMODITIES ETL SUMMARY")
        print("=" * 50)
        print(f"Total commodities processed: {len(commodity_list)}")
        print(f"Total records inserted: {total_inserted}")
        print("Status breakdown:")
        for status, count in status_summary.items():
            print(f"  - {status}: {count}")
        print("=" * 50)

        return total_inserted, status_summary

    def create_daily_table_if_not_exists(self, db):
        """Create the commodities_daily table if it doesn't exist."""
        if not db.table_exists("commodities_daily", schema_name="extracted"):
            create_table_sql = """
                CREATE TABLE IF NOT EXISTS extracted.commodities_daily (
                    daily_commodity_id       SERIAL PRIMARY KEY,
                    commodity_name           VARCHAR(100) NOT NULL,
                    function_name            VARCHAR(50) NOT NULL,
                    date                     DATE NOT NULL,
                    original_interval        VARCHAR(15) NOT NULL CHECK (original_interval IN ('daily','monthly','quarterly')),
                    updated_interval         VARCHAR(15) NOT NULL DEFAULT 'daily',
                    unit                     VARCHAR(50),
                    value                    NUMERIC(15,6),
                    name                     VARCHAR(255),
                    is_forward_filled        BOOLEAN DEFAULT FALSE,
                    original_date            DATE,
                    created_at               TIMESTAMP DEFAULT NOW(),
                    updated_at               TIMESTAMP DEFAULT NOW(),
                    UNIQUE(commodity_name, function_name, date)
                );

                CREATE INDEX IF NOT EXISTS idx_commodities_daily_name ON extracted.commodities_daily(commodity_name);
                CREATE INDEX IF NOT EXISTS idx_commodities_daily_date ON extracted.commodities_daily(date);
                CREATE INDEX IF NOT EXISTS idx_commodities_daily_interval ON extracted.commodities_daily(original_interval);
                CREATE INDEX IF NOT EXISTS idx_commodities_daily_forward_filled ON extracted.commodities_daily(is_forward_filled);
            """
            db.execute_query(create_table_sql)
            print("Created extracted.commodities_daily table")

    def transform_to_daily_data(self, db):  # noqa: C901, PLR0912, PLR0915
        """Transform all commodities data to daily frequency using forward filling."""
        print("\nStarting daily transformation of commodities...")

        # Create the daily table if it doesn't exist
        self.create_daily_table_if_not_exists(db)

        # Get all data records from the main commodities table
        query = """
            SELECT commodity_name, function_name, date, interval as original_interval,
                   unit, value, name
            FROM extracted.commodities
            WHERE api_response_status = 'data' AND date IS NOT NULL
            ORDER BY commodity_name, function_name, date
        """
        source_data = db.fetch_query(query)
        if not source_data:
            print("No source data found for transformation")
            return 0

        print(f"Found {len(source_data)} source records to transform")

        df = pd.DataFrame(
            source_data,
            columns=[
                "commodity_name",
                "function_name",
                "date",
                "original_interval",
                "unit",
                "value",
                "name",
            ],
        )
        df["date"] = pd.to_datetime(df["date"])

        total_inserted = 0

        for (commodity_name, function_name), group in df.groupby(
            ["commodity_name", "function_name"]
        ):
            try:
                group_df = group.sort_values("date")
                original_interval = group_df["original_interval"].iloc[0]
                unit = group_df["unit"].iloc[0]
                name = group_df["name"].iloc[0]

                if original_interval == "daily":
                    records = []
                    for _, row in group_df.iterrows():
                        records.append(
                            (
                                commodity_name,
                                function_name,
                                row["date"].date(),
                                original_interval,
                                "daily",
                                unit,
                                row["value"],
                                name,
                                False,
                                row["date"].date(),
                            )
                        )
                    print(f"  Daily data: {len(records)} records")

                elif original_interval in ["monthly", "quarterly"]:
                    records = []
                    start_date = group_df["date"].min().date()
                    end_date = date.today()
                    print(
                        f"  {original_interval.title()} data: Forward filling from {start_date} to {end_date}"
                    )
                    date_range = pd.date_range(start=start_date, end=end_date, freq="D")
                    current_value = None
                    current_original_date = None
                    for current_ts in date_range:
                        current_date = current_ts.date()
                        matching_rows = group_df[
                            group_df["date"].dt.date == current_date
                        ]
                        if not matching_rows.empty:
                            current_value = matching_rows.iloc[0]["value"]
                            current_original_date = current_date
                            is_forward_filled = False
                        else:
                            is_forward_filled = current_value is not None
                        if current_value is not None:
                            records.append(
                                (
                                    commodity_name,
                                    function_name,
                                    current_date,
                                    original_interval,
                                    "daily",
                                    unit,
                                    current_value,
                                    name,
                                    is_forward_filled,
                                    current_original_date,
                                )
                            )
                    print(
                        f"  {original_interval.title()} data: {len(records)} daily records (forward-filled from {len(group_df)} original records)"
                    )

                else:
                    print(
                        f"  Unsupported interval {original_interval} for {commodity_name}, skipping"
                    )
                    continue

                if records:
                    insert_query = """
                        INSERT INTO extracted.commodities_daily
                        (commodity_name, function_name, date, original_interval,
                         updated_interval, unit, value, name, is_forward_filled, original_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (commodity_name, function_name, date) DO NOTHING
                    """
                    inserted_count = db.execute_many(insert_query, records)
                    total_inserted += inserted_count
                    print(f"  Inserted {inserted_count} daily records")
                else:
                    print(f"  No records to insert for {commodity_name}")

            except Exception as e:
                print(f"  ERROR processing {commodity_name}: {str(e)}")
                continue

        print(
            f"\nDaily transformation completed: {total_inserted} total records inserted"
        )
        return total_inserted

    def run_etl_with_daily_transform(
        self,
        commodity_list=None,
        batch_size=5,
        transform_to_daily=True,
    ):
        """Run ETL for commodities and optionally transform to daily data."""
        print("Starting commodities ETL with daily transformation...")

        daily_inserted = 0
        with self.db_manager as db:
            total_inserted, status_summary = self.run_etl_batch_with_db(
                db, commodity_list, batch_size
            )

            if transform_to_daily:
                daily_inserted = self.transform_to_daily_data(db)

        print("\nTotal ETL Summary:")
        print(f"  Original data inserted: {total_inserted}")
        print(f"  Daily records inserted: {daily_inserted}")
        print("Status breakdown:")
        for status, count in status_summary.items():
            print(f"  - {status}: {count}")

        return total_inserted, status_summary, daily_inserted

    def run_etl_batch_with_db(
        self, db, commodity_list=None, batch_size=5
    ):
        """Run ETL for multiple commodities using provided database connection."""
        if commodity_list is None:
            commodity_list = list(COMMODITY_CONFIGS.keys())

        print(f"Starting commodities ETL for {len(commodity_list)} commodities...")
        print(f"Batch size: {batch_size}")
        print("-" * 50)

        total_inserted = 0
        status_summary = {"data": 0, "empty": 0, "error": 0, "pass": 0}

        for i, function_name in enumerate(commodity_list):
            if function_name not in COMMODITY_CONFIGS:
                print(f"Unknown commodity function: {function_name}")
                continue

            display_name = COMMODITY_CONFIGS[function_name][1]
            print(f"Processing {i+1}/{len(commodity_list)}: {display_name}")

            try:
                inserted_count, status = self.extract_and_load_commodity_with_db(
                    db, function_name
                )
                total_inserted += inserted_count
                status_summary[status] += 1

                if i < len(commodity_list) - 1:
                    print(
                        f"Rate limiting: sleeping for {self.rate_limit_delay} seconds..."
                    )
                    time.sleep(self.rate_limit_delay)

                if (i + 1) % batch_size == 0 and i < len(commodity_list) - 1:
                    print(
                        f"Batch {(i + 1) // batch_size} completed. Pausing for 0.8 seconds..."
                    )
                    time.sleep(0.8)

            except Exception as e:
                print(f"Error processing {display_name}: {str(e)}")
                status_summary["error"] += 1
                continue

            print("-" * 30)

        print("\n" + "=" * 50)
        print("COMMODITIES ETL SUMMARY")
        print("=" * 50)
        print(f"Total commodities processed: {len(commodity_list)}")
        print(f"Total records inserted: {total_inserted}")
        print("Status breakdown:")
        for status, count in status_summary.items():
            print(f"  - {status}: {count}")
        print("=" * 50)

        return total_inserted, status_summary

    def get_database_summary(self):
        """Get summary of commodities data in the database."""
        # Create a fresh database manager instance to avoid connection issues
        db_manager = PostgresDatabaseManager()

        with db_manager as db:
            # Total records by status
            status_query = """
                SELECT api_response_status, COUNT(*) as count
                FROM extracted.commodities
                GROUP BY api_response_status
                ORDER BY api_response_status
            """
            status_results = db.fetch_query(status_query)

            # Data records by commodity
            commodity_query = """
                SELECT commodity_name, interval, COUNT(*) as count, MIN(date) as earliest, MAX(date) as latest
                FROM extracted.commodities
                WHERE api_response_status = 'data'
                GROUP BY commodity_name, interval
                ORDER BY commodity_name, interval
            """
            commodity_results = db.fetch_query(commodity_query)

            # Latest values for each commodity
            latest_query = """
                SELECT c1.commodity_name, c1.interval, c1.date, c1.value, c1.unit
                FROM extracted.commodities c1
                INNER JOIN (
                    SELECT commodity_name, interval, MAX(date) as max_date
                    FROM extracted.commodities
                    WHERE api_response_status = 'data'
                    GROUP BY commodity_name, interval
                ) c2 ON c1.commodity_name = c2.commodity_name
                     AND c1.interval = c2.interval
                     AND c1.date = c2.max_date
                ORDER BY c1.commodity_name, c1.interval
            """
            latest_results = db.fetch_query(latest_query)

        print("\n" + "=" * 60)
        print("COMMODITIES DATABASE SUMMARY")
        print("=" * 60)

        print("\nRecords by Status:")
        if status_results:
            for status, count in status_results:
                print(f"  {status}: {count}")
        else:
            print("  No records found")

        print("\nData Records by Commodity:")
        if commodity_results:
            for commodity, interval, count, earliest, latest in commodity_results:
                print(
                    f"  {commodity} ({interval}): {count} records from {earliest} to {latest}"
                )
        else:
            print("  No data records found")

        print("\nLatest Values:")
        if latest_results:
            for commodity, interval, date, value, unit in latest_results:
                if value is not None:
                    print(f"  {commodity} ({interval}): ${value:.2f} {unit} on {date}")
                else:
                    print(f"  {commodity} ({interval}): No value on {date}")
        else:
            print("  No latest values found")

        print("=" * 60)


def main():
    """Main function for commodities extraction."""

    extractor = CommoditiesExtractor()

    # Configuration options for different use cases:

    # === INITIAL DATA COLLECTION ===
    # Option 1: Test with a few commodities first (recommended for first run)
    # test_commodities = ['WTI', 'BRENT', 'NATURAL_GAS']
    # extractor.run_etl_batch(test_commodities, batch_size=2)

    # Option 2: Extract all oil and gas commodities
    # oil_gas_commodities = ['WTI', 'BRENT', 'NATURAL_GAS']
    # extractor.run_etl_batch(oil_gas_commodities, batch_size=2)

    # Option 3: Extract all metals commodities
    # metals_commodities = ['COPPER', 'ALUMINUM']
    # extractor.run_etl_batch(metals_commodities, batch_size=2)

    # Option 4: Extract all agricultural commodities
    # agriculture_commodities = ['WHEAT', 'CORN', 'COTTON', 'SUGAR', 'COFFEE']
    # extractor.run_etl_batch(agriculture_commodities, batch_size=3)

    # Option 5: Extract all commodities (full dataset) and transform to daily
    extractor.run_etl_with_daily_transform()
    # extractor.run_etl_latest_periods(days_threshold=30, batch_size=3)

    print("\nFinal Database Summary:")
    # Small delay to ensure previous connection is fully closed
    time.sleep(0.1)
    extractor.get_database_summary()


if __name__ == "__main__":
    main()
