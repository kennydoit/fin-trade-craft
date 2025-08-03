"""
Test the updated listing status extractor with PostgreSQL.
"""

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from data_pipeline.extract.extract_listing_status import ListingStatusExtractor


def test_listing_status_extractor():
    """Test the listing status extractor with PostgreSQL."""

    print("Testing Listing Status Extractor with PostgreSQL...")

    try:
        # Initialize extractor
        extractor = ListingStatusExtractor()
        print("✅ Extractor initialized successfully")

        # Test data extraction (only get a small sample to avoid API limits)
        print("Note: This test extracts real data from Alpha Vantage API")
        print("This may take a few seconds...")

        # Run the full ETL process
        extractor.run_etl()

        print("✅ Listing Status ETL completed successfully!")

        # Verify data was loaded
        db_manager = extractor.db_manager.__class__()  # Create a new instance
        with db_manager as db:
            count = db.fetch_query("SELECT COUNT(*) FROM listing_status")[0][0]
            print(f"✅ Total symbols in database: {count}")

            # Show a few sample records
            sample_records = db.fetch_query(
                """
                SELECT symbol, name, exchange, asset_type, status 
                FROM listing_status 
                ORDER BY symbol 
                LIMIT 10
            """
            )

            print("\nSample records:")
            for record in sample_records:
                print(f"  {record[0]} - {record[1]} ({record[2]}) - {record[4]}")

        return True

    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        return False


if __name__ == "__main__":
    success = test_listing_status_extractor()
    if not success:
        sys.exit(1)
