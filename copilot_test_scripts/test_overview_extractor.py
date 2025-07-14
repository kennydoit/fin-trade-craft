"""
Test overview extractor with PostgreSQL.
"""

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from data_pipeline.extract.extract_overview import OverviewExtractor


def test_overview_extractor():
    """Test the overview extractor with PostgreSQL."""

    print("Testing Overview Extractor with PostgreSQL...")

    try:
        # Initialize extractor
        extractor = OverviewExtractor()
        print("✅ Extractor initialized successfully")

        # Test with a small batch of symbols
        print("Testing with a small batch (limit=2)...")
        extractor.run_etl_incremental(exchange_filter="NYSE", limit=2)

        print("✅ Overview ETL completed successfully!")

        return True

    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        return False


if __name__ == "__main__":
    success = test_overview_extractor()
    if not success:
        sys.exit(1)
