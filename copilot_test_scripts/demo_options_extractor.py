#!/usr/bin/env python3
"""
Demo script showing how to use the historical options extractor.
"""

import sys
from pathlib import Path

# Add the parent directory to the path
sys.path.append(str(Path(__file__).parent))


def main():
    """Demo of the historical options extractor functionality."""
    from data_pipeline.extract.extract_historical_options import (
        HistoricalOptionsExtractor,
    )

    print("Historical Options Extractor Demo")
    print("=" * 50)

    # Create extractor instance
    extractor = HistoricalOptionsExtractor()

    # Example 1: Extract options for a specific date with limit
    print("\nExample 1: Extract options for recent date (small test)")
    extractor.run_etl_historical_date(
        target_date="2024-12-19",  # Another recent trading day
        exchange_filter="NASDAQ",
        limit=3,  # Small test
    )

    print("\n" + "=" * 50)
    print("Demo completed! You can now use the extractor with:")
    print("1. Single date extraction: run_etl_historical_date()")
    print("2. Date range extraction: run_etl_date_range()")
    print("3. Different exchanges: 'NYSE', 'NASDAQ', ['NYSE', 'NASDAQ']")
    print("4. Batch processing with limits")


if __name__ == "__main__":
    main()
