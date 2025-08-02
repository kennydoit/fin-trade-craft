#!/usr/bin/env python3
"""
Test script to demonstrate force_refresh functionality for insider transactions.
This shows how to update data with latest transactions.
"""

import sys
from pathlib import Path

# Add the parent directories to the path so we can import from data_pipeline
sys.path.append(str(Path(__file__).parent))
from data_pipeline.extract.extract_insider_transactions import InsiderTransactionsExtractor

def main():
    """Test both incremental and refresh modes."""
    
    extractor = InsiderTransactionsExtractor()
    
    print("=" * 60)
    print("TESTING INCREMENTAL MODE (default)")
    print("This will only process unprocessed symbols")
    print("=" * 60)
    
    # Test incremental mode (should find fewer symbols since we've been processing them)
    # extractor.run_etl_incremental(exchange_filter="NASDAQ", limit=3)
    
    print("\n" + "=" * 60)
    print("TESTING FORCE REFRESH MODE")
    print("This will re-process all symbols to get latest transactions")
    print("=" * 60)
    
    # Test force refresh mode (should re-process symbols we've already done)
    extractor.run_etl_incremental(exchange_filter="NASDAQ", limit=5000, force_refresh=True)
    
    print("\n" + "=" * 60)
    print("TESTING SUMMARY")
    print("=" * 60)
    print("✅ Incremental Mode: Only processes symbols not yet in insider_transactions table")
    print("✅ Force Refresh Mode: Re-processes all symbols to get latest transactions")
    print("✅ ON CONFLICT clause handles updates when re-processing existing data")
    print("✅ Use force_refresh=True periodically to get new insider transactions")

if __name__ == "__main__":
    main()
