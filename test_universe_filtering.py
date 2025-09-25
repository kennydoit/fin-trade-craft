#!/usr/bin/env python3
"""Test script to verify universe filtering is working in transformers."""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from data_pipeline.transform.transform_cash_flow import CashFlowTransformer


def main():
    print("Testing universe filtering...")
    
    # Test without universe_id (should process all records)
    print("\n=== Test 1: CashFlowTransformer without universe_id ===")
    transformer_all = CashFlowTransformer()
    transformer_all.db.connect()
    try:
        df_all = transformer_all._fetch_cash_flow()
        print(f"Records fetched without universe filtering: {len(df_all)}")
        if not df_all.empty:
            print(f"Sample symbols: {df_all['symbol'].head().tolist()}")
    finally:
        transformer_all.db.close()
    
    # Test with a specific universe_id
    print("\n=== Test 2: CashFlowTransformer with universe_id ===")
    test_universe_id = "some-test-uuid-123"  # This will likely return 0 records since it doesn't exist
    transformer_filtered = CashFlowTransformer(universe_id=test_universe_id)
    transformer_filtered.db.connect()
    try:
        df_filtered = transformer_filtered._fetch_cash_flow()
        print(f"Records fetched with universe_id '{test_universe_id}': {len(df_filtered)}")
        if not df_filtered.empty:
            print(f"Sample symbols: {df_filtered['symbol'].head().tolist()}")
        else:
            print("No records found (expected for test universe_id)")
    except Exception as e:
        print(f"Query executed but returned error/no results: {e}")
    finally:
        transformer_filtered.db.close()
    
    print("\n=== Test completed ===")
    print("The fix is working if:")
    print("1. Test 1 returns many records (all symbols)")
    print("2. Test 2 returns fewer/no records (filtered by universe)")
    print("3. The queries are different (one has JOIN, one doesn't)")


if __name__ == "__main__":
    main()
