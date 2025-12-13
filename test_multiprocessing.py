"""Test multiprocessing implementation with a small sample."""

import sys
from pathlib import Path
from multiprocessing import cpu_count

sys.path.append(str(Path(__file__).parent))

from transforms.transformation_watermark_manager import TransformationWatermarkManager
from transforms.transform_time_series_daily_adjusted import TimeSeriesDailyAdjustedTransformer

def main():
    print("🔍 Testing multiprocessing implementation\n")
    
    # Show CPU count
    cpus = cpu_count()
    print(f"CPU cores available: {cpus}")
    print(f"Default workers: {max(1, cpus - 1)}\n")
    
    # Get a small sample of symbols
    mgr = TransformationWatermarkManager()
    symbols = mgr.get_symbols_needing_transformation(
        transformation_group='time_series_daily_adjusted',
        staleness_hours=999999,
        limit=10  # Just test with 10 symbols
    )
    
    print(f"Testing with {len(symbols)} symbols:")
    for sym in symbols[:5]:
        print(f"  - {sym['symbol']} (ID: {sym['symbol_id']})")
    print(f"  ... and {len(symbols) - 5} more\n")
    
    # Test with 2 workers
    print("=" * 80)
    print("Testing with 2 parallel workers")
    print("=" * 80)
    
    transformer = TimeSeriesDailyAdjustedTransformer()
    
    # Test parallel processing
    try:
        total_records, success_count, failed_symbols = transformer._process_parallel(
            symbols, mode='full', workers=2
        )
        
        print("\n✅ Parallel processing test completed!")
        print(f"   Success: {success_count}/{len(symbols)}")
        print(f"   Records: {total_records:,}")
        if failed_symbols:
            print(f"   Failed: {', '.join(failed_symbols)}")
    
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()
