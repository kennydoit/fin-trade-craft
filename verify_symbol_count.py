"""Verify that get_symbols_needing_transformation returns correct count after DISTINCT fix."""

from transforms.transformation_watermark_manager import TransformationWatermarkManager

def main():
    manager = TransformationWatermarkManager()
    
    # Get symbols needing transformation for time_series_daily_adjusted
    print("🔍 Getting symbols needing transformation for time_series_daily_adjusted...")
    symbols = manager.get_symbols_needing_transformation(
        transformation_group='time_series_daily_adjusted',
        staleness_hours=24
    )
    
    print(f"\n✅ Found {len(symbols):,} symbols needing transformation")
    print(f"   Expected: ~12,576")
    print(f"   Match: {'✅ YES' if len(symbols) <= 13000 else '❌ NO - still has duplicates'}")
    
    if len(symbols) > 0:
        print(f"\n📋 First 5 symbols:")
        for i, sym in enumerate(symbols[:5], 1):
            print(f"   {i}. {sym['symbol']} (ID: {sym['symbol_id']})")
    
    # Check for duplicates in the result
    symbol_ids = [s['symbol_id'] for s in symbols]
    unique_ids = set(symbol_ids)
    
    if len(symbol_ids) != len(unique_ids):
        print(f"\n⚠️  WARNING: Found duplicate symbol_ids in results!")
        print(f"   Total: {len(symbol_ids):,}, Unique: {len(unique_ids):,}")
    else:
        print(f"\n✅ No duplicates in results - all {len(unique_ids):,} symbol_ids are unique")

if __name__ == '__main__':
    main()
