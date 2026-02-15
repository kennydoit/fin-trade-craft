"""Quick test to demonstrate the improved diagnostic messages in transform_trading_signals.py"""

import sys
import logging
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from transforms.transform_trading_signals import TradingSignalsTransformer

# Set logging level to show DEBUG messages
logging.basicConfig(
    level=logging.DEBUG,
    format='%(levelname)s - %(message)s'
)

print("="*80)
print("TESTING IMPROVED DIAGNOSTICS FOR TRADING SIGNALS")
print("="*80)
print()

# Create transformer
transformer = TradingSignalsTransformer()
transformer.db.connect()

# Test with a few symbols that have raw data but no indicators
test_symbol_ids = [98244, 214201, 377192]

print(f"Testing {len(test_symbol_ids)} symbols with missing technical indicators:\n")

success_count = 0
for symbol_id in test_symbol_ids:
    signals = transformer.process_symbol(symbol_id, days_back=7)
    if signals > 0:
        success_count += 1
        print(f"  ✓ Symbol {symbol_id}: {signals} signals generated")
    else:
        print(f"  ✗ Symbol {symbol_id}: 0 signals (see DEBUG messages above)")

print()
print("="*80)
print(f"RESULT: {success_count}/{len(test_symbol_ids)} symbols generated signals")
print("="*80)

if success_count == 0:
    print()
    print("⚠️  As expected, no signals were generated because technical indicators are missing.")
    print()
    print("SOLUTION:")
    print("  1. First run: python transforms\\transform_time_series_daily_adjusted.py --mode incremental")
    print("  2. Then run:  python transforms\\transform_trading_signals.py --mode incremental")
    print()

transformer.db.close()
