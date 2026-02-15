"""Test the RSI Crossing strategy logic"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Mock data to test the RSI Crossing logic
test_data = {
    'symbol': ['TEST'] * 15,
    'symbol_id': [1] * 15,
    'date': [datetime(2025, 1, 1) + timedelta(days=i) for i in range(15)],
    'ohlcv_rsi_14': [
        35,  # Above 30
        31,  # Below 30 - enters oversold zone
        30,  # Still below 30
        28,  # Still below 30
        22,  # Still below 30
        28,  # Still below 30
        29,  # Still below 30
        31,  # Crosses back above 30 - SHOULD TRIGGER BUY
        40,  # Neutral
        68,  # Below 70
        72,  # Above 70 - enters overbought zone
        73,  # Still above 70
        74,  # Still above 70
        70,  # Still at 70
        65,  # Crosses below 70 - SHOULD TRIGGER SELL
    ]
}

df = pd.DataFrame(test_data)

# Simulate the strategy logic
signals = []
in_oversold_zone = False
in_overbought_zone = False

for i in range(len(df)):
    curr_rsi = df.iloc[i]['ohlcv_rsi_14']
    
    # Check if in or entering oversold zone (RSI <= 30)
    if curr_rsi <= 30:
        in_oversold_zone = True
        in_overbought_zone = False
        print(f"Day {i}: RSI={curr_rsi:.0f} - Entered/In oversold zone")
    
    # Check if in or entering overbought zone (RSI >= 70)
    elif curr_rsi >= 70:
        in_overbought_zone = True
        in_oversold_zone = False
        print(f"Day {i}: RSI={curr_rsi:.0f} - Entered/In overbought zone")
    
    # Check for buy signal: RSI crosses back above 30 after being at or below
    elif in_oversold_zone and curr_rsi > 30:
        if i > 0 and df.iloc[i-1]['ohlcv_rsi_14'] <= 30:
            print(f"Day {i}: RSI={curr_rsi:.0f} - *** BUY SIGNAL *** (crossed above 30 after being at/below)")
            signals.append(('BUY', i, curr_rsi))
            in_oversold_zone = False
    
    # Check for sell signal: RSI crosses back below 70 after being at or above
    elif in_overbought_zone and curr_rsi < 70:
        if i > 0 and df.iloc[i-1]['ohlcv_rsi_14'] >= 70:
            print(f"Day {i}: RSI={curr_rsi:.0f} - *** SELL SIGNAL *** (crossed below 70 after being at/above)")
            signals.append(('SELL', i, curr_rsi))
            in_overbought_zone = False
    else:
        print(f"Day {i}: RSI={curr_rsi:.0f} - Neutral (oversold_zone={in_oversold_zone}, overbought_zone={in_overbought_zone})")

print("\n" + "="*80)
print("SUMMARY")
print("="*80)
print(f"Total signals: {len(signals)}")
for signal_type, day, rsi in signals:
    print(f"  {signal_type} on day {day} at RSI={rsi:.0f}")

print("\nExpected:")
print("  BUY on day 7 at RSI=31")
print("  SELL on day 14 at RSI=65")

if len(signals) == 2:
    if signals[0] == ('BUY', 7, 31) and signals[1] == ('SELL', 14, 65):
        print("\n✓ TEST PASSED!")
    else:
        print("\n✗ TEST FAILED - Signal values don't match expected")
else:
    print(f"\n✗ TEST FAILED - Expected 2 signals, got {len(signals)}")
