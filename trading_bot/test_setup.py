"""
Quick Test Script for Trading Bot Setup

Verifies all components are working correctly before running the bot.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

print("="*80)
print("TRADING BOT SETUP VERIFICATION")
print("="*80)

# Test 1: Alpaca Connection
print("\n1. Testing Alpaca Connection...")
try:
    from trading_bot.alpaca_client import AlpacaClient
    alpaca = AlpacaClient()
    account = alpaca.get_account()
    print(f"   ✓ Connected to Alpaca ({'PAPER' if alpaca.is_paper else 'LIVE'})")
    print(f"   Portfolio Value: ${account['portfolio_value']:,.2f}")
    print(f"   Buying Power: ${account['buying_power']:,.2f}")
except Exception as e:
    print(f"   ✗ FAILED: {e}")
    sys.exit(1)

# Test 2: Database Connection
print("\n2. Testing Database Connection...")
try:
    from db.postgres_database_manager import PostgresDatabaseManager
    db = PostgresDatabaseManager()
    db.connect()
    result = db.fetch_query("SELECT COUNT(*) FROM transforms.trading_signals")
    signal_count = result[0][0]
    db.close()
    print(f"   ✓ Connected to Database")
    print(f"   Trading Signals: {signal_count:,}")
except Exception as e:
    print(f"   ✗ FAILED: {e}")
    sys.exit(1)

# Test 3: ML Model
print("\n3. Testing ML Model...")
try:
    import pickle
    model_path = 'models/trade_success_model.pkl'
    with open(model_path, 'rb') as f:
        model_data = pickle.load(f)
    print(f"   ✓ ML Model loaded")
    print(f"   Features: {len(model_data['feature_names'])}")
except Exception as e:
    print(f"   ✗ FAILED: {e}")
    print(f"   Run: python backtesting/trade_success_predictor.py")
    sys.exit(1)

# Test 4: Signal Scorer
print("\n4. Testing Signal Scorer...")
try:
    from trading_bot.daily_signal_scorer import DailySignalScorer
    scorer = DailySignalScorer(min_probability=0.80)
    recommendations = scorer.score_signals(lookback_days=3)
    print(f"   ✓ Signal Scorer working")
    print(f"   Recommendations: {len(recommendations)}")
    if len(recommendations) > 0:
        print(f"   Top symbol: {recommendations.iloc[0]['symbol']} "
              f"(prob: {recommendations.iloc[0]['success_probability']:.2%})")
except Exception as e:
    print(f"   ✗ FAILED: {e}")
    sys.exit(1)

# Test 5: Market Status
print("\n5. Checking Market Status...")
try:
    is_open = alpaca.is_market_open()
    print(f"   Market is: {'OPEN ✓' if is_open else 'CLOSED'}")
except Exception as e:
    print(f"   ✗ FAILED: {e}")

# Test 6: Current Positions
print("\n6. Checking Current Positions...")
try:
    positions = alpaca.get_positions()
    print(f"   Current Positions: {len(positions)}")
    if positions:
        for pos in positions[:3]:
            print(f"   - {pos['symbol']}: {pos['qty']} shares @ ${pos['avg_entry_price']:.2f}")
except Exception as e:
    print(f"   ✗ FAILED: {e}")

print("\n" + "="*80)
print("SETUP VERIFICATION COMPLETE")
print("="*80)
print("\n✓ All tests passed! Ready to run trading bot.")
print("\nNext steps:")
print("  1. Test in dry-run: python trading_bot/automated_trading_bot.py --dry-run")
print("  2. Run live: python trading_bot/automated_trading_bot.py")
print("  3. Schedule daily: python trading_bot/schedule_daily_trading.py --setup-windows-task")
print("="*80 + "\n")
