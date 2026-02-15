# 🤖 Automated Trading Bot - Complete System

Your automated trading bot is ready! Here's everything you need to know.

## 📋 What Was Created

### Core Components

1. **`trading_bot/alpaca_client.py`** - Alpaca API wrapper
   - Connect to Alpaca
   - Get account info & positions
   - Place/cancel orders
   - Get market data

2. **`trading_bot/daily_signal_scorer.py`** - ML-based signal scoring
   - Fetches latest trading signals
   - Scores with trained ML model
   - Filters by probability & quality
   - Ranks recommendations

3. **`trading_bot/automated_trading_bot.py`** - Main trading logic
   - Processes exits (stop loss, take profit, sell signals)
   - Validates entry conditions
   - Places buy orders
   - Risk management

4. **`trading_bot/schedule_daily_trading.py`** - Scheduling system
   - Run once or as daemon
   - Windows Task Scheduler setup
   - Daily execution at market open

### Supporting Files

- **`trading_bot/test_setup.py`** - Verify everything works
- **`trading_bot/README.md`** - Component documentation
- **`TRADING_BOT_SETUP.md`** - Complete setup guide
- **`trading_bot/requirements.txt`** - Additional dependencies

## 🚀 Quick Start

### Step 1: Install Dependencies

```powershell
pip install alpaca-py schedule
```

### Step 2: Test Setup

```powershell
python trading_bot\test_setup.py
```

This verifies:
- ✅ Alpaca connection works
- ✅ Database has signals
- ✅ ML model is loaded
- ✅ Signal scorer works
- ✅ Market status check
- ✅ Current positions visible

### Step 3: Test Bot in Dry-Run

```powershell
python trading_bot\automated_trading_bot.py --dry-run
```

This shows what the bot WOULD do without placing real orders.

### Step 4: Run Live (Paper Trading)

```powershell
python trading_bot\automated_trading_bot.py
```

This places actual orders in your Alpaca **paper trading** account.

### Step 5: Schedule Daily (Optional)

```powershell
# Get Windows Task Scheduler setup
python trading_bot\schedule_daily_trading.py --setup-windows-task

# OR run as daemon
python trading_bot\schedule_daily_trading.py --daemon --time "09:35"
```

## 🎯 How It Works

### Daily Workflow (9:35 AM Market Open)

```
1. CHECK MARKET STATUS
   └─> If market closed, exit

2. PROCESS EXITS
   ├─> For each open position:
   │   ├─> Check stop loss (-10%)
   │   ├─> Check take profit (+15%)
   │   └─> Check for sell signals
   └─> Place sell orders as needed

3. SCORE SIGNALS
   ├─> Get recent buy signals (last 3 days)
   ├─> Fetch fundamental data
   ├─> Predict success probability with ML
   ├─> Filter: probability >= 85%, quality >= 50
   └─> Rank by composite score

4. PROCESS ENTRIES
   ├─> Check available position slots
   ├─> For each top recommendation:
   │   ├─> Validate entry conditions
   │   ├─> Check price movement
   │   ├─> Calculate position size (5%)
   │   └─> Place buy order
   └─> Stop when slots filled or buying power exhausted

5. LOG RESULTS
   └─> Summary of exits, entries, final positions
```

## ⚙️ Configuration

### Default Settings (Conservative)

```python
max_positions = 10          # Max concurrent trades
position_size = 5%          # 5% of portfolio per trade
min_probability = 85%       # High confidence threshold
stop_loss = 10%            # Exit if down 10%
take_profit = 15%          # Exit if up 15%
```

### Custom Settings (Aggressive Example)

```powershell
python trading_bot\automated_trading_bot.py `
  --max-positions 15 `
  --position-size 0.08 `
  --min-probability 0.75 `
  --stop-loss 0.15 `
  --take-profit 0.20
```

## 📊 Example Output

```
================================================================================
AUTOMATED TRADING BOT INITIALIZED (LIVE)
================================================================================
Max Positions: 10
Position Size: 5.0%
Min Probability: 85%
Stop Loss: 10%
Take Profit: 15%
================================================================================

================================================================================
PROCESSING EXITS
================================================================================
Checking 3 open positions...

Checking AAPL:
  Quantity: 10
  Entry: $150.00
  Current: $165.00
  P&L: $150.00 (10.00%)
  Hold position

Checking MSFT:
  Quantity: 5
  Entry: $300.00
  Current: $285.00
  P&L: -$75.00 (-5.00%)
  Hold position

Checking TSLA:
  Quantity: 15
  Entry: $200.00
  Current: $232.00
  P&L: $480.00 (16.00%)
  EXIT SIGNAL: Take profit triggered (16.00%)
  ✓ SELL ORDER PLACED: 15 shares

Exits executed: 1

================================================================================
SCORING SIGNALS
================================================================================
Fetched 127 recent BUY signals
After quality filter (>=50): 89 signals
After probability filter (>=0.85): 23 signals

================================================================================
PROCESSING ENTRIES
================================================================================
Portfolio Value: $105,480.00
Buying Power: $52,740.00
Current Positions: 3/10
Available slots: 7

NVDA:
  Strategy: trend_following
  Signal Date: 2026-02-09
  Current Price: $875.50
  Success Probability: 91.2%
  Quality Score: 87
  Composite Score: 0.892
  Entry Valid: True - Valid
  Position Size: 6 shares ($5,253.00)
  ✓ BUY ORDER PLACED

AMD:
  Strategy: ema_crossover
  Signal Date: 2026-02-08
  Current Price: $142.30
  Success Probability: 88.5%
  Quality Score: 79
  Composite Score: 0.871
  Entry Valid: True - Valid
  Position Size: 37 shares ($5,265.10)
  ✓ BUY ORDER PLACED

Entries executed: 2

================================================================================
EXECUTION SUMMARY
================================================================================
Exits: 1
Entries: 2
Total Actions: 3

Final Position Count: 4
Current Holdings:
  AAPL: 10 shares @ $150.00 (P&L: $150.00)
  MSFT: 5 shares @ $300.00 (P&L: -$75.00)
  NVDA: 6 shares @ $875.50 (P&L: $0.00)
  AMD: 37 shares @ $142.30 (P&L: $0.00)

================================================================================
TRADING BOT EXECUTION COMPLETE
================================================================================
```

## 🛡️ Safety Features

✅ **Paper Trading** - Uses Alpaca paper account (no real money)  
✅ **Dry Run Mode** - Test without placing orders  
✅ **Position Limits** - Max 10 concurrent positions  
✅ **Stop Losses** - Automatic -10% exits  
✅ **Entry Validation** - Checks price movement & signal age  
✅ **Comprehensive Logging** - Full audit trail  
✅ **Risk Management** - Fixed position sizing  

## 📈 Performance Monitoring

### Check Alpaca Dashboard
https://app.alpaca.markets/paper/dashboard/overview

### View Logs
```powershell
# Execution log
Get-Content trading_bot\bot_execution.log -Tail 50

# Recommendations
Get-Content trading_bot\daily_recommendations.csv | ConvertFrom-Csv | Format-Table
```

### Get Current Status
```powershell
# Account info
python -c "from trading_bot.alpaca_client import AlpacaClient; c=AlpacaClient(); print(c.get_account())"

# Positions
python -c "from trading_bot.alpaca_client import AlpacaClient; c=AlpacaClient(); [print(p) for p in c.get_positions()]"
```

## 🔄 Daily Routine

### Morning (Before 9:35 AM)
1. Check signals updated: `python transforms\transform_trading_signals.py --mode incremental`
2. Test bot: `python trading_bot\automated_trading_bot.py --dry-run`
3. Bot runs automatically at 9:35 AM (if scheduled)

### Evening (After 4:00 PM)
1. Review executed trades
2. Check position performance
3. Review logs for any issues

## 🚨 Troubleshooting

### No recommendations?
```powershell
# Lower thresholds
python trading_bot\daily_signal_scorer.py --min-probability 0.75 --min-quality 40
```

### Orders failing?
- Check Alpaca dashboard for API issues
- Verify buying power available
- Check if symbol is tradeable

### Bot not running?
- Verify Task Scheduler task is enabled
- Check scheduler.log for errors
- Ensure market is open

## 📚 Documentation

- **Complete Setup**: `TRADING_BOT_SETUP.md`
- **Component Docs**: `trading_bot/README.md`
- **Alpaca API**: https://alpaca.markets/docs/

## 💡 Tips for Success

1. **Start Small** - Use 2-3% position sizes initially
2. **Monitor Daily** - Check logs and performance every day
3. **Tune Parameters** - Adjust based on results after 1-2 weeks
4. **Quality > Quantity** - Better to have 5 high-probability trades than 10 mediocre ones
5. **Keep Stop Losses** - Protect capital with disciplined exits
6. **Review Weekly** - Analyze what's working and what's not

## 🎉 You're Ready!

Your automated trading system is complete and ready to use. Start with dry-run testing, then move to paper trading, and monitor results carefully.

**Next Command:**
```powershell
python trading_bot\test_setup.py
```

Good luck! 🚀
