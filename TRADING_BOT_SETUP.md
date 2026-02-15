# Automated Trading Bot - Setup Guide

Complete setup instructions for your automated trading system.

## ✅ Prerequisites Checklist

- [x] Alpaca account with paper trading enabled
- [x] Alpaca credentials in `.env` file
- [x] Trading signals generated (`transforms.trading_signals` table populated)
- [x] ML model trained (`models/trade_success_model.pkl` exists)
- [x] Fundamental data in database

## 📦 Installation

### 1. Install Required Packages

```powershell
pip install alpaca-py schedule
```

### 2. Verify Setup

Check that all components work:

```powershell
# Test Alpaca connection
python -c "from trading_bot.alpaca_client import AlpacaClient; c = AlpacaClient(); print(c.get_account())"

# Test signal scorer
python trading_bot\daily_signal_scorer.py --lookback-days 3

# Test bot in dry-run mode
python trading_bot\automated_trading_bot.py --dry-run
```

## 🚀 Usage

### Option 1: Manual Execution (Recommended for Testing)

Run the bot manually each morning:

```powershell
# DRY RUN (test without placing orders)
python trading_bot\automated_trading_bot.py --dry-run

# LIVE TRADING (places real orders in paper account)
python trading_bot\automated_trading_bot.py
```

### Option 2: Windows Task Scheduler (Automated Daily)

Set up Windows Task Scheduler to run automatically:

```powershell
# Get setup instructions
python trading_bot\schedule_daily_trading.py --setup-windows-task
```

Then follow the PowerShell command or manual steps shown.

### Option 3: Daemon Process (Keep Running)

Run as a background process that executes daily at market open:

```powershell
# Start daemon (will run at 9:35 AM ET daily)
python trading_bot\schedule_daily_trading.py --daemon --time "09:35"
```

## ⚙️ Configuration

Customize bot behavior with command-line arguments:

```powershell
python trading_bot\automated_trading_bot.py \
  --max-positions 10 \
  --position-size 0.05 \
  --min-probability 0.85 \
  --stop-loss 0.10 \
  --take-profit 0.15
```

### Parameters Explained

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--max-positions` | 10 | Maximum concurrent positions |
| `--position-size` | 0.05 | Position size (5% of portfolio) |
| `--min-probability` | 0.85 | Minimum ML success probability (85%) |
| `--stop-loss` | 0.10 | Stop loss percentage (10%) |
| `--take-profit` | 0.15 | Take profit percentage (15%) |
| `--dry-run` | False | Test mode (no real orders) |

## 📊 What the Bot Does

### Daily Workflow (runs at market open: 9:35 AM ET)

1. **Check Market Status** - Verify market is open
2. **Process Exits** - Check all open positions:
   - Stop loss: Exit if down 10%
   - Take profit: Exit if up 15%
   - Sell signals: Exit if new sell signal detected
3. **Score Signals** - Get latest signals and score with ML model:
   - Filter by success probability (>= 85%)
   - Filter by quality score (>= 50)
   - Rank by composite score
4. **Process Entries** - Buy high-probability signals:
   - Validate entry conditions still valid
   - Check price hasn't moved too much
   - Calculate position size (5% of portfolio)
   - Place market orders

### Risk Management

- **Position Limits**: Maximum 10 concurrent positions
- **Position Sizing**: 5% of portfolio per trade
- **Stop Losses**: Automatic -10% exit
- **Take Profits**: Automatic +15% exit
- **Signal Validation**: Only trade signals < 3 days old
- **Price Movement Check**: Skip if price moved >5% since signal

## 📝 Monitoring

### Check Bot Activity

```powershell
# View execution logs
Get-Content trading_bot\bot_execution.log -Tail 50

# View scheduler logs
Get-Content trading_bot\scheduler.log -Tail 50

# View daily recommendations
Get-Content trading_bot\daily_recommendations.csv
```

### Monitor Alpaca Account

- Paper Trading Dashboard: https://app.alpaca.markets/paper/dashboard/overview
- Check positions, orders, and account value

### Check Bot Status

```powershell
# Get current positions
python -c "from trading_bot.alpaca_client import AlpacaClient; c = AlpacaClient(); import json; print(json.dumps(c.get_positions(), indent=2))"

# Get account info
python -c "from trading_bot.alpaca_client import AlpacaClient; c = AlpacaClient(); import json; print(json.dumps(c.get_account(), indent=2))"
```

## 🔄 Daily Maintenance

### Before Market Open (9:00 AM)

1. **Update Data** - Ensure latest data is loaded:
   ```powershell
   # Run daily transforms (if you extract new data daily)
   python rebuild_signals_from_scratch.py --incremental
   ```

2. **Check Bot Health** - Verify bot is ready:
   ```powershell
   python trading_bot\automated_trading_bot.py --dry-run
   ```

### After Market Close (4:00 PM)

1. **Review Performance** - Check executed trades
2. **Monitor Positions** - Review open positions
3. **Check Logs** - Look for any errors or warnings

## 🛡️ Safety Features

- ✅ Paper trading by default (from `.env` config)
- ✅ Dry-run mode for testing
- ✅ Stop losses on all positions
- ✅ Position size limits
- ✅ Maximum position limits
- ✅ Entry validation
- ✅ Comprehensive logging

## 🚨 Troubleshooting

### Issue: "Alpaca credentials not found"

Check `.env` file has:
```
ALPACA_API_KEY=your_key_here
ALPACA_API_SECRET=your_secret_here
ALPACA_ENDPOINT=https://paper-api.alpaca.markets
```

### Issue: "No signals found"

Run:
```powershell
python transforms\transform_trading_signals.py --mode incremental
```

### Issue: "Model file not found"

Train the model first:
```powershell
python backtesting\trade_success_predictor.py
```

### Issue: "Insufficient buying power"

- Reduce `--position-size` (e.g., 0.03 = 3%)
- Reduce `--max-positions` (e.g., 5)
- Add funds to paper account

## 📈 Next Steps

1. **Test Thoroughly** - Run in dry-run mode for several days
2. **Monitor Results** - Track paper trading performance
3. **Tune Parameters** - Adjust based on performance
4. **Scale Up** - Increase position sizes gradually
5. **Go Live** - Switch to live trading when confident

## 💡 Tips

- Start with small position sizes (2-3%)
- Monitor closely for first week
- Keep stop losses tight initially
- Review logs daily
- Adjust min-probability based on results
- Don't overtrade - quality over quantity

## 📞 Support

For issues:
1. Check logs: `trading_bot/bot_execution.log`
2. Test in dry-run mode
3. Verify database has recent data
4. Check Alpaca dashboard for API issues
