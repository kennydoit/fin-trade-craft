# Daily Execution Order

Since your EOD data is up to date, run the following programs in this order:

## 1. **transforms/run_daily_transform.py**
Runs the complete data pipeline:
- ✅ Updates all fundamental data (Balance Sheet, Cash Flow, Income Statement)
- ✅ Calculates fundamental quality scores
- ✅ Updates insider transaction data
- ✅ Updates economic indicators and commodities
- ✅ Updates earnings sentiment analysis
- ✅ **Scores trading signals with ML model**
- ✅ **Generates signal visualization charts**

```powershell
python transforms/run_daily_transform.py
```

**Output:**
- Updated database tables in PostgreSQL
- `backtesting/daily_signals_scored_YYYYMMDD.csv` - Scored signals
- `backtesting/signal_charts_indicators/` - Visualization charts for top 25 signals

## 2. **trading_bot/automated_trading_bot.py**
Executes trading decisions:
- Processes exits for current positions (stop loss, take profit, sell signals)
- Scores signals internally (using latest data from step 1)
- Places new buy orders based on ML recommendations
- Implements risk management

```powershell
python trading_bot/automated_trading_bot.py
```

---

### Optional Variations

**Dry run mode (test without placing orders):**
```powershell
python trading_bot/automated_trading_bot.py --dry-run
```

**Custom parameters:**
```powershell
python trading_bot/automated_trading_bot.py --max-positions 15 --min-probability 0.80 --position-size 0.10
```

**Schedule automatically (9:35 AM ET daily):**
```powershell
python trading_bot/schedule_daily_trading.py --setup-windows-task
```
