# Trading Bot - Automated Trading System

An automated trading bot that uses machine learning to score and execute trades via Alpaca.

## Components

1. **alpaca_client.py** - Alpaca API wrapper
2. **daily_signal_scorer.py** - ML-based signal scoring
3. **automated_trading_bot.py** - Main trading logic
4. **schedule_daily_trading.py** - Scheduling system

## Quick Start

### 1. Install Dependencies

```bash
pip install alpaca-py schedule python-dotenv
```

### 2. Test in Dry Run Mode

```bash
# Test signal scoring
python trading_bot/daily_signal_scorer.py

# Test bot execution (no real orders)
python trading_bot/automated_trading_bot.py --dry-run

# Test scheduled execution
python trading_bot/schedule_daily_trading.py --once --dry-run
```

### 3. Run Live

```bash
# Single execution
python trading_bot/automated_trading_bot.py

# Schedule daily at 9:35 AM (market open)
python trading_bot/schedule_daily_trading.py --daemon --time "09:35"
```

## Configuration

Bot parameters (adjust in commands or scheduler):
- `--max-positions 10` - Maximum concurrent positions
- `--position-size 0.05` - Position size (5% of portfolio)
- `--min-probability 0.85` - Minimum 85% success probability
- `--stop-loss 0.10` - 10% stop loss
- `--take-profit 0.15` - 15% take profit

## How It Works

### Daily Workflow

1. **Score Signals** - Analyzes recent trading signals with ML model
2. **Exit Check** - Checks existing positions for exit conditions:
   - Stop loss hit (-10%)
   - Take profit hit (+15%)
   - Sell signal detected
3. **Entry Check** - Validates entry conditions for new trades:
   - Signal < 3 days old
   - Price hasn't moved too much
   - Success probability >= 85%
   - Quality score >= 50
4. **Execute** - Places market orders via Alpaca

### Risk Management

- Position sizing: 5% of portfolio per trade
- Maximum positions: 10 concurrent
- Stop loss: -10%
- Take profit: +15%
- Signal validation before entry

## Windows Task Scheduler Setup

```bash
# Get setup instructions
python trading_bot/schedule_daily_trading.py --setup-windows-task
```

Or manually create task:
1. Open Task Scheduler
2. Create Basic Task → Daily at 9:35 AM
3. Action: Start a program
4. Program: Your Python executable
5. Arguments: `"trading_bot/schedule_daily_trading.py" --once`

## Logs

- `trading_bot/bot_execution.log` - Trading activity
- `trading_bot/scheduler.log` - Scheduler events
- `trading_bot/daily_recommendations.csv` - Daily scored signals

## Safety Features

- Dry run mode for testing
- Paper trading account (configured in .env)
- Maximum position limits
- Stop losses on all trades
- Entry validation (price movement checks)
- Detailed logging

## Monitoring

Check bot performance:
```bash
# View recent logs
tail -f trading_bot/bot_execution.log

# Check Alpaca account
# https://app.alpaca.markets/paper/dashboard/overview
```
