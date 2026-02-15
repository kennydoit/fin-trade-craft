# Trading Signal Triggers - Technical Explanation

## Overview
This document explains exactly what triggers each trading signal in your system. Each strategy looks for specific technical indicator patterns or crossovers.

---

## 1. RSI Divergence Strategy ⭐ (Most signals in top 25)

### What It Looks For:
**Bullish Divergence (BUY Signal):**
- Price makes a **lower low** (new low below previous low)
- BUT RSI makes a **higher low** (RSI bottom is higher than previous RSI bottom)
- This indicates weakening downward momentum despite lower prices = potential reversal

**Bearish Divergence (SELL Signal):**
- Price makes a **higher high** (new high above previous high)
- BUT RSI makes a **lower high** (RSI peak is lower than previous RSI peak)
- This indicates weakening upward momentum despite higher prices = potential reversal

### How To See It:
- Look at the RSI subplot (middle panel)
- Compare RSI values at local price extremes
- Divergence occurs when RSI and price move in opposite directions at peaks/troughs

### Example:
```
Price:  $50 → $45 → $42  (making lower lows)
RSI:     30 →  28 →  32  (making higher lows) = BULLISH DIVERGENCE → BUY
```

---

## 2. RSI Mean Reversion Strategy

### What It Looks For:
**BUY Signal:**
- RSI **crosses ABOVE 30** (oversold recovery)
- Previous bar: RSI ≤ 30
- Current bar: RSI > 30

**SELL Signal:**
- RSI **crosses BELOW 70** (overbought reversal)
- Previous bar: RSI ≥ 70
- Current bar: RSI < 70

### How To See It:
- RSI subplot shows horizontal lines at 30 (green) and 70 (red)
- BUY when RSI line crosses UP through 30
- SELL when RSI line crosses DOWN through 70

### Visual Zones:
- **Below 30**: Oversold (looking for bounce)
- **30-70**: Neutral zone
- **Above 70**: Overbought (looking for pullback)

---

## 3. RSI Crossing Strategy ⭐ NEW

### What It Looks For:
**BUY Signal (LONG Position):**
- RSI must **first drop to or below 30** (confirmation of oversold)
- RSI **stays at or below 30** for at least one bar
- RSI then **crosses back ABOVE 30** (recovery signal)
- Example: 35, 31, 30, 28, 22, 28, 29, **31** ← BUY triggers here

**SELL Signal (SHORT Position):**
- RSI must **first rise to or above 70** (confirmation of overbought)
- RSI **stays at or above 70** for at least one bar
- RSI then **crosses back BELOW 70** (reversal signal)
- Example: 68, 69, 72, 73, 74, 70, **65** ← SELL triggers here

### How To See It:
- RSI subplot shows horizontal lines at 30 (green) and 70 (red)
- **BUY**: Look for RSI dipping below 30, staying there, then bouncing back above
- **SELL**: Look for RSI rising above 70, staying there, then falling back below

### Key Difference from RSI Mean Reversion:
- **Mean Reversion**: Triggers immediately when RSI crosses threshold (may be false signal)
- **RSI Crossing**: Requires confirmation that RSI actually entered extreme zone before reversing (more reliable)

### Visual Pattern:
```
LONG Example:
      35 (above 30 - no signal yet)
      31 (above 30 - no signal yet)  
      30 (AT threshold - enters oversold zone)
      28 (below 30 - stays in oversold)
      22 (below 30 - confirms oversold)
      28 (below 30 - still oversold)
      29 (below 30 - still oversold)
      31 (CROSSES ABOVE 30) ← BUY SIGNAL!

SHORT Example:
      68 (below 70 - no signal yet)
      69 (below 70 - no signal yet)
      72 (above 70 - enters overbought zone)
      73 (above 70 - stays in overbought)
      74 (above 70 - confirms overbought)
      70 (AT threshold - still overbought)
      65 (CROSSES BELOW 70) ← SELL SIGNAL!
```

### Advantage:
This strategy filters out "whipsaw" signals where RSI briefly touches 30/70 and immediately reverses without actually confirming an extreme condition.

---

## 4. Williams %R Extremes Strategy

### What It Looks For:
**BUY Signal:**
- Williams %R **crosses ABOVE -80** (oversold recovery)
- Previous bar: %R ≤ -80
- Current bar: %R > -80

**SELL Signal:**
- Williams %R **crosses BELOW -20** (overbought reversal)
- Previous bar: %R ≥ -20
- Current bar: %R < -20

### How To See It:
- Williams %R subplot shows horizontal lines at -20 (red) and -80 (green)
- BUY when %R line crosses UP through -80
- SELL when %R line crosses DOWN through -20

### Range:
- Williams %R ranges from 0 to -100
- -20 to 0 = Overbought
- -80 to -100 = Oversold

---

## 5. EMA Crossover Strategy

### What It Looks For:
**BUY Signal:**
- Fast EMA (8-period) **crosses ABOVE** Slow EMA (21-period)
- Previous bar: EMA8 ≤ EMA21
- Current bar: EMA8 > EMA21

**SELL Signal:**
- Fast EMA (8-period) **crosses BELOW** Slow EMA (21-period)
- Previous bar: EMA8 ≥ EMA21
- Current bar: EMA8 < EMA21

### How To See It:
- Price chart shows two lines: EMA 8 (green dashed) and EMA 21 (orange dashed)
- BUY when green line crosses UP and above orange line
- SELL when green line crosses DOWN and below orange line

### Classic Momentum:
- EMA 8 > EMA 21 = Bullish trend (short-term above long-term)
- EMA 8 < EMA 21 = Bearish trend (short-term below long-term)

---

## 5. Trend Following Strategy

### What It Looks For:
**BUY Signal (ALL 3 conditions must be true):**
1. Price > SMA(50) - trading above 50-day average
2. EMA(8) > EMA(21) - short-term momentum positive
3. RSI > 50 - in bullish RSI zone

**SELL Signal (ALL 3 conditions must be true):**
1. Price < SMA(50) - trading below 50-day average
2. EMA(8) < EMA(21) - short-term momentum negative
3. RSI < 50 - in bearish RSI zone

### How To See It:
- Price chart: Price relative to purple SMA 50 line
- Price chart: EMA 8 vs EMA 21 positioning
- RSI subplot: RSI above/below 50 line (blue dashed)

### Multi-Confirmation:
This strategy requires ALL indicators to align before signaling

---

## 6. MACD Histogram Reversal

### What It Looks For:
**BUY Signal:**
- MACD Histogram **crosses ABOVE zero**
- Previous bar: Histogram ≤ 0 (negative/red)
- Current bar: Histogram > 0 (positive/green)

**SELL Signal:**
- MACD Histogram **crosses BELOW zero**
- Previous bar: Histogram ≥ 0 (positive/green)
- Current bar: Histogram < 0 (negative/red)

### How To See It:
- MACD subplot shows bars (green = positive, red = negative)
- BUY when bars change from red to green (crossing zero line)
- SELL when bars change from green to red (crossing zero line)

### Components:
- **Histogram bars**: Difference between MACD line and Signal line
- **Blue line**: MACD line
- **Orange line**: Signal line
- Histogram crossing zero = momentum shift

---

## 7. Volume Spike Strategy

### What It Looks For:
**BUY Signal:**
- Volume > 2x average volume (20-day)
- Price closes UP for the day
- Confirms strong buying pressure

**SELL Signal:**
- Volume > 2x average volume (20-day)
- Price closes DOWN for the day
- Confirms strong selling pressure

### How To See It:
- Volume subplot (bottom panel)
- Green bars = up days, Red bars = down days
- Look for exceptionally tall bars (2x+ normal height)

---

## 8. Price Breakout Strategy

### What It Looks For:
**BUY Signal:**
- Price breaks ABOVE 20-day high
- Current close > highest close of last 20 days

**SELL Signal:**
- Price breaks BELOW 20-day low
- Current close < lowest close of last 20 days

### How To See It:
- Look at price action over last 20 days
- BUY = price reaches new 20-day high
- SELL = price reaches new 20-day low

---

## 9. Moving Average Ribbon Strategy

### What It Looks For:
**BUY Signal (Perfect Bullish Alignment):**
- Price > SMA(5) > SMA(10) > SMA(20) > SMA(50)
- All moving averages stacked in order
- Indicates strong uptrend with all timeframes aligned

**SELL Signal (Perfect Bearish Alignment):**
- Price < SMA(5) < SMA(10) < SMA(20) < SMA(50)
- All moving averages stacked in reverse order
- Indicates strong downtrend with all timeframes aligned

### How To See It:
- Multiple moving average lines on price chart
- BUY = all lines layered with shortest on top
- SELL = all lines layered with longest on top

---

## Chart Elements Legend

### Price Chart (Top):
- **Blue line**: Stock price
- **Green ▲**: Buy signals from strategy
- **Red ▼**: Sell signals from strategy
- **Gold ★**: ML-scored high-probability signal (90%+ success)
- **Green ◆**: Winning trade (entry/exit with P&L)
- **Red ◆**: Losing trade (entry/exit with P&L)
- **Dashed lines**: Moving averages (EMA 8, EMA 21, SMA 50)

### Indicator Chart (Middle):
- **Purple line**: RSI (0-100 scale)
- **Red line**: Williams %R (-100 to 0 scale)
- **Bars**: MACD Histogram (green = bullish, red = bearish)
- **Horizontal lines**: Key levels (30/70 for RSI, -20/-80 for %R, 0 for MACD)
- **Shaded zones**: Overbought/oversold regions

### Volume Chart (Bottom):
- **Green bars**: Up day (close > open)
- **Red bars**: Down day (close < open)
- **Height**: Volume amount (M = millions, K = thousands)

---

## How Signals Become Trades

### Signal Generation:
1. Strategy detects pattern (RSI crosses 30, EMA crossover, etc.)
2. Signal is generated and stored in database
3. ALL signals are shown as triangles on charts

### ML Filtering:
4. XGBoost model scores each signal (0-100% success probability)
5. Only signals with 80%+ (or 90%+) probability are considered

### Cooldown Rule:
6. If symbol was traded in last 60 days → SKIP (cooldown prevents overtrading)
7. If no recent trade → EXECUTE (shown as diamond on chart)

### Result:
- **Many triangles** = All potential signals from strategy
- **Few diamonds** = Only high-probability trades that respect cooldown
- This is why you see 10-20 triangles but only 1-2 diamonds per symbol

---

## Example Walkthrough: DUOL (2025-01-14)

**Strategy**: RSI Divergence  
**Setup**:
- December 2024: Price drops from $280 → $250 → $235 (lower lows)
- RSI during drops: 25 → 23 → 28 (higher lows)
- **Bullish Divergence Detected** on 2025-01-14

**Signal Generated**:
- Date: 2025-01-14
- Type: BUY
- Strategy: rsi_divergence
- Shown as green triangle on chart

**ML Scoring**:
- Model evaluates: fundamentals, sector (Technology), RSI level, divergence strength
- **Score: 98.4%** success probability
- Passes 90% threshold → shown as gold star

**Trade Execution**:
- Check cooldown: No DUOL trade in last 60 days ✓
- **Trade executed** → shown as green diamond
- Exit when RSI reaches overbought or stop loss triggers

---

## Questions?

The enhanced charts in `backtesting/signal_charts_indicators/` show:
1. **What triggered the signal** (indicator crossing threshold)
2. **When it triggered** (green/red triangle at that date)
3. **Why ML scored it high** (gold star at signal date)
4. **What happened after** (diamond showing actual trade result)

Each chart has a description box at bottom explaining the specific strategy's trigger logic.
