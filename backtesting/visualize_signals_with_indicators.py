"""Enhanced Signal Visualizer with Technical Indicators

Shows the actual indicators that trigger each strategy:
- RSI Divergence: RSI with divergence zones
- RSI Mean Reversion: RSI with 30/70 levels
- Williams Extremes: Williams %R with -80/-20 levels
- EMA Crossover: EMA 8/21 crossovers
- Trend Following: EMA 8/21, SMA 50, RSI 50 level

Usage:
    python visualize_signals_with_indicators.py --input signals.csv --top 10
"""

import sys
import os
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.gridspec import GridSpec
import seaborn as sns

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.postgres_database_manager import PostgresDatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Set style
sns.set_style("darkgrid")


def load_signals(input_file):
    """Load scored signals from CSV."""
    logger.info(f"Loading signals from {input_file}...")
    df = pd.read_csv(input_file)
    
    # Handle both 'date' and 'signal_date' column names
    if 'signal_date' in df.columns:
        df['date'] = pd.to_datetime(df['signal_date'])
    else:
        df['date'] = pd.to_datetime(df['date'])
    
    logger.info(f"Loaded {len(df):,} signals")
    return df


def get_price_and_indicators(symbol, start_date, end_date):
    """Get historical price data AND technical indicators for a symbol."""
    db = PostgresDatabaseManager()
    db.connect()
    
    # Get symbol_id first
    query_id = f"""
        SELECT symbol_id 
        FROM raw.company_overview 
        WHERE symbol = '{symbol}'
        LIMIT 1
    """
    
    try:
        result = pd.read_sql(query_id, db.connection)
        if len(result) == 0:
            db.close()
            return pd.DataFrame()
        
        symbol_id = int(result['symbol_id'].iloc[0])
        
        # Get price data from raw schema
        query_price = f"""
            SELECT 
                date,
                open,
                high,
                low,
                close,
                adjusted_close,
                volume
            FROM raw.time_series_daily_adjusted
            WHERE symbol_id = '{symbol_id}'
                AND date >= '{start_date}'
                AND date <= '{end_date}'
            ORDER BY date
        """
        
        # Get technical indicators from transforms schema
        query_indicators = f"""
            SELECT 
                date,
                ohlcv_rsi_14,
                ohlcv_willr_14,
                ohlcv_ema_8,
                ohlcv_ema_21,
                ohlcv_sma_20,
                ohlcv_sma_50,
                ohlcv_macd,
                ohlcv_macd_signal,
                ohlcv_macd_histogram
            FROM transforms.time_series_daily_adjusted
            WHERE symbol_id = {symbol_id}
                AND date >= '{start_date}'
                AND date <= '{end_date}'
            ORDER BY date
        """
        
        price_df = pd.read_sql(query_price, db.connection)
        indicators_df = pd.read_sql(query_indicators, db.connection)
        
        # Merge price and indicators
        price_df['date'] = pd.to_datetime(price_df['date'])
        indicators_df['date'] = pd.to_datetime(indicators_df['date'])
        df = pd.merge(price_df, indicators_df, on='date', how='left')

    except Exception as e:
        logger.warning(f"Could not load data for {symbol}: {e}")
        df = pd.DataFrame()
    
    db.close()
    return df


def get_all_signals_for_symbol(symbol, start_date, end_date):
    """Get all buy/sell signals for a symbol in the date range."""
    db = PostgresDatabaseManager()
    db.connect()
    
    query_id = f"""
        SELECT symbol_id 
        FROM raw.company_overview 
        WHERE symbol = '{symbol}'
        LIMIT 1
    """
    result = pd.read_sql(query_id, db.connection)
    
    if len(result) == 0:
        db.close()
        return pd.DataFrame()
    
    symbol_id = result['symbol_id'].iloc[0]
    
    query = f"""
        SELECT 
            date,
            buy_signal,
            sell_signal,
            trade_strategy,
            signal_strength
        FROM transforms.trading_signals
        WHERE symbol_id = {symbol_id}
            AND date >= '{start_date}'
            AND date <= '{end_date}'
            AND processed_at IS NOT NULL
        ORDER BY date
    """
    
    signals_df = pd.read_sql(query, db.connection)
    signals_df['date'] = pd.to_datetime(signals_df['date'])
    
    db.close()
    return signals_df


def get_completed_trades_for_symbol(symbol, trades_file='backtesting/trades_filtered_80pct.csv'):
    """Get completed trades for backtesting visualization."""
    if os.path.exists(trades_file):
        try:
            trades_df = pd.read_csv(trades_file)
            trades_df = trades_df[trades_df['symbol'] == symbol].copy()
            trades_df['entry_date'] = pd.to_datetime(trades_df['entry_date'])
            trades_df['exit_date'] = pd.to_datetime(trades_df['exit_date'])
            return trades_df
        except Exception as e:
            logger.warning(f"Could not load trade data from {trades_file}: {e}")
    
    return pd.DataFrame()


def create_strategy_specific_chart(symbol, signal_row, df, all_signals_df, trades_df, output_dir):
    """Create chart with strategy-specific indicators."""
    
    strategy = signal_row['trade_strategy']
    
    # Create figure with 3 subplots (price, indicator, volume)
    fig = plt.figure(figsize=(16, 12))
    gs = GridSpec(4, 1, figure=fig, height_ratios=[3, 2, 1, 1], hspace=0.05)
    
    ax_price = fig.add_subplot(gs[0])
    ax_indicator = fig.add_subplot(gs[1], sharex=ax_price)
    ax_volume = fig.add_subplot(gs[2], sharex=ax_price)
    ax_legend = fig.add_subplot(gs[3])
    ax_legend.axis('off')
    
    # === PRICE SUBPLOT ===
    ax_price.plot(df['date'], df['adjusted_close'], 
                  color='#2E86AB', linewidth=2, label='Price', zorder=1)
    
    # Add moving averages based on strategy
    if strategy in ['ema_crossover', 'trend_following']:
        if 'ohlcv_ema_8' in df.columns:
            ax_price.plot(df['date'], df['ohlcv_ema_8'], 
                         color='#00C853', linewidth=1.5, alpha=0.8, label='EMA 8', linestyle='--')
        if 'ohlcv_ema_21' in df.columns:
            ax_price.plot(df['date'], df['ohlcv_ema_21'], 
                         color='#FF6F00', linewidth=1.5, alpha=0.8, label='EMA 21', linestyle='--')
    
    if strategy == 'trend_following' and 'ohlcv_sma_50' in df.columns:
        ax_price.plot(df['date'], df['ohlcv_sma_50'], 
                     color='#9C27B0', linewidth=1.5, alpha=0.8, label='SMA 50', linestyle=':')
    
    # Plot buy/sell signals
    buy_signals = all_signals_df[all_signals_df['buy_signal'] == True]
    if len(buy_signals) > 0:
        buy_with_price = buy_signals.merge(df[['date', 'adjusted_close']], on='date', how='left')
        ax_price.scatter(buy_with_price['date'], buy_with_price['adjusted_close'],
                        color='green', marker='^', s=80, alpha=0.6, 
                        label='Buy Signal', zorder=3, edgecolors='darkgreen', linewidths=1)
    
    sell_signals = all_signals_df[all_signals_df['sell_signal'] == True]
    if len(sell_signals) > 0:
        sell_with_price = sell_signals.merge(df[['date', 'adjusted_close']], on='date', how='left')
        ax_price.scatter(sell_with_price['date'], sell_with_price['adjusted_close'],
                        color='red', marker='v', s=80, alpha=0.6, 
                        label='Sell Signal', zorder=3, edgecolors='darkred', linewidths=1)
    
    # Highlight the scored signal
    signal_price = df[df['date'] == signal_row['date']]['adjusted_close']
    if len(signal_price) > 0:
        star_date = signal_row['date'].strftime('%m/%d')
        ax_price.scatter(signal_row['date'], signal_price.iloc[0],
                        color='gold', marker='*', s=600, 
                        label=f"★ Scored Signal ({signal_row['success_probability']:.0%}, {star_date})", 
                        zorder=5, edgecolors='orange', linewidths=2)
        ax_price.annotate(star_date,
                        xy=(signal_row['date'], signal_price.iloc[0]),
                        xytext=(0, 18), textcoords='offset points',
                        fontsize=10, color='black', weight='bold',
                        ha='center', va='bottom',
                        bbox=dict(boxstyle='round,pad=0.2', facecolor='gold', alpha=0.85, edgecolor='orange'))
    
    # Plot completed trades
    if len(trades_df) > 0:
        for _, trade in trades_df.iterrows():
            entry_price = df[df['date'] == trade['entry_date']]['adjusted_close']
            exit_price = df[df['date'] == trade['exit_date']]['adjusted_close']
            
            if len(entry_price) > 0 and len(exit_price) > 0:
                color = 'green' if trade['pnl'] > 0 else 'red'
                ax_price.scatter([trade['entry_date'], trade['exit_date']], 
                               [entry_price.iloc[0], exit_price.iloc[0]],
                               color=color, marker='D', s=150, alpha=0.9,
                               zorder=4, edgecolors='black', linewidths=1.5)
                
                ax_price.plot([trade['entry_date'], trade['exit_date']], 
                            [entry_price.iloc[0], exit_price.iloc[0]],
                            color=color, linestyle='--', alpha=0.6, linewidth=2.5, zorder=2)
                
                # P&L annotation
                mid_date = trade['entry_date'] + (trade['exit_date'] - trade['entry_date']) / 2
                mid_price = (entry_price.iloc[0] + exit_price.iloc[0]) / 2
                pnl_text = f"${trade['pnl']:,.0f} ({trade['pnl_pct']:.1f}%)"
                ax_price.annotate(pnl_text, xy=(mid_date, mid_price), 
                                xytext=(0, 15), textcoords='offset points',
                                fontsize=10, color=color, weight='bold',
                                bbox=dict(boxstyle='round,pad=0.4', facecolor='white', 
                                        edgecolor=color, alpha=0.9, linewidth=2))
    
    ax_price.set_ylabel('Price ($)', fontsize=13, fontweight='bold')
    
    # Build title - handle both 'name' column (backtesting) and missing name (current recommendations)
    company_name = signal_row.get('name', symbol)
    title = f"{symbol}"
    if company_name != symbol:
        title += f" - {company_name}"
    title += f"\nStrategy: {signal_row['trade_strategy'].upper()} | "
    if 'sector' in signal_row and pd.notna(signal_row['sector']):
        title += f"Sector: {signal_row['sector']} | "
    title += f"Success Probability: {signal_row['success_probability']:.1%}"
    end_date = df['date'].max().strftime('%m/%d/%Y')
    title += f" | EOD End: {end_date}"
    
    ax_price.set_title(title, fontsize=15, fontweight='bold', pad=15)
    ax_price.grid(True, alpha=0.3)
    ax_price.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
    ax_price.legend(loc='upper left', framealpha=0.95, fontsize=10)
    plt.setp(ax_price.get_xticklabels(), visible=False)
    
    # === INDICATOR SUBPLOT (Strategy-specific) ===
    if strategy in ['rsi_divergence', 'rsi_mean_reversion', 'rsi_crossing'] and 'ohlcv_rsi_14' in df.columns:
        ax_indicator.plot(df['date'], df['ohlcv_rsi_14'], 
                         color='#7B1FA2', linewidth=2, label='RSI(14)')
        ax_indicator.axhline(y=70, color='red', linestyle='--', alpha=0.7, linewidth=1.5, label='Overbought (70)')
        ax_indicator.axhline(y=30, color='green', linestyle='--', alpha=0.7, linewidth=1.5, label='Oversold (30)')
        ax_indicator.fill_between(df['date'], 30, 70, alpha=0.1, color='gray')
        
        # For rsi_crossing, highlight the zones more clearly
        if strategy == 'rsi_crossing':
            ax_indicator.fill_between(df['date'], 0, 30, alpha=0.15, color='green', label='Oversold Zone (≤30)')
            ax_indicator.fill_between(df['date'], 70, 100, alpha=0.15, color='red', label='Overbought Zone (≥70)')
        
        ax_indicator.set_ylabel('RSI', fontsize=12, fontweight='bold')
        ax_indicator.set_ylim([0, 100])
        ax_indicator.legend(loc='upper left', framealpha=0.9, fontsize=9)
        
    elif strategy == 'williams_extremes' and 'ohlcv_willr_14' in df.columns:
        ax_indicator.plot(df['date'], df['ohlcv_willr_14'], 
                         color='#E64A19', linewidth=2, label='Williams %R(14)')
        ax_indicator.axhline(y=-20, color='red', linestyle='--', alpha=0.7, linewidth=1.5, label='Overbought (-20)')
        ax_indicator.axhline(y=-80, color='green', linestyle='--', alpha=0.7, linewidth=1.5, label='Oversold (-80)')
        ax_indicator.fill_between(df['date'], -20, -80, alpha=0.1, color='gray')
        ax_indicator.set_ylabel('Williams %R', fontsize=12, fontweight='bold')
        ax_indicator.set_ylim([-100, 0])
        ax_indicator.legend(loc='upper left', framealpha=0.9, fontsize=9)
        
    elif strategy == 'macd_histogram_reversal' and 'ohlcv_macd_histogram' in df.columns:
        colors = ['green' if x > 0 else 'red' for x in df['ohlcv_macd_histogram']]
        ax_indicator.bar(df['date'], df['ohlcv_macd_histogram'], 
                        color=colors, alpha=0.7, width=0.8, label='MACD Histogram')
        ax_indicator.axhline(y=0, color='black', linestyle='-', alpha=0.8, linewidth=1.5)
        if 'ohlcv_macd' in df.columns and 'ohlcv_macd_signal' in df.columns:
            ax_indicator.plot(df['date'], df['ohlcv_macd'], 
                            color='#2196F3', linewidth=1.5, alpha=0.8, label='MACD Line')
            ax_indicator.plot(df['date'], df['ohlcv_macd_signal'], 
                            color='#FF9800', linewidth=1.5, alpha=0.8, label='Signal Line')
        ax_indicator.set_ylabel('MACD', fontsize=12, fontweight='bold')
        ax_indicator.legend(loc='upper left', framealpha=0.9, fontsize=9)
        
    elif strategy == 'trend_following' and 'ohlcv_rsi_14' in df.columns:
        ax_indicator.plot(df['date'], df['ohlcv_rsi_14'], 
                         color='#7B1FA2', linewidth=2, label='RSI(14)')
        ax_indicator.axhline(y=50, color='blue', linestyle='--', alpha=0.7, linewidth=2, label='Trend Line (50)')
        ax_indicator.fill_between(df['date'], 0, 50, alpha=0.1, color='red', label='Bearish Zone')
        ax_indicator.fill_between(df['date'], 50, 100, alpha=0.1, color='green', label='Bullish Zone')
        ax_indicator.set_ylabel('RSI', fontsize=12, fontweight='bold')
        ax_indicator.set_ylim([0, 100])
        ax_indicator.legend(loc='upper left', framealpha=0.9, fontsize=9)
        
    else:
        # Default: show RSI if available
        if 'ohlcv_rsi_14' in df.columns:
            ax_indicator.plot(df['date'], df['ohlcv_rsi_14'], 
                             color='#7B1FA2', linewidth=2, label='RSI(14)')
            ax_indicator.axhline(y=70, color='red', linestyle='--', alpha=0.5, linewidth=1)
            ax_indicator.axhline(y=30, color='green', linestyle='--', alpha=0.5, linewidth=1)
            ax_indicator.set_ylabel('RSI', fontsize=12, fontweight='bold')
            ax_indicator.set_ylim([0, 100])
            ax_indicator.legend(loc='upper left', framealpha=0.9, fontsize=9)
    
    ax_indicator.grid(True, alpha=0.3)
    plt.setp(ax_indicator.get_xticklabels(), visible=False)
    
    # === VOLUME SUBPLOT ===
    colors = ['green' if close >= open else 'red' 
              for close, open in zip(df['adjusted_close'], df['open'])]
    ax_volume.bar(df['date'], df['volume'], color=colors, alpha=0.6, width=0.8)
    ax_volume.set_ylabel('Volume', fontsize=12, fontweight='bold')
    ax_volume.grid(True, alpha=0.3)
    ax_volume.yaxis.set_major_formatter(plt.FuncFormatter(
        lambda x, p: f'{x/1e6:.1f}M' if x >= 1e6 else f'{x/1e3:.0f}K'))
    plt.setp(ax_volume.get_xticklabels(), visible=False)
    
    # === LEGEND AREA ===
    strategy_descriptions = {
        'rsi_divergence': 'RSI Divergence: Price makes lower low but RSI makes higher low (bullish) or vice versa (bearish)',
        'rsi_mean_reversion': 'RSI Mean Reversion: BUY when RSI crosses above 30 (oversold recovery), SELL when RSI crosses below 70 (overbought)',
        'rsi_crossing': 'RSI Crossing: BUY when RSI drops below 30, stays below, then crosses back above | SELL when RSI rises above 70, stays above, then crosses back below',
        'williams_extremes': 'Williams %R: BUY when crosses above -80 (oversold recovery), SELL when crosses below -20 (overbought)',
        'ema_crossover': 'EMA Crossover: BUY when EMA(8) crosses above EMA(21), SELL when EMA(8) crosses below EMA(21)',
        'macd_histogram_reversal': 'MACD Histogram: BUY when histogram crosses above 0, SELL when crosses below 0',
        'trend_following': 'Trend Following: BUY when Price > SMA(50) + EMA(8) > EMA(21) + RSI > 50, opposite for SELL',
        'volume_spike': 'Volume Spike: Unusual volume with price confirmation',
        'price_breakout': 'Price Breakout: Price breaks above 20-day high (buy) or below 20-day low (sell)',
        'ma_ribbon': 'MA Ribbon: All moving averages aligned (5>10>20>50 for bullish)',
        'bollinger_breakout': 'Bollinger Breakout: Price breaks outside Bollinger Bands'
    }
    
    description = strategy_descriptions.get(strategy, f"Strategy: {strategy}")
    
    # Build info text with optional fields
    info_lines = [f"STRATEGY: {description}", "", "SIGNAL DETAILS:"]
    info_lines.append(f"  • Date: {signal_row['date'].strftime('%Y-%m-%d')}")
    info_lines.append(f"  • Signal Strength: {signal_row['signal_strength']:.2f}")
    info_lines.append(f"  • Success Probability: {signal_row['success_probability']:.1%}")
    
    if 'market_capitalization' in signal_row and pd.notna(signal_row['market_capitalization']):
        market_cap = float(signal_row['market_capitalization']) / 1e9
        info_lines.append(f"  • Market Cap: ${market_cap:.2f}B")
    
    if 'overall_quality_score' in signal_row and pd.notna(signal_row['overall_quality_score']):
        info_lines.append(f"  • Quality Score: {signal_row['overall_quality_score']:.1f}")
    elif 'composite_score' in signal_row and pd.notna(signal_row['composite_score']):
        info_lines.append(f"  • Composite Score: {signal_row['composite_score']:.3f}")
    
    info_lines.extend(["", "LEGEND:"])
    info_lines.append("  ▲ Green Triangle = BUY signal from strategy   |   ▼ Red Triangle = SELL signal from strategy")
    info_lines.append("  ★ Gold Star = ML-scored high-probability signal (this opportunity)")
    info_lines.append("  ◆ Diamond = Completed trade (90%+ ML filtered, 60-day cooldown respected)")
    
    info_text = "\n".join(info_lines)
    
    ax_legend.text(0.02, 0.98, info_text, transform=ax_legend.transAxes, 
                   fontsize=10, verticalalignment='top', family='monospace',
                   bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.9, pad=1))
    
    # Format x-axis on bottom subplot
    ax_volume.set_xlabel('Date', fontsize=12, fontweight='bold')
    ax_volume.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax_volume.xaxis.set_major_locator(mdates.MonthLocator())
    plt.setp(ax_volume.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    
    # Save figure with YYYY_MM_DD prefix for proper sorting
    date_prefix = signal_row['date'].strftime('%Y_%m_%d')
    output_file = output_dir / f"{date_prefix}_{symbol}_{signal_row['trade_strategy']}_indicators.png"
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    plt.close()
    
    return output_file


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description='Visualize trading signals with technical indicators'
    )
    
    parser.add_argument('--input', type=str, required=True,
                       help='Input CSV file with scored signals')
    parser.add_argument('--top', type=int, default=None,
                       help='Limit to top N signals by probability')
    parser.add_argument('--days', type=int, default=90,
                       help='Days of price history to show (default: 90)')
    parser.add_argument('--output-dir', type=str, default='backtesting/signal_charts_indicators',
                       help='Output directory for charts')
    parser.add_argument('--trades-file', type=str, default='backtesting/trades_filtered_80pct.csv',
                       help='Path to filtered trades CSV file')
    
    args = parser.parse_args()
    
    logger.info("=" * 100)
    logger.info("TRADING SIGNAL VISUALIZER WITH INDICATORS")
    logger.info("=" * 100)
    logger.info(f"Input file: {args.input}")
    logger.info(f"Top signals: {args.top if args.top else 'All'}")
    logger.info(f"Price history days: {args.days}")
    logger.info(f"Output directory: {args.output_dir}")
    logger.info(f"Trades file: {args.trades_file}")
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)
    
    # Load signals
    signals_df = load_signals(args.input)
    
    # Limit to top N
    if args.top and args.top < len(signals_df):
        signals_df = signals_df.head(args.top)
        logger.info(f"Limiting to top {args.top} signals")
    
    logger.info(f"\nGenerating charts for {len(signals_df)} signals...")
    
    # Generate chart for each signal
    for idx, (_, signal_row) in enumerate(signals_df.iterrows(), 1):
        logger.info(f"\n[{idx}/{len(signals_df)}] Processing {signal_row['symbol']} ({signal_row['date'].strftime('%Y-%m-%d')})...")
        
        try:
            # Date range for price data
            end_date = datetime.now()
            start_date = signal_row['date'] - timedelta(days=args.days)
            
            # Get data
            df = get_price_and_indicators(signal_row['symbol'], start_date, end_date)
            
            if df.empty:
                logger.warning(f"  No price data found for {signal_row['symbol']}")
                continue
            
            # Get all signals
            all_signals_df = get_all_signals_for_symbol(signal_row['symbol'], start_date, end_date)
            
            # Get trades
            trades_df = get_completed_trades_for_symbol(signal_row['symbol'], args.trades_file)
            
            # Create chart
            output_file = create_strategy_specific_chart(
                signal_row['symbol'], signal_row, df, all_signals_df, trades_df, output_dir
            )
            
            logger.info(f"  Saved chart: {output_file.name}")
            
        except Exception as e:
            logger.error(f"  Error creating chart for {signal_row['symbol']}: {e}")
            continue
    
    logger.info("\n" + "=" * 100)
    logger.info(f"Visualization complete! Charts saved to: {output_dir}")
    logger.info("=" * 100)


if __name__ == '__main__':
    main()
