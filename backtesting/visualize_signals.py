"""Visualize Trading Signals with Price Charts

Creates interactive charts for each symbol showing:
- Price candlesticks or line chart
- Buy/sell signal markers with success probability
- Entry/exit points for completed trades
- Signal strength on secondary axis

Usage:
    # Visualize signals from daily scorer output
    python visualize_signals.py --input backtesting/daily_signals_scored_20251231.csv
    
    # Limit to top N signals
    python visualize_signals.py --input signals.csv --top 10
    
    # Custom date range for price data
    python visualize_signals.py --input signals.csv --days 90
    
    # Output to specific directory
    python visualize_signals.py --input signals.csv --output-dir signal_charts/
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
from matplotlib.patches import Rectangle
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
plt.rcParams['figure.figsize'] = (14, 8)


def load_signals(input_file):
    """Load scored signals from CSV."""
    logger.info(f"Loading signals from {input_file}...")
    df = pd.read_csv(input_file)
    df['date'] = pd.to_datetime(df['date'])
    logger.info(f"Loaded {len(df):,} signals")
    return df


def get_price_data(symbol, start_date, end_date):
    """Get historical price data for a symbol."""
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
        
        # Get price data from raw schema using symbol_id (cast to text for comparison)
        query = f"""
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
        
        price_df = pd.read_sql(query, db.connection)
        price_df['date'] = pd.to_datetime(price_df['date'])
    except Exception as e:
        logger.warning(f"Could not load price data for {symbol}: {e}")
        price_df = pd.DataFrame()
    
    db.close()
    return price_df


def get_all_signals_for_symbol(symbol, start_date, end_date):
    """Get all buy/sell signals for a symbol in the date range."""
    db = PostgresDatabaseManager()
    db.connect()
    
    # Get symbol_id first
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
    """Get completed trades (entry/exit) for backtesting visualization.
    
    Uses ML-filtered trades (80%+ success probability) to show only
    trades that would have been taken with the ML system.
    """
    # Try to load from specified trades file
    if os.path.exists(trades_file):
        try:
            trades_df = pd.read_csv(trades_file)
            trades_df = trades_df[trades_df['symbol'] == symbol].copy()
            trades_df['entry_date'] = pd.to_datetime(trades_df['entry_date'])
            trades_df['exit_date'] = pd.to_datetime(trades_df['exit_date'])
            return trades_df
        except Exception as e:
            logger.warning(f"Could not load trade data from {trades_file}: {e}")
    
    # Fallback to unfiltered trades if specified file not available
    backtest_file = 'backtesting/strategy_performance_with_cooldown_trades.csv'
    
    if not os.path.exists(backtest_file):
        return pd.DataFrame()
    
    try:
        trades_df = pd.read_csv(backtest_file)
        trades_df = trades_df[trades_df['symbol'] == symbol].copy()
        trades_df['entry_date'] = pd.to_datetime(trades_df['entry_date'])
        trades_df['exit_date'] = pd.to_datetime(trades_df['exit_date'])
        logger.warning(f"Using unfiltered trades for {symbol} - {trades_file} not found")
        return trades_df
    except Exception as e:
        logger.warning(f"Could not load trade data: {e}")
        return pd.DataFrame()


def create_signal_chart(symbol, signal_row, price_df, all_signals_df, trades_df, output_dir):
    """Create chart for a single symbol showing price and signals."""
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), 
                                     gridspec_kw={'height_ratios': [3, 1]}, 
                                     sharex=True)
    
    # Plot 1: Price with signals
    ax1.plot(price_df['date'], price_df['adjusted_close'], 
             color='#2E86AB', linewidth=1.5, label='Price', zorder=1)
    
    # Add 20-day and 50-day moving averages
    if len(price_df) >= 20:
        ma20 = price_df['adjusted_close'].rolling(window=20).mean()
        ax1.plot(price_df['date'], ma20, color='#A23B72', 
                linewidth=1, alpha=0.7, label='MA20', zorder=1)
    
    if len(price_df) >= 50:
        ma50 = price_df['adjusted_close'].rolling(window=50).mean()
        ax1.plot(price_df['date'], ma50, color='#F18F01', 
                linewidth=1, alpha=0.7, label='MA50', zorder=1)
    
    # Plot buy signals
    buy_signals = all_signals_df[all_signals_df['buy_signal'] == True]
    if len(buy_signals) > 0:
        # Merge with price data to get price at signal date
        buy_with_price = buy_signals.merge(
            price_df[['date', 'adjusted_close']], 
            on='date', 
            how='left'
        )
        ax1.scatter(buy_with_price['date'], buy_with_price['adjusted_close'],
                   color='green', marker='^', s=100, alpha=0.7, 
                   label='Buy Signal', zorder=3, edgecolors='darkgreen', linewidths=1.5)
    
    # Plot sell signals
    sell_signals = all_signals_df[all_signals_df['sell_signal'] == True]
    if len(sell_signals) > 0:
        sell_with_price = sell_signals.merge(
            price_df[['date', 'adjusted_close']], 
            on='date', 
            how='left'
        )
        ax1.scatter(sell_with_price['date'], sell_with_price['adjusted_close'],
                   color='red', marker='v', s=100, alpha=0.7, 
                   label='Sell Signal', zorder=3, edgecolors='darkred', linewidths=1.5)
    
    # Highlight the scored signal (the one we're analyzing)
    signal_price = price_df[price_df['date'] == signal_row['date']]['adjusted_close']
    if len(signal_price) > 0:
        ax1.scatter(signal_row['date'], signal_price.iloc[0],
                   color='gold', marker='*', s=500, 
                   label=f"Scored Signal ({signal_row['success_probability']:.0%})", 
                   zorder=5, edgecolors='orange', linewidths=2)
    
    # Plot completed trades (entry/exit with P&L)
    if len(trades_df) > 0:
        for _, trade in trades_df.iterrows():
            # Entry point
            entry_price = price_df[price_df['date'] == trade['entry_date']]['adjusted_close']
            if len(entry_price) > 0:
                color = 'green' if trade['pnl'] > 0 else 'red'
                ax1.scatter(trade['entry_date'], entry_price.iloc[0],
                           color=color, marker='D', s=150, alpha=0.8,
                           zorder=4, edgecolors='black', linewidths=1)
            
            # Exit point
            exit_price = price_df[price_df['date'] == trade['exit_date']]['adjusted_close']
            if len(exit_price) > 0 and len(entry_price) > 0:
                ax1.scatter(trade['exit_date'], exit_price.iloc[0],
                           color=color, marker='D', s=150, alpha=0.8,
                           zorder=4, edgecolors='black', linewidths=1)
                
                # Draw line connecting entry to exit
                ax1.plot([trade['entry_date'], trade['exit_date']], 
                        [entry_price.iloc[0], exit_price.iloc[0]],
                        color=color, linestyle='--', alpha=0.5, linewidth=2, zorder=2)
                
                # Add P&L annotation
                mid_date = trade['entry_date'] + (trade['exit_date'] - trade['entry_date']) / 2
                mid_price = (entry_price.iloc[0] + exit_price.iloc[0]) / 2
                pnl_text = f"${trade['pnl']:,.0f}"
                ax1.annotate(pnl_text, xy=(mid_date, mid_price), 
                            xytext=(0, 10), textcoords='offset points',
                            fontsize=9, color=color, weight='bold',
                            bbox=dict(boxstyle='round,pad=0.3', facecolor='white', 
                                    edgecolor=color, alpha=0.8))
    
    # Format price axis
    ax1.set_ylabel('Price ($)', fontsize=12, fontweight='bold')
    ax1.set_title(f"{symbol} - {signal_row['name']}\n"
                  f"{signal_row['trade_strategy']} | Sector: {signal_row['sector']} | "
                  f"Success Probability: {signal_row['success_probability']:.1%}",
                  fontsize=14, fontweight='bold', pad=20)
    ax1.legend(loc='upper left', framealpha=0.9)
    ax1.grid(True, alpha=0.3)
    
    # Format y-axis as currency
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
    
    # Plot 2: Volume
    colors = ['green' if close >= open else 'red' 
              for close, open in zip(price_df['adjusted_close'], price_df['open'])]
    ax2.bar(price_df['date'], price_df['volume'], color=colors, alpha=0.6, width=0.8)
    ax2.set_ylabel('Volume', fontsize=12, fontweight='bold')
    ax2.set_xlabel('Date', fontsize=12, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    
    # Format volume axis
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x/1e6:.1f}M' if x >= 1e6 else f'{x/1e3:.0f}K'))
    
    # Format x-axis dates
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax2.xaxis.set_major_locator(mdates.MonthLocator())
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # Add text box with key metrics
    if 'overall_quality_score' in signal_row:
        textstr = '\n'.join([
            f"Signal Date: {signal_row['date'].strftime('%Y-%m-%d')}",
            f"Signal Strength: {signal_row['signal_strength']:.2f}",
            f"Market Cap: ${float(signal_row['market_capitalization'])/1e9:.2f}B" if 'market_capitalization' in signal_row else "",
            f"Quality Score: {signal_row['overall_quality_score']:.2f}" if not pd.isna(signal_row['overall_quality_score']) else "Quality Score: N/A",
            f"Industry: {signal_row['industry']}" if 'industry' in signal_row else ""
        ])
        
        props = dict(boxstyle='round', facecolor='wheat', alpha=0.8)
        ax1.text(0.02, 0.98, textstr, transform=ax1.transAxes, fontsize=10,
                verticalalignment='top', bbox=props)
    
    plt.tight_layout()
    
    # Save figure
    output_file = output_dir / f"{symbol}_{signal_row['date'].strftime('%Y%m%d')}_{signal_row['trade_strategy']}.png"
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    plt.close()
    
    logger.info(f"  Saved chart: {output_file.name}")


def main():
    parser = argparse.ArgumentParser(description='Visualize trading signals with price charts')
    parser.add_argument('--input', type=str, required=True,
                       help='Input CSV file with scored signals')
    parser.add_argument('--top', type=int, default=None,
                       help='Only visualize top N signals (default: all)')
    parser.add_argument('--days', type=int, default=90,
                       help='Number of days of price history to show (default: 90)')
    parser.add_argument('--output-dir', type=str, default='backtesting/signal_charts',
                       help='Output directory for charts')
    parser.add_argument('--trades-file', type=str, default='backtesting/trades_filtered_80pct.csv',
                       help='Backtest trades file to show (default: ML-filtered 80%% trades)')
    
    args = parser.parse_args()
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info("="*100)
    logger.info("TRADING SIGNAL VISUALIZER")
    logger.info("="*100)
    logger.info(f"Input file: {args.input}")
    logger.info(f"Top signals: {args.top if args.top else 'All'}")
    logger.info(f"Price history days: {args.days}")
    logger.info(f"Output directory: {args.output_dir}")
    logger.info(f"Trades file: {args.trades_file}")
    
    # Check if using filtered or unfiltered trades
    if 'filtered' in args.trades_file.lower():
        logger.info("Using ML-filtered trades (high-probability only)")
    else:
        logger.info("Using all backtest trades (unfiltered)")
    
    # Load signals
    signals_df = load_signals(args.input)
    
    if len(signals_df) == 0:
        logger.warning("No signals to visualize!")
        return
    
    # Limit to top N if specified
    if args.top:
        signals_df = signals_df.head(args.top)
        logger.info(f"Limiting to top {args.top} signals")
    
    # Process each signal
    logger.info(f"\nGenerating charts for {len(signals_df):,} signals...")
    
    for idx, signal_row in signals_df.iterrows():
        symbol = signal_row['symbol']
        signal_date = signal_row['date']
        
        logger.info(f"\n[{idx+1}/{len(signals_df)}] Processing {symbol} ({signal_date.strftime('%Y-%m-%d')})...")
        
        # Calculate date range for price data
        end_date = signal_date + timedelta(days=30)  # Show some future context
        start_date = signal_date - timedelta(days=args.days)
        
        # Get price data
        price_df = get_price_data(symbol, start_date.strftime('%Y-%m-%d'), 
                                  end_date.strftime('%Y-%m-%d'))
        
        if len(price_df) == 0:
            logger.warning(f"  No price data found for {symbol}, skipping...")
            continue
        
        # Get all signals for this symbol in the date range
        all_signals_df = get_all_signals_for_symbol(symbol, 
                                                     start_date.strftime('%Y-%m-%d'),
                                                     end_date.strftime('%Y-%m-%d'))
        
        # Get completed trades (if available)
        trades_df = get_completed_trades_for_symbol(symbol, args.trades_file)
        if len(trades_df) > 0:
            # Filter to trades in the visible date range
            trades_df = trades_df[
                (trades_df['entry_date'] >= start_date) & 
                (trades_df['entry_date'] <= end_date)
            ]
        
        # Create chart
        try:
            create_signal_chart(symbol, signal_row, price_df, all_signals_df, 
                              trades_df, output_dir)
        except Exception as e:
            logger.error(f"  Error creating chart for {symbol}: {e}")
            continue
    
    logger.info("\n" + "="*100)
    logger.info(f"Visualization complete! Charts saved to: {output_dir}")
    logger.info("="*100)


if __name__ == '__main__':
    main()
