"""
Backtest Trading Strategies

This module backtests all trading strategies from transforms.trading_signals table
and generates performance reports with key metrics.

Metrics Calculated:
- Win Rate: Percentage of profitable trades
- Total Return: Overall portfolio return
- Sharpe Ratio: Risk-adjusted return
- Max Drawdown: Largest peak-to-trough decline
- Average Trade Return: Mean return per trade
- Number of Trades: Total trades executed
- Profit Factor: Ratio of gross profit to gross loss

Usage:
    # Backtest all strategies
    python backtest_strategies.py --start-date 2024-01-01 --end-date 2024-12-31
    
    # Backtest specific strategy
    python backtest_strategies.py --strategy ema_crossover --start-date 2024-01-01
    
    # Export results to CSV
    python backtest_strategies.py --start-date 2024-01-01 --output results.csv
"""

import sys
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import numpy as np

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from db.postgres_database_manager import PostgresDatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


class StrategyBacktester:
    """Backtest trading strategies with comprehensive performance metrics."""
    
    def __init__(self, initial_capital=100000, position_size=0.02, commission=0.001):
        """
        Initialize backtester.
        
        Args:
            initial_capital (float): Starting capital in dollars
            position_size (float): Fraction of capital per trade (0.02 = 2%)
            commission (float): Commission rate per trade (0.001 = 0.1%)
        """
        self.db = PostgresDatabaseManager()
        self.initial_capital = initial_capital
        self.position_size = position_size
        self.commission = commission
        
    def get_signals(self, strategy=None, start_date=None, end_date=None):
        """
        Fetch trading signals from database.
        
        Args:
            strategy (str, optional): Filter by specific strategy
            start_date (str, optional): Start date (YYYY-MM-DD)
            end_date (str, optional): End date (YYYY-MM-DD)
            
        Returns:
            pd.DataFrame: Trading signals with price data
        """
        try:
            self.db.connect()
            
            # Build query filters
            filters = []
            params = []
            
            if strategy:
                filters.append("s.trade_strategy = %s")
                params.append(strategy)
            
            if start_date:
                filters.append("s.date >= %s")
                params.append(start_date)
            
            if end_date:
                filters.append("s.date <= %s")
                params.append(end_date)
            
            where_clause = "WHERE " + " AND ".join(filters) if filters else ""
            
            query = f"""
                SELECT 
                    s.symbol,
                    s.symbol_id,
                    s.date,
                    s.buy_signal,
                    s.sell_signal,
                    s.trade_strategy,
                    s.signal_strength,
                    r.open,
                    r.high,
                    r.low,
                    r.close,
                    r.volume
                FROM transforms.trading_signals s
                INNER JOIN raw.time_series_daily_adjusted r
                    ON s.symbol_id = r.symbol_id::integer 
                    AND s.date = r.date
                {where_clause}
                ORDER BY s.trade_strategy, s.date, s.symbol
            """
            
            df = pd.read_sql(query, self.db.connection, params=params if params else None)
            df['date'] = pd.to_datetime(df['date'])
            
            logger.info(f"Loaded {len(df):,} signals")
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching signals: {e}")
            return pd.DataFrame()
        finally:
            self.db.close()
    
    def get_price_data(self, symbol_id, start_date, end_date):
        """
        Fetch historical price data for a symbol.
        
        Args:
            symbol_id (int): Symbol ID
            start_date (datetime): Start date
            end_date (datetime): End date
            
        Returns:
            pd.DataFrame: Price data
        """
        try:
            if not self.db.connection or self.db.connection.closed:
                self.db.connect()
            
            query = """
                SELECT date, open, high, low, close, volume
                FROM raw.time_series_daily_adjusted
                WHERE symbol_id = %s
                  AND date >= %s
                  AND date <= %s
                ORDER BY date
            """
            
            df = pd.read_sql(query, self.db.connection, 
                           params=(str(symbol_id), start_date, end_date))
            df['date'] = pd.to_datetime(df['date'])
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching price data for {symbol_id}: {e}")
            return pd.DataFrame()
    
    def simulate_trades(self, signals_df, strategy_name, cooldown_days=60):
        """
        Simulate trades for a specific strategy with cooldown period.
        
        Args:
            signals_df (pd.DataFrame): Signals for the strategy
            strategy_name (str): Strategy name
            cooldown_days (int): Days to wait before buying same symbol again (default: 60)
            
        Returns:
            pd.DataFrame: Trade history
        """
        logger.info(f"Simulating trades for {strategy_name}...")
        
        trades = []
        positions = {}  # symbol -> position info
        cooldowns = {}  # symbol -> last_exit_date (to track cooldown period)
        
        # Sort by date to ensure chronological processing
        signals_df = signals_df.sort_values('date')
        
        for idx, row in signals_df.iterrows():
            symbol = row['symbol']
            symbol_id = row['symbol_id']
            date = row['date']
            
            # Check if symbol is in cooldown period
            in_cooldown = False
            if symbol in cooldowns:
                days_since_exit = (date - cooldowns[symbol]).days
                in_cooldown = days_since_exit < cooldown_days
            
            # BUY SIGNAL - Open position (only if not in cooldown and no existing position)
            if row['buy_signal'] and symbol not in positions and not in_cooldown:
                # Use close price of signal day as entry
                entry_price = row['close']
                shares = int((self.initial_capital * self.position_size) / entry_price)
                
                if shares > 0:
                    position_value = shares * entry_price
                    commission_cost = position_value * self.commission
                    
                    positions[symbol] = {
                        'symbol': symbol,
                        'symbol_id': symbol_id,
                        'entry_date': date,
                        'entry_price': entry_price,
                        'shares': shares,
                        'commission': commission_cost
                    }
            
            # SELL SIGNAL - Close position
            elif row['sell_signal'] and symbol in positions:
                position = positions[symbol]
                
                # Use close price of signal day as exit
                exit_price = row['close']
                exit_value = position['shares'] * exit_price
                exit_commission = exit_value * self.commission
                
                # Calculate returns
                entry_value = position['shares'] * position['entry_price']
                total_commission = position['commission'] + exit_commission
                pnl = exit_value - entry_value - total_commission
                pnl_pct = (pnl / entry_value) * 100
                
                # Calculate holding period
                holding_days = (date - position['entry_date']).days
                
                trades.append({
                    'strategy': strategy_name,
                    'symbol': symbol,
                    'entry_date': position['entry_date'],
                    'exit_date': date,
                    'holding_days': holding_days,
                    'entry_price': position['entry_price'],
                    'exit_price': exit_price,
                    'shares': position['shares'],
                    'pnl': pnl,
                    'pnl_pct': pnl_pct,
                    'commission': total_commission,
                    'entry_value': entry_value,
                    'exit_value': exit_value
                })
                
                # Remove closed position and set cooldown
                del positions[symbol]
                cooldowns[symbol] = date  # Track exit date for cooldown period
        
        # Close any remaining open positions at the last date
        if positions:
            last_date = signals_df['date'].max()
            
            for symbol, position in positions.items():
                # Get last known price
                last_price_row = signals_df[
                    (signals_df['symbol'] == symbol) & 
                    (signals_df['date'] == last_date)
                ]
                
                if not last_price_row.empty:
                    exit_price = last_price_row.iloc[0]['close']
                    exit_value = position['shares'] * exit_price
                    exit_commission = exit_value * self.commission
                    
                    entry_value = position['shares'] * position['entry_price']
                    total_commission = position['commission'] + exit_commission
                    pnl = exit_value - entry_value - total_commission
                    pnl_pct = (pnl / entry_value) * 100
                    
                    holding_days = (last_date - position['entry_date']).days
                    
                    trades.append({
                        'strategy': strategy_name,
                        'symbol': symbol,
                        'entry_date': position['entry_date'],
                        'exit_date': last_date,
                        'holding_days': holding_days,
                        'entry_price': position['entry_price'],
                        'exit_price': exit_price,
                        'shares': position['shares'],
                        'pnl': pnl,
                        'pnl_pct': pnl_pct,
                        'commission': total_commission,
                        'entry_value': entry_value,
                        'exit_value': exit_value
                    })
        
        trades_df = pd.DataFrame(trades)
        logger.info(f"Simulated {len(trades_df)} trades for {strategy_name}")
        
        return trades_df
    
    def calculate_metrics(self, trades_df, strategy_name):
        """
        Calculate performance metrics for a strategy.
        
        Args:
            trades_df (pd.DataFrame): Trade history
            strategy_name (str): Strategy name
            
        Returns:
            dict: Performance metrics
        """
        if trades_df.empty:
            return {
                'strategy': strategy_name,
                'total_trades': 0,
                'win_rate': 0.0,
                'total_return': 0.0,
                'total_return_pct': 0.0,
                'avg_trade_return': 0.0,
                'avg_trade_return_pct': 0.0,
                'max_drawdown': 0.0,
                'sharpe_ratio': 0.0,
                'profit_factor': 0.0,
                'avg_holding_days': 0.0,
                'total_commission': 0.0,
                'winning_trades': 0,
                'losing_trades': 0
            }
        
        # Basic metrics
        total_trades = len(trades_df)
        winning_trades = len(trades_df[trades_df['pnl'] > 0])
        losing_trades = len(trades_df[trades_df['pnl'] <= 0])
        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
        
        # Return metrics
        total_return = trades_df['pnl'].sum()
        total_return_pct = (total_return / self.initial_capital) * 100
        avg_trade_return = trades_df['pnl'].mean()
        avg_trade_return_pct = trades_df['pnl_pct'].mean()
        
        # Drawdown calculation
        trades_df = trades_df.sort_values('exit_date')
        trades_df['cumulative_pnl'] = trades_df['pnl'].cumsum()
        trades_df['cumulative_capital'] = self.initial_capital + trades_df['cumulative_pnl']
        trades_df['running_max'] = trades_df['cumulative_capital'].cummax()
        trades_df['drawdown'] = (trades_df['cumulative_capital'] - trades_df['running_max']) / trades_df['running_max']
        max_drawdown = trades_df['drawdown'].min() * 100  # Convert to percentage
        
        # Sharpe Ratio (annualized, assuming 252 trading days)
        returns = trades_df['pnl_pct'].values
        if len(returns) > 1:
            avg_return = np.mean(returns)
            std_return = np.std(returns)
            sharpe_ratio = (avg_return / std_return * np.sqrt(252)) if std_return != 0 else 0
        else:
            sharpe_ratio = 0
        
        # Profit Factor
        gross_profit = trades_df[trades_df['pnl'] > 0]['pnl'].sum()
        gross_loss = abs(trades_df[trades_df['pnl'] < 0]['pnl'].sum())
        profit_factor = (gross_profit / gross_loss) if gross_loss != 0 else 0
        
        # Other metrics
        avg_holding_days = trades_df['holding_days'].mean()
        total_commission = trades_df['commission'].sum()
        
        return {
            'strategy': strategy_name,
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': win_rate,
            'total_return': total_return,
            'total_return_pct': total_return_pct,
            'avg_trade_return': avg_trade_return,
            'avg_trade_return_pct': avg_trade_return_pct,
            'max_drawdown': max_drawdown,
            'sharpe_ratio': sharpe_ratio,
            'profit_factor': profit_factor,
            'avg_holding_days': avg_holding_days,
            'total_commission': total_commission,
            'gross_profit': gross_profit,
            'gross_loss': gross_loss
        }
    
    def backtest_all_strategies(self, start_date=None, end_date=None, strategy=None, cooldown_days=60):
        """
        Backtest all strategies and generate performance report.
        
        Args:
            start_date (str, optional): Start date (YYYY-MM-DD)
            end_date (str, optional): End date (YYYY-MM-DD)
            strategy (str, optional): Test specific strategy only
            cooldown_days (int): Days to wait before buying same symbol again (default: 60)
            
        Returns:
            tuple: (performance_df, all_trades_df)
        """
        logger.info("=" * 80)
        logger.info("BACKTESTING TRADING STRATEGIES")
        logger.info("=" * 80)
        logger.info(f"Initial Capital: ${self.initial_capital:,.2f}")
        logger.info(f"Position Size: {self.position_size * 100}%")
        logger.info(f"Commission Rate: {self.commission * 100}%")
        logger.info(f"Cooldown Period: {cooldown_days} days")
        logger.info(f"Date Range: {start_date or 'All'} to {end_date or 'All'}")
        logger.info("=" * 80)
        
        # Fetch all signals
        signals_df = self.get_signals(strategy, start_date, end_date)
        
        if signals_df.empty:
            logger.warning("No signals found for backtesting")
            return pd.DataFrame(), pd.DataFrame()
        
        # Get unique strategies
        strategies = signals_df['trade_strategy'].unique()
        logger.info(f"Found {len(strategies)} strategies to backtest")
        
        # Backtest each strategy
        performance_results = []
        all_trades = []
        
        for strategy_name in strategies:
            strategy_signals = signals_df[signals_df['trade_strategy'] == strategy_name]
            
            # Simulate trades with cooldown period
            trades_df = self.simulate_trades(strategy_signals, strategy_name, cooldown_days)
            
            if not trades_df.empty:
                all_trades.append(trades_df)
                
                # Calculate metrics
                metrics = self.calculate_metrics(trades_df, strategy_name)
                performance_results.append(metrics)
        
        # Create performance summary
        performance_df = pd.DataFrame(performance_results)
        
        if not performance_df.empty:
            # Sort by total return
            performance_df = performance_df.sort_values('total_return_pct', ascending=False)
        
        # Combine all trades
        all_trades_df = pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()
        
        return performance_df, all_trades_df
    
    def print_report(self, performance_df):
        """
        Print formatted performance report.
        
        Args:
            performance_df (pd.DataFrame): Performance metrics by strategy
        """
        if performance_df.empty:
            logger.warning("No results to display")
            return
        
        print("\n" + "=" * 120)
        print("STRATEGY PERFORMANCE REPORT")
        print("=" * 120)
        
        # Format the dataframe for display
        display_df = performance_df.copy()
        
        # Format columns
        display_df['win_rate'] = display_df['win_rate'].apply(lambda x: f"{x:.2f}%")
        display_df['total_return'] = display_df['total_return'].apply(lambda x: f"${x:,.2f}")
        display_df['total_return_pct'] = display_df['total_return_pct'].apply(lambda x: f"{x:.2f}%")
        display_df['avg_trade_return'] = display_df['avg_trade_return'].apply(lambda x: f"${x:,.2f}")
        display_df['avg_trade_return_pct'] = display_df['avg_trade_return_pct'].apply(lambda x: f"{x:.2f}%")
        display_df['max_drawdown'] = display_df['max_drawdown'].apply(lambda x: f"{x:.2f}%")
        display_df['sharpe_ratio'] = display_df['sharpe_ratio'].apply(lambda x: f"{x:.2f}")
        display_df['profit_factor'] = display_df['profit_factor'].apply(lambda x: f"{x:.2f}")
        display_df['avg_holding_days'] = display_df['avg_holding_days'].apply(lambda x: f"{x:.1f}")
        display_df['total_commission'] = display_df['total_commission'].apply(lambda x: f"${x:,.2f}")
        
        # Select columns to display
        cols_to_display = [
            'strategy', 'total_trades', 'winning_trades', 'losing_trades',
            'win_rate', 'total_return_pct', 'avg_trade_return_pct',
            'max_drawdown', 'sharpe_ratio', 'profit_factor'
        ]
        
        print(display_df[cols_to_display].to_string(index=False))
        print("=" * 120)
        
        # Print summary statistics
        print("\nSUMMARY STATISTICS:")
        print("-" * 60)
        print(f"Best Strategy: {performance_df.iloc[0]['strategy']}")
        print(f"Best Return: {performance_df.iloc[0]['total_return_pct']:.2f}%")
        print(f"Worst Strategy: {performance_df.iloc[-1]['strategy']}")
        print(f"Worst Return: {performance_df.iloc[-1]['total_return_pct']:.2f}%")
        print(f"Average Win Rate: {performance_df['win_rate'].mean():.2f}%")
        print(f"Average Sharpe Ratio: {performance_df['sharpe_ratio'].mean():.2f}")
        print("=" * 120)
    
    def export_results(self, performance_df, all_trades_df, output_file):
        """
        Export results to CSV files.
        
        Args:
            performance_df (pd.DataFrame): Performance metrics
            all_trades_df (pd.DataFrame): All trade history
            output_file (str): Output file path
        """
        try:
            # Export performance summary
            performance_df.to_csv(output_file, index=False)
            logger.info(f"Performance report exported to {output_file}")
            
            # Export trade history
            if not all_trades_df.empty:
                trades_file = output_file.replace('.csv', '_trades.csv')
                all_trades_df.to_csv(trades_file, index=False)
                logger.info(f"Trade history exported to {trades_file}")
            
        except Exception as e:
            logger.error(f"Error exporting results: {e}")


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description='Backtest trading strategies from transforms.trading_signals',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backtest all strategies for 2024
  python backtest_strategies.py --start-date 2024-01-01 --end-date 2024-12-31
  
  # Backtest specific strategy
  python backtest_strategies.py --strategy ema_crossover --start-date 2024-01-01
  
  # Export results
  python backtest_strategies.py --start-date 2024-01-01 --output results.csv
  
  # Custom capital and position size
  python backtest_strategies.py --capital 50000 --position-size 0.05 --start-date 2024-01-01
        """
    )
    
    parser.add_argument('--start-date', type=str,
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str,
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--strategy', type=str,
                       help='Backtest specific strategy only')
    parser.add_argument('--capital', type=float, default=100000,
                       help='Initial capital (default: 100000)')
    parser.add_argument('--position-size', type=float, default=0.02,
                       help='Position size as fraction of capital (default: 0.02 = 2%%)')
    parser.add_argument('--commission', type=float, default=0.001,
                       help='Commission rate (default: 0.001 = 0.1%%)')
    parser.add_argument('--cooldown-days', type=int, default=60,
                       help='Days to wait before buying same symbol again (default: 60)')
    parser.add_argument('--output', type=str,
                       help='Output CSV file path')
    
    args = parser.parse_args()
    
    # Validate dates
    if args.start_date:
        try:
            datetime.strptime(args.start_date, '%Y-%m-%d')
        except ValueError:
            logger.error("Invalid start date format. Use YYYY-MM-DD")
            return
    
    if args.end_date:
        try:
            datetime.strptime(args.end_date, '%Y-%m-%d')
        except ValueError:
            logger.error("Invalid end date format. Use YYYY-MM-DD")
            return
    
    # Initialize backtester
    backtester = StrategyBacktester(
        initial_capital=args.capital,
        position_size=args.position_size,
        commission=args.commission
    )
    
    # Run backtest
    performance_df, all_trades_df = backtester.backtest_all_strategies(
        start_date=args.start_date,
        end_date=args.end_date,
        strategy=args.strategy,
        cooldown_days=args.cooldown_days
    )
    
    # Print report
    if not performance_df.empty:
        backtester.print_report(performance_df)
        
        # Export if requested
        if args.output:
            backtester.export_results(performance_df, all_trades_df, args.output)
    
    logger.info("Backtesting completed!")


if __name__ == "__main__":
    main()
