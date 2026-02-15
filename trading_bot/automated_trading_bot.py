"""
Automated Trading Bot

Executes automated trading strategy:
1. Scores signals daily and identifies buy opportunities
2. Checks if entry conditions are still valid
3. Places buy orders for high-probability trades
4. Monitors existing positions and exits when conditions are met
5. Implements risk management (position sizing, max positions, stop losses)

Usage:
    # Run daily trading execution
    python automated_trading_bot.py --dry-run  # Test mode
    python automated_trading_bot.py            # Live execution
"""

import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import time

import pandas as pd

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from trading_bot.alpaca_client import AlpacaClient
from trading_bot.daily_signal_scorer import DailySignalScorer
from db.postgres_database_manager import PostgresDatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('trading_bot/bot_execution.log')
    ]
)
logger = logging.getLogger(__name__)


class AutomatedTradingBot:
    """Automated trading bot with risk management."""
    
    def __init__(self, 
                 dry_run=False,
                 max_positions=10,
                 position_size_pct=0.05,
                 min_probability=0.85,
                 stop_loss_pct=0.10,
                 take_profit_pct=0.15,
                 lookback_days=3):
        """
        Initialize trading bot.
        
        Args:
            dry_run: If True, don't place actual orders
            max_positions: Maximum number of concurrent positions
            position_size_pct: Position size as % of portfolio (0.05 = 5%)
            min_probability: Minimum success probability for entry
            stop_loss_pct: Stop loss percentage (0.10 = 10%)
            take_profit_pct: Take profit percentage (0.15 = 15%)
            lookback_days: Days to look back for signals (default: 3)
        """
        self.dry_run = dry_run
        self.max_positions = max_positions
        self.position_size_pct = position_size_pct
        self.min_probability = min_probability
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.lookback_days = lookback_days
        
        # Initialize clients
        self.alpaca = AlpacaClient()
        self.scorer = DailySignalScorer(min_probability=min_probability)
        self.db = PostgresDatabaseManager()
        
        logger.info("="*80)
        logger.info(f"AUTOMATED TRADING BOT INITIALIZED ({'DRY RUN' if dry_run else 'LIVE'})")
        logger.info("="*80)
        logger.info(f"Max Positions: {max_positions}")
        logger.info(f"Position Size: {position_size_pct*100:.1f}%")
        logger.info(f"Min Probability: {min_probability*100:.0f}%")
        logger.info(f"Stop Loss: {stop_loss_pct*100:.0f}%")
        logger.info(f"Take Profit: {take_profit_pct*100:.0f}%")
        logger.info(f"Lookback Days: {lookback_days}")
        logger.info("="*80)
    
    def get_current_positions(self) -> Dict[str, Dict]:
        """Get current positions as dictionary keyed by symbol."""
        positions = self.alpaca.get_positions()
        return {pos['symbol']: pos for pos in positions}
    
    def check_exit_conditions(self, symbol: str, position: Dict) -> Tuple[bool, str]:
        """
        Check if position should be exited.
        
        Returns:
            (should_exit, reason)
        """
        # Calculate P&L percentage
        pnl_pct = position['unrealized_plpc']
        
        # Stop loss check
        if pnl_pct <= -self.stop_loss_pct:
            return True, f"Stop loss triggered ({pnl_pct*100:.2f}%)"
        
        # Take profit check
        if pnl_pct >= self.take_profit_pct:
            return True, f"Take profit triggered ({pnl_pct*100:.2f}%)"
        
        # Check for sell signals
        try:
            self.db.connect()
            query = """
                SELECT sell_signal, trade_strategy
                FROM transforms.trading_signals
                WHERE symbol = %s
                    AND sell_signal = TRUE
                    AND date >= CURRENT_DATE - INTERVAL '2 days'
                ORDER BY date DESC
                LIMIT 1
            """
            result = self.db.fetch_query(query, (symbol,))
            self.db.close()
            
            if result:
                return True, f"Sell signal detected ({result[0][1]})"
        except Exception as e:
            logger.error(f"Error checking sell signals for {symbol}: {e}")
            self.db.close()
        
        return False, ""
    
    def process_exits(self) -> int:
        """Process all exit conditions for existing positions."""
        logger.info("\n" + "="*80)
        logger.info("PROCESSING EXITS")
        logger.info("="*80)
        
        positions = self.get_current_positions()
        
        if not positions:
            logger.info("No open positions")
            return 0
        
        logger.info(f"Checking {len(positions)} open positions...")
        exits_executed = 0
        
        for symbol, pos in positions.items():
            logger.info(f"\nChecking {symbol}:")
            logger.info(f"  Quantity: {pos['qty']}")
            logger.info(f"  Entry: ${pos['avg_entry_price']:.2f}")
            logger.info(f"  Current: ${pos['current_price']:.2f}")
            logger.info(f"  P&L: ${pos['unrealized_pl']:.2f} ({pos['unrealized_plpc']*100:.2f}%)")
            
            should_exit, reason = self.check_exit_conditions(symbol, pos)
            
            if should_exit:
                logger.info(f"  EXIT SIGNAL: {reason}")
                
                if not self.dry_run:
                    result = self.alpaca.place_market_order(symbol, int(pos['qty']), 'sell')
                    if result:
                        logger.info(f"  ✓ SELL ORDER PLACED: {pos['qty']} shares")
                        exits_executed += 1
                    else:
                        logger.error(f"  ✗ Failed to place sell order")
                else:
                    logger.info(f"  [DRY RUN] Would sell {pos['qty']} shares")
                    exits_executed += 1
            else:
                logger.info(f"  Hold position")
        
        logger.info(f"\nExits executed: {exits_executed}")
        return exits_executed
    
    def validate_entry_conditions(self, symbol: str, current_price: float, 
                                   signal_date: datetime) -> Tuple[bool, str]:
        """
        Validate that entry conditions are still valid.
        
        Returns:
            (is_valid, reason)
        """
        # Check if signal is too old (> 8 days)
        days_old = (datetime.now().date() - signal_date).days
        if days_old > 8:
            return False, f"Signal too old ({days_old} days)"
        
        # Check if price hasn't moved too much from signal date
        # This prevents buying if stock has already rallied significantly
        try:
            self.db.connect()
            query = """
                SELECT close
                FROM raw.time_series_daily_adjusted
                WHERE symbol_id::integer IN (
                    SELECT symbol_id FROM raw.time_series_daily_adjusted 
                    WHERE symbol = %s LIMIT 1
                )
                AND date = %s
            """
            result = self.db.fetch_query(query, (symbol, signal_date))
            self.db.close()
            
            if result:
                signal_price = float(result[0][0])
                price_change_pct = (current_price - signal_price) / signal_price
                
                # Don't buy if price has moved up > 5% already
                if price_change_pct > 0.05:
                    return False, f"Price moved +{price_change_pct*100:.1f}% since signal"
                
                # Don't buy if price has moved down > 3% (negative momentum)
                if price_change_pct < -0.03:
                    return False, f"Price moved {price_change_pct*100:.1f}% since signal"
        except Exception as e:
            logger.warning(f"Could not validate price for {symbol}: {e}")
            self.db.close()
        
        return True, "Valid"
    
    def calculate_position_size(self, portfolio_value: float, price: float) -> int:
        """Calculate number of shares to buy based on position sizing."""
        position_value = portfolio_value * self.position_size_pct
        shares = int(position_value / price)
        return max(1, shares)  # At least 1 share
    
    def process_entries(self, recommendations: pd.DataFrame) -> int:
        """Process entry signals and place buy orders."""
        logger.info("\n" + "="*80)
        logger.info("PROCESSING ENTRIES")
        logger.info("="*80)
        
        if recommendations.empty:
            logger.info("No recommendations for entry")
            return 0
        
        # Get current account and positions
        account = self.alpaca.get_account()
        current_positions = self.get_current_positions()
        
        logger.info(f"Portfolio Value: ${account['portfolio_value']:,.2f}")
        logger.info(f"Buying Power: ${account['buying_power']:,.2f}")
        logger.info(f"Current Positions: {len(current_positions)}/{self.max_positions}")
        
        # Check if we can add more positions
        available_slots = self.max_positions - len(current_positions)
        if available_slots <= 0:
            logger.info("Maximum positions reached")
            return 0
        
        logger.info(f"Available slots: {available_slots}")
        
        entries_executed = 0
        
        # Process top recommendations
        for idx, row in recommendations.head(available_slots * 2).iterrows():
            symbol = row['symbol']
            
            # Skip if already have position
            if symbol in current_positions:
                logger.info(f"\n{symbol}: Already have position, skipping")
                continue
            
            # Get current price
            current_price = self.alpaca.get_latest_price(symbol)
            if not current_price:
                logger.warning(f"\n{symbol}: Could not get current price, skipping")
                continue
            
            # Validate entry conditions
            signal_date = pd.to_datetime(row['signal_date']).date()
            is_valid, reason = self.validate_entry_conditions(symbol, current_price, signal_date)
            
            logger.info(f"\n{symbol}:")
            logger.info(f"  Strategy: {row['trade_strategy']}")
            logger.info(f"  Signal Date: {signal_date}")
            logger.info(f"  Current Price: ${current_price:.2f}")
            logger.info(f"  Success Probability: {row['success_probability']*100:.1f}%")
            logger.info(f"  Quality Score: {row['overall_quality_score']:.0f}")
            logger.info(f"  Composite Score: {row['composite_score']:.3f}")
            logger.info(f"  Entry Valid: {is_valid} - {reason}")
            
            if not is_valid:
                continue
            
            # Calculate position size
            shares = self.calculate_position_size(account['portfolio_value'], current_price)
            position_value = shares * current_price
            
            # Check if we have enough buying power
            if position_value > account['buying_power']:
                logger.warning(f"  Insufficient buying power (need ${position_value:.2f})")
                continue
            
            logger.info(f"  Position Size: {shares} shares (${position_value:.2f})")
            
            # Place order
            if not self.dry_run:
                result = self.alpaca.place_market_order(symbol, shares, 'buy')
                if result:
                    logger.info(f"  ✓ BUY ORDER PLACED")
                    entries_executed += 1
                    time.sleep(1)  # Rate limiting
                    
                    # Stop if we've filled available slots
                    if entries_executed >= available_slots:
                        logger.info(f"\nFilled all {available_slots} available slots")
                        break
                else:
                    logger.error(f"  ✗ Failed to place buy order")
            else:
                logger.info(f"  [DRY RUN] Would buy {shares} shares")
                entries_executed += 1
                
                if entries_executed >= available_slots:
                    logger.info(f"\n[DRY RUN] Would fill all {available_slots} slots")
                    break
        
        logger.info(f"\nEntries executed: {entries_executed}")
        return entries_executed
    
    def run(self, output_file=None):
        """Execute daily trading routine.
        
        Args:
            output_file: Optional path to save recommendations CSV
        """
        logger.info("\n" + "="*80)
        logger.info(f"STARTING TRADING BOT EXECUTION - {datetime.now()}")
        logger.info("="*80)
        
        # Check if market is open
        if not self.dry_run:
            is_open = self.alpaca.is_market_open()
            if not is_open:
                logger.warning("Market is closed. Exiting.")
                return
        
        try:
            # Step 1: Process exits for existing positions
            exits = self.process_exits()
            
            # Step 2: Score signals and get recommendations
            logger.info("\n" + "="*80)
            logger.info("SCORING SIGNALS")
            logger.info("="*80)
            recommendations = self.scorer.score_signals(lookback_days=self.lookback_days)
            
            if not recommendations.empty:
                logger.info(f"\nTop 10 Recommendations:")
                print(recommendations.head(10)[['symbol', 'trade_strategy', 'success_probability', 
                                                'composite_score']].to_string(index=False))
                
                # Save recommendations to file if requested
                if output_file:
                    recommendations.to_csv(output_file, index=False)
                    logger.info(f"\nRecommendations saved to: {output_file}")
            
            # Step 3: Process entries
            entries = self.process_entries(recommendations)
            
            # Summary
            logger.info("\n" + "="*80)
            logger.info("EXECUTION SUMMARY")
            logger.info("="*80)
            logger.info(f"Exits: {exits}")
            logger.info(f"Entries: {entries}")
            logger.info(f"Total Actions: {exits + entries}")
            
            # Final positions
            final_positions = self.get_current_positions()
            logger.info(f"\nFinal Position Count: {len(final_positions)}")
            if final_positions:
                logger.info("Current Holdings:")
                for symbol, pos in final_positions.items():
                    logger.info(f"  {symbol}: {pos['qty']} shares @ ${pos['avg_entry_price']:.2f} "
                              f"(P&L: ${pos['unrealized_pl']:.2f})")
            
        except Exception as e:
            logger.error(f"Error during execution: {e}", exc_info=True)
        finally:
            logger.info("\n" + "="*80)
            logger.info("TRADING BOT EXECUTION COMPLETE")
            logger.info("="*80)


def main():
    """Command-line interface."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Automated Trading Bot',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--dry-run', action='store_true',
                       help='Test mode - no actual orders placed')
    parser.add_argument('--max-positions', type=int, default=10,
                       help='Maximum concurrent positions (default: 10)')
    parser.add_argument('--position-size', type=float, default=0.05,
                       help='Position size as fraction (default: 0.05 = 5%%)')
    parser.add_argument('--min-probability', type=float, default=0.85,
                       help='Minimum success probability (default: 0.85)')
    parser.add_argument('--stop-loss', type=float, default=0.10,
                       help='Stop loss percentage (default: 0.10 = 10%%)')
    parser.add_argument('--take-profit', type=float, default=0.15,
                       help='Take profit percentage (default: 0.15 = 15%%)')
    parser.add_argument('--lookback-days', type=int, default=3,
                       help='Days to look back for signals (default: 3)')
    parser.add_argument('--output-file', type=str, default=None,
                       help='Save recommendations to CSV file (e.g., recommendations.csv)')
    
    args = parser.parse_args()
    
    # Initialize and run bot
    bot = AutomatedTradingBot(
        dry_run=args.dry_run,
        max_positions=args.max_positions,
        position_size_pct=args.position_size,
        min_probability=args.min_probability,
        stop_loss_pct=args.stop_loss,
        take_profit_pct=args.take_profit,
        lookback_days=args.lookback_days
    )
    
    bot.run(output_file=args.output_file)


if __name__ == '__main__':
    main()
