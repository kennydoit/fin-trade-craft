"""Join fundamental quality scores to backtest trades with 45-day lag.

This program:
1. Loads backtest trades from CSV (or regenerates from database)
2. Loads fundamental quality scores with +45 day lag applied
3. Joins fundamentals to trades where trade date is within 90 days after lagged fundamental date
4. Outputs enriched dataset with same number of records as input trades

Lag Logic:
- Fundamental data for fiscal_date_ending 2024-09-30
- Add 45 days → 2024-11-14 (publication lag)
- Join to trades where entry_date is between 2024-11-14 and 2024-02-12 (90 day window)
- This ensures fundamentals are available before trades and maximizes sample size

Usage:
    # From CSV (faster)
    python join_fundamentals_to_trades.py --input strategy_performance_report_trades.csv
    
    # Regenerate from database
    python join_fundamentals_to_trades.py --regenerate
    
    # Custom output
    python join_fundamentals_to_trades.py --output trades_with_fundamentals.parquet
"""

import sys
import os
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.postgres_database_manager import PostgresDatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FundamentalTradeJoiner:
    """Join fundamental quality scores to backtest trades with publication lag."""
    
    def __init__(self, publication_lag_days: int = 45, lookforward_window_days: int = 90):
        """
        Initialize joiner.
        
        Args:
            publication_lag_days: Days to add to fiscal_date_ending (default 45)
            lookforward_window_days: Days after publication to match trades (default 90)
        """
        self.db = PostgresDatabaseManager()
        self.publication_lag_days = publication_lag_days
        self.lookforward_window_days = lookforward_window_days
        
    def load_trades_from_csv(self, csv_path: str) -> pd.DataFrame:
        """Load trades from CSV file."""
        logger.info(f"Loading trades from {csv_path}...")
        df = pd.read_csv(csv_path)
        
        # Ensure date columns are datetime
        df['entry_date'] = pd.to_datetime(df['entry_date'])
        df['exit_date'] = pd.to_datetime(df['exit_date'])
        
        logger.info(f"Loaded {len(df):,} trades")
        logger.info(f"Date range: {df['entry_date'].min()} to {df['entry_date'].max()}")
        logger.info(f"Unique symbols: {df['symbol'].nunique():,}")
        
        return df
    
    def load_trades_from_database(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """Regenerate trades from database (expensive operation)."""
        logger.info("Regenerating trades from database...")
        logger.info("This may take several minutes...")
        
        # Build date filter
        date_filter = ""
        params = []
        if start_date and end_date:
            date_filter = "AND ts.date BETWEEN %s AND %s"
            params = [start_date, end_date]
        
        query = f"""
            WITH signals_with_prices AS (
                SELECT 
                    ts.trade_strategy,
                    ts.symbol,
                    ts.symbol_id,
                    ts.date,
                    ts.buy_signal,
                    ts.sell_signal,
                    r.open,
                    r.high,
                    r.low,
                    r.close,
                    r.volume,
                    r.adjusted_close
                FROM transforms.trading_signals ts
                INNER JOIN raw.time_series_daily_adjusted r
                    ON r.symbol_id::integer = ts.symbol_id
                    AND r.date = ts.date
                WHERE ts.processed_at IS NOT NULL
                {date_filter}
                ORDER BY ts.symbol_id, ts.date, ts.trade_strategy
            )
            SELECT * FROM signals_with_prices
        """
        
        self.db.connect()
        df = pd.read_sql(query, self.db.connection, params=params if params else None)
        
        logger.info(f"Loaded {len(df):,} signals from database")
        return df
    
    def load_fundamental_scores(self) -> pd.DataFrame:
        """Load fundamental quality scores from database."""
        logger.info("Loading fundamental quality scores...")
        
        query = """
            SELECT 
                symbol_id,
                symbol,
                fiscal_date_ending,
                balance_sheet_quality_score,
                cash_flow_quality_score,
                income_statement_quality_score,
                overall_quality_score,
                bs_liquidity_score,
                bs_leverage_score,
                bs_asset_quality_score,
                cf_generation_score,
                cf_efficiency_score,
                cf_sustainability_score,
                is_profitability_score,
                is_margin_score,
                is_growth_score,
                is_high_quality,
                is_investment_grade,
                has_red_flags,
                processed_at
            FROM transforms.fundamental_quality_scores
            WHERE processed_at IS NOT NULL
            ORDER BY symbol_id, fiscal_date_ending
        """
        
        self.db.connect()
        df = pd.read_sql(query, self.db.connection)
        
        # Convert dates
        df['fiscal_date_ending'] = pd.to_datetime(df['fiscal_date_ending'])
        
        # Apply publication lag
        df['publication_date'] = df['fiscal_date_ending'] + pd.Timedelta(days=self.publication_lag_days)
        df['valid_until_date'] = df['publication_date'] + pd.Timedelta(days=self.lookforward_window_days)
        
        logger.info(f"Loaded {len(df):,} fundamental records")
        logger.info(f"Date range: {df['fiscal_date_ending'].min()} to {df['fiscal_date_ending'].max()}")
        logger.info(f"Publication date range: {df['publication_date'].min()} to {df['publication_date'].max()}")
        logger.info(f"Unique symbols: {df['symbol'].nunique():,}")
        
        return df
    
    def join_fundamentals_to_trades(self, trades_df: pd.DataFrame, fundamentals_df: pd.DataFrame) -> pd.DataFrame:
        """
        Join fundamentals to trades with lag logic.
        
        For each trade, find the most recent fundamental data where:
        - Same symbol
        - publication_date <= trade.entry_date < valid_until_date
        
        This ensures:
        1. Fundamentals were published before the trade (no lookahead bias)
        2. Fundamentals are reasonably recent (within 90 days)
        3. Same fundamental data can match multiple trades (maximizes sample size)
        """
        logger.info("Joining fundamentals to trades...")
        logger.info(f"Publication lag: {self.publication_lag_days} days")
        logger.info(f"Lookforward window: {self.lookforward_window_days} days")
        
        # Add unique trade ID to preserve all original trades
        trades_df = trades_df.copy()
        trades_df['_trade_id'] = range(len(trades_df))
        
        # Prepare fundamentals for merge
        fund_cols = [
            'symbol', 'fiscal_date_ending', 'publication_date', 'valid_until_date',
            'balance_sheet_quality_score', 'cash_flow_quality_score', 
            'income_statement_quality_score', 'overall_quality_score',
            'bs_liquidity_score', 'bs_leverage_score', 'bs_asset_quality_score',
            'cf_generation_score', 'cf_efficiency_score', 'cf_sustainability_score',
            'is_profitability_score', 'is_margin_score', 'is_growth_score',
            'is_high_quality', 'is_investment_grade', 'has_red_flags'
        ]
        fundamentals_df = fundamentals_df[fund_cols].copy()
        
        # Use conditional merge: join all matching symbol/date pairs, then filter
        logger.info("Performing conditional merge (this may take a few minutes)...")
        
        # Step 1: Cross join on symbol (use left join to preserve all trades)
        enriched_df = trades_df.merge(fundamentals_df, on='symbol', how='left', suffixes=('', '_fund'))
        
        logger.info(f"After initial merge: {len(enriched_df):,} rows")
        
        # Step 2: Filter to valid date ranges OR keep if no fundamental match
        # Keep only where: publication_date <= entry_date <= valid_until_date OR fundamentals are null
        valid_mask = (
            enriched_df['publication_date'].isna() |  # Keep unmatched trades
            (
                (enriched_df['publication_date'] <= enriched_df['entry_date']) &
                (enriched_df['entry_date'] <= enriched_df['valid_until_date'])
            )
        )
        enriched_df = enriched_df[valid_mask].copy()
        
        logger.info(f"After date filtering: {len(enriched_df):,} rows")
        
        # Step 3: For each trade, keep only the most recent fundamental data (or the null match)
        # Sort by fiscal_date_ending descending so we keep the latest fundamental
        enriched_df = enriched_df.sort_values('fiscal_date_ending', ascending=False, na_position='last')
        enriched_df = enriched_df.drop_duplicates(
            subset=['_trade_id'],  # Use unique trade ID to preserve all original trades
            keep='first'
        ).sort_index()
        
        # Step 4: Add back trades that were completely filtered out (no valid fundamental match)
        matched_trade_ids = set(enriched_df['_trade_id'])
        all_trade_ids = set(trades_df['_trade_id'])
        missing_trade_ids = all_trade_ids - matched_trade_ids
        
        if len(missing_trade_ids) > 0:
            logger.info(f"Adding back {len(missing_trade_ids):,} trades with no valid fundamental match")
            missing_trades = trades_df[trades_df['_trade_id'].isin(missing_trade_ids)].copy()
            
            # Add null fundamental columns to match schema
            fund_cols_to_add = [col for col in enriched_df.columns if col not in missing_trades.columns]
            for col in fund_cols_to_add:
                missing_trades[col] = None
            
            # Combine
            enriched_df = pd.concat([enriched_df, missing_trades], ignore_index=True)
        
        # Drop the temporary trade ID
        enriched_df = enriched_df.drop(columns=['_trade_id'])
        
        logger.info(f"After adding missing trades: {len(enriched_df):,} rows")
        
        # Count matches
        matched = enriched_df['fiscal_date_ending'].notna().sum()
        total = len(enriched_df)
        match_rate = (matched / total * 100) if total > 0 else 0
        
        logger.info(f"Join complete:")
        logger.info(f"  Total trades: {total:,}")
        logger.info(f"  Matched with fundamentals: {matched:,} ({match_rate:.1f}%)")
        logger.info(f"  Unmatched: {total - matched:,} ({100-match_rate:.1f}%)")
        
        # Validate: Check that publication dates are before trade dates
        if matched > 0:
            valid_joins = (enriched_df['publication_date'] <= enriched_df['entry_date']).sum()
            logger.info(f"  Valid joins (no lookahead): {valid_joins:,} ({valid_joins/matched*100:.1f}%)")
        
        return enriched_df
    
    def export_to_parquet(self, df: pd.DataFrame, output_path: str):
        """Export enriched dataset to parquet."""
        logger.info(f"Exporting to {output_path}...")
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
        
        df.to_parquet(output_path, index=False, compression='snappy')
        
        file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
        logger.info(f"Exported {len(df):,} records ({file_size_mb:.1f} MB)")
        
    def export_summary_stats(self, df: pd.DataFrame, output_dir: str = 'backtesting'):
        """Generate summary statistics on the joined dataset."""
        logger.info("Generating summary statistics...")
        
        # Overall stats
        print("\n" + "="*80)
        print("FUNDAMENTAL-TRADE JOIN SUMMARY")
        print("="*80)
        print(f"Total records: {len(df):,}")
        print(f"Date range: {df['entry_date'].min()} to {df['entry_date'].max()}")
        print(f"Unique symbols: {df['symbol'].nunique():,}")
        print(f"Unique strategies: {df['strategy'].nunique()}")
        
        # Match rate by strategy
        print("\nMatch Rate by Strategy:")
        match_by_strategy = df.groupby('strategy').agg({
            'fiscal_date_ending': lambda x: x.notna().sum(),
            'symbol': 'count'
        }).rename(columns={'fiscal_date_ending': 'matched', 'symbol': 'total'})
        match_by_strategy['match_rate'] = (match_by_strategy['matched'] / match_by_strategy['total'] * 100).round(1)
        print(match_by_strategy.to_string())
        
        # Quality score distributions (for matched trades)
        matched_df = df[df['fiscal_date_ending'].notna()]
        if len(matched_df) > 0:
            print("\nQuality Score Distributions (Matched Trades):")
            quality_cols = ['overall_quality_score', 'balance_sheet_quality_score', 
                          'cash_flow_quality_score', 'income_statement_quality_score']
            print(matched_df[quality_cols].describe().round(2).to_string())
            
            # High quality trades
            high_quality_trades = matched_df[matched_df['is_high_quality'] == True]
            print(f"\nHigh Quality Trades: {len(high_quality_trades):,} ({len(high_quality_trades)/len(matched_df)*100:.1f}%)")
            
            # Investment grade trades
            investment_grade_trades = matched_df[matched_df['is_investment_grade'] == True]
            print(f"Investment Grade Trades: {len(investment_grade_trades):,} ({len(investment_grade_trades)/len(matched_df)*100:.1f}%)")
            
            # Red flag trades
            red_flag_trades = matched_df[matched_df['has_red_flags'] == True]
            print(f"Red Flag Trades: {len(red_flag_trades):,} ({len(red_flag_trades)/len(matched_df)*100:.1f}%)")
        
        print("="*80 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description='Join fundamental quality scores to backtest trades with publication lag'
    )
    parser.add_argument(
        '--input',
        type=str,
        default='backtesting/strategy_performance_report_trades.csv',
        help='Input CSV file with backtest trades'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='backtesting/trades_with_fundamentals.parquet',
        help='Output parquet file for enriched dataset'
    )
    parser.add_argument(
        '--regenerate',
        action='store_true',
        help='Regenerate trades from database instead of loading from CSV'
    )
    parser.add_argument(
        '--lag-days',
        type=int,
        default=45,
        help='Publication lag days to add to fiscal_date_ending (default: 45)'
    )
    parser.add_argument(
        '--window-days',
        type=int,
        default=90,
        help='Lookforward window days for matching trades (default: 90)'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date for trade filtering (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        help='End date for trade filtering (YYYY-MM-DD)'
    )
    
    args = parser.parse_args()
    
    # Initialize joiner
    logger.info("="*80)
    logger.info("FUNDAMENTAL-TRADE JOINER")
    logger.info("="*80)
    logger.info(f"Publication lag: {args.lag_days} days")
    logger.info(f"Lookforward window: {args.window_days} days")
    
    joiner = FundamentalTradeJoiner(
        publication_lag_days=args.lag_days,
        lookforward_window_days=args.window_days
    )
    
    # Load trades
    if args.regenerate:
        trades_df = joiner.load_trades_from_database(args.start_date, args.end_date)
    else:
        trades_df = joiner.load_trades_from_csv(args.input)
    
    # Load fundamentals
    fundamentals_df = joiner.load_fundamental_scores()
    
    # Join
    enriched_df = joiner.join_fundamentals_to_trades(trades_df, fundamentals_df)
    
    # Export
    joiner.export_to_parquet(enriched_df, args.output)
    
    # Summary stats
    joiner.export_summary_stats(enriched_df)
    
    logger.info("Processing complete!")


if __name__ == '__main__':
    main()
