#!/usr/bin/env python3
"""Transform Economic Indicators into ML-ready features.

Creates transforms.economic_indicators table with time-series features
from FRED economic indicators. Uses self-watermarking with processed_at column.

Features include:
- Z-score normalization (21d, 63d, 252d windows)
- Momentum (returns over 1d, 5d, 21d, 63d)
- Volatility (21d, 63d)
- Moving average ratios and trend slopes
- Yield curve spreads
- Cross-indicator relationships
- Rankings across indicators
"""

import sys
import logging
import argparse
from pathlib import Path

import numpy as np
import pandas as pd

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EconomicIndicatorsTransformer:
    """Transform economic indicators with self-watermarking."""
    
    def __init__(self):
        self.db = PostgresDatabaseManager()
    
    def initialize(self):
        """Initialize transforms.economic_indicators table."""
        logger.info("Initializing transforms.economic_indicators...")
        
        self.db.connect()
        try:
            # Drop and recreate table
            self.db.execute_query("DROP TABLE IF EXISTS transforms.economic_indicators")
            
            logger.info("Creating base table from raw.economic_indicators...")
            self.db.execute_query("""
                CREATE TABLE transforms.economic_indicators AS
                SELECT DISTINCT ON (function_name, date)
                    function_name AS indicator,
                    date,
                    value,
                    interval,
                    load_date
                FROM raw.economic_indicators
                WHERE value IS NOT NULL
                  AND date IS NOT NULL
                ORDER BY function_name, date, load_date DESC
            """)
            
            self.db.execute_query("""
                ALTER TABLE transforms.economic_indicators 
                ADD PRIMARY KEY (indicator, date)
            """)
            
            # Add feature columns
            logger.info("Adding feature columns...")
            feature_cols = [
                # Price/value normalization (z-scores)
                'econ_value_zscore_21d', 'econ_value_zscore_63d', 'econ_value_zscore_252d',
                
                # Momentum (returns/changes)
                'econ_return_1d', 'econ_return_5d', 'econ_return_21d', 'econ_return_63d',
                
                # Volatility
                'econ_volatility_21d', 'econ_volatility_63d',
                
                # Trend indicators
                'econ_ma5_ma21_ratio', 'econ_ma21_ma63_ratio',
                'econ_trend_slope_21d', 'econ_trend_slope_63d',
                
                # Momentum strength
                'econ_rsi_14d',
                
                # Rankings (cross-indicator comparisons on same date)
                'econ_return_5d_rank', 'econ_return_21d_rank',
                'econ_volatility_21d_rank',
                
                # Flags
                'econ_sharp_move_flag', 'econ_high_volatility_flag'
            ]
            
            for col in feature_cols:
                col_type = 'INTEGER' if 'flag' in col else 'REAL'
                self.db.execute_query(f"ALTER TABLE transforms.economic_indicators ADD COLUMN {col} {col_type}")
            
            # Add processed timestamp
            self.db.execute_query("""
                ALTER TABLE transforms.economic_indicators 
                ADD COLUMN processed_at TIMESTAMPTZ
            """)
            
            # Create indexes
            self.db.execute_query("""
                CREATE INDEX idx_econ_unprocessed 
                ON transforms.economic_indicators (indicator, date) 
                WHERE processed_at IS NULL
            """)
            
            self.db.execute_query("""
                CREATE INDEX idx_econ_date 
                ON transforms.economic_indicators (date DESC)
            """)
            
            count = self.db.fetch_query("SELECT COUNT(*) FROM transforms.economic_indicators")[0][0]
            logger.info(f"✅ Initialized with {count:,} records")
            
        finally:
            self.db.close()
    
    def process(self):
        """Process unprocessed records."""
        logger.info("=" * 80)
        logger.info("PROCESSING UNPROCESSED ECONOMIC INDICATORS")
        logger.info("=" * 80)
        
        self.db.connect()
        try:
            # Count unprocessed
            unprocessed = self.db.fetch_query("""
                SELECT COUNT(*) FROM transforms.economic_indicators 
                WHERE processed_at IS NULL
            """)[0][0]
            
            if unprocessed == 0:
                logger.info("No unprocessed records")
                return
            
            logger.info(f"Found {unprocessed:,} unprocessed records")
            logger.info("Fetching data...")
            
            # Fetch all data (need historical context)
            query = """
                SELECT 
                    indicator,
                    date,
                    value
                FROM transforms.economic_indicators
                ORDER BY indicator, date
            """
            
            df = pd.read_sql(query, self.db.connection)
            logger.info(f"Loaded {len(df):,} total records for feature computation")
            
            # Compute features
            logger.info("Computing features...")
            df = self._compute_all_features(df)
            
            # Filter to unprocessed records
            unprocessed_dates = pd.read_sql("""
                SELECT indicator, date
                FROM transforms.economic_indicators
                WHERE processed_at IS NULL
            """, self.db.connection)
            
            df_update = df.merge(
                unprocessed_dates,
                on=['indicator', 'date'],
                how='inner'
            )
            
            logger.info(f"Updating {len(df_update):,} unprocessed records...")
            
            # Update in batches
            self._batch_update(df_update)
            
            logger.info("=" * 80)
            logger.info(f"✅ Processed {len(df_update):,} records")
            logger.info("=" * 80)
            
        finally:
            self.db.close()
    
    def _compute_all_features(self, df):
        """Compute all features at once."""
        df = df.copy()
        
        # Sort by indicator and date
        df = df.sort_values(['indicator', 'date'])
        
        # Group by indicator for time-series features
        grouped = df.groupby('indicator')
        
        # Value normalization (z-scores using rolling windows)
        for window in [21, 63, 252]:
            df[f'econ_value_zscore_{window}d'] = grouped['value'].transform(
                lambda x: (x - x.rolling(window, min_periods=max(2, window//2)).mean()) / 
                         (x.rolling(window, min_periods=max(2, window//2)).std() + 1e-8)
            )
        
        # Returns (momentum)
        df['econ_return_1d'] = grouped['value'].pct_change(1, fill_method=None)
        df['econ_return_5d'] = grouped['value'].pct_change(5, fill_method=None)
        df['econ_return_21d'] = grouped['value'].pct_change(21, fill_method=None)
        df['econ_return_63d'] = grouped['value'].pct_change(63, fill_method=None)
        
        # Volatility (std of daily returns)
        df['econ_volatility_21d'] = grouped['econ_return_1d'].transform(
            lambda x: x.rolling(21, min_periods=10).std()
        )
        df['econ_volatility_63d'] = grouped['econ_return_1d'].transform(
            lambda x: x.rolling(63, min_periods=30).std()
        )
        
        # Moving averages and ratios
        ma5 = grouped['value'].transform(lambda x: x.rolling(5, min_periods=3).mean())
        ma21 = grouped['value'].transform(lambda x: x.rolling(21, min_periods=10).mean())
        ma63 = grouped['value'].transform(lambda x: x.rolling(63, min_periods=30).mean())
        
        df['econ_ma5_ma21_ratio'] = self._safe_div(ma5, ma21)
        df['econ_ma21_ma63_ratio'] = self._safe_div(ma21, ma63)
        
        # Trend slopes (linear regression over windows)
        for window in [21, 63]:
            df[f'econ_trend_slope_{window}d'] = grouped['value'].transform(
                lambda x: x.rolling(window, min_periods=max(2, window//2)).apply(
                    self._calculate_slope, raw=False
                )
            )
        
        # RSI (Relative Strength Index)
        df['econ_rsi_14d'] = grouped.apply(self._calculate_rsi).reset_index(level=0, drop=True)
        
        # Cross-indicator rankings (per date)
        df['econ_return_5d_rank'] = df.groupby('date')['econ_return_5d'].rank(pct=True)
        df['econ_return_21d_rank'] = df.groupby('date')['econ_return_21d'].rank(pct=True)
        df['econ_volatility_21d_rank'] = df.groupby('date')['econ_volatility_21d'].rank(pct=True)
        
        # Flags
        # Sharp move: >2 std deviation move
        df['econ_sharp_move_flag'] = (
            df['econ_value_zscore_21d'].abs() > 2
        ).astype(int)
        
        # High volatility: top quartile volatility
        df['econ_high_volatility_flag'] = (
            df['econ_volatility_21d_rank'] > 0.75
        ).astype(int)
        
        return df
    
    def _safe_div(self, num, denom, epsilon=1e-6):
        """Safely divide with small epsilon."""
        return num / (denom + epsilon)
    
    def _calculate_slope(self, series):
        """Calculate linear regression slope."""
        if len(series) < 2 or series.isna().all():
            return np.nan
        
        clean_series = series.dropna()
        if len(clean_series) < 2:
            return np.nan
        
        x = np.arange(len(clean_series))
        y = clean_series.values
        
        # Linear regression slope
        slope = np.polyfit(x, y, 1)[0]
        return slope
    
    def _calculate_rsi(self, group_df):
        """Calculate RSI (Relative Strength Index) for an indicator."""
        if 'econ_return_1d' not in group_df.columns:
            return pd.Series([50] * len(group_df), index=group_df.index)
        
        returns = group_df['econ_return_1d']
        
        # Separate gains and losses
        gains = returns.where(returns > 0, 0)
        losses = -returns.where(returns < 0, 0)
        
        # Calculate rolling averages
        avg_gains = gains.rolling(14, min_periods=7).mean()
        avg_losses = losses.rolling(14, min_periods=7).mean()
        
        # Calculate RS and RSI
        rs = self._safe_div(avg_gains, avg_losses)
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def _batch_update(self, df):
        """Update transforms table in batches."""
        feature_cols = [c for c in df.columns if c.startswith('econ_')]
        
        # Replace inf with None
        for col in feature_cols:
            if df[col].dtype in ['float64', 'int64']:
                df[col] = df[col].replace([np.inf, -np.inf], None)
        
        # Build update query
        set_clause = ', '.join([f"{col} = %s" for col in feature_cols])
        update_sql = f"""
            UPDATE transforms.economic_indicators
            SET {set_clause}, processed_at = NOW()
            WHERE indicator = %s AND date = %s
        """
        
        # Prepare records
        records = []
        for _, row in df.iterrows():
            values = [None if pd.isna(row[col]) else row[col] for col in feature_cols]
            values.extend([row['indicator'], row['date']])
            records.append(tuple(values))
        
        # Execute batch update
        self.db.execute_many(update_sql, records)
        logger.info(f"Updated {len(records)} records")


def main():
    parser = argparse.ArgumentParser(description='Transform economic indicators')
    parser.add_argument('--init', action='store_true', help='Initialize table')
    parser.add_argument('--process', action='store_true', help='Process unprocessed records')
    
    args = parser.parse_args()
    
    transformer = EconomicIndicatorsTransformer()
    
    if args.init:
        transformer.initialize()
    elif args.process:
        transformer.process()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
