"""Transform commodities data - Simplified append-only approach.

Commodities data is organized by date. This transformer:
1. Copies source.commodities → transforms.commodities (one-time init)
2. Adds feature columns with NULL values
3. Processes unprocessed records (WHERE processed_at IS NULL)
4. Future runs only process new dates

Usage:
    # One-time initialization
    python transform_commodities.py --init
    
    # Process unprocessed records (run after new data arrives)
    python transform_commodities.py --process
"""

import sys
import logging
import argparse
from pathlib import Path

import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CommoditiesTransformer:
    """Simplified commodities transformer with embedded watermark."""

    def __init__(self):
        self.db = PostgresDatabaseManager()

    def _safe_div(self, num, denom, epsilon=1e-6):
        """Safe division avoiding divide by zero."""
        return num / (denom + epsilon)

    def initialize(self):
        """Initialize transforms.commodities from source data."""
        logger.info("Initializing transforms.commodities...")
        
        self.db.connect()
        try:
            # Create table from source
            logger.info("Creating base table from source.commodities...")
            self.db.execute_query("DROP TABLE IF EXISTS transforms.commodities CASCADE")
            
            self.db.execute_query("""
                CREATE TABLE transforms.commodities AS
                SELECT 
                    commodity,
                    date,
                    value,
                    update_frequency,
                    load_date
                FROM raw.fred_commodities
                WHERE value IS NOT NULL
                  AND date IS NOT NULL
                ORDER BY commodity, date
            """)
            
            # Add primary key
            self.db.execute_query("""
                ALTER TABLE transforms.commodities 
                ADD PRIMARY KEY (commodity, date)
            """)
            
            # Add feature columns (all start as NULL)
            logger.info("Adding feature columns...")
            feature_cols = [
                # Price levels (normalized)
                'comm_price_zscore_21d', 'comm_price_zscore_63d', 'comm_price_zscore_252d',
                
                # Momentum (returns)
                'comm_return_1d', 'comm_return_5d', 'comm_return_21d', 'comm_return_63d',
                
                # Volatility
                'comm_volatility_21d', 'comm_volatility_63d',
                
                # Trend indicators
                'comm_ma5_ma21_ratio', 'comm_ma21_ma63_ratio',
                'comm_trend_slope_21d', 'comm_trend_slope_63d',
                
                # Relative strength
                'comm_rsi_14d',
                
                # Rankings (across commodities on same date)
                'comm_return_5d_rank', 'comm_return_21d_rank',
                'comm_volatility_21d_rank',
                
                # Flags
                'comm_sharp_move_flag', 'comm_high_volatility_flag'
            ]
            
            for col in feature_cols:
                col_type = 'INTEGER' if 'flag' in col else 'NUMERIC'
                self.db.execute_query(f"ALTER TABLE transforms.commodities ADD COLUMN {col} {col_type}")
            
            # Add processed timestamp
            self.db.execute_query("""
                ALTER TABLE transforms.commodities 
                ADD COLUMN processed_at TIMESTAMPTZ
            """)
            
            # Create index on unprocessed
            self.db.execute_query("""
                CREATE INDEX idx_comm_unprocessed 
                ON transforms.commodities (commodity, date) 
                WHERE processed_at IS NULL
            """)
            
            # Create index on date for time-series queries
            self.db.execute_query("""
                CREATE INDEX idx_comm_date 
                ON transforms.commodities (date DESC)
            """)
            
            count = self.db.fetch_query("SELECT COUNT(*) FROM transforms.commodities")[0][0]
            logger.info(f"✅ Initialized with {count:,} records")
            
        finally:
            self.db.close()

    def process(self):
        """Process unprocessed records."""
        logger.info("=" * 80)
        logger.info("PROCESSING UNPROCESSED COMMODITIES RECORDS")
        logger.info("=" * 80)
        
        self.db.connect()
        try:
            # Count unprocessed
            unprocessed = self.db.fetch_query("""
                SELECT COUNT(*) FROM transforms.commodities 
                WHERE processed_at IS NULL
            """)[0][0]
            
            if unprocessed == 0:
                logger.info("No unprocessed records")
                return
            
            logger.info(f"Found {unprocessed:,} unprocessed records")
            logger.info("Fetching data...")
            
            # Fetch all data for processing (need historical context for features)
            query = """
                SELECT 
                    commodity,
                    date,
                    value
                FROM transforms.commodities
                ORDER BY commodity, date
            """
            
            df = pd.read_sql(query, self.db.connection)
            logger.info(f"Loaded {len(df):,} total records for feature computation")
            
            # Compute features
            logger.info("Computing features...")
            df = self._compute_all_features(df)
            
            # Filter to only unprocessed records for update
            unprocessed_dates = pd.read_sql("""
                SELECT commodity, date
                FROM transforms.commodities
                WHERE processed_at IS NULL
            """, self.db.connection)
            
            df_update = df.merge(
                unprocessed_dates,
                on=['commodity', 'date'],
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
        
        # Sort by commodity and date
        df = df.sort_values(['commodity', 'date'])
        
        # Group by commodity for time-series features
        grouped = df.groupby('commodity')
        
        # Price normalization (z-scores using rolling windows)
        for window in [21, 63, 252]:
            df[f'comm_price_zscore_{window}d'] = grouped['value'].transform(
                lambda x: (x - x.rolling(window, min_periods=max(2, window//2)).mean()) / 
                         (x.rolling(window, min_periods=max(2, window//2)).std() + 1e-8)
            )
        
        # Returns (momentum)
        df['comm_return_1d'] = grouped['value'].pct_change(1, fill_method=None)
        df['comm_return_5d'] = grouped['value'].pct_change(5, fill_method=None)
        df['comm_return_21d'] = grouped['value'].pct_change(21, fill_method=None)
        df['comm_return_63d'] = grouped['value'].pct_change(63, fill_method=None)
        
        # Volatility (std of daily returns)
        df['comm_volatility_21d'] = grouped['comm_return_1d'].transform(
            lambda x: x.rolling(21, min_periods=10).std()
        )
        df['comm_volatility_63d'] = grouped['comm_return_1d'].transform(
            lambda x: x.rolling(63, min_periods=30).std()
        )
        
        # Moving averages and ratios
        ma5 = grouped['value'].transform(lambda x: x.rolling(5, min_periods=3).mean())
        ma21 = grouped['value'].transform(lambda x: x.rolling(21, min_periods=10).mean())
        ma63 = grouped['value'].transform(lambda x: x.rolling(63, min_periods=30).mean())
        
        df['comm_ma5_ma21_ratio'] = self._safe_div(ma5, ma21)
        df['comm_ma21_ma63_ratio'] = self._safe_div(ma21, ma63)
        
        # Trend slopes (linear regression over windows)
        for window in [21, 63]:
            df[f'comm_trend_slope_{window}d'] = grouped['value'].transform(
                lambda x: x.rolling(window, min_periods=max(2, window//2)).apply(
                    self._calculate_slope, raw=False
                )
            )
        
        # RSI (Relative Strength Index)
        df['comm_rsi_14d'] = grouped.apply(self._calculate_rsi).reset_index(level=0, drop=True)
        
        # Cross-commodity rankings (per date)
        df['comm_return_5d_rank'] = df.groupby('date')['comm_return_5d'].rank(pct=True)
        df['comm_return_21d_rank'] = df.groupby('date')['comm_return_21d'].rank(pct=True)
        df['comm_volatility_21d_rank'] = df.groupby('date')['comm_volatility_21d'].rank(pct=True)
        
        # Flags
        # Sharp move: >2 std deviation move
        df['comm_sharp_move_flag'] = (
            (df['comm_price_zscore_21d'].abs() > 2.0)
        ).astype(int)
        
        # High volatility: >75th percentile
        df['comm_high_volatility_flag'] = (
            df['comm_volatility_21d_rank'] > 0.75
        ).astype(int)
        
        return df

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
        """Calculate RSI (Relative Strength Index) for a commodity."""
        if 'comm_return_1d' not in group_df.columns:
            return pd.Series([50] * len(group_df), index=group_df.index)
        
        returns = group_df['comm_return_1d']
        
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
        feature_cols = [c for c in df.columns if c.startswith('comm_')]
        
        # Replace inf with None
        for col in feature_cols:
            if df[col].dtype in ['float64', 'int64']:
                df[col] = df[col].replace([np.inf, -np.inf], None)
        
        # Build update query
        set_clause = ', '.join([f"{col} = %s" for col in feature_cols])
        update_sql = f"""
            UPDATE transforms.commodities
            SET {set_clause}, processed_at = NOW()
            WHERE commodity = %s AND date = %s
        """
        
        # Prepare records
        records = []
        for _, row in df.iterrows():
            values = [None if pd.isna(row[col]) else row[col] for col in feature_cols]
            values.extend([row['commodity'], row['date']])
            records.append(tuple(values))
        
        # Execute batch
        self.db.execute_many(update_sql, records)
        logger.info(f"Updated {len(records):,} records")


def main():
    parser = argparse.ArgumentParser(description='Commodities transformer')
    parser.add_argument('--init', action='store_true', help='Initialize table')
    parser.add_argument('--process', action='store_true', help='Process unprocessed records')
    
    args = parser.parse_args()
    
    if not (args.init or args.process):
        parser.error("Must specify --init or --process")
    
    transformer = CommoditiesTransformer()
    
    if args.init:
        transformer.initialize()
    
    if args.process:
        transformer.process()


if __name__ == "__main__":
    main()
