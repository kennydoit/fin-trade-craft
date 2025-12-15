"""
Transform Time Series Daily Adjusted Data with Watermark Integration

This module transforms raw time series data into ML-ready features with comprehensive
technical indicators for swing trading. Features watermark-based incremental processing.

Features Generated:
- Trend: SMA, EMA, crossovers, ratios
- Momentum: RSI, MACD, ROC, Williams %R
- Volatility: ATR, Bollinger Bands
- Volume: OBV, CMF, AD, volume ratios
- Targets: Forward returns, directions (binary/ternary)

Usage:
    # Full mode (recreate table)
    python transform_time_series_daily_adjusted.py --mode full
    
    # Incremental mode (watermark-based)
    python transform_time_series_daily_adjusted.py --mode incremental --staleness-hours 168
    
    # Initialize transformation group
    python transform_time_series_daily_adjusted.py --init-group time_series_daily_adjusted
    
    # Show watermark summary
    python transform_time_series_daily_adjusted.py --show-summary
"""

import sys
import logging
import argparse
from datetime import datetime
from pathlib import Path
import yaml
from multiprocessing import Pool, cpu_count
from functools import partial

import pandas as pd
import numpy as np
import pandas_ta as ta

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from db.postgres_database_manager import PostgresDatabaseManager
from transforms.transformation_watermark_manager import TransformationWatermarkManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def _process_symbol_worker(symbol_data, config, mode='full'):
    """
    Worker function for parallel processing of symbols.
    
    This function is defined at module level to be picklable for multiprocessing.
    Each worker creates its own database connection to avoid conflicts.
    
    Args:
        symbol_data (dict): Symbol information from watermark table
        config (dict): Configuration dictionary
        mode (str): Processing mode ('full' or 'incremental')
        
    Returns:
        dict: Result with success status, records loaded, and symbol info
    """
    symbol_id = symbol_data['symbol_id']
    symbol = symbol_data['symbol']
    
    try:
        # Create transformer instance (each worker gets its own DB connection)
        transformer = TimeSeriesDailyAdjustedTransformer.__new__(TimeSeriesDailyAdjustedTransformer)
        transformer.db = PostgresDatabaseManager()
        transformer.config = config
        transformer.transformation_group = 'time_series_daily_adjusted'
        
        # Load configuration parameters
        transformer.rolling_window = config.get('rolling_window', 8)
        transformer.ma_periods = config.get('ma_periods', [5, 10, 20, 50])
        transformer.ema_periods = config.get('ema_periods', [8, 21, 34, 55])
        transformer.rsi_periods = config.get('rsi_periods', [7, 14])
        transformer.atr_periods = config.get('atr_periods', [10, 14])
        transformer.target_horizons = config.get('target_horizons', [5, 10, 20, 30, 40])
        
        # Connect to database
        transformer.db.connect()
        
        # Transform and load
        result = transformer.transform_and_load(symbol_id, symbol, mode=mode)
        
        # Close connection
        transformer.db.close()
        
        return result
        
    except Exception as e:
        logger.error(f"Worker error processing {symbol}: {e}")
        return {
            'symbol': symbol,
            'symbol_id': symbol_id,
            'success': False,
            'records_loaded': 0,
            'error': str(e)
        }


class TimeSeriesDailyAdjustedTransformer:
    """Transform time series data with watermark-based incremental processing."""
    
    def __init__(self, config_path=None):
        """
        Initialize transformer with database connections and configuration.
        
        Args:
            config_path (str, optional): Path to YAML configuration file
        """
        self.db = PostgresDatabaseManager()
        self.watermark_mgr = TransformationWatermarkManager()
        self.transformation_group = 'time_series_daily_adjusted'
        
        # Load configuration
        self.config = self._load_config(config_path)
        
        # Feature parameters from config
        self.rolling_window = self.config.get('rolling_window', 8)
        self.ma_periods = self.config.get('ma_periods', [5, 10, 20, 50])
        self.ema_periods = self.config.get('ema_periods', [8, 21, 34, 55])
        self.rsi_periods = self.config.get('rsi_periods', [7, 14])
        self.atr_periods = self.config.get('atr_periods', [10, 14])
        self.target_horizons = self.config.get('target_horizons', [5, 10, 20, 30, 40])
        
    def _load_config(self, config_path):
        """Load configuration from YAML file or return defaults."""
        if config_path and Path(config_path).exists():
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        
        # Default configuration
        return {
            'rolling_window': 8,
            'ma_periods': [5, 10, 20, 50],
            'ema_periods': [8, 21, 34, 55],
            'rsi_periods': [7, 14],
            'atr_periods': [10, 14],
            'target_horizons': [5, 10, 20, 30, 40]
        }
    
    @staticmethod
    def safe_divide(numerator, denominator, fillvalue=np.nan):
        """Safely divide two series, handling division by zero."""
        result = numerator / denominator
        if isinstance(result, pd.Series):
            result.replace([np.inf, -np.inf], fillvalue, inplace=True)
        return result
    
    def create_trend_features(self, df):
        """
        Create trend-based technical indicators.
        
        Features:
        - Simple Moving Averages with price ratios
        - Exponential Moving Averages with price ratios
        - EMA crossover signals
        
        Args:
            df (pd.DataFrame): Input dataframe with OHLCV data
            
        Returns:
            pd.DataFrame: DataFrame with added trend features
        """
        logger.info('Creating trend features...')
        
        for symbol in df['symbol'].unique():
            mask = df['symbol'] == symbol
            symbol_data = df.loc[mask].copy().sort_values('date')
            
            # Simple Moving Averages
            for period in self.ma_periods:
                sma = symbol_data['close'].rolling(period).mean()
                df.loc[mask, f'ohlcv_sma_{period}'] = sma
                df.loc[mask, f'ohlcv_sma_{period}_ratio'] = self.safe_divide(
                    symbol_data['close'], sma
                )
            
            # Exponential Moving Averages
            for period in self.ema_periods:
                ema = symbol_data['close'].ewm(span=period).mean()
                df.loc[mask, f'ohlcv_ema_{period}'] = ema
                df.loc[mask, f'ohlcv_ema_{period}_ratio'] = self.safe_divide(
                    symbol_data['close'], ema
                )
            
            # EMA crossovers (8/21 popular for swing trading)
            ema8 = symbol_data['close'].ewm(span=8).mean()
            ema21 = symbol_data['close'].ewm(span=21).mean()
            df.loc[mask, 'ohlcv_ema_8_21_cross'] = (ema8 > ema21).astype(int)
            df.loc[mask, 'ohlcv_ema_8_21_ratio'] = self.safe_divide(ema8, ema21)
            
        return df
    
    def create_momentum_features(self, df):
        """
        Create momentum-based technical indicators.
        
        Features:
        - Relative Strength Index (RSI) with overbought/oversold signals
        - MACD with signal line and histogram
        - Rate of Change (ROC)
        - Williams %R
        
        Args:
            df (pd.DataFrame): Input dataframe with OHLCV data
            
        Returns:
            pd.DataFrame: DataFrame with added momentum features
        """
        logger.info('Creating momentum features...')
        
        for symbol in df['symbol'].unique():
            mask = df['symbol'] == symbol
            symbol_data = df.loc[mask].copy().sort_values('date')
            
            # RSI with overbought/oversold signals
            for period in self.rsi_periods:
                rsi_values = ta.rsi(symbol_data['close'], length=period)
                df.loc[mask, f'ohlcv_rsi_{period}'] = rsi_values
                df.loc[mask, f'ohlcv_rsi_{period}_oversold'] = (rsi_values < 30).astype(int)
                df.loc[mask, f'ohlcv_rsi_{period}_overbought'] = (rsi_values > 70).astype(int)
            
            # MACD (12,26,9 - standard parameters)
            try:
                macd = ta.macd(symbol_data['close'], fast=12, slow=26, signal=9)
                if not macd.empty:
                    df.loc[mask, 'ohlcv_macd'] = macd['MACD_12_26_9']
                    df.loc[mask, 'ohlcv_macd_signal'] = macd['MACDs_12_26_9']
                    df.loc[mask, 'ohlcv_macd_histogram'] = macd['MACDh_12_26_9']
                    df.loc[mask, 'ohlcv_macd_bullish'] = (
                        macd['MACD_12_26_9'] > macd['MACDs_12_26_9']
                    ).astype(int)
            except Exception as e:
                logger.warning(f"MACD calculation failed for {symbol}: {e}")
            
            # Rate of Change
            df.loc[mask, 'ohlcv_roc_10'] = ta.roc(symbol_data['close'], length=10)
            df.loc[mask, 'ohlcv_roc_20'] = ta.roc(symbol_data['close'], length=20)
            
            # Williams %R
            df.loc[mask, 'ohlcv_willr_14'] = ta.willr(
                symbol_data['high'], symbol_data['low'], 
                symbol_data['close'], length=14
            )
            
        return df
    
    def create_volatility_features(self, df):
        """
        Create volatility-based technical indicators.
        
        Features:
        - Average True Range (ATR) and percentage
        - Bollinger Bands with position and width
        
        Args:
            df (pd.DataFrame): Input dataframe with OHLCV data
            
        Returns:
            pd.DataFrame: DataFrame with added volatility features
        """
        logger.info('Creating volatility features...')
        
        for symbol in df['symbol'].unique():
            mask = df['symbol'] == symbol
            symbol_data = df.loc[mask].copy().sort_values('date')
            
            # Average True Range
            for period in self.atr_periods:
                try:
                    atr = ta.atr(
                        symbol_data['high'], symbol_data['low'], 
                        symbol_data['close'], length=period
                    )
                    df.loc[mask, f'ohlcv_atr_{period}'] = atr
                    df.loc[mask, f'ohlcv_atr_{period}_pct'] = self.safe_divide(
                        atr, symbol_data['close']
                    ) * 100
                except Exception as e:
                    logger.warning(f"ATR calculation failed for {symbol}: {e}")
                    
            # Bollinger Bands (20,2 - standard parameters)
            try:
                bb = ta.bbands(symbol_data['close'], length=20, std=2)
                if not bb.empty:
                    df.loc[mask, 'ohlcv_bb_upper'] = bb['BBU_20_2.0']
                    df.loc[mask, 'ohlcv_bb_middle'] = bb['BBM_20_2.0']
                    df.loc[mask, 'ohlcv_bb_lower'] = bb['BBL_20_2.0']
                    df.loc[mask, 'ohlcv_bb_width'] = (
                        bb['BBU_20_2.0'] - bb['BBL_20_2.0']
                    ) / bb['BBM_20_2.0']
                    df.loc[mask, 'ohlcv_bb_position'] = (
                        symbol_data['close'] - bb['BBL_20_2.0']
                    ) / (bb['BBU_20_2.0'] - bb['BBL_20_2.0'])
            except Exception as e:
                logger.warning(f"Bollinger Bands calculation failed for {symbol}: {e}")
                
        return df
    
    def create_volume_features(self, df):
        """
        Create volume-based technical indicators.
        
        Features:
        - On-Balance Volume (OBV)
        - Chaikin Money Flow (CMF)
        - Accumulation/Distribution Line (AD)
        - Volume moving averages and ratios
        
        Args:
            df (pd.DataFrame): Input dataframe with OHLCV data
            
        Returns:
            pd.DataFrame: DataFrame with added volume features
        """
        logger.info('Creating volume features...')
        
        for symbol in df['symbol'].unique():
            mask = df['symbol'] == symbol
            symbol_data = df.loc[mask].copy().sort_values('date')
            
            # On-Balance Volume
            df.loc[mask, 'ohlcv_obv'] = ta.obv(symbol_data['close'], symbol_data['volume'])
            
            # Chaikin Money Flow
            df.loc[mask, 'ohlcv_cmf'] = ta.cmf(
                symbol_data['high'], symbol_data['low'], 
                symbol_data['close'], symbol_data['volume'], length=20
            )
            
            # Accumulation/Distribution Line
            df.loc[mask, 'ohlcv_ad'] = ta.ad(
                symbol_data['high'], symbol_data['low'], 
                symbol_data['close'], symbol_data['volume']
            )
            
            # Volume Moving Averages
            df.loc[mask, 'ohlcv_volume_sma_20'] = symbol_data['volume'].rolling(20).mean()
            df.loc[mask, 'ohlcv_volume_sma_50'] = symbol_data['volume'].rolling(50).mean()
            df.loc[mask, 'ohlcv_volume_ratio'] = self.safe_divide(
                symbol_data['volume'], 
                symbol_data['volume'].rolling(20).mean()
            )
            
        return df
    
    def create_target_variables(self, df):
        """
        Create forward-looking target variables for multiple time horizons.
        
        Features:
        - Percentage returns
        - Log returns
        - Binary direction (up/down)
        - Ternary classification (Up >2%, Flat ±2%, Down <-2%)
        
        Args:
            df (pd.DataFrame): Input dataframe with OHLCV data
            
        Returns:
            pd.DataFrame: DataFrame with added target variables
        """
        logger.info('Creating target variables...')
        
        for symbol in df['symbol'].unique():
            mask = df['symbol'] == symbol
            symbol_data = df.loc[mask].copy().sort_values('date')
            
            # Forward returns for different horizons
            for horizon in self.target_horizons:
                # Percentage returns
                future_close = symbol_data['close'].shift(-horizon)
                pct_return = self.safe_divide(
                    future_close - symbol_data['close'], 
                    symbol_data['close']
                )
                df.loc[mask, f'target_return_{horizon}d'] = pct_return
                
                # Log returns (handle zero/negative prices safely)
                log_return = np.log(self.safe_divide(
                    future_close, symbol_data['close'], fillvalue=1
                ))
                df.loc[mask, f'target_log_return_{horizon}d'] = log_return
                
                # Directional targets (binary classification)
                df.loc[mask, f'target_direction_{horizon}d'] = (pct_return > 0).astype(int)
                
                # Ternary classification (Down <-2%, Flat ±2%, Up >2%)
                ternary = pd.cut(
                    pct_return, 
                    bins=[-np.inf, -0.02, 0.02, np.inf], 
                    labels=[0, 1, 2]  # Down, Flat, Up
                )
                df.loc[mask, f'target_ternary_{horizon}d'] = ternary.astype('Int64')
                
        return df
    
    def transform_symbol(self, symbol_id, symbol):
        """
        Transform time series data for a single symbol.
        
        Args:
            symbol_id (int): Symbol ID from watermark table (will be converted to string for raw table)
            symbol (str): Symbol ticker
            
        Returns:
            pd.DataFrame: Transformed data with features, or None if failed
        """
        try:
            # Convert symbol_id to string for raw table comparison (raw.symbol_id is TEXT)
            symbol_id_str = str(symbol_id)
            
            # Fetch time series data for symbol (last 250 periods for efficiency)
            # 250 periods covers: 55 EMA lookback + 40 forward targets + 155 buffer
            query = """
                SELECT symbol_id, symbol, date, open, high, low, 
                       close, adjusted_close, volume
                FROM (
                    SELECT symbol_id, symbol, date, open, high, low, 
                           close, adjusted_close, volume
                    FROM raw.time_series_daily_adjusted
                    WHERE symbol_id = %s
                    ORDER BY date DESC
                    LIMIT 250
                ) subq
                ORDER BY date ASC
            """
            
            df = pd.read_sql(query, self.db.connection, params=(symbol_id_str,))
            
            if df.empty:
                logger.warning(f"No data found for {symbol} (ID: {symbol_id})")
                return None
            
            # Ensure date column is datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # Convert symbol_id back to integer for transforms table
            df['symbol_id'] = symbol_id  # Use the integer from watermark table
            
            # Create comprehensive features
            df = self.create_trend_features(df)
            df = self.create_momentum_features(df)
            df = self.create_volatility_features(df)
            df = self.create_volume_features(df)
            df = self.create_target_variables(df)
            
            # Prepare final dataframe
            feature_columns = [col for col in df.columns if col.startswith(('ohlcv_', 'target_'))]
            final_df = df[['symbol_id', 'symbol', 'date'] + feature_columns].copy()
            final_df['created_at'] = pd.Timestamp.now()
            final_df['updated_at'] = pd.Timestamp.now()
            
            # Replace pandas NA with None for database insertion
            final_df = final_df.replace({pd.NA: None})
            
            return final_df
            
        except Exception as e:
            logger.error(f"Error transforming {symbol}: {e}")
            return None
    
    def transform_and_load(self, symbol_id, symbol, mode='full'):
        """
        Transform and load data for a single symbol.
        
        Args:
            symbol_id (str): Symbol ID (TEXT type)
            symbol (str): Symbol ticker
            mode (str): 'full' to replace, 'incremental' to append
            
        Returns:
            dict: Statistics about the transformation
        """
        try:
            transformed_df = self.transform_symbol(symbol_id, symbol)
            
            if transformed_df is None or transformed_df.empty:
                return {
                    'symbol': symbol,
                    'symbol_id': symbol_id,
                    'success': False,
                    'records_loaded': 0,
                    'error': 'No data to transform'
                }
            
            # Load to database
            # Always delete existing records for this symbol before inserting
            # (Since we're only fetching last 250 periods, we replace them all)
            delete_query = """
                DELETE FROM transforms.time_series_daily_adjusted
                WHERE symbol_id = %s
            """
            self.db.execute_query(delete_query, (symbol_id,))
            
            # Insert transformed data
            insert_query = """
                INSERT INTO transforms.time_series_daily_adjusted 
                (symbol_id, symbol, date, {columns}, created_at, updated_at)
                VALUES ({placeholders})
            """
            
            feature_columns = [col for col in transformed_df.columns 
                             if col.startswith(('ohlcv_', 'target_'))]
            columns_str = ', '.join(feature_columns)
            placeholders = ', '.join(['%s'] * (len(feature_columns) + 5))
            
            insert_query = insert_query.format(
                columns=columns_str,
                placeholders=placeholders
            )
            
            records = []
            for _, row in transformed_df.iterrows():
                record = [
                    row['symbol_id'], row['symbol'], row['date']
                ] + [row[col] for col in feature_columns] + [
                    row['created_at'], row['updated_at']
                ]
                records.append(tuple(record))
            
            self.db.execute_many(insert_query, records)
            
            logger.info(f"Loaded {len(records)} records for {symbol}")
            
            return {
                'symbol': symbol,
                'symbol_id': symbol_id,
                'success': True,
                'records_loaded': len(records),
                'error': None
            }
            
        except Exception as e:
            logger.error(f"Error in transform_and_load for {symbol}: {e}")
            return {
                'symbol': symbol,
                'symbol_id': symbol_id,
                'success': False,
                'records_loaded': 0,
                'error': str(e)
            }
    
    def create_transforms_table(self):
        """Create the transforms.time_series_daily_adjusted table if it doesn't exist."""
        try:
            self.db.connect()
            
            # Generate dynamic column definitions for all features
            feature_cols = []
            
            # Trend features
            for period in self.ma_periods:
                feature_cols.extend([
                    f'ohlcv_sma_{period} NUMERIC(20, 6)',
                    f'ohlcv_sma_{period}_ratio NUMERIC(20, 6)'
                ])
            for period in self.ema_periods:
                feature_cols.extend([
                    f'ohlcv_ema_{period} NUMERIC(20, 6)',
                    f'ohlcv_ema_{period}_ratio NUMERIC(20, 6)'
                ])
            feature_cols.extend([
                'ohlcv_ema_8_21_cross INTEGER',
                'ohlcv_ema_8_21_ratio NUMERIC(20, 6)'
            ])
            
            # Momentum features
            for period in self.rsi_periods:
                feature_cols.extend([
                    f'ohlcv_rsi_{period} NUMERIC(20, 6)',
                    f'ohlcv_rsi_{period}_oversold INTEGER',
                    f'ohlcv_rsi_{period}_overbought INTEGER'
                ])
            feature_cols.extend([
                'ohlcv_macd NUMERIC(20, 6)',
                'ohlcv_macd_signal NUMERIC(20, 6)',
                'ohlcv_macd_histogram NUMERIC(20, 6)',
                'ohlcv_macd_bullish INTEGER',
                'ohlcv_roc_10 NUMERIC(20, 6)',
                'ohlcv_roc_20 NUMERIC(20, 6)',
                'ohlcv_willr_14 NUMERIC(20, 6)'
            ])
            
            # Volatility features
            for period in self.atr_periods:
                feature_cols.extend([
                    f'ohlcv_atr_{period} NUMERIC(20, 6)',
                    f'ohlcv_atr_{period}_pct NUMERIC(20, 6)'
                ])
            feature_cols.extend([
                'ohlcv_bb_upper NUMERIC(20, 6)',
                'ohlcv_bb_middle NUMERIC(20, 6)',
                'ohlcv_bb_lower NUMERIC(20, 6)',
                'ohlcv_bb_width NUMERIC(20, 6)',
                'ohlcv_bb_position NUMERIC(20, 6)'
            ])
            
            # Volume features
            feature_cols.extend([
                'ohlcv_obv NUMERIC(20, 6)',
                'ohlcv_cmf NUMERIC(20, 6)',
                'ohlcv_ad NUMERIC(20, 6)',
                'ohlcv_volume_sma_20 NUMERIC(20, 6)',
                'ohlcv_volume_sma_50 NUMERIC(20, 6)',
                'ohlcv_volume_ratio NUMERIC(20, 6)'
            ])
            
            # Target variables
            for horizon in self.target_horizons:
                feature_cols.extend([
                    f'target_return_{horizon}d NUMERIC(20, 6)',
                    f'target_log_return_{horizon}d NUMERIC(20, 6)',
                    f'target_direction_{horizon}d INTEGER',
                    f'target_ternary_{horizon}d INTEGER'
                ])
            
            feature_cols_str = ',\n    '.join(feature_cols)
            
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS transforms.time_series_daily_adjusted (
                    id SERIAL PRIMARY KEY,
                    symbol_id INTEGER NOT NULL,
                    symbol VARCHAR(10) NOT NULL,
                    date DATE NOT NULL,
                    {feature_cols_str},
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (symbol_id, date)
                );
                
                CREATE INDEX IF NOT EXISTS idx_ts_daily_adj_symbol_id 
                    ON transforms.time_series_daily_adjusted(symbol_id);
                CREATE INDEX IF NOT EXISTS idx_ts_daily_adj_symbol 
                    ON transforms.time_series_daily_adjusted(symbol);
                CREATE INDEX IF NOT EXISTS idx_ts_daily_adj_date 
                    ON transforms.time_series_daily_adjusted(date);
            """
            
            self.db.execute_query(create_table_sql)
            logger.info("Created transforms.time_series_daily_adjusted table")
            
        except Exception as e:
            logger.error(f"Error creating transforms table: {e}")
            raise
    
    def run_full_mode(self, workers=None):
        """Run transformation in full mode with optional parallel processing."""
        logger.info("=" * 80)
        logger.info("FULL MODE: Recreating transforms.time_series_daily_adjusted table")
        logger.info("=" * 80)
        
        try:
            # Drop and recreate table
            self.db.connect()
            self.db.execute_query("DROP TABLE IF EXISTS transforms.time_series_daily_adjusted CASCADE")
            self.create_transforms_table()
            
            # Get all symbols from watermark table
            symbols = self.watermark_mgr.get_symbols_needing_transformation(
                self.transformation_group,
                staleness_hours=999999  # Process all
            )
            
            if not symbols:
                logger.warning("No symbols found in watermark table")
                return
            
            # Determine number of workers
            if workers is None:
                workers = max(1, cpu_count() - 1)
            
            logger.info(f"Processing {len(symbols)} symbols with {workers} workers")
            
            # Process symbols
            if workers > 1:
                total_records, success_count, failed_symbols = self._process_parallel(
                    symbols, mode='full', workers=workers
                )
            else:
                total_records, success_count, failed_symbols = self._process_sequential(
                    symbols, mode='full'
                )
            
            # Bulk update watermarks
            logger.info("Updating watermarks...")
            self.update_all_watermarks()
            
            # Summary
            logger.info("=" * 80)
            logger.info("FULL MODE SUMMARY")
            logger.info("=" * 80)
            logger.info(f"Total symbols processed: {len(symbols)}")
            logger.info(f"Successful: {success_count}")
            logger.info(f"Failed: {len(failed_symbols)}")
            logger.info(f"Total records loaded: {total_records:,}")
            
            if failed_symbols:
                logger.warning(f"Failed symbols: {', '.join(failed_symbols[:10])}")
                if len(failed_symbols) > 10:
                    logger.warning(f"... and {len(failed_symbols) - 10} more")
            
        except Exception as e:
            logger.error(f"Error in full mode: {e}")
            raise
        finally:
            self.db.close()
    
    def _process_sequential(self, symbols, mode='full'):
        """
        Process symbols sequentially (single-threaded).
        
        Args:
            symbols (list): List of symbol data dictionaries
            mode (str): Processing mode ('full' or 'incremental')
            
        Returns:
            tuple: (total_records, success_count, failed_symbols)
        """
        total_records = 0
        success_count = 0
        failed_symbols = []
        
        for idx, symbol_data in enumerate(symbols, 1):
            symbol_id = symbol_data['symbol_id']
            symbol = symbol_data['symbol']
            logger.info(f"[{idx}/{len(symbols)}] Processing {symbol} (ID: {symbol_id})")
            
            result = self.transform_and_load(symbol_id, symbol, mode=mode)
            
            if result['success']:
                success_count += 1
                total_records += result['records_loaded']
            else:
                failed_symbols.append(symbol)
        
        return total_records, success_count, failed_symbols
    
    def _process_parallel(self, symbols, mode='full', workers=4):
        """
        Process symbols in parallel using multiprocessing.
        
        Args:
            symbols (list): List of symbol data dictionaries
            mode (str): Processing mode ('full' or 'incremental')
            workers (int): Number of parallel workers
            
        Returns:
            tuple: (total_records, success_count, failed_symbols)
        """
        # Create worker function with bound parameters
        worker_func = partial(
            _process_symbol_worker,
            config=self.config,
            mode=mode
        )
        
        # Process in parallel
        total_records = 0
        success_count = 0
        failed_symbols = []
        
        with Pool(processes=workers) as pool:
            # Process symbols and collect results
            for idx, result in enumerate(pool.imap_unordered(worker_func, symbols), 1):
                if idx % 50 == 0 or idx == len(symbols):
                    logger.info(f"Progress: {idx}/{len(symbols)} symbols processed")
                
                if result['success']:
                    success_count += 1
                    total_records += result['records_loaded']
                else:
                    failed_symbols.append(result['symbol'])
        
        return total_records, success_count, failed_symbols
    
    def run_incremental_mode(self, staleness_hours=168, workers=None):
        """
        Run transformation in incremental mode (watermark-based).
        
        Args:
            staleness_hours (int): Hours after which to re-process symbols
            workers (int, optional): Number of parallel workers. Defaults to CPU count - 1.
        """
        logger.info("=" * 80)
        logger.info(f"INCREMENTAL MODE: Processing stale symbols (>{staleness_hours}h)")
        logger.info("=" * 80)
        
        try:
            self.db.connect()
            
            # Ensure transforms table exists
            self.create_transforms_table()
            
            # Get symbols needing transformation
            symbols = self.watermark_mgr.get_symbols_needing_transformation(
                self.transformation_group,
                staleness_hours=staleness_hours
            )
            
            if not symbols:
                logger.info("No symbols need transformation")
                return
            
            # Determine number of workers
            if workers is None:
                workers = max(1, cpu_count() - 1)
            
            logger.info(f"Processing {len(symbols)} symbols in incremental mode with {workers} workers")
            
            # Process symbols in parallel
            if workers > 1:
                total_records, success_count, failed_symbols = self._process_parallel(
                    symbols, mode='full', workers=workers
                )
            else:
                # Single-threaded fallback
                total_records, success_count, failed_symbols = self._process_sequential(
                    symbols, mode='full'
                )
            
            # Bulk update watermarks
            logger.info("Updating watermarks...")
            self.update_all_watermarks()
            
            # Summary
            logger.info("=" * 80)
            logger.info("INCREMENTAL MODE SUMMARY")
            logger.info("=" * 80)
            logger.info(f"Symbols processed: {len(symbols)}")
            logger.info(f"Successful: {success_count}")
            logger.info(f"Failed: {len(failed_symbols)}")
            logger.info(f"Total records loaded: {total_records:,}")
            
            if failed_symbols:
                logger.warning(f"Failed symbols: {', '.join(failed_symbols[:10])}")
                if len(failed_symbols) > 10:
                    logger.warning(f"... and {len(failed_symbols) - 10} more")
            
        except Exception as e:
            logger.error(f"Error in incremental mode: {e}")
            raise
        finally:
            self.db.close()
    
    def update_all_watermarks(self):
        """Bulk update watermarks from transformed data."""
        try:
            self.db.connect()
            
            # Bulk update using subquery
            update_query = """
                UPDATE transforms.transformation_watermarks w
                SET 
                    first_date_processed = t.min_date,
                    last_date_processed = t.max_date,
                    last_successful_run = CURRENT_TIMESTAMP,
                    last_run_status = 'success',
                    consecutive_failures = 0,
                    updated_at = CURRENT_TIMESTAMP
                FROM (
                    SELECT 
                        symbol_id,
                        MIN(date) as min_date,
                        MAX(date) as max_date
                    FROM transforms.time_series_daily_adjusted
                    GROUP BY symbol_id
                ) t
                WHERE w.symbol_id = t.symbol_id
                  AND w.transformation_group = %s
            """
            
            self.db.execute_query(update_query, (self.transformation_group,))
            logger.info("Watermarks updated successfully")
            
            # Update listing_status to 'DEL' for Inactive symbols
            del_update_query = """
                UPDATE transforms.transformation_watermarks w
                SET listing_status = 'DEL'
                FROM raw.etl_watermarks e
                WHERE w.symbol_id = e.symbol_id
                  AND w.transformation_group = %s
                  AND e.status = 'Inactive'
            """
            
            self.db.execute_query(del_update_query, (self.transformation_group,))
            
            # Count how many were marked as DEL
            count_result = self.db.fetch_query("""
                SELECT COUNT(*) 
                FROM transforms.transformation_watermarks 
                WHERE transformation_group = %s 
                  AND listing_status = 'DEL'
            """, (self.transformation_group,))
            
            del_count = count_result[0][0] if count_result else 0
            if del_count > 0:
                logger.info(f"Marked {del_count:,} Inactive symbols as 'DEL'")
            
        except Exception as e:
            logger.error(f"Error updating watermarks: {e}")
            raise
        finally:
            self.db.close()


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description='Transform time series data with watermark integration',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Initialize transformation group
  python transform_time_series_daily_adjusted.py --init-group time_series_daily_adjusted
  
  # Full mode (recreate table)
  python transform_time_series_daily_adjusted.py --mode full
  
  # Incremental mode (1 week staleness)
  python transform_time_series_daily_adjusted.py --mode incremental --staleness-hours 168
  
  # Show watermark summary
  python transform_time_series_daily_adjusted.py --show-summary
        """
    )
    
    parser.add_argument('--mode', choices=['full', 'incremental'], 
                       help='Transformation mode')
    parser.add_argument('--staleness-hours', type=int, default=168,
                       help='Hours before re-processing (incremental mode)')
    parser.add_argument('--workers', type=int, default=None,
                       help='Number of parallel workers (default: CPU count - 1)')
    parser.add_argument('--config', type=str, help='Path to configuration file')
    parser.add_argument('--init-group', type=str, 
                       help='Initialize transformation group')
    parser.add_argument('--show-summary', action='store_true',
                       help='Show watermark summary for group')
    
    args = parser.parse_args()
    
    # Handle init-group
    if args.init_group:
        logger.info(f"Initializing transformation group: {args.init_group}")
        mgr = TransformationWatermarkManager()
        mgr.initialize_transformation_group(args.init_group)
        return
    
    # Handle show-summary
    if args.show_summary:
        mgr = TransformationWatermarkManager()
        mgr.show_group_summary('time_series_daily_adjusted')
        return
    
    # Require mode if not init or summary
    if not args.mode:
        parser.error("--mode is required (unless using --init-group or --show-summary)")
    
    # Initialize transformer
    transformer = TimeSeriesDailyAdjustedTransformer(config_path=args.config)
    
    # Execute transformation
    if args.mode == 'full':
        transformer.run_full_mode(workers=args.workers)
    elif args.mode == 'incremental':
        transformer.run_incremental_mode(staleness_hours=args.staleness_hours, workers=args.workers)
    
    logger.info("✅ Time series transformation completed!")


if __name__ == "__main__":
    main()
