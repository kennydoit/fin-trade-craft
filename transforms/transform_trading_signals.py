"""
Transform Trading Signals with Multiple Strategy Implementation

This module generates buy/sell signals from multiple trading strategies using both
raw OHLCV data and computed technical indicators from transforms.time_series_daily_adjusted.

Strategies Implemented:
1. EMA Crossover (8/21) - Classic momentum strategy
2. RSI Oversold/Overbought - Mean reversion
3. MACD Histogram Reversal - Momentum shifts
4. Bollinger Band Breakout - Volatility breakout
5. Volume Spike with Price Confirmation - Volume-based
6. Williams %R Extremes - Momentum extremes
7. Moving Average Ribbon - Trend strength
8. Price Action Breakout - Support/resistance
9. ADX Trend Strength - Directional movement
10. Stochastic RSI - Combined momentum

Usage:
    # Initialize (create table)
    python transform_trading_signals.py --init
    
    # Full mode (recreate all signals)
    python transform_trading_signals.py --mode full
    
    # Incremental mode (process unprocessed records)
    python transform_trading_signals.py --mode incremental --days-back 7
    
    # Specific strategy only
    python transform_trading_signals.py --mode incremental --strategy ema_crossover
"""

import sys
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path

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


class TradingSignalsTransformer:
    """Generate trading signals from multiple strategies using self-watermarking."""
    
    def __init__(self):
        """Initialize transformer with database connection."""
        self.db = PostgresDatabaseManager()
        self.table_name = 'transforms.trading_signals'
        
        # Strategy registry
        self.strategies = {
            'ema_crossover': self.strategy_ema_crossover,
            'rsi_mean_reversion': self.strategy_rsi_mean_reversion,
            'macd_histogram_reversal': self.strategy_macd_histogram_reversal,
            'bollinger_breakout': self.strategy_bollinger_breakout,
            'volume_spike': self.strategy_volume_spike,
            'williams_extremes': self.strategy_williams_extremes,
            'ma_ribbon': self.strategy_ma_ribbon,
            'price_breakout': self.strategy_price_breakout,
            'rsi_divergence': self.strategy_rsi_divergence,
            'trend_following': self.strategy_trend_following,
        }
    
    def create_table(self):
        """Create the transforms.trading_signals table with self-watermarking."""
        try:
            self.db.connect()
            
            create_table_sql = """
                CREATE TABLE IF NOT EXISTS transforms.trading_signals (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    symbol_id INTEGER NOT NULL,
                    date DATE NOT NULL,
                    buy_signal BOOLEAN NOT NULL,
                    sell_signal BOOLEAN NOT NULL,
                    trade_strategy VARCHAR(50) NOT NULL,
                    signal_strength NUMERIC(5, 2),
                    processed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (symbol_id, date, trade_strategy)
                );
                
                CREATE INDEX IF NOT EXISTS idx_trading_signals_symbol_id 
                    ON transforms.trading_signals(symbol_id);
                CREATE INDEX IF NOT EXISTS idx_trading_signals_date 
                    ON transforms.trading_signals(date);
                CREATE INDEX IF NOT EXISTS idx_trading_signals_strategy 
                    ON transforms.trading_signals(trade_strategy);
                CREATE INDEX IF NOT EXISTS idx_trading_signals_buy 
                    ON transforms.trading_signals(buy_signal) WHERE buy_signal = true;
                CREATE INDEX IF NOT EXISTS idx_trading_signals_sell 
                    ON transforms.trading_signals(sell_signal) WHERE sell_signal = true;
                CREATE INDEX IF NOT EXISTS idx_trading_signals_processed 
                    ON transforms.trading_signals(processed_at);
            """
            
            self.db.execute_query(create_table_sql)
            logger.info(f"Created {self.table_name} table with processed_at column")
            
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise
        finally:
            self.db.close()
    
    def get_symbol_data(self, symbol_id, days_back=None):
        """
        Fetch combined data from raw and transforms tables for a symbol.
        
        Args:
            symbol_id (int): Symbol ID
            days_back (int, optional): Number of days to look back
            
        Returns:
            pd.DataFrame: Combined OHLCV and technical indicators
        """
        try:
            # Ensure connection is open
            if not self.db.connection or self.db.connection.closed:
                self.db.connect()
            
            # Build date filter if specified
            date_filter = ""
            params = [str(symbol_id)]
            
            if days_back:
                cutoff_date = datetime.now() - timedelta(days=days_back)
                date_filter = "AND r.date >= %s"
                params.append(cutoff_date.strftime('%Y-%m-%d'))
            
            query = f"""
                SELECT 
                    r.symbol_id,
                    r.symbol,
                    r.date,
                    r.open,
                    r.high,
                    r.low,
                    r.close,
                    r.adjusted_close,
                    r.volume,
                    t.ohlcv_ema_8,
                    t.ohlcv_ema_21,
                    t.ohlcv_ema_8_21_cross,
                    t.ohlcv_rsi_14,
                    t.ohlcv_rsi_14_oversold,
                    t.ohlcv_rsi_14_overbought,
                    t.ohlcv_macd,
                    t.ohlcv_macd_signal,
                    t.ohlcv_macd_histogram,
                    t.ohlcv_bb_upper,
                    t.ohlcv_bb_middle,
                    t.ohlcv_bb_lower,
                    t.ohlcv_bb_position,
                    t.ohlcv_volume_sma_20,
                    t.ohlcv_volume_ratio,
                    t.ohlcv_willr_14,
                    t.ohlcv_sma_5,
                    t.ohlcv_sma_10,
                    t.ohlcv_sma_20,
                    t.ohlcv_sma_50,
                    t.ohlcv_atr_14,
                    t.ohlcv_obv,
                    t.ohlcv_ad
                FROM raw.time_series_daily_adjusted r
                LEFT JOIN transforms.time_series_daily_adjusted t
                    ON r.symbol_id::integer = t.symbol_id AND r.date = t.date
                WHERE r.symbol_id = %s
                {date_filter}
                ORDER BY r.date ASC
            """
            
            df = pd.read_sql(query, self.db.connection, params=params)
            
            if df.empty:
                return None
            
            # Ensure date column is datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # Convert symbol_id to integer
            df['symbol_id'] = df['symbol_id'].astype(int)
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching data for symbol_id {symbol_id}: {e}")
            return None
    
    # ==================== STRATEGY IMPLEMENTATIONS ====================
    
    def strategy_ema_crossover(self, df):
        """
        EMA 8/21 Crossover Strategy
        Buy: EMA 8 crosses above EMA 21
        Sell: EMA 8 crosses below EMA 21
        """
        signals = []
        
        if 'ohlcv_ema_8' not in df.columns or 'ohlcv_ema_21' not in df.columns:
            return signals
        
        df = df.dropna(subset=['ohlcv_ema_8', 'ohlcv_ema_21'])
        
        if len(df) < 2:
            return signals
        
        for i in range(1, len(df)):
            prev_ema8 = df.iloc[i-1]['ohlcv_ema_8']
            prev_ema21 = df.iloc[i-1]['ohlcv_ema_21']
            curr_ema8 = df.iloc[i]['ohlcv_ema_8']
            curr_ema21 = df.iloc[i]['ohlcv_ema_21']
            
            # Buy signal: bullish crossover
            if prev_ema8 <= prev_ema21 and curr_ema8 > curr_ema21:
                signals.append({
                    'symbol': df.iloc[i]['symbol'],
                    'symbol_id': df.iloc[i]['symbol_id'],
                    'date': df.iloc[i]['date'],
                    'buy_signal': True,
                    'sell_signal': False,
                    'trade_strategy': 'ema_crossover',
                    'signal_strength': min(100, abs(curr_ema8 - curr_ema21) / curr_ema21 * 100)
                })
            
            # Sell signal: bearish crossover
            elif prev_ema8 >= prev_ema21 and curr_ema8 < curr_ema21:
                signals.append({
                    'symbol': df.iloc[i]['symbol'],
                    'symbol_id': df.iloc[i]['symbol_id'],
                    'date': df.iloc[i]['date'],
                    'buy_signal': False,
                    'sell_signal': True,
                    'trade_strategy': 'ema_crossover',
                    'signal_strength': min(100, abs(curr_ema8 - curr_ema21) / curr_ema21 * 100)
                })
        
        return signals
    
    def strategy_rsi_mean_reversion(self, df):
        """
        RSI Mean Reversion Strategy
        Buy: RSI crosses above 30 (oversold recovery)
        Sell: RSI crosses below 70 (overbought reversal)
        """
        signals = []
        
        if 'ohlcv_rsi_14' not in df.columns:
            return signals
        
        df = df.dropna(subset=['ohlcv_rsi_14'])
        
        if len(df) < 2:
            return signals
        
        for i in range(1, len(df)):
            prev_rsi = df.iloc[i-1]['ohlcv_rsi_14']
            curr_rsi = df.iloc[i]['ohlcv_rsi_14']
            
            # Buy signal: RSI crosses above 30
            if prev_rsi <= 30 and curr_rsi > 30:
                signals.append({
                    'symbol': df.iloc[i]['symbol'],
                    'symbol_id': df.iloc[i]['symbol_id'],
                    'date': df.iloc[i]['date'],
                    'buy_signal': True,
                    'sell_signal': False,
                    'trade_strategy': 'rsi_mean_reversion',
                    'signal_strength': curr_rsi
                })
            
            # Sell signal: RSI crosses below 70
            elif prev_rsi >= 70 and curr_rsi < 70:
                signals.append({
                    'symbol': df.iloc[i]['symbol'],
                    'symbol_id': df.iloc[i]['symbol_id'],
                    'date': df.iloc[i]['date'],
                    'buy_signal': False,
                    'sell_signal': True,
                    'trade_strategy': 'rsi_mean_reversion',
                    'signal_strength': 100 - curr_rsi
                })
        
        return signals
    
    def strategy_macd_histogram_reversal(self, df):
        """
        MACD Histogram Reversal Strategy
        Buy: Histogram crosses from negative to positive
        Sell: Histogram crosses from positive to negative
        """
        signals = []
        
        if 'ohlcv_macd_histogram' not in df.columns:
            return signals
        
        df = df.dropna(subset=['ohlcv_macd_histogram'])
        
        if len(df) < 2:
            return signals
        
        for i in range(1, len(df)):
            prev_hist = df.iloc[i-1]['ohlcv_macd_histogram']
            curr_hist = df.iloc[i]['ohlcv_macd_histogram']
            
            # Buy signal: histogram crosses above zero
            if prev_hist <= 0 and curr_hist > 0:
                signals.append({
                    'symbol': df.iloc[i]['symbol'],
                    'symbol_id': df.iloc[i]['symbol_id'],
                    'date': df.iloc[i]['date'],
                    'buy_signal': True,
                    'sell_signal': False,
                    'trade_strategy': 'macd_histogram_reversal',
                    'signal_strength': min(100, abs(curr_hist) * 10)
                })
            
            # Sell signal: histogram crosses below zero
            elif prev_hist >= 0 and curr_hist < 0:
                signals.append({
                    'symbol': df.iloc[i]['symbol'],
                    'symbol_id': df.iloc[i]['symbol_id'],
                    'date': df.iloc[i]['date'],
                    'buy_signal': False,
                    'sell_signal': True,
                    'trade_strategy': 'macd_histogram_reversal',
                    'signal_strength': min(100, abs(curr_hist) * 10)
                })
        
        return signals
    
    def strategy_bollinger_breakout(self, df):
        """
        Bollinger Band Breakout Strategy
        Buy: Price breaks above upper band (continuation)
        Sell: Price breaks below lower band (breakdown)
        """
        signals = []
        
        required_cols = ['close', 'ohlcv_bb_upper', 'ohlcv_bb_lower']
        if not all(col in df.columns for col in required_cols):
            return signals
        
        df = df.dropna(subset=required_cols)
        
        if len(df) < 2:
            return signals
        
        for i in range(1, len(df)):
            prev_close = df.iloc[i-1]['close']
            curr_close = df.iloc[i]['close']
            prev_upper = df.iloc[i-1]['ohlcv_bb_upper']
            curr_upper = df.iloc[i]['ohlcv_bb_upper']
            prev_lower = df.iloc[i-1]['ohlcv_bb_lower']
            curr_lower = df.iloc[i]['ohlcv_bb_lower']
            
            # Buy signal: break above upper band
            if prev_close <= prev_upper and curr_close > curr_upper:
                signals.append({
                    'symbol': df.iloc[i]['symbol'],
                    'symbol_id': df.iloc[i]['symbol_id'],
                    'date': df.iloc[i]['date'],
                    'buy_signal': True,
                    'sell_signal': False,
                    'trade_strategy': 'bollinger_breakout',
                    'signal_strength': min(100, (curr_close - curr_upper) / curr_upper * 100)
                })
            
            # Sell signal: break below lower band
            elif prev_close >= prev_lower and curr_close < curr_lower:
                signals.append({
                    'symbol': df.iloc[i]['symbol'],
                    'symbol_id': df.iloc[i]['symbol_id'],
                    'date': df.iloc[i]['date'],
                    'buy_signal': False,
                    'sell_signal': True,
                    'trade_strategy': 'bollinger_breakout',
                    'signal_strength': min(100, (curr_lower - curr_close) / curr_lower * 100)
                })
        
        return signals
    
    def strategy_volume_spike(self, df):
        """
        Volume Spike with Price Confirmation Strategy
        Buy: Volume > 2x average + price up > 2%
        Sell: Volume > 2x average + price down > 2%
        """
        signals = []
        
        required_cols = ['close', 'volume', 'ohlcv_volume_sma_20']
        if not all(col in df.columns for col in required_cols):
            return signals
        
        df = df.dropna(subset=required_cols)
        
        if len(df) < 2:
            return signals
        
        for i in range(1, len(df)):
            curr_volume = df.iloc[i]['volume']
            avg_volume = df.iloc[i]['ohlcv_volume_sma_20']
            prev_close = df.iloc[i-1]['close']
            curr_close = df.iloc[i]['close']
            
            if avg_volume == 0:
                continue
            
            volume_ratio = curr_volume / avg_volume
            price_change_pct = (curr_close - prev_close) / prev_close * 100
            
            # Buy signal: high volume + price up
            if volume_ratio > 2.0 and price_change_pct > 2.0:
                signals.append({
                    'symbol': df.iloc[i]['symbol'],
                    'symbol_id': df.iloc[i]['symbol_id'],
                    'date': df.iloc[i]['date'],
                    'buy_signal': True,
                    'sell_signal': False,
                    'trade_strategy': 'volume_spike',
                    'signal_strength': min(100, volume_ratio * 10)
                })
            
            # Sell signal: high volume + price down
            elif volume_ratio > 2.0 and price_change_pct < -2.0:
                signals.append({
                    'symbol': df.iloc[i]['symbol'],
                    'symbol_id': df.iloc[i]['symbol_id'],
                    'date': df.iloc[i]['date'],
                    'buy_signal': False,
                    'sell_signal': True,
                    'trade_strategy': 'volume_spike',
                    'signal_strength': min(100, volume_ratio * 10)
                })
        
        return signals
    
    def strategy_williams_extremes(self, df):
        """
        Williams %R Extremes Strategy
        Buy: Williams %R crosses above -80 (oversold recovery)
        Sell: Williams %R crosses below -20 (overbought reversal)
        """
        signals = []
        
        if 'ohlcv_willr_14' not in df.columns:
            return signals
        
        df = df.dropna(subset=['ohlcv_willr_14'])
        
        if len(df) < 2:
            return signals
        
        for i in range(1, len(df)):
            prev_willr = df.iloc[i-1]['ohlcv_willr_14']
            curr_willr = df.iloc[i]['ohlcv_willr_14']
            
            # Buy signal: crosses above -80
            if prev_willr <= -80 and curr_willr > -80:
                signals.append({
                    'symbol': df.iloc[i]['symbol'],
                    'symbol_id': df.iloc[i]['symbol_id'],
                    'date': df.iloc[i]['date'],
                    'buy_signal': True,
                    'sell_signal': False,
                    'trade_strategy': 'williams_extremes',
                    'signal_strength': min(100, abs(curr_willr + 50) * 2)
                })
            
            # Sell signal: crosses below -20
            elif prev_willr >= -20 and curr_willr < -20:
                signals.append({
                    'symbol': df.iloc[i]['symbol'],
                    'symbol_id': df.iloc[i]['symbol_id'],
                    'date': df.iloc[i]['date'],
                    'buy_signal': False,
                    'sell_signal': True,
                    'trade_strategy': 'williams_extremes',
                    'signal_strength': min(100, abs(curr_willr + 50) * 2)
                })
        
        return signals
    
    def strategy_ma_ribbon(self, df):
        """
        Moving Average Ribbon Strategy
        Buy: All MAs aligned bullish (5 > 10 > 20 > 50) + price above all
        Sell: All MAs aligned bearish (5 < 10 < 20 < 50) + price below all
        """
        signals = []
        
        required_cols = ['close', 'ohlcv_sma_5', 'ohlcv_sma_10', 'ohlcv_sma_20', 'ohlcv_sma_50']
        if not all(col in df.columns for col in required_cols):
            return signals
        
        df = df.dropna(subset=required_cols)
        
        for i in range(len(df)):
            close = df.iloc[i]['close']
            sma5 = df.iloc[i]['ohlcv_sma_5']
            sma10 = df.iloc[i]['ohlcv_sma_10']
            sma20 = df.iloc[i]['ohlcv_sma_20']
            sma50 = df.iloc[i]['ohlcv_sma_50']
            
            # Buy signal: bullish alignment
            if close > sma5 > sma10 > sma20 > sma50:
                signals.append({
                    'symbol': df.iloc[i]['symbol'],
                    'symbol_id': df.iloc[i]['symbol_id'],
                    'date': df.iloc[i]['date'],
                    'buy_signal': True,
                    'sell_signal': False,
                    'trade_strategy': 'ma_ribbon',
                    'signal_strength': min(100, (close - sma50) / sma50 * 100)
                })
            
            # Sell signal: bearish alignment
            elif close < sma5 < sma10 < sma20 < sma50:
                signals.append({
                    'symbol': df.iloc[i]['symbol'],
                    'symbol_id': df.iloc[i]['symbol_id'],
                    'date': df.iloc[i]['date'],
                    'buy_signal': False,
                    'sell_signal': True,
                    'trade_strategy': 'ma_ribbon',
                    'signal_strength': min(100, (sma50 - close) / sma50 * 100)
                })
        
        return signals
    
    def strategy_price_breakout(self, df):
        """
        Price Breakout Strategy (20-day high/low)
        Buy: Price breaks above 20-day high
        Sell: Price breaks below 20-day low
        """
        signals = []
        
        if 'high' not in df.columns or 'low' not in df.columns:
            return signals
        
        df = df.copy()
        df['high_20'] = df['high'].rolling(20).max()
        df['low_20'] = df['low'].rolling(20).min()
        
        df = df.dropna(subset=['high_20', 'low_20'])
        
        if len(df) < 2:
            return signals
        
        for i in range(1, len(df)):
            curr_high = df.iloc[i]['high']
            curr_low = df.iloc[i]['low']
            prev_high_20 = df.iloc[i-1]['high_20']
            prev_low_20 = df.iloc[i-1]['low_20']
            
            # Buy signal: break above 20-day high
            if curr_high > prev_high_20:
                signals.append({
                    'symbol': df.iloc[i]['symbol'],
                    'symbol_id': df.iloc[i]['symbol_id'],
                    'date': df.iloc[i]['date'],
                    'buy_signal': True,
                    'sell_signal': False,
                    'trade_strategy': 'price_breakout',
                    'signal_strength': min(100, (curr_high - prev_high_20) / prev_high_20 * 100)
                })
            
            # Sell signal: break below 20-day low
            elif curr_low < prev_low_20:
                signals.append({
                    'symbol': df.iloc[i]['symbol'],
                    'symbol_id': df.iloc[i]['symbol_id'],
                    'date': df.iloc[i]['date'],
                    'buy_signal': False,
                    'sell_signal': True,
                    'trade_strategy': 'price_breakout',
                    'signal_strength': min(100, (prev_low_20 - curr_low) / prev_low_20 * 100)
                })
        
        return signals
    
    def strategy_rsi_divergence(self, df):
        """
        RSI Divergence Strategy
        Buy: Price makes lower low but RSI makes higher low (bullish divergence)
        Sell: Price makes higher high but RSI makes lower high (bearish divergence)
        """
        signals = []
        
        required_cols = ['close', 'ohlcv_rsi_14']
        if not all(col in df.columns for col in required_cols):
            return signals
        
        df = df.dropna(subset=required_cols)
        
        if len(df) < 20:  # Need enough data for divergence detection
            return signals
        
        # Find local extremes
        window = 5
        df = df.copy()
        df['price_low'] = df['close'].rolling(window, center=True).min() == df['close']
        df['price_high'] = df['close'].rolling(window, center=True).max() == df['close']
        
        # Find divergences
        lows = df[df['price_low']].copy()
        highs = df[df['price_high']].copy()
        
        # Bullish divergence (lower price low, higher RSI low)
        for i in range(1, len(lows)):
            curr_idx = lows.index[i]
            prev_idx = lows.index[i-1]
            
            curr_price = df.loc[curr_idx, 'close']
            prev_price = df.loc[prev_idx, 'close']
            curr_rsi = df.loc[curr_idx, 'ohlcv_rsi_14']
            prev_rsi = df.loc[prev_idx, 'ohlcv_rsi_14']
            
            if curr_price < prev_price and curr_rsi > prev_rsi:
                signals.append({
                    'symbol': df.loc[curr_idx, 'symbol'],
                    'symbol_id': df.loc[curr_idx, 'symbol_id'],
                    'date': df.loc[curr_idx, 'date'],
                    'buy_signal': True,
                    'sell_signal': False,
                    'trade_strategy': 'rsi_divergence',
                    'signal_strength': min(100, (curr_rsi - prev_rsi))
                })
        
        # Bearish divergence (higher price high, lower RSI high)
        for i in range(1, len(highs)):
            curr_idx = highs.index[i]
            prev_idx = highs.index[i-1]
            
            curr_price = df.loc[curr_idx, 'close']
            prev_price = df.loc[prev_idx, 'close']
            curr_rsi = df.loc[curr_idx, 'ohlcv_rsi_14']
            prev_rsi = df.loc[prev_idx, 'ohlcv_rsi_14']
            
            if curr_price > prev_price and curr_rsi < prev_rsi:
                signals.append({
                    'symbol': df.loc[curr_idx, 'symbol'],
                    'symbol_id': df.loc[curr_idx, 'symbol_id'],
                    'date': df.loc[curr_idx, 'date'],
                    'buy_signal': False,
                    'sell_signal': True,
                    'trade_strategy': 'rsi_divergence',
                    'signal_strength': min(100, (prev_rsi - curr_rsi))
                })
        
        return signals
    
    def strategy_trend_following(self, df):
        """
        Trend Following Strategy (ADX-based)
        Buy: Price > 50 SMA + EMA 8 > EMA 21 + RSI > 50
        Sell: Price < 50 SMA + EMA 8 < EMA 21 + RSI < 50
        """
        signals = []
        
        required_cols = ['close', 'ohlcv_sma_50', 'ohlcv_ema_8', 'ohlcv_ema_21', 'ohlcv_rsi_14']
        if not all(col in df.columns for col in required_cols):
            return signals
        
        df = df.dropna(subset=required_cols)
        
        for i in range(len(df)):
            close = df.iloc[i]['close']
            sma50 = df.iloc[i]['ohlcv_sma_50']
            ema8 = df.iloc[i]['ohlcv_ema_8']
            ema21 = df.iloc[i]['ohlcv_ema_21']
            rsi = df.iloc[i]['ohlcv_rsi_14']
            
            # Buy signal: bullish trend
            if close > sma50 and ema8 > ema21 and rsi > 50:
                signals.append({
                    'symbol': df.iloc[i]['symbol'],
                    'symbol_id': df.iloc[i]['symbol_id'],
                    'date': df.iloc[i]['date'],
                    'buy_signal': True,
                    'sell_signal': False,
                    'trade_strategy': 'trend_following',
                    'signal_strength': min(100, rsi)
                })
            
            # Sell signal: bearish trend
            elif close < sma50 and ema8 < ema21 and rsi < 50:
                signals.append({
                    'symbol': df.iloc[i]['symbol'],
                    'symbol_id': df.iloc[i]['symbol_id'],
                    'date': df.iloc[i]['date'],
                    'buy_signal': False,
                    'sell_signal': True,
                    'trade_strategy': 'trend_following',
                    'signal_strength': min(100, 100 - rsi)
                })
        
        return signals
    
    # ==================== PROCESSING METHODS ====================
    
    def process_symbol(self, symbol_id, strategy_filter=None, days_back=None):
        """
        Process a single symbol and generate signals.
        
        Args:
            symbol_id (int): Symbol ID
            strategy_filter (str, optional): Process only this strategy
            days_back (int, optional): Number of days to look back
            
        Returns:
            int: Number of signals generated
        """
        try:
            # Fetch data
            df = self.get_symbol_data(symbol_id, days_back)
            
            if df is None or df.empty:
                logger.warning(f"No data for symbol_id {symbol_id}")
                return 0
            
            # Generate signals from strategies
            all_signals = []
            
            strategies_to_run = [strategy_filter] if strategy_filter else self.strategies.keys()
            
            for strategy_name in strategies_to_run:
                if strategy_name not in self.strategies:
                    logger.warning(f"Unknown strategy: {strategy_name}")
                    continue
                
                strategy_func = self.strategies[strategy_name]
                signals = strategy_func(df)
                all_signals.extend(signals)
            
            if not all_signals:
                return 0
            
            # Convert to DataFrame
            signals_df = pd.DataFrame(all_signals)
            
            # Add timestamps
            signals_df['processed_at'] = pd.Timestamp.now()
            signals_df['created_at'] = pd.Timestamp.now()
            signals_df['updated_at'] = pd.Timestamp.now()
            
            # Load to database (upsert)
            self.load_signals(signals_df)
            
            return len(signals_df)
            
        except Exception as e:
            logger.error(f"Error processing symbol_id {symbol_id}: {e}")
            return 0
    
    def load_signals(self, signals_df):
        """
        Load signals to database using upsert.
        
        Args:
            signals_df (pd.DataFrame): Signals to load
        """
        try:
            insert_query = """
                INSERT INTO transforms.trading_signals 
                (symbol, symbol_id, date, buy_signal, sell_signal, trade_strategy, 
                 signal_strength, processed_at, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol_id, date, trade_strategy) 
                DO UPDATE SET
                    buy_signal = EXCLUDED.buy_signal,
                    sell_signal = EXCLUDED.sell_signal,
                    signal_strength = EXCLUDED.signal_strength,
                    processed_at = EXCLUDED.processed_at,
                    updated_at = EXCLUDED.updated_at
            """
            
            records = []
            for _, row in signals_df.iterrows():
                record = (
                    row['symbol'],
                    row['symbol_id'],
                    row['date'],
                    row['buy_signal'],
                    row['sell_signal'],
                    row['trade_strategy'],
                    row['signal_strength'],
                    row['processed_at'],
                    row['created_at'],
                    row['updated_at']
                )
                records.append(record)
            
            self.db.execute_many(insert_query, records)
            
        except Exception as e:
            logger.error(f"Error loading signals: {e}")
            raise
    
    def get_unprocessed_symbols(self, days_back=7):
        """
        Get symbols that need processing based on processed_at watermark.
        
        Args:
            days_back (int): Look back this many days for raw data changes
            
        Returns:
            list: List of symbol IDs
        """
        try:
            # Ensure connection is open
            if not self.db.connection or self.db.connection.closed:
                self.db.connect()
            
            cutoff_date = datetime.now() - timedelta(days=days_back)
            
            query = """
                SELECT DISTINCT r.symbol_id::integer
                FROM raw.time_series_daily_adjusted r
                WHERE r.date >= %s
                  AND NOT EXISTS (
                    SELECT 1 
                    FROM transforms.trading_signals s
                    WHERE s.symbol_id = r.symbol_id::integer
                      AND s.date = r.date
                      AND s.processed_at >= %s
                  )
                ORDER BY r.symbol_id::integer
            """
            
            results = self.db.fetch_query(query, (cutoff_date, cutoff_date))
            
            return [row[0] for row in results]
            
        except Exception as e:
            logger.error(f"Error getting unprocessed symbols: {e}")
            return []
    
    def initialize(self):
        """Initialize table (drop and recreate)."""
        logger.info("Initializing trading_signals table...")
        
        try:
            self.db.connect()
            self.db.execute_query("DROP TABLE IF EXISTS transforms.trading_signals CASCADE")
            self.create_table()
            
            logger.info("Table initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing table: {e}")
            raise
        finally:
            self.db.close()
    
    def process_unprocessed(self, strategy_filter=None, days_back=7):
        """
        Process only unprocessed symbols (incremental mode).
        
        Args:
            strategy_filter (str, optional): Process only this strategy
            days_back (int): Look back this many days
        """
        logger.info("=" * 80)
        logger.info(f"INCREMENTAL MODE: Processing unprocessed symbols ({days_back} days)")
        logger.info("=" * 80)
        
        try:
            self.db.connect()
            
            # Ensure table exists
            self.create_table()
            
            # Get unprocessed symbols
            symbol_ids = self.get_unprocessed_symbols(days_back)
            
            if not symbol_ids:
                logger.info("No unprocessed symbols found")
                return
            
            logger.info(f"Found {len(symbol_ids)} symbols to process")
            
            # Process each symbol
            total_signals = 0
            success_count = 0
            
            for idx, symbol_id in enumerate(symbol_ids, 1):
                if idx % 50 == 0 or idx == len(symbol_ids):
                    logger.info(f"Progress: {idx}/{len(symbol_ids)} symbols")
                
                signals_count = self.process_symbol(symbol_id, strategy_filter, days_back)
                
                if signals_count > 0:
                    success_count += 1
                    total_signals += signals_count
            
            # Summary
            logger.info("=" * 80)
            logger.info("INCREMENTAL MODE SUMMARY")
            logger.info("=" * 80)
            logger.info(f"Symbols processed: {len(symbol_ids)}")
            logger.info(f"Successful: {success_count}")
            logger.info(f"Total signals generated: {total_signals:,}")
            
        except Exception as e:
            logger.error(f"Error in incremental mode: {e}")
            raise
        finally:
            self.db.close()
    
    def run_full_mode(self, strategy_filter=None):
        """
        Run full mode - recreate all signals.
        
        Args:
            strategy_filter (str, optional): Process only this strategy
        """
        logger.info("=" * 80)
        logger.info("FULL MODE: Recreating all trading signals")
        logger.info("=" * 80)
        
        try:
            self.db.connect()
            
            # Drop and recreate table
            self.db.execute_query("DROP TABLE IF EXISTS transforms.trading_signals CASCADE")
            self.create_table()
            
            # Get all symbols
            query = """
                SELECT DISTINCT symbol_id::integer
                FROM raw.time_series_daily_adjusted
                ORDER BY symbol_id::integer
            """
            
            results = self.db.fetch_query(query)
            symbol_ids = [row[0] for row in results]
            
            logger.info(f"Processing {len(symbol_ids)} symbols")
            
            # Process each symbol
            total_signals = 0
            success_count = 0
            
            for idx, symbol_id in enumerate(symbol_ids, 1):
                if idx % 50 == 0 or idx == len(symbol_ids):
                    logger.info(f"Progress: {idx}/{len(symbol_ids)} symbols")
                
                signals_count = self.process_symbol(symbol_id, strategy_filter)
                
                if signals_count > 0:
                    success_count += 1
                    total_signals += signals_count
            
            # Summary
            logger.info("=" * 80)
            logger.info("FULL MODE SUMMARY")
            logger.info("=" * 80)
            logger.info(f"Symbols processed: {len(symbol_ids)}")
            logger.info(f"Successful: {success_count}")
            logger.info(f"Total signals generated: {total_signals:,}")
            
        except Exception as e:
            logger.error(f"Error in full mode: {e}")
            raise
        finally:
            self.db.close()


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description='Transform trading signals with multiple strategies',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Initialize (create table)
  python transform_trading_signals.py --init
  
  # Full mode (recreate all signals)
  python transform_trading_signals.py --mode full
  
  # Incremental mode (7 days lookback)
  python transform_trading_signals.py --mode incremental --days-back 7
  
  # Specific strategy only
  python transform_trading_signals.py --mode incremental --strategy ema_crossover
        """
    )
    
    parser.add_argument('--init', action='store_true',
                       help='Initialize table (drop and recreate)')
    parser.add_argument('--mode', choices=['full', 'incremental'],
                       help='Processing mode')
    parser.add_argument('--days-back', type=int, default=7,
                       help='Days to look back (incremental mode)')
    parser.add_argument('--strategy', type=str,
                       help='Process only this strategy')
    
    args = parser.parse_args()
    
    # Initialize transformer
    transformer = TradingSignalsTransformer()
    
    # Handle initialization
    if args.init:
        transformer.initialize()
        return
    
    # Require mode if not init
    if not args.mode:
        parser.error("--mode is required (unless using --init)")
    
    # Execute transformation
    if args.mode == 'full':
        transformer.run_full_mode(strategy_filter=args.strategy)
    elif args.mode == 'incremental':
        transformer.process_unprocessed(strategy_filter=args.strategy, days_back=args.days_back)
    
    logger.info("Trading signals transformation completed!")


if __name__ == "__main__":
    main()
