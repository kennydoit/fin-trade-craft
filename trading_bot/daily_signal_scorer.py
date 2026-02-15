"""
Daily Signal Scorer

This module scores and ranks trading signals for the next trading day using:
1. Latest trading signals from transforms.trading_signals
2. ML model predictions for trade success probability
3. Fundamental quality scores
4. Risk management filters

Output: Ranked list of buy recommendations for the next trading day
"""

import sys
import os
import pickle
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List

import pandas as pd
import numpy as np

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from db.postgres_database_manager import PostgresDatabaseManager

logger = logging.getLogger(__name__)


class DailySignalScorer:
    """Score and rank trading signals for daily trading decisions."""
    
    def __init__(self, model_path='models/trade_success_model.pkl', 
                 min_probability=0.80, min_quality_score=50):
        """
        Initialize scorer.
        
        Args:
            model_path: Path to trained ML model
            min_probability: Minimum success probability (default: 80%)
            min_quality_score: Minimum fundamental quality score (default: 50)
        """
        self.db = PostgresDatabaseManager()
        self.min_probability = min_probability
        self.min_quality_score = min_quality_score
        
        # Load ML model
        logger.info(f"Loading ML model from {model_path}...")
        with open(model_path, 'rb') as f:
            model_data = pickle.load(f)
        self.model = model_data['model']
        self.feature_names = model_data['feature_names']
        logger.info(f"Model loaded successfully")
    
    def get_latest_signals(self, lookback_days=3) -> pd.DataFrame:
        """
        Get latest BUY signals from the database.
        
        Args:
            lookback_days: How many days back to look for signals
            
        Returns:
            DataFrame of buy signals
        """
        try:
            self.db.connect()
            
            cutoff_date = datetime.now() - timedelta(days=lookback_days)
            
            query = """
                SELECT DISTINCT ON (s.symbol_id, s.trade_strategy)
                    s.symbol,
                    s.symbol_id,
                    s.date as signal_date,
                    s.trade_strategy,
                    s.signal_strength,
                    s.buy_signal,
                    r.close,
                    r.volume
                FROM transforms.trading_signals s
                INNER JOIN raw.time_series_daily_adjusted r
                    ON s.symbol_id = r.symbol_id::integer 
                    AND s.date = r.date
                WHERE s.buy_signal = TRUE
                    AND s.date >= %s
                ORDER BY s.symbol_id, s.trade_strategy, s.date DESC
            """
            
            df = pd.read_sql(query, self.db.connection, params=(cutoff_date,))
            logger.info(f"Fetched {len(df):,} recent BUY signals")
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching signals: {e}")
            return pd.DataFrame()
        finally:
            self.db.close()
    
    def get_fundamental_data(self, symbols: List[str]) -> pd.DataFrame:
        """Get fundamental quality scores for symbols."""
        try:
            self.db.connect()
            
            # Get latest fundamental scores
            query = """
                SELECT DISTINCT ON (symbol)
                    symbol,
                    overall_quality_score,
                    balance_sheet_quality_score,
                    cash_flow_quality_score,
                    income_statement_quality_score,
                    bs_liquidity_score,
                    bs_leverage_score,
                    bs_asset_quality_score,
                    cf_generation_score,
                    cf_efficiency_score,
                    cf_sustainability_score,
                    is_profitability_score,
                    is_margin_score,
                    is_growth_score
                FROM transforms.fundamental_quality_scores
                WHERE symbol = ANY(%s)
                ORDER BY symbol, fiscal_date_ending DESC
            """
            
            df = pd.read_sql(query, self.db.connection, params=(symbols,))
            logger.info(f"Fetched fundamental data for {len(df):,} symbols")
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching fundamental data: {e}")
            return pd.DataFrame()
        finally:
            self.db.close()
    
    def get_sector_data(self, symbols: List[str]) -> pd.DataFrame:
        """Get sector information for symbols."""
        try:
            self.db.connect()
            
            query = """
                SELECT symbol, sector
                FROM raw.company_overview
                WHERE symbol = ANY(%s)
                    AND sector IS NOT NULL
            """
            
            df = pd.read_sql(query, self.db.connection, params=(symbols,))
            logger.info(f"Fetched sector data for {len(df):,} symbols")
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching sector data: {e}")
            return pd.DataFrame()
        finally:
            self.db.close()
    
    def prepare_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare features for ML model prediction."""
        # Numeric features
        numeric_features = [
            'overall_quality_score', 'balance_sheet_quality_score',
            'cash_flow_quality_score', 'income_statement_quality_score',
            'bs_liquidity_score', 'bs_leverage_score', 'bs_asset_quality_score',
            'cf_generation_score', 'cf_efficiency_score', 'cf_sustainability_score',
            'is_profitability_score', 'is_margin_score', 'is_growth_score'
        ]
        
        # Filter to signals with fundamentals
        df_filtered = df.dropna(subset=['overall_quality_score']).copy()
        
        if len(df_filtered) == 0:
            logger.warning("No signals with fundamental data!")
            return None
        
        # Create feature matrix
        X = df_filtered[numeric_features].copy()
        
        # Fill missing values with median
        for col in X.columns:
            if X[col].isna().any():
                X[col] = X[col].fillna(X[col].median())
        
        # One-hot encode sector
        sector_dummies = pd.get_dummies(df_filtered['sector'], prefix='sector', drop_first=False)
        X = pd.concat([X, sector_dummies], axis=1)
        
        # One-hot encode strategy
        strategy_dummies = pd.get_dummies(df_filtered['trade_strategy'], prefix='strategy', drop_first=False)
        X = pd.concat([X, strategy_dummies], axis=1)
        
        # Ensure all required features exist
        for feat in self.feature_names:
            if feat not in X.columns:
                X[feat] = 0
        
        # Remove extra features and reorder
        X = X[self.feature_names]
        
        return X, df_filtered
    
    def score_signals(self, lookback_days=3) -> pd.DataFrame:
        """
        Score all recent signals and return ranked recommendations.
        
        Returns:
            DataFrame with scored signals, sorted by probability
        """
        logger.info("="*80)
        logger.info("DAILY SIGNAL SCORING")
        logger.info("="*80)
        
        # Get latest signals
        signals_df = self.get_latest_signals(lookback_days)
        
        if signals_df.empty:
            logger.warning("No signals found!")
            return pd.DataFrame()
        
        logger.info(f"Processing {len(signals_df):,} signals...")
        
        # Get fundamental data
        symbols = signals_df['symbol'].unique().tolist()
        fundamentals_df = self.get_fundamental_data(symbols)
        sector_df = self.get_sector_data(symbols)
        
        # Merge data
        df = signals_df.merge(sector_df, on='symbol', how='left')
        df = df.merge(fundamentals_df, on='symbol', how='left')
        df['sector'] = df['sector'].fillna('UNKNOWN')
        
        # Filter by minimum quality score
        df = df[df['overall_quality_score'] >= self.min_quality_score].copy()
        logger.info(f"After quality filter (>={self.min_quality_score}): {len(df):,} signals")
        
        if df.empty:
            logger.warning("No signals passed quality filter!")
            return pd.DataFrame()
        
        # Prepare features for prediction
        X, df_filtered = self.prepare_features(df)
        
        if X is None or X.empty:
            logger.warning("Could not prepare features!")
            return pd.DataFrame()
        
        # Predict success probability
        logger.info("Predicting success probabilities...")
        probabilities = self.model.predict_proba(X)[:, 1]
        df_filtered['success_probability'] = probabilities
        
        # Filter by minimum probability
        df_scored = df_filtered[df_filtered['success_probability'] >= self.min_probability].copy()
        logger.info(f"After probability filter (>={self.min_probability}): {len(df_scored):,} signals")
        
        if df_scored.empty:
            logger.warning(f"No signals with probability >= {self.min_probability}!")
            return pd.DataFrame()
        
        # Calculate composite score
        df_scored['composite_score'] = (
            df_scored['success_probability'] * 0.6 +
            df_scored['signal_strength'] / 100 * 0.2 +
            df_scored['overall_quality_score'] / 100 * 0.2
        )
        
        # Sort by composite score
        df_scored = df_scored.sort_values('composite_score', ascending=False)
        
        # Select key columns for output
        output_cols = [
            'symbol', 'signal_date', 'trade_strategy', 'close',
            'success_probability', 'signal_strength', 'overall_quality_score',
            'composite_score', 'sector', 'volume'
        ]
        
        result = df_scored[output_cols].copy()
        
        logger.info("="*80)
        logger.info(f"SCORING COMPLETE: {len(result):,} recommendations")
        logger.info("="*80)
        
        return result
    
    def export_recommendations(self, scored_df: pd.DataFrame, output_path: str):
        """Export scored recommendations to CSV."""
        scored_df.to_csv(output_path, index=False)
        logger.info(f"Recommendations exported to: {output_path}")


def main():
    """Command-line interface."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Score daily trading signals')
    parser.add_argument('--min-probability', type=float, default=0.80,
                       help='Minimum success probability (default: 0.80)')
    parser.add_argument('--min-quality', type=float, default=50,
                       help='Minimum quality score (default: 50)')
    parser.add_argument('--lookback-days', type=int, default=3,
                       help='Days to look back for signals (default: 3)')
    parser.add_argument('--output', type=str,
                       default='trading_bot/daily_recommendations.csv',
                       help='Output CSV file')
    
    args = parser.parse_args()
    
    # Initialize scorer
    scorer = DailySignalScorer(
        min_probability=args.min_probability,
        min_quality_score=args.min_quality
    )
    
    # Score signals
    recommendations = scorer.score_signals(lookback_days=args.lookback_days)
    
    if not recommendations.empty:
        # Print top recommendations
        print("\n" + "="*100)
        print(f"TOP 10 RECOMMENDATIONS FOR NEXT TRADING DAY")
        print("="*100)
        print(recommendations.head(10).to_string(index=False))
        print("="*100 + "\n")
        
        # Export
        scorer.export_recommendations(recommendations, args.output)
    
    logger.info("Daily scoring completed!")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                       format='%(asctime)s - %(levelname)s - %(message)s')
    main()
