"""Daily Trading Signal Scorer

Processes recent trading signals (last 30 days), joins with company overview and fundamentals,
then scores each signal with the trained XGBoost model to predict trade success probability.

This script is designed to run daily and identify high-probability trading opportunities.

Usage:
    # Score last 30 days of signals
    python daily_signal_scorer.py
    
    # Custom date range
    python daily_signal_scorer.py --days 60
    
    # Custom threshold for filtering
    python daily_signal_scorer.py --threshold 0.9
    
    # Output to custom file
    python daily_signal_scorer.py --output signals_scored_2025_12_30.csv
"""

import sys
import os
import argparse
import logging
import pickle
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.postgres_database_manager import PostgresDatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_model(model_path):
    """Load trained XGBoost model and feature names."""
    logger.info(f"Loading model from {model_path}...")
    with open(model_path, 'rb') as f:
        model_data = pickle.load(f)
    logger.info(f"Model loaded with {len(model_data['feature_names'])} features")
    return model_data['model'], model_data['feature_names']


def load_recent_signals(days=30):
    """Load trading signals from the last N days."""
    logger.info(f"Loading trading signals from the last {days} days...")
    
    db = PostgresDatabaseManager()
    db.connect()
    
    # Calculate cutoff date
    cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    
    query = f"""
        SELECT 
            symbol_id,
            date,
            buy_signal,
            sell_signal,
            trade_strategy,
            signal_strength
        FROM transforms.trading_signals
        WHERE date >= '{cutoff_date}'
            AND buy_signal = TRUE
            AND processed_at IS NOT NULL
        ORDER BY date DESC, symbol_id
    """
    
    signals_df = pd.read_sql(query, db.connection)
    db.close()
    
    logger.info(f"Loaded {len(signals_df):,} buy signals")
    return signals_df


def get_symbol_lookup():
    """Get symbol_id to symbol mapping."""
    logger.info("Loading symbol lookup table...")
    
    db = PostgresDatabaseManager()
    db.connect()
    
    query = """
        SELECT DISTINCT symbol_id, symbol
        FROM raw.company_overview
        WHERE symbol_id IS NOT NULL
    """
    
    lookup_df = pd.read_sql(query, db.connection)
    db.close()
    
    logger.info(f"Loaded {len(lookup_df):,} symbol mappings")
    return lookup_df


def join_company_overview(signals_df):
    """Join signals with company overview to get sector and industry."""
    logger.info("Joining with company overview data...")
    
    db = PostgresDatabaseManager()
    db.connect()
    
    query = """
        SELECT 
            symbol_id,
            symbol,
            name,
            sector,
            industry,
            market_capitalization,
            exchange
        FROM raw.company_overview
        WHERE symbol_id IS NOT NULL
    """
    
    overview_df = pd.read_sql(query, db.connection)
    db.close()
    
    # Join with signals
    df = signals_df.merge(overview_df, on='symbol_id', how='left')
    
    # Fill missing sector/industry with UNKNOWN
    df['sector'] = df['sector'].fillna('UNKNOWN')
    df['industry'] = df['industry'].fillna('UNKNOWN')
    
    logger.info(f"Joined {len(df):,} signals with company data")
    logger.info(f"Signals with sector data: {df['sector'].notna().sum():,}")
    
    return df


def join_fundamental_scores(df):
    """Join signals with fundamental quality scores.
    
    Applies 45-day publication lag to ensure no lookahead bias.
    """
    logger.info("Joining with fundamental quality scores...")
    
    db = PostgresDatabaseManager()
    db.connect()
    
    # Get all fundamental scores
    query = """
        SELECT 
            symbol,
            fiscal_date_ending,
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
        WHERE processed_at IS NOT NULL
        ORDER BY symbol, fiscal_date_ending DESC
    """
    
    fundamentals_df = pd.read_sql(query, db.connection)
    db.close()
    
    # Add 45-day publication lag
    fundamentals_df['fiscal_date_ending'] = pd.to_datetime(fundamentals_df['fiscal_date_ending'])
    fundamentals_df['publication_date'] = fundamentals_df['fiscal_date_ending'] + pd.Timedelta(days=45)
    fundamentals_df['valid_until_date'] = fundamentals_df['publication_date'] + pd.Timedelta(days=90)
    
    # Convert signal date to datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # For each signal, find the most recent published fundamental data
    # where publication_date <= signal_date <= valid_until_date
    df_with_fundamentals = []
    
    for _, signal in df.iterrows():
        symbol = signal['symbol']
        signal_date = signal['date']
        
        # Get fundamentals for this symbol that were published before the signal date
        available = fundamentals_df[
            (fundamentals_df['symbol'] == symbol) &
            (fundamentals_df['publication_date'] <= signal_date) &
            (fundamentals_df['valid_until_date'] >= signal_date)
        ]
        
        if len(available) > 0:
            # Use the most recent one
            most_recent = available.iloc[0]
            signal_dict = signal.to_dict()
            
            # Add fundamental scores
            for col in fundamentals_df.columns:
                if col not in ['symbol', 'fiscal_date_ending', 'publication_date', 'valid_until_date']:
                    signal_dict[col] = most_recent[col]
            
            df_with_fundamentals.append(signal_dict)
        else:
            # No fundamentals available - add signal with NaN fundamentals
            signal_dict = signal.to_dict()
            for col in fundamentals_df.columns:
                if col not in ['symbol', 'fiscal_date_ending', 'publication_date', 'valid_until_date']:
                    signal_dict[col] = np.nan
            df_with_fundamentals.append(signal_dict)
    
    result_df = pd.DataFrame(df_with_fundamentals)
    
    fundamentals_count = result_df['overall_quality_score'].notna().sum()
    logger.info(f"Signals with fundamental data: {fundamentals_count:,} ({100*fundamentals_count/len(result_df):.1f}%)")
    
    return result_df


def prepare_features_for_prediction(df, feature_names):
    """Prepare features matching the training data format."""
    logger.info("Preparing features for prediction...")
    
    # Numeric features
    numeric_features = [
        'overall_quality_score',
        'balance_sheet_quality_score',
        'cash_flow_quality_score', 
        'income_statement_quality_score',
        'bs_liquidity_score',
        'bs_leverage_score',
        'bs_asset_quality_score',
        'cf_generation_score',
        'cf_efficiency_score',
        'cf_sustainability_score',
        'is_profitability_score',
        'is_margin_score',
        'is_growth_score'
    ]
    
    # Filter to signals with fundamental data
    df_with_fundamentals = df.dropna(subset=['overall_quality_score']).copy()
    logger.info(f"Signals with fundamentals: {len(df_with_fundamentals):,}")
    
    if len(df_with_fundamentals) == 0:
        logger.warning("No signals with fundamental data found!")
        return None, None
    
    # Create feature matrix starting with numeric features
    X = df_with_fundamentals[numeric_features].copy()
    
    # Fill missing numeric values with median (same as training)
    for col in X.columns:
        if X[col].isna().any():
            median_val = X[col].median()
            X[col] = X[col].fillna(median_val)
    
    # One-hot encode sector
    sector_dummies = pd.get_dummies(df_with_fundamentals['sector'], prefix='sector', drop_first=False)
    X = pd.concat([X, sector_dummies], axis=1)
    
    # One-hot encode strategy (rename to match training data format)
    df_with_fundamentals['strategy'] = df_with_fundamentals['trade_strategy']
    strategy_dummies = pd.get_dummies(df_with_fundamentals['strategy'], prefix='strategy', drop_first=False)
    X = pd.concat([X, strategy_dummies], axis=1)
    
    # Ensure all training features are present
    for feature in feature_names:
        if feature not in X.columns:
            X[feature] = 0
    
    # Reorder columns to match training data
    X = X[feature_names]
    
    logger.info(f"Feature matrix prepared: {X.shape[0]:,} rows × {X.shape[1]} features")
    
    return X, df_with_fundamentals


def predict_success_probability(model, X, df_signals):
    """Predict trade success probability for each signal."""
    logger.info("Predicting trade success probabilities...")
    
    # Get probability predictions
    probabilities = model.predict_proba(X)[:, 1]  # Probability of class 1 (success)
    
    # Add to dataframe
    df_signals['success_probability'] = probabilities
    
    # Log distribution
    logger.info(f"Prediction distribution:")
    logger.info(f"  Mean probability: {probabilities.mean():.3f}")
    logger.info(f"  Median probability: {np.median(probabilities):.3f}")
    logger.info(f"  Min probability: {probabilities.min():.3f}")
    logger.info(f"  Max probability: {probabilities.max():.3f}")
    
    return df_signals


def filter_and_rank_signals(df, threshold=0.8):
    """Filter signals by probability threshold and rank by probability."""
    logger.info(f"\nFiltering signals with probability >= {threshold:.0%}...")
    
    # Filter by threshold
    filtered_df = df[df['success_probability'] >= threshold].copy()
    
    # Sort by probability (highest first) and then by date (most recent first)
    filtered_df = filtered_df.sort_values(['success_probability', 'date'], ascending=[False, False])
    
    logger.info(f"Signals passing threshold: {len(filtered_df):,} ({100*len(filtered_df)/len(df):.1f}% of signals with fundamentals)")
    
    # Strategy breakdown
    if len(filtered_df) > 0:
        logger.info("\nStrategy breakdown:")
        for strategy in filtered_df['trade_strategy'].value_counts().head(5).items():
            count = strategy[1]
            avg_prob = filtered_df[filtered_df['trade_strategy'] == strategy[0]]['success_probability'].mean()
            logger.info(f"  {strategy[0]}: {count:,} signals (avg prob: {avg_prob:.1%})")
    
    return filtered_df


def main():
    parser = argparse.ArgumentParser(description='Score daily trading signals with XGBoost model')
    parser.add_argument('--days', type=int, default=30, help='Number of days to look back (default: 30)')
    parser.add_argument('--model', type=str, default='models/trade_success_model.pkl', 
                       help='Path to trained model')
    parser.add_argument('--threshold', type=float, default=0.8, 
                       help='Minimum success probability threshold (default: 0.8)')
    parser.add_argument('--output', type=str, default=None,
                       help='Output CSV file (default: backtesting/daily_signals_scored_YYYYMMDD.csv)')
    
    args = parser.parse_args()
    
    # Set default output filename with today's date
    if args.output is None:
        today = datetime.now().strftime('%Y%m%d')
        args.output = f'backtesting/daily_signals_scored_{today}.csv'
    
    logger.info("="*100)
    logger.info("DAILY TRADING SIGNAL SCORER")
    logger.info("="*100)
    logger.info(f"Model: {args.model}")
    logger.info(f"Days: {args.days}")
    logger.info(f"Threshold: {args.threshold:.0%}")
    logger.info(f"Output: {args.output}")
    
    # Load model
    model, feature_names = load_model(args.model)
    
    # Load recent signals
    signals_df = load_recent_signals(args.days)
    
    if len(signals_df) == 0:
        logger.warning("No signals found in the specified time period!")
        return
    
    # Join with company overview (sector, industry)
    signals_df = join_company_overview(signals_df)
    
    # Join with fundamental scores (with publication lag)
    signals_df = join_fundamental_scores(signals_df)
    
    # Prepare features
    X, df_with_fundamentals = prepare_features_for_prediction(signals_df, feature_names)
    
    if X is None:
        logger.error("Could not prepare features. Exiting.")
        return
    
    # Predict success probability
    df_scored = predict_success_probability(model, X, df_with_fundamentals)
    
    # Filter and rank signals
    df_filtered = filter_and_rank_signals(df_scored, args.threshold)
    
    # Export results
    logger.info(f"\nExporting results to {args.output}...")
    
    # Select relevant columns for output
    output_columns = [
        'date', 'symbol', 'name', 'sector', 'industry',
        'trade_strategy', 'signal_strength', 'success_probability',
        'market_capitalization', 'exchange',
        'overall_quality_score', 'balance_sheet_quality_score',
        'cash_flow_quality_score', 'income_statement_quality_score'
    ]
    
    # Only include columns that exist
    output_columns = [col for col in output_columns if col in df_filtered.columns]
    
    df_filtered[output_columns].to_csv(args.output, index=False)
    
    logger.info("="*100)
    logger.info("SUMMARY")
    logger.info("="*100)
    logger.info(f"Total signals processed: {len(signals_df):,}")
    logger.info(f"Signals with fundamentals: {len(df_scored):,}")
    logger.info(f"High-probability signals (>= {args.threshold:.0%}): {len(df_filtered):,}")
    logger.info(f"Average success probability: {df_filtered['success_probability'].mean():.1%}")
    
    if len(df_filtered) > 0:
        logger.info(f"\nTop 5 signals:")
        for idx, row in df_filtered.head(5).iterrows():
            logger.info(f"  {row['date'].strftime('%Y-%m-%d')} | {row['symbol']} | {row['trade_strategy']} | {row['success_probability']:.1%}")
    
    logger.info("="*100)
    logger.info("Processing complete!")
    logger.info("="*100)


if __name__ == '__main__':
    main()
