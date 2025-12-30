"""Filter trades by prediction probability and generate filtered backtest report.

This script uses the trained XGBoost model to predict success probability for each trade,
then filters to only trades with 80%+ probability of success and generates a new
performance report.

Usage:
    # Filter with 80% threshold (default)
    python filter_trades_by_prediction.py
    
    # Custom threshold
    python filter_trades_by_prediction.py --threshold 0.75
    
    # Custom input/output
    python filter_trades_by_prediction.py --model models/trade_success_model.pkl --threshold 0.8
"""

import sys
import os
import argparse
import logging
import pickle

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
    """Load trained model."""
    logger.info(f"Loading model from {model_path}...")
    with open(model_path, 'rb') as f:
        model_data = pickle.load(f)
    return model_data['model'], model_data['feature_names']


def load_trades_with_sector(trades_csv):
    """Load trades and attach sector and fundamental information."""
    logger.info(f"Loading trades from {trades_csv}...")
    df = pd.read_csv(trades_csv)
    logger.info(f"Loaded {len(df):,} trades")
    
    # Get sector and fundamentals data
    logger.info("Fetching sector and fundamental data...")
    db = PostgresDatabaseManager()
    db.connect()
    
    # Get sector data
    query_sector = """
        SELECT symbol, sector
        FROM raw.company_overview
        WHERE sector IS NOT NULL
    """
    sector_df = pd.read_sql(query_sector, db.connection)
    
    # Get fundamental quality scores
    query_fundamentals = """
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
    fundamentals_df = pd.read_sql(query_fundamentals, db.connection)
    db.close()
    
    # For each symbol, keep only the most recent fundamental data
    fundamentals_df = fundamentals_df.drop_duplicates(subset=['symbol'], keep='first')
    
    # Join sector
    df = df.merge(sector_df, on='symbol', how='left')
    df['sector'] = df['sector'].fillna('UNKNOWN')
    
    # Join fundamentals
    df = df.merge(fundamentals_df.drop(columns=['fiscal_date_ending']), on='symbol', how='left')
    
    logger.info(f"Sector data attached for {df['sector'].notna().sum():,} trades")
    logger.info(f"Fundamental data attached for {df['overall_quality_score'].notna().sum():,} trades")
    
    return df


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
    
    # For trades without fundamentals, we can't make predictions
    # Filter to trades with fundamental data
    df_with_fundamentals = df.dropna(subset=['overall_quality_score']).copy()
    logger.info(f"Trades with fundamentals: {len(df_with_fundamentals):,}")
    
    if len(df_with_fundamentals) == 0:
        logger.error("No trades with fundamental data found!")
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
    
    # One-hot encode strategy
    strategy_dummies = pd.get_dummies(df_with_fundamentals['strategy'], prefix='strategy', drop_first=False)
    X = pd.concat([X, strategy_dummies], axis=1)
    
    # Ensure all features from training are present
    current_features = set(X.columns)
    required_features = set(feature_names)
    
    # Add missing features as zeros
    missing_features = required_features - current_features
    if missing_features:
        logger.info(f"Adding {len(missing_features)} missing features as zeros")
        for feat in missing_features:
            X[feat] = 0
    
    # Remove extra features
    extra_features = current_features - required_features
    if extra_features:
        logger.info(f"Removing {len(extra_features)} extra features")
        X = X.drop(columns=list(extra_features))
    
    # Reorder columns to match training
    X = X[feature_names]
    
    logger.info(f"Feature matrix prepared: {X.shape[0]:,} rows × {X.shape[1]} features")
    
    return X, df_with_fundamentals


def predict_and_filter(df_with_fundamentals, X, model, threshold=0.8):
    """Predict success probability and filter trades."""
    logger.info("Predicting trade success probabilities...")
    
    # Get probability predictions
    proba = model.predict_proba(X)[:, 1]  # Probability of success
    
    # Add predictions to dataframe
    df_with_fundamentals = df_with_fundamentals.copy()
    df_with_fundamentals['success_probability'] = proba
    
    logger.info(f"Prediction distribution:")
    logger.info(f"  Mean probability: {proba.mean():.3f}")
    logger.info(f"  Median probability: {np.median(proba):.3f}")
    logger.info(f"  Min probability: {proba.min():.3f}")
    logger.info(f"  Max probability: {proba.max():.3f}")
    
    # Filter by threshold
    filtered_df = df_with_fundamentals[df_with_fundamentals['success_probability'] >= threshold].copy()
    
    logger.info(f"\nFiltering results (threshold >= {threshold}):")
    logger.info(f"  Original trades: {len(df_with_fundamentals):,}")
    logger.info(f"  Filtered trades: {len(filtered_df):,} ({len(filtered_df)/len(df_with_fundamentals)*100:.1f}%)")
    
    return filtered_df


def calculate_strategy_performance(df):
    """Calculate performance metrics by strategy."""
    logger.info("Calculating strategy performance...")
    
    results = []
    
    for strategy in df['strategy'].unique():
        strategy_df = df[df['strategy'] == strategy]
        
        total_trades = len(strategy_df)
        winning_trades = (strategy_df['pnl'] > 0).sum()
        losing_trades = (strategy_df['pnl'] <= 0).sum()
        win_rate = winning_trades / total_trades * 100
        
        total_pnl = strategy_df['pnl'].sum()
        total_return_pct = (total_pnl / 100000) * 100  # Assuming $100k initial capital
        avg_trade_return_pct = strategy_df['pnl_pct'].mean()
        
        # Calculate max drawdown
        cumulative_pnl = strategy_df.sort_values('entry_date')['pnl'].cumsum()
        running_max = cumulative_pnl.cummax()
        drawdown = (cumulative_pnl - running_max) / 100000 * 100
        max_drawdown = drawdown.min()
        
        # Sharpe ratio
        returns = strategy_df['pnl_pct'].values
        if len(returns) > 1 and returns.std() > 0:
            sharpe_ratio = (returns.mean() / returns.std()) * np.sqrt(252)
        else:
            sharpe_ratio = 0
        
        # Profit factor
        gross_profit = strategy_df[strategy_df['pnl'] > 0]['pnl'].sum()
        gross_loss = abs(strategy_df[strategy_df['pnl'] < 0]['pnl'].sum())
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0
        
        avg_holding_days = strategy_df['holding_days'].mean()
        total_commission = strategy_df['commission'].sum()
        
        results.append({
            'strategy': strategy,
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': f"{win_rate:.2f}%",
            'total_return': total_pnl,
            'total_return_pct': f"{total_return_pct:.2f}%",
            'avg_trade_return': strategy_df['pnl'].mean(),
            'avg_trade_return_pct': f"{avg_trade_return_pct:.2f}%",
            'max_drawdown': f"{max_drawdown:.2f}%",
            'sharpe_ratio': f"{sharpe_ratio:.2f}",
            'profit_factor': f"{profit_factor:.2f}",
            'avg_holding_days': f"{avg_holding_days:.1f}",
            'total_commission': total_commission,
            'gross_profit': gross_profit,
            'gross_loss': gross_loss
        })
    
    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values('total_return', ascending=False)
    
    return results_df


def print_report(performance_df, trades_df, threshold):
    """Print formatted performance report."""
    print("\n" + "=" * 100)
    print(f"FILTERED BACKTEST REPORT - TRADES WITH >= {threshold*100:.0f}% SUCCESS PROBABILITY")
    print("=" * 100)
    
    # Overall statistics
    print(f"\nOVERALL STATISTICS:")
    print(f"  Total filtered trades: {len(trades_df):,}")
    print(f"  Winning trades: {(trades_df['pnl'] > 0).sum():,} ({(trades_df['pnl'] > 0).sum()/len(trades_df)*100:.1f}%)")
    print(f"  Losing trades: {(trades_df['pnl'] <= 0).sum():,} ({(trades_df['pnl'] <= 0).sum()/len(trades_df)*100:.1f}%)")
    print(f"  Total P&L: ${trades_df['pnl'].sum():,.2f}")
    print(f"  Average P&L per trade: ${trades_df['pnl'].mean():.2f}")
    print(f"  Average holding days: {trades_df['holding_days'].mean():.1f}")
    print(f"  Average success probability: {trades_df['success_probability'].mean():.3f}")
    
    # Strategy performance
    print(f"\n{'='*100}")
    print("STRATEGY PERFORMANCE")
    print("=" * 100)
    print(performance_df.to_string(index=False))
    
    # Summary
    print("\n" + "=" * 100)
    print("SUMMARY")
    print("=" * 100)
    best_strategy = performance_df.iloc[0]
    worst_strategy = performance_df.iloc[-1]
    print(f"Best Strategy: {best_strategy['strategy']} ({best_strategy['total_return_pct']} return)")
    print(f"Worst Strategy: {worst_strategy['strategy']} ({worst_strategy['total_return_pct']} return)")
    print(f"Average Win Rate: {trades_df.groupby('strategy').apply(lambda x: (x['pnl'] > 0).sum() / len(x) * 100).mean():.1f}%")
    print("=" * 100 + "\n")


def main():
    """Main execution."""
    parser = argparse.ArgumentParser(
        description='Filter trades by ML prediction probability and generate performance report'
    )
    
    parser.add_argument(
        '--model',
        type=str,
        default='models/trade_success_model.pkl',
        help='Path to trained model (default: models/trade_success_model.pkl)'
    )
    parser.add_argument(
        '--trades',
        type=str,
        default='backtesting/strategy_performance_with_cooldown_trades.csv',
        help='Path to trades CSV (default: strategy_performance_with_cooldown_trades.csv)'
    )
    parser.add_argument(
        '--threshold',
        type=float,
        default=0.8,
        help='Minimum success probability threshold (default: 0.8)'
    )
    parser.add_argument(
        '--output-performance',
        type=str,
        default='backtesting/strategy_performance_filtered_80pct.csv',
        help='Output CSV for performance report'
    )
    parser.add_argument(
        '--output-trades',
        type=str,
        default='backtesting/trades_filtered_80pct.csv',
        help='Output CSV for filtered trades'
    )
    
    args = parser.parse_args()
    
    # Validate threshold
    if not 0 < args.threshold <= 1:
        logger.error("Threshold must be between 0 and 1")
        return
    
    logger.info("=" * 80)
    logger.info("FILTERING TRADES BY PREDICTION PROBABILITY")
    logger.info("=" * 80)
    logger.info(f"Model: {args.model}")
    logger.info(f"Threshold: {args.threshold * 100:.0f}%")
    
    # Load model
    model, feature_names = load_model(args.model)
    
    # Load trades
    df = load_trades_with_sector(args.trades)
    
    # Prepare features
    X, df_with_fundamentals = prepare_features_for_prediction(df, feature_names)
    
    if X is None:
        logger.error("Failed to prepare features")
        return
    
    # Predict and filter
    filtered_df = predict_and_filter(df_with_fundamentals, X, model, args.threshold)
    
    if len(filtered_df) == 0:
        logger.warning(f"No trades met the {args.threshold} probability threshold!")
        return
    
    # Calculate performance
    performance_df = calculate_strategy_performance(filtered_df)
    
    # Print report
    print_report(performance_df, filtered_df, args.threshold)
    
    # Export results
    logger.info(f"\nExporting results...")
    performance_df.to_csv(args.output_performance, index=False)
    logger.info(f"  Performance report: {args.output_performance}")
    
    filtered_df.to_csv(args.output_trades, index=False)
    logger.info(f"  Filtered trades: {args.output_trades}")
    
    logger.info("\n" + "=" * 80)
    logger.info("Processing complete!")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
