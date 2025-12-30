"""Trade Success Prediction Model using XGBoost.

This script builds a classification model to predict successful trades (positive returns)
based on fundamental quality scores, sector, and trading strategy.

Model Specifications:
- Type: XGBoost Classification
- Target: Binary (1 = positive return, 0 = negative/zero return)
- Features:
  - Numeric: All fundamental quality scores
  - Categorical (one-hot encoded): SECTOR, STRATEGY

Usage:
    # Train model and evaluate
    python trade_success_predictor.py
    
    # Train with custom test split
    python trade_success_predictor.py --test-size 0.3
    
    # Export model
    python trade_success_predictor.py --output models/trade_predictor.pkl
    
    # Load trades with fundamentals
    python trade_success_predictor.py --input backtesting/trades_with_fundamentals.csv
"""

import sys
import os
import argparse
import logging
from pathlib import Path
from datetime import datetime
import pickle

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import (
    classification_report, confusion_matrix, roc_auc_score, 
    roc_curve, precision_recall_curve, f1_score
)
from sklearn.preprocessing import LabelEncoder
import xgboost as xgb
import matplotlib.pyplot as plt
import seaborn as sns

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.postgres_database_manager import PostgresDatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TradeSuccessPredictor:
    """XGBoost model to predict trade success based on fundamentals and sector."""
    
    def __init__(self, test_size=0.2, random_state=42):
        """
        Initialize predictor.
        
        Args:
            test_size (float): Fraction of data for testing (default: 0.2)
            random_state (int): Random seed for reproducibility (default: 42)
        """
        self.test_size = test_size
        self.random_state = random_state
        self.model = None
        self.feature_names = None
        self.label_encoders = {}
        
    def load_data_with_sector(self, input_file=None):
        """
        Load trades with fundamentals and join sector/industry from company_overview.
        
        Args:
            input_file (str, optional): Path to trades CSV. If None, loads from parquet.
            
        Returns:
            pd.DataFrame: Trades with sector/industry information
        """
        logger.info("Loading trade data...")
        
        if input_file and input_file.endswith('.csv'):
            df = pd.read_csv(input_file)
        else:
            df = pd.read_parquet('backtesting/trades_with_fundamentals.parquet')
        
        logger.info(f"Loaded {len(df):,} trades")
        
        # Get sector and industry data
        logger.info("Fetching sector and industry data...")
        db = PostgresDatabaseManager()
        db.connect()
        
        query = """
            SELECT 
                symbol,
                sector,
                industry
            FROM raw.company_overview
            WHERE sector IS NOT NULL
        """
        
        sector_df = pd.read_sql(query, db.connection)
        db.close()
        
        logger.info(f"Loaded sector data for {len(sector_df):,} companies")
        
        # Join sector data
        df = df.merge(sector_df, on='symbol', how='left')
        
        # Log missing sectors
        missing_sector = df['sector'].isna().sum()
        if missing_sector > 0:
            logger.warning(f"{missing_sector:,} trades missing sector information ({missing_sector/len(df)*100:.1f}%)")
        
        return df
    
    def prepare_features(self, df):
        """
        Prepare features for modeling.
        
        Args:
            df (pd.DataFrame): Raw trade data with fundamentals
            
        Returns:
            tuple: (X, y, feature_names)
        """
        logger.info("Preparing features...")
        
        # Filter to trades with fundamental data
        df_with_fundamentals = df[df['overall_quality_score'].notna()].copy()
        logger.info(f"Using {len(df_with_fundamentals):,} trades with fundamental data")
        
        # Create target variable: 1 = successful trade (pnl > 0), 0 = unsuccessful
        df_with_fundamentals['success'] = (df_with_fundamentals['pnl'] > 0).astype(int)
        
        # Log class distribution
        success_rate = df_with_fundamentals['success'].mean()
        logger.info(f"Success rate: {success_rate*100:.1f}% (baseline accuracy)")
        
        # Numeric features: fundamental quality scores
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
        
        # Filter to available numeric features
        available_numeric = [col for col in numeric_features if col in df_with_fundamentals.columns]
        logger.info(f"Using {len(available_numeric)} numeric features")
        
        # Start with numeric features
        X = df_with_fundamentals[available_numeric].copy()
        
        # Fill missing numeric values with median
        for col in X.columns:
            if X[col].isna().any():
                median_val = X[col].median()
                X[col].fillna(median_val, inplace=True)
                logger.info(f"Filled {col} missing values with median: {median_val:.2f}")
        
        # One-hot encode SECTOR
        if 'sector' in df_with_fundamentals.columns:
            # Fill missing sectors with 'UNKNOWN'
            df_with_fundamentals['sector'] = df_with_fundamentals['sector'].fillna('UNKNOWN')
            
            sector_dummies = pd.get_dummies(df_with_fundamentals['sector'], prefix='sector', drop_first=False)
            X = pd.concat([X, sector_dummies], axis=1)
            logger.info(f"Added {len(sector_dummies.columns)} sector features")
        
        # One-hot encode STRATEGY
        if 'strategy' in df_with_fundamentals.columns:
            strategy_dummies = pd.get_dummies(df_with_fundamentals['strategy'], prefix='strategy', drop_first=False)
            X = pd.concat([X, strategy_dummies], axis=1)
            logger.info(f"Added {len(strategy_dummies.columns)} strategy features")
        
        # Get target variable
        y = df_with_fundamentals['success'].values
        
        # Store feature names
        feature_names = X.columns.tolist()
        
        logger.info(f"Final feature matrix: {X.shape[0]:,} rows × {X.shape[1]} features")
        
        return X, y, feature_names
    
    def train_model(self, X_train, y_train, X_test, y_test):
        """
        Train XGBoost classification model.
        
        Args:
            X_train (pd.DataFrame): Training features
            y_train (np.array): Training labels
            X_test (pd.DataFrame): Test features
            y_test (np.array): Test labels
            
        Returns:
            xgb.XGBClassifier: Trained model
        """
        logger.info("Training XGBoost model...")
        
        # Calculate scale_pos_weight for class imbalance
        neg_count = (y_train == 0).sum()
        pos_count = (y_train == 1).sum()
        scale_pos_weight = neg_count / pos_count
        
        logger.info(f"Class distribution - Negative: {neg_count:,}, Positive: {pos_count:,}")
        logger.info(f"Using scale_pos_weight: {scale_pos_weight:.2f}")
        
        # Initialize XGBoost classifier
        self.model = xgb.XGBClassifier(
            objective='binary:logistic',
            n_estimators=200,
            max_depth=6,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            scale_pos_weight=scale_pos_weight,
            random_state=self.random_state,
            n_jobs=-1,
            eval_metric='logloss'
        )
        
        # Train with early stopping
        self.model.fit(
            X_train, y_train,
            eval_set=[(X_train, y_train), (X_test, y_test)],
            verbose=50
        )
        
        logger.info("Training complete!")
        
        return self.model
    
    def evaluate_model(self, X_test, y_test):
        """
        Evaluate model performance.
        
        Args:
            X_test (pd.DataFrame): Test features
            y_test (np.array): Test labels
            
        Returns:
            dict: Performance metrics
        """
        logger.info("Evaluating model...")
        
        # Predictions
        y_pred = self.model.predict(X_test)
        y_pred_proba = self.model.predict_proba(X_test)[:, 1]
        
        # Calculate metrics
        auc_score = roc_auc_score(y_test, y_pred_proba)
        f1 = f1_score(y_test, y_pred)
        
        logger.info(f"AUC-ROC: {auc_score:.4f}")
        logger.info(f"F1 Score: {f1:.4f}")
        
        # Classification report
        logger.info("\nClassification Report:")
        print(classification_report(y_test, y_pred, target_names=['Unsuccessful', 'Successful']))
        
        # Confusion matrix
        cm = confusion_matrix(y_test, y_pred)
        logger.info("\nConfusion Matrix:")
        logger.info(f"True Negatives: {cm[0,0]:,}, False Positives: {cm[0,1]:,}")
        logger.info(f"False Negatives: {cm[1,0]:,}, True Positives: {cm[1,1]:,}")
        
        metrics = {
            'auc_score': auc_score,
            'f1_score': f1,
            'confusion_matrix': cm,
            'y_pred': y_pred,
            'y_pred_proba': y_pred_proba
        }
        
        return metrics
    
    def get_feature_importance(self, top_n=20):
        """
        Get feature importance from trained model.
        
        Args:
            top_n (int): Number of top features to return (default: 20)
            
        Returns:
            pd.DataFrame: Feature importance scores
        """
        if self.model is None:
            logger.error("Model not trained yet!")
            return None
        
        # Get feature importance
        importance_df = pd.DataFrame({
            'feature': self.feature_names,
            'importance': self.model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        logger.info(f"\nTop {top_n} Most Important Features:")
        print(importance_df.head(top_n).to_string(index=False))
        
        return importance_df
    
    def plot_results(self, y_test, metrics, output_dir='backtesting'):
        """
        Generate visualizations of model performance.
        
        Args:
            y_test (np.array): Test labels
            metrics (dict): Performance metrics from evaluate_model
            output_dir (str): Directory to save plots
        """
        logger.info("Generating visualizations...")
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Create figure with subplots
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        
        # 1. Confusion Matrix
        cm = metrics['confusion_matrix']
        sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', ax=axes[0, 0])
        axes[0, 0].set_title('Confusion Matrix')
        axes[0, 0].set_xlabel('Predicted')
        axes[0, 0].set_ylabel('Actual')
        axes[0, 0].set_xticklabels(['Unsuccessful', 'Successful'])
        axes[0, 0].set_yticklabels(['Unsuccessful', 'Successful'])
        
        # 2. ROC Curve
        fpr, tpr, _ = roc_curve(y_test, metrics['y_pred_proba'])
        axes[0, 1].plot(fpr, tpr, label=f"AUC = {metrics['auc_score']:.4f}")
        axes[0, 1].plot([0, 1], [0, 1], 'k--', label='Random')
        axes[0, 1].set_xlabel('False Positive Rate')
        axes[0, 1].set_ylabel('True Positive Rate')
        axes[0, 1].set_title('ROC Curve')
        axes[0, 1].legend()
        axes[0, 1].grid(True, alpha=0.3)
        
        # 3. Precision-Recall Curve
        precision, recall, _ = precision_recall_curve(y_test, metrics['y_pred_proba'])
        axes[1, 0].plot(recall, precision)
        axes[1, 0].set_xlabel('Recall')
        axes[1, 0].set_ylabel('Precision')
        axes[1, 0].set_title('Precision-Recall Curve')
        axes[1, 0].grid(True, alpha=0.3)
        
        # 4. Feature Importance (Top 15)
        importance_df = self.get_feature_importance(top_n=15)
        axes[1, 1].barh(range(15), importance_df['importance'].head(15))
        axes[1, 1].set_yticks(range(15))
        axes[1, 1].set_yticklabels(importance_df['feature'].head(15))
        axes[1, 1].set_xlabel('Importance')
        axes[1, 1].set_title('Top 15 Feature Importance')
        axes[1, 1].invert_yaxis()
        
        plt.tight_layout()
        
        # Save plot
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        plot_path = os.path.join(output_dir, f'trade_predictor_results_{timestamp}.png')
        plt.savefig(plot_path, dpi=300, bbox_inches='tight')
        logger.info(f"Plots saved to {plot_path}")
        
        plt.close()
    
    def save_model(self, output_path):
        """
        Save trained model to disk.
        
        Args:
            output_path (str): Path to save model
        """
        if self.model is None:
            logger.error("No model to save!")
            return
        
        os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
        
        model_data = {
            'model': self.model,
            'feature_names': self.feature_names,
            'test_size': self.test_size,
            'random_state': self.random_state
        }
        
        with open(output_path, 'wb') as f:
            pickle.dump(model_data, f)
        
        logger.info(f"Model saved to {output_path}")
    
    def load_model(self, model_path):
        """
        Load trained model from disk.
        
        Args:
            model_path (str): Path to saved model
        """
        with open(model_path, 'rb') as f:
            model_data = pickle.load(f)
        
        self.model = model_data['model']
        self.feature_names = model_data['feature_names']
        self.test_size = model_data.get('test_size', 0.2)
        self.random_state = model_data.get('random_state', 42)
        
        logger.info(f"Model loaded from {model_path}")


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description='Train XGBoost model to predict successful trades',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--input',
        type=str,
        default='backtesting/trades_with_fundamentals.parquet',
        help='Input file with trades and fundamentals'
    )
    parser.add_argument(
        '--test-size',
        type=float,
        default=0.2,
        help='Fraction of data for testing (default: 0.2)'
    )
    parser.add_argument(
        '--output',
        type=str,
        help='Path to save trained model (optional)'
    )
    parser.add_argument(
        '--random-seed',
        type=int,
        default=42,
        help='Random seed for reproducibility (default: 42)'
    )
    
    args = parser.parse_args()
    
    # Initialize predictor
    logger.info("=" * 80)
    logger.info("TRADE SUCCESS PREDICTION MODEL")
    logger.info("=" * 80)
    
    predictor = TradeSuccessPredictor(
        test_size=args.test_size,
        random_state=args.random_seed
    )
    
    # Load data
    df = predictor.load_data_with_sector(args.input)
    
    # Prepare features
    X, y, feature_names = predictor.prepare_features(df)
    predictor.feature_names = feature_names
    
    # Split data
    logger.info(f"Splitting data: {(1-args.test_size)*100:.0f}% train, {args.test_size*100:.0f}% test")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=args.test_size, random_state=args.random_seed, stratify=y
    )
    
    logger.info(f"Training set: {len(X_train):,} samples")
    logger.info(f"Test set: {len(X_test):,} samples")
    
    # Train model
    predictor.train_model(X_train, y_train, X_test, y_test)
    
    # Evaluate
    metrics = predictor.evaluate_model(X_test, y_test)
    
    # Feature importance
    importance_df = predictor.get_feature_importance(top_n=20)
    
    # Generate plots
    predictor.plot_results(y_test, metrics)
    
    # Save model if requested
    if args.output:
        predictor.save_model(args.output)
    
    logger.info("=" * 80)
    logger.info("Model training completed!")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
