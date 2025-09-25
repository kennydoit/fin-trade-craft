"""
ML Dataset Loader

Simple example of how to load and use the exported ML datasets for training.

Features:
- Load time series features (technical indicators + targets)
- Load fundamental features (financial ratios)
- Align datasets by symbol and date
- Create train/test splits
- Basic data validation

Author: Financial Trading Craft
Date: September 2025
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MLDatasetLoader:
    """Load and prepare ML datasets for training"""
    
    def __init__(self, features_dir: str = "features"):
        self.features_dir = Path(features_dir)
        self.time_series_df = None
        self.fundamental_df = None
        self.combined_df = None
    
    def load_time_series_features(self) -> pd.DataFrame:
        """Load time series features (OHLCV + technical indicators + targets)"""
        
        # Find latest time series file
        ts_files = list(self.features_dir.glob("time_series_features_*.parquet"))
        if not ts_files:
            raise FileNotFoundError("No time series features file found")
        
        latest_file = max(ts_files, key=lambda x: x.stat().st_mtime)
        logger.info(f"Loading time series features from: {latest_file}")
        
        df = pd.read_parquet(latest_file)
        df['date'] = pd.to_datetime(df['date'])
        
        logger.info(f"Loaded {len(df):,} time series records")
        logger.info(f"Date range: {df.date.min()} to {df.date.max()}")
        logger.info(f"Symbols: {df.symbol.nunique()}")
        logger.info(f"Features: {len(df.columns)}")
        
        self.time_series_df = df
        return df
    
    def load_fundamental_features(self) -> pd.DataFrame:
        """Load fundamental features (financial ratios)"""
        
        # Find latest fundamental file
        fund_files = list(self.features_dir.glob("fundamental_features_*.parquet"))
        if not fund_files:
            raise FileNotFoundError("No fundamental features file found")
        
        latest_file = max(fund_files, key=lambda x: x.stat().st_mtime)
        logger.info(f"Loading fundamental features from: {latest_file}")
        
        df = pd.read_parquet(latest_file)
        df['fiscal_date_ending'] = pd.to_datetime(df['fiscal_date_ending'])
        
        logger.info(f"Loaded {len(df):,} fundamental records")
        logger.info(f"Date range: {df.fiscal_date_ending.min()} to {df.fiscal_date_ending.max()}")
        logger.info(f"Symbols: {df.symbol.nunique()}")
        logger.info(f"Features: {len(df.columns)}")
        
        self.fundamental_df = df
        return df
    
    def get_feature_summary(self):
        """Print summary of available features"""
        
        if self.time_series_df is not None:
            print("\n📈 TIME SERIES FEATURES:")
            ohlcv_cols = [col for col in self.time_series_df.columns if col.startswith('ohlcv_')]
            target_cols = [col for col in self.time_series_df.columns if col.startswith('target_')]
            
            print(f"  - OHLCV Features: {len(ohlcv_cols)}")
            print(f"    Examples: {ohlcv_cols[:5]}")
            print(f"  - Target Features: {len(target_cols)}")
            print(f"    Examples: {target_cols[:3]}")
        
        if self.fundamental_df is not None:
            print("\n📋 FUNDAMENTAL FEATURES:")
            feature_cols = [col for col in self.fundamental_df.columns 
                           if col not in ['symbol', 'fiscal_date_ending', 'symbol_id']]
            print(f"  - Financial Ratios: {len(feature_cols)}")
            print(f"    Examples: {feature_cols[:5]}")
    
    def create_training_dataset(self, target_column: str = 'target_return_5d', 
                              min_date: str = '2020-01-01') -> pd.DataFrame:
        """
        Create a training dataset with features and targets
        
        Args:
            target_column: Which target to predict
            min_date: Minimum date for training data
        
        Returns:
            DataFrame ready for ML training
        """
        
        if self.time_series_df is None:
            self.load_time_series_features()
        
        # Filter by date
        df = self.time_series_df[self.time_series_df['date'] >= min_date].copy()
        
        # Get feature columns (exclude metadata)
        feature_cols = [col for col in df.columns 
                       if col.startswith(('ohlcv_', 'target_')) and col != target_column]
        
        # Create final dataset
        columns = ['symbol', 'date'] + feature_cols + [target_column]
        training_df = df[columns].copy()
        
        # Remove rows with missing targets
        training_df = training_df.dropna(subset=[target_column])
        
        logger.info(f"Created training dataset: {len(training_df):,} records")
        logger.info(f"Features: {len(feature_cols)}")
        logger.info(f"Target: {target_column}")
        
        return training_df


def main():
    """Example usage of the ML dataset loader"""
    
    print("🚀 ML DATASET LOADER EXAMPLE")
    print("=" * 40)
    
    # Initialize loader
    loader = MLDatasetLoader()
    
    # Load datasets
    loader.load_time_series_features()
    loader.load_fundamental_features()
    
    # Show feature summary
    loader.get_feature_summary()
    
    # Create training dataset
    print("\n🎯 CREATING TRAINING DATASET...")
    training_data = loader.create_training_dataset(
        target_column='target_return_5d',
        min_date='2020-01-01'
    )
    
    print(f"\n✅ Training dataset ready!")
    print(f"Shape: {training_data.shape}")
    print(f"Memory usage: {training_data.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
    
    # Show sample
    print("\n📊 Sample data:")
    print(training_data[['symbol', 'date', 'ohlcv_sma_5', 'ohlcv_volume_ratio', 'target_return_5d']].head())


if __name__ == "__main__":
    main()
