"""
ML Feature Exporter

Enhanced exporter for ML-ready features supporting:
1. Fundamental features export (balance sheet, income, cash flow)
2. Time series features export (OHLCV technical indicators + targets)
3. Combined dataset creation (fundamentals + time series)
4. Multiple export formats (Parquet, CSV)
5. Universe-specific filtering via config.yaml

Author: Financial Trading Craft
Date: September 2025
"""

import os
import sys
from pathlib import Path
import pandas as pd
import numpy as np
import yaml
import logging
from datetime import datetime
from typing import Optional, Dict, List, Tuple

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from db.postgres_database_manager import PostgresDatabaseManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MLFeatureExporter:
    """
    Enhanced ML Feature Exporter with support for multiple data sources and formats.
    
    Capabilities:
    - Export fundamental features (balance sheet, income, cash flow, macro)
    - Export time series features (OHLCV technical indicators + targets)
    - Create combined datasets joining fundamentals with time series
    - Support universe filtering via config.yaml
    - Export to optimized Parquet format
    """
    
    def __init__(self, config_path: Optional[str] = None, universe_id: Optional[str] = None):
        """
        Initialize the ML Feature Exporter.
        
        Args:
            config_path: Path to configuration file
            universe_id: Specific universe to export
        """
        self.db = PostgresDatabaseManager()
        self.universe_id = universe_id
        
        # Load configuration
        self.config = self._load_config(config_path)
        
        # Export settings
        self.export_format = self.config.get('export_format', 'parquet')
        self.output_dir = Path(self.config.get('output_dir', 'features'))
        self.output_dir.mkdir(exist_ok=True)
        
        # Feature selection
        self.include_fundamentals = self.config.get('include_fundamentals', True)
        self.include_time_series = self.config.get('include_time_series', True)
        self.create_combined = self.config.get('create_combined', False)
        
    def _load_config(self, config_path: Optional[str]) -> Dict:
        """Load configuration from YAML file."""
        if config_path and Path(config_path).exists():
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        
        # Try default config locations
        default_configs = [
            Path.cwd() / 'features' / 'config.yaml',
            Path.cwd() / 'config.yaml'
        ]
        
        for config_file in default_configs:
            if config_file.exists():
                with open(config_file, 'r') as f:
                    config = yaml.safe_load(f)
                    logger.info(f"Loaded configuration from {config_file}")
                    return config
        
        logger.warning("No configuration file found, using defaults")
        return {}
    
    def get_universe_symbols(self, universe_id: str) -> List[str]:
        """
        Get symbols for a specific universe.
        
        Args:
            universe_id: Universe identifier from config.yaml
            
        Returns:
            List of symbols in the universe
        """
        try:
            self.db.connect()
            query = """
                SELECT DISTINCT symbol 
                FROM transformed.symbol_universes 
                WHERE universe_id = %s
                ORDER BY symbol
            """
            result = self.db.fetch_all(query, (universe_id,))
            symbols = [row[0] for row in result]
            logger.info(f"Found {len(symbols)} symbols for universe '{universe_id}'")
            return symbols
        except Exception as e:
            logger.error(f"Error fetching universe symbols: {e}")
            return []
        finally:
            self.db.close()
    
    def export_fundamental_features(self, universe_id: Optional[str] = None) -> Tuple[str, Dict]:
        """
        Export fundamental features (balance sheet, income, cash flow, macro indicators).
        
        Args:
            universe_id: Optional universe filter
            
        Returns:
            Tuple of (file_path, metadata)
        """
        logger.info("Exporting fundamental features...")
        
        try:
            # Build fundamental features using existing FeatureBuild
            df = self.feature_builder.build_features(universe_id=universe_id)
            
            if df.empty:
                logger.warning("No fundamental features data found")
                return None, {}
            
            # Separate features by category
            balance_sheet_cols = [col for col in df.columns if 'balance_sheet' in col.lower()]
            income_cols = [col for col in df.columns if 'income' in col.lower()]
            cash_flow_cols = [col for col in df.columns if 'cash_flow' in col.lower()]
            macro_cols = [col for col in df.columns if col.startswith(('GDP', 'INFLATION', 'INTEREST', 'UNEMPLOYMENT'))]
            
            # Create output filename
            universe_suffix = f"_{universe_id}" if universe_id else ""
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"fundamental_features{universe_suffix}_{timestamp}.parquet"
            filepath = self.output_dir / filename
            
            # Export to Parquet with optimal settings
            df.to_parquet(
                filepath,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            
            # Generate metadata
            metadata = {
                'export_type': 'fundamental_features',
                'universe_id': universe_id,
                'total_features': len(df.columns),
                'balance_sheet_features': len(balance_sheet_cols),
                'income_features': len(income_cols),
                'cash_flow_features': len(cash_flow_cols),
                'macro_features': len(macro_cols),
                'symbols_count': df['symbol'].nunique() if 'symbol' in df.columns else 0,
                'date_range': {
                    'min': str(df.index.min()) if hasattr(df.index, 'min') else None,
                    'max': str(df.index.max()) if hasattr(df.index, 'max') else None
                },
                'file_size_mb': round(filepath.stat().st_size / (1024 * 1024), 2),
                'created_at': timestamp
            }
            
            logger.info(f"✅ Exported {len(df)} records with {len(df.columns)} fundamental features")
            logger.info(f"📁 File: {filepath} ({metadata['file_size_mb']} MB)")
            
            return str(filepath), metadata
            
        except Exception as e:
            logger.error(f"Error exporting fundamental features: {e}")
            import traceback
            traceback.print_exc()
            return None, {}
    
    def export_time_series_features(self, universe_id: Optional[str] = None) -> Tuple[str, Dict]:
        """
        Export time series features (OHLCV technical indicators + targets).
        
        Args:
            universe_id: Optional universe filter
            
        Returns:
            Tuple of (file_path, metadata)
        """
        logger.info("Exporting time series features...")
        
        try:
            self.db.connect()
            
            # Base query for time series features
            query = """
                SELECT * FROM transformed.time_series_features
            """
            
            # Apply universe filter if specified
            if universe_id:
                query += f"""
                    WHERE symbol_id IN (
                        SELECT DISTINCT symbol_id 
                        FROM transformed.symbol_universes 
                        WHERE universe_id = '{universe_id}'
                    )
                """
            
            query += " ORDER BY symbol, date"
            
            # Fetch data
            df = self.db.fetch_dataframe(query)
            
            if df.empty:
                logger.warning("No time series features data found")
                return None, {}
            
            # Separate features by category
            ohlcv_cols = [col for col in df.columns if col.startswith('ohlcv_')]
            target_cols = [col for col in df.columns if col.startswith('target_')]
            
            # Create output filename
            universe_suffix = f"_{universe_id}" if universe_id else ""
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"time_series_features{universe_suffix}_{timestamp}.parquet"
            filepath = self.output_dir / filename
            
            # Export to Parquet with optimal settings
            df.to_parquet(
                filepath,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            
            # Generate metadata
            metadata = {
                'export_type': 'time_series_features',
                'universe_id': universe_id,
                'total_features': len(ohlcv_cols) + len(target_cols),
                'ohlcv_features': len(ohlcv_cols),
                'target_features': len(target_cols),
                'symbols_count': df['symbol'].nunique() if 'symbol' in df.columns else 0,
                'records_count': len(df),
                'date_range': {
                    'min': str(df['date'].min()) if 'date' in df.columns else None,
                    'max': str(df['date'].max()) if 'date' in df.columns else None
                },
                'file_size_mb': round(filepath.stat().st_size / (1024 * 1024), 2),
                'created_at': timestamp
            }
            
            logger.info(f"✅ Exported {len(df)} records with {len(ohlcv_cols)} OHLCV + {len(target_cols)} TARGET features")
            logger.info(f"📁 File: {filepath} ({metadata['file_size_mb']} MB)")
            
            return str(filepath), metadata
            
        except Exception as e:
            logger.error(f"Error exporting time series features: {e}")
            import traceback
            traceback.print_exc()
            return None, {}
        finally:
            self.db.close()
    
    def create_combined_dataset(self, universe_id: Optional[str] = None) -> Tuple[str, Dict]:
        """
        Create combined dataset joining fundamental and time series features.
        
        Args:
            universe_id: Optional universe filter
            
        Returns:
            Tuple of (file_path, metadata)
        """
        logger.info("Creating combined dataset (fundamentals + time series)...")
        
        try:
            self.db.connect()
            
            # Get fundamental features
            fund_df = self.feature_builder.build_features(universe_id=universe_id)
            if fund_df.empty:
                logger.warning("No fundamental features found for combined dataset")
                return None, {}
            
            # Get time series features
            ts_query = """
                SELECT * FROM transformed.time_series_features
            """
            
            if universe_id:
                ts_query += f"""
                    WHERE symbol_id IN (
                        SELECT DISTINCT symbol_id 
                        FROM transformed.symbol_universes 
                        WHERE universe_id = '{universe_id}'
                    )
                """
            
            ts_df = self.db.fetch_dataframe(ts_query)
            if ts_df.empty:
                logger.warning("No time series features found for combined dataset")
                return None, {}
            
            # Prepare fundamental features for joining
            # Reset index if symbol/date are in index
            if hasattr(fund_df.index, 'names') and fund_df.index.names[0] is not None:
                fund_df = fund_df.reset_index()
            
            # Ensure we have symbol column for joining
            if 'symbol' not in fund_df.columns:
                logger.error("Symbol column missing from fundamental features")
                return None, {}
            
            # Join datasets on symbol (broadcast fundamentals to all time series dates)
            logger.info(f"Joining {len(fund_df)} fundamental records with {len(ts_df)} time series records")
            
            # Perform left join to keep all time series data
            combined_df = ts_df.merge(
                fund_df, 
                on='symbol', 
                how='left', 
                suffixes=('_ts', '_fund')
            )
            
            if combined_df.empty:
                logger.warning("Combined dataset is empty after join")
                return None, {}
            
            # Analyze feature categories
            ohlcv_cols = [col for col in combined_df.columns if col.startswith('ohlcv_')]
            target_cols = [col for col in combined_df.columns if col.startswith('target_')]
            fundamental_cols = [col for col in combined_df.columns 
                              if any(x in col.lower() for x in ['balance_sheet', 'income', 'cash_flow']) 
                              or col.startswith(('GDP', 'INFLATION', 'INTEREST', 'UNEMPLOYMENT'))]
            
            # Create output filename
            universe_suffix = f"_{universe_id}" if universe_id else ""
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"combined_features{universe_suffix}_{timestamp}.parquet"
            filepath = self.output_dir / filename
            
            # Export to Parquet with optimal settings
            combined_df.to_parquet(
                filepath,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            
            # Generate metadata
            metadata = {
                'export_type': 'combined_features',
                'universe_id': universe_id,
                'total_features': len(ohlcv_cols) + len(target_cols) + len(fundamental_cols),
                'ohlcv_features': len(ohlcv_cols),
                'target_features': len(target_cols),
                'fundamental_features': len(fundamental_cols),
                'symbols_count': combined_df['symbol'].nunique() if 'symbol' in combined_df.columns else 0,
                'records_count': len(combined_df),
                'date_range': {
                    'min': str(combined_df['date'].min()) if 'date' in combined_df.columns else None,
                    'max': str(combined_df['date'].max()) if 'date' in combined_df.columns else None
                },
                'file_size_mb': round(filepath.stat().st_size / (1024 * 1024), 2),
                'created_at': timestamp
            }
            
            logger.info(f"✅ Created combined dataset: {len(combined_df)} records")
            logger.info(f"📊 Features: {len(ohlcv_cols)} OHLCV + {len(target_cols)} TARGET + {len(fundamental_cols)} Fundamental")
            logger.info(f"📁 File: {filepath} ({metadata['file_size_mb']} MB)")
            
            return str(filepath), metadata
            
        except Exception as e:
            logger.error(f"Error creating combined dataset: {e}")
            import traceback
            traceback.print_exc()
            return None, {}
        finally:
            self.db.close()
    
    def export_all(self, universe_id: Optional[str] = None) -> Dict:
        """
        Export all requested feature types based on configuration.
        
        Args:
            universe_id: Optional universe filter
            
        Returns:
            Dictionary with export results and metadata
        """
        results = {
            'universe_id': universe_id or self.universe_id,
            'exports': {},
            'summary': {}
        }
        
        universe_filter = universe_id or self.universe_id
        
        # Export fundamental features
        if self.include_fundamentals:
            filepath, metadata = self.export_fundamental_features(universe_filter)
            if filepath:
                results['exports']['fundamental_features'] = {
                    'filepath': filepath,
                    'metadata': metadata
                }
        
        # Export time series features  
        if self.include_time_series:
            filepath, metadata = self.export_time_series_features(universe_filter)
            if filepath:
                results['exports']['time_series_features'] = {
                    'filepath': filepath,
                    'metadata': metadata
                }
        
        # Create combined dataset
        if self.create_combined:
            filepath, metadata = self.create_combined_dataset(universe_filter)
            if filepath:
                results['exports']['combined_features'] = {
                    'filepath': filepath,
                    'metadata': metadata
                }
        
        # Generate summary
        total_files = len(results['exports'])
        total_size_mb = sum(
            export['metadata'].get('file_size_mb', 0) 
            for export in results['exports'].values()
        )
        
        results['summary'] = {
            'total_exports': total_files,
            'total_size_mb': round(total_size_mb, 2),
            'export_directory': str(self.output_dir),
            'completed_at': datetime.now().isoformat()
        }
        
        logger.info(f"\n🎉 EXPORT COMPLETED! 🎉")
        logger.info(f"📁 {total_files} files exported ({total_size_mb:.2f} MB total)")
        logger.info(f"📂 Output directory: {self.output_dir}")
        
        return results


def main():
    """Main execution function with CLI support."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Export ML-ready features')
    parser.add_argument('--config', type=str, help='Path to configuration file')
    parser.add_argument('--universe-id', type=str, help='Specific universe to export')
    parser.add_argument('--fundamentals', action='store_true', help='Export fundamental features')
    parser.add_argument('--time-series', action='store_true', help='Export time series features')
    parser.add_argument('--combined', action='store_true', help='Create combined dataset')
    parser.add_argument('--all', action='store_true', help='Export all feature types')
    
    args = parser.parse_args()
    
    # Initialize exporter
    exporter = MLFeatureExporter(
        config_path=args.config,
        universe_id=args.universe_id
    )
    
    # Override config with CLI flags
    if args.fundamentals or args.all:
        exporter.include_fundamentals = True
    if args.time_series or args.all:
        exporter.include_time_series = True  
    if args.combined or args.all:
        exporter.create_combined = True
    
    # Execute export
    results = exporter.export_all(args.universe_id)
    
    # Print results
    if results['exports']:
        print(f"\n✅ Successfully exported {results['summary']['total_exports']} datasets")
        for export_type, details in results['exports'].items():
            print(f"  - {export_type}: {details['filepath']}")
    else:
        print("\n❌ No data exported")
        sys.exit(1)


if __name__ == "__main__":
    main()
