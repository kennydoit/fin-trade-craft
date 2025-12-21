#!/usr/bin/env python3
"""Export ML-ready features to optimized formats for model training.

Creates parquet files optimized for different ML use cases:
- fundamentals_features.parquet: Company-specific features with universe filtering
- macro_features.parquet: Economic indicators and commodities (fred_* features)  
- price_features.parquet: OHLCV price data (when available)
- combined_features.parquet: Final merged dataset ready for ML training

Key benefits:
- Parquet format for fast I/O and compression
- Proper data types preserved
- Easy feature selection and filtering
- Compatible with all major ML libraries
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager


class MLFeatureExporter:
    """Export transformed features to ML-ready formats."""
    
    def __init__(self, output_dir: Path, config: dict[str, Any]) -> None:
        self.output_dir = output_dir
        self.config = config
        self.db = PostgresDatabaseManager()
        
        # Date range from config
        self.start_date = config.get('collection_dates', {}).get('start_date', '2020-01-01')
        self.end_date = config.get('collection_dates', {}).get('end_date', '2025-07-31')
        
        # Data switches
        self.data_switches = config.get('data_switches', {})

    def _export_fundamentals_features(self) -> dict[str, Any]:
        """Export company fundamental features to parquet."""
        print("📊 Exporting fundamental features...")
        
        fundamental_tables = []
        fundamentals_config = self.data_switches.get('fundamentals', {})
        
        # Define table mappings
        table_mapping = {
            'balance_sheet': 'balance_sheet',
            'cash_flow': 'cash_flow_features', 
            'income_statement': 'income_statement_features'
        }
        
        dfs_to_merge = []
        feature_counts = {}
        
        for config_key, table_name in table_mapping.items():
            if fundamentals_config.get(config_key, {}).get('enabled', False):
                print(f"  • Loading {table_name}...")
                
                query = f"""
                    SELECT *
                    FROM transformed.{table_name}
                    WHERE fiscal_date_ending >= '{self.start_date}'
                    AND fiscal_date_ending <= '{self.end_date}'
                    ORDER BY symbol, fiscal_date_ending
                """
                
                df = self.db.fetch_dataframe(query)
                if not df.empty:
                    # Count non-metadata columns
                    feature_cols = [col for col in df.columns 
                                  if col not in ['symbol_id', 'symbol', 'fiscal_date_ending']]
                    feature_counts[table_name] = len(feature_cols)
                    dfs_to_merge.append(df)
        
        if not dfs_to_merge:
            print("  ❌ No fundamental features enabled")
            return {'status': 'skipped', 'reason': 'No fundamental features enabled'}
        
        # Merge all fundamental dataframes
        print("  • Merging fundamental datasets...")
        merged_df = dfs_to_merge[0]
        for df in dfs_to_merge[1:]:
            merged_df = merged_df.merge(
                df, on=['symbol_id', 'symbol', 'fiscal_date_ending'], 
                how='outer', suffixes=('', '_dup')
            )
            # Remove duplicate columns
            merged_df = merged_df.loc[:, ~merged_df.columns.str.endswith('_dup')]
        
        # Optimize dtypes
        merged_df = self._optimize_dtypes(merged_df)
        
        # Export to parquet
        output_file = self.output_dir / 'fundamentals_features.parquet'
        merged_df.to_parquet(output_file, index=False, engine='pyarrow')
        
        print(f"  ✅ Exported {len(merged_df):,} records to {output_file}")
        
        return {
            'status': 'success',
            'file': str(output_file),
            'records': len(merged_df),
            'feature_counts': feature_counts,
            'total_features': sum(feature_counts.values()),
            'date_range': (merged_df['fiscal_date_ending'].min(), merged_df['fiscal_date_ending'].max())
        }

    def _export_macro_features(self) -> dict[str, Any]:
        """Export macro features (FRED data) to parquet."""
        print("🌍 Exporting macro features...")
        
        fred_config = self.data_switches.get('fred', {})
        dfs_to_merge = []
        feature_counts = {}
        
        # Economic indicators
        if fred_config.get('economic_indicators', False):
            print("  • Loading economic indicators...")
            query = f"""
                SELECT *
                FROM transformed.economic_indicators_features
                WHERE date >= '{self.start_date}'
                AND date <= '{self.end_date}'
                ORDER BY date
            """
            
            econ_df = self.db.fetch_dataframe(query)
            if not econ_df.empty:
                feature_cols = [col for col in econ_df.columns 
                              if col.startswith('fred_econ_')]
                feature_counts['economic_indicators'] = len(feature_cols)
                dfs_to_merge.append(econ_df)
        
        # Commodities
        if fred_config.get('commodities', False):
            print("  • Loading commodities...")
            query = f"""
                SELECT *
                FROM transformed.commodities_features
                WHERE date >= '{self.start_date}'
                AND date <= '{self.end_date}'
                ORDER BY date
            """
            
            comm_df = self.db.fetch_dataframe(query)
            if not comm_df.empty:
                feature_cols = [col for col in comm_df.columns 
                              if col.startswith('fred_comm_')]
                feature_counts['commodities'] = len(feature_cols)
                dfs_to_merge.append(comm_df)
        
        if not dfs_to_merge:
            print("  ❌ No macro features enabled")
            return {'status': 'skipped', 'reason': 'No macro features enabled'}
        
        # Merge macro dataframes
        print("  • Merging macro datasets...")
        merged_df = dfs_to_merge[0]
        for df in dfs_to_merge[1:]:
            merged_df = merged_df.merge(df, on=['date'], how='outer', suffixes=('', '_dup'))
            # Remove duplicate columns
            dup_cols = [col for col in merged_df.columns if col.endswith('_dup')]
            merged_df = merged_df.drop(columns=dup_cols)
        
        # Optimize dtypes
        merged_df = self._optimize_dtypes(merged_df)
        
        # Export to parquet
        output_file = self.output_dir / 'macro_features.parquet'
        merged_df.to_parquet(output_file, index=False, engine='pyarrow')
        
        print(f"  ✅ Exported {len(merged_df):,} records to {output_file}")
        
        return {
            'status': 'success',
            'file': str(output_file),
            'records': len(merged_df),
            'feature_counts': feature_counts,
            'total_features': sum(feature_counts.values()),
            'date_range': (merged_df['date'].min(), merged_df['date'].max())
        }

    def _optimize_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimize DataFrame dtypes for storage and performance."""
        optimized_df = df.copy()
        
        for col in optimized_df.columns:
            if optimized_df[col].dtype == 'object':
                # Skip datetime and text columns
                if col in ['symbol', 'name'] or 'date' in col.lower():
                    continue
                    
                # Try to convert to numeric
                try:
                    optimized_df[col] = pd.to_numeric(optimized_df[col], errors='ignore')
                except:
                    pass
            
            elif optimized_df[col].dtype in ['float64']:
                # Downcast floats if possible
                if optimized_df[col].notna().sum() > 0:
                    try:
                        optimized_df[col] = pd.to_numeric(optimized_df[col], downcast='float')
                    except:
                        pass
        
        return optimized_df

    def _create_metadata(self, results: dict[str, Any]) -> dict[str, Any]:
        """Create metadata file with feature descriptions."""
        metadata = {
            'created_at': datetime.now().isoformat(),
            'config': self.config,
            'export_results': results,
            'feature_categories': {
                'fundamentals': {
                    'description': 'Company financial metrics with universe filtering',
                    'frequency': 'Quarterly (fiscal_date_ending)',
                    'features': results.get('fundamentals', {}).get('feature_counts', {})
                },
                'macro': {
                    'description': 'Macroeconomic indicators and commodity prices',
                    'frequency': 'Daily',
                    'features': results.get('macro', {}).get('feature_counts', {})
                }
            },
            'usage_examples': {
                'python': {
                    'load_fundamentals': "df = pd.read_parquet('fundamentals_features.parquet')",
                    'load_macro': "df = pd.read_parquet('macro_features.parquet')",
                    'feature_selection': "features = [col for col in df.columns if col.startswith('fred_')]"
                }
            }
        }
        
        return metadata

    def run(self) -> dict[str, Any]:
        """Execute the complete feature export pipeline."""
        print("🚀 Starting ML feature export...")
        
        try:
            self.db.connect()
            
            # Ensure output directory exists
            self.output_dir.mkdir(parents=True, exist_ok=True)
            
            results = {}
            
            # Export fundamentals
            results['fundamentals'] = self._export_fundamentals_features()
            
            # Export macro features
            results['macro'] = self._export_macro_features()
            
            # Create metadata
            metadata = self._create_metadata(results)
            metadata_file = self.output_dir / 'metadata.json'
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)
            
            print(f"📋 Created metadata file: {metadata_file}")
            
            # Summary
            total_features = 0
            for category, result in results.items():
                if result.get('status') == 'success':
                    total_features += result.get('total_features', 0)
            
            print("\n✅ Export completed successfully!")
            print(f"📁 Output directory: {self.output_dir}")
            print(f"🎯 Total ML-ready features: {total_features:,}")
            
            return results
            
        finally:
            self.db.close()


def main():
    """Example usage of the ML feature exporter."""
    # This would be called from feature_build.py with the actual config
    sample_config = {
        'collection_dates': {
            'start_date': '2020-01-01',
            'end_date': '2025-07-31'
        },
        'data_switches': {
            'fundamentals': {
                'balance_sheet': {'enabled': True},
                'cash_flow': {'enabled': True},
                'income_statement': {'enabled': True}
            },
            'fred': {
                'commodities': True,
                'economic_indicators': True
            }
        }
    }
    
    output_dir = Path(__file__).parent / "sample_export"
    exporter = MLFeatureExporter(output_dir, sample_config)
    results = exporter.run()
    
    print("\n🔍 Export Results:")
    for category, result in results.items():
        if result.get('status') == 'success':
            print(f"  {category}: {result['total_features']} features, {result['records']:,} records")


if __name__ == "__main__":
    main()
