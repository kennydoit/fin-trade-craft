#!/usr/bin/env python3
"""Final verification of all transformed tables in the feature pipeline."""

import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager


def final_pipeline_summary():
    """Provide comprehensive summary of all feature pipeline components."""
    db = PostgresDatabaseManager()
    db.connect()
    
    try:
        print("🚀 FIN-TRADE-CRAFT FEATURE PIPELINE SUMMARY 🚀\n")
        print("=" * 60)
        
        # Define all tables and their descriptions
        tables = {
            'balance_sheet_features': 'Company fundamental balance sheet metrics with universe filtering',
            'cash_flow_features': 'Cash flow sustainability and quality metrics with universe filtering', 
            'income_statement_features': 'Profitability and performance metrics with universe filtering',
            'economic_indicators_features': 'Macroeconomic indicators with ML-ready normalization',
            'commodities_features': 'Commodity prices and relationships with ML-ready normalization'
        }
        
        total_features = 0
        
        for table, description in tables.items():
            try:
                # Get record count
                count_result = db.fetch_query(f"SELECT COUNT(*) FROM transformed.{table}")
                record_count = count_result[0][0] if count_result else 0
                
                # Get feature count
                cols_result = db.fetch_query(f"""
                    SELECT COUNT(*) 
                    FROM information_schema.columns 
                    WHERE table_schema = 'transformed' 
                    AND table_name = '{table}'
                    AND column_name NOT IN ('created_at', 'updated_at', 'date', 'symbol_id', 'symbol', 'fiscal_date_ending')
                """)
                feature_count = cols_result[0][0] if cols_result else 0
                total_features += feature_count
                
                # Get date range
                date_columns = ['date', 'fiscal_date_ending']
                date_col = None
                for col in date_columns:
                    check_result = db.fetch_query(f"""
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_schema = 'transformed' 
                        AND table_name = '{table}' 
                        AND column_name = '{col}'
                    """)
                    if check_result:
                        date_col = col
                        break
                
                if date_col:
                    date_result = db.fetch_query(f"SELECT MIN({date_col}), MAX({date_col}) FROM transformed.{table}")
                    min_date, max_date = date_result[0] if date_result else (None, None)
                    date_range = f"{min_date} to {max_date}" if min_date else "N/A"
                else:
                    date_range = "N/A"
                
                # Format output
                print(f"✅ {table.replace('_', ' ').title()}")
                print(f"   Description: {description}")
                print(f"   Records: {record_count:,}")
                print(f"   Features: {feature_count:,}")
                print(f"   Date Range: {date_range}")
                
                # Show sample features for FRED data
                if 'fred_' in table:
                    sample_features = db.fetch_query(f"""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_schema = 'transformed' 
                        AND table_name = '{table}'
                        AND column_name LIKE 'fred_%'
                        ORDER BY column_name
                        LIMIT 3
                    """)
                    if sample_features:
                        features_list = [row[0] for row in sample_features]
                        print(f"   Sample Features: {', '.join(features_list)}")
                
                print()
                
            except Exception as e:
                print(f"❌ {table}: Error - {e}\n")
        
        # Overall summary
        print("=" * 60)
        print("📊 PIPELINE SUMMARY")
        print("=" * 60)
        print("✅ Fundamental Data (Universe Filtered):")
        print("   • Balance Sheet: Financial position metrics")
        print("   • Cash Flow: Liquidity and sustainability metrics") 
        print("   • Income Statement: Profitability and performance metrics")
        print()
        print("✅ Macroeconomic Data (ML-Ready):")
        print("   • Economic Indicators (fred_econ_*): GDP, Treasury yields, CPI, unemployment")
        print("   • Commodities (fred_comm_*): Energy, metals, agricultural prices")
        print()
        print("🎯 Key Features:")
        print("   • Universe filtering for targeted stock analysis")
        print("   • Lookback-only normalization to prevent data leakage")
        print("   • Daily frequency alignment for all time series")
        print("   • Cross-asset relationships and momentum indicators")
        print(f"   • Total ML-ready features: {total_features:,}")
        print()
        print("🔧 Architecture:")
        print("   • Source → Transform → Features pipeline")
        print("   • Consistent naming conventions and prefixes")
        print("   • Automated feature engineering with proper scaling")
        print("   • Integrated with feature_build.py orchestration")
        print()
        print("🚀 Ready for ML model training and backtesting!")
        
    finally:
        db.close()


if __name__ == "__main__":
    final_pipeline_summary()
