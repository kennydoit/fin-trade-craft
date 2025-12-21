"""Transform balance sheet data - Simplified append-only approach.

Quarterly balance sheet data is immutable once published. This transformer:
1. Copies raw.balance_sheet → transforms.balance_sheet (one-time init)
2. Adds feature columns with NULL values
3. Processes unprocessed records (WHERE feature columns IS NULL)
4. Future runs only process new quarters

Usage:
    # One-time initialization
    python transform_balance_sheet.py --init
    
    # Process unprocessed records (run after new data arrives)
    python transform_balance_sheet.py --process
"""

import sys
import logging
import argparse
from pathlib import Path

import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BalanceSheetTransformer:
    """Simplified balance sheet transformer with embedded watermark."""

    def __init__(self):
        self.db = PostgresDatabaseManager()

    def _safe_div(self, num, denom, epsilon=1e-6):
        """Safe division avoiding divide by zero."""
        return num / (denom + epsilon)

    def initialize(self):
        """Initialize transforms.balance_sheet from raw data."""
        logger.info("Initializing transforms.balance_sheet...")
        
        self.db.connect()
        try:
            # Create table from raw
            logger.info("Creating base table from raw.balance_sheet...")
            self.db.execute_query("DROP TABLE IF EXISTS transforms.balance_sheet CASCADE")
            
            self.db.execute_query("""
                CREATE TABLE transforms.balance_sheet AS
                SELECT 
                    bs.*,
                    ov.sector,
                    ov.industry
                FROM raw.balance_sheet bs
                LEFT JOIN raw.company_overview ov ON bs.symbol = ov.symbol
                WHERE bs.period_type = 'quarterly'
                  AND bs.symbol_id IS NOT NULL
            """)
            
            # Add primary key
            self.db.execute_query("""
                ALTER TABLE transforms.balance_sheet 
                ADD PRIMARY KEY (symbol_id, fiscal_date_ending)
            """)
            
            # Add feature columns (all start as NULL)
            logger.info("Adding feature columns...")
            feature_cols = [
                'fbs_current_ratio', 'fbs_quick_ratio', 'fbs_cash_ratio', 'fbs_working_capital',
                'fbs_debt_to_equity', 'fbs_current_debt_ratio', 'fbs_long_term_debt_ratio', 'fbs_debt_to_assets',
                'fbs_tangible_asset_ratio', 'fbs_intangibles_share', 'fbs_ppe_intensity', 'fbs_cash_to_assets',
                'fbs_book_value_per_share', 'fbs_retained_earnings_ratio', 'fbs_treasury_stock_effect',
                'fbs_balance_sheet_leverage', 'fbs_financial_leverage', 'fbs_interest_coverage_proxy', 'fbs_asset_turnover',
                'fbs_current_ratio_qoq_pct', 'fbs_quick_ratio_qoq_pct', 'fbs_working_capital_qoq_pct',
                'fbs_debt_to_equity_qoq_pct', 'fbs_current_ratio_yoy_pct', 'fbs_quick_ratio_yoy_pct',
                'fbs_current_ratio_volatility', 'fbs_quick_ratio_volatility',
                'fbs_current_ratio_sector_rank', 'fbs_quick_ratio_sector_rank',
                'fbs_current_ratio_industry_rank', 'fbs_quick_ratio_industry_rank',
                'fbs_liquidity_shock_flag'
            ]
            
            for col in feature_cols:
                col_type = 'INTEGER' if 'flag' in col else 'NUMERIC'
                self.db.execute_query(f"ALTER TABLE transforms.balance_sheet ADD COLUMN {col} {col_type}")
            
            # Add processed timestamp
            self.db.execute_query("""
                ALTER TABLE transforms.balance_sheet 
                ADD COLUMN processed_at TIMESTAMPTZ
            """)
            
            # Create index on unprocessed
            self.db.execute_query("""
                CREATE INDEX idx_bs_unprocessed 
                ON transforms.balance_sheet (symbol_id, fiscal_date_ending) 
                WHERE processed_at IS NULL
            """)
            
            count = self.db.fetch_query("SELECT COUNT(*) FROM transforms.balance_sheet")[0][0]
            logger.info(f"✅ Initialized with {count:,} quarterly records")
            
        finally:
            self.db.close()

    def sync(self):
        """Sync new quarters from raw.balance_sheet into transforms."""
        logger.info("Syncing new quarters from raw.balance_sheet...")
        
        self.db.connect()
        try:
            inserted = self.db.execute_query("""
                INSERT INTO transforms.balance_sheet
                SELECT 
                    bs.*,
                    ov.sector,
                    ov.industry,
                    NULL as processed_at
                FROM raw.balance_sheet bs
                LEFT JOIN raw.company_overview ov ON bs.symbol = ov.symbol
                WHERE bs.period_type = 'quarterly'
                  AND bs.symbol_id IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM transforms.balance_sheet t
                      WHERE t.symbol_id = bs.symbol_id 
                        AND t.fiscal_date_ending = bs.fiscal_date_ending
                  )
            """)
            
            logger.info(f"✅ Synced {inserted:,} new quarters")
            
        finally:
            self.db.close()

    def process(self):
        """Process unprocessed records."""
        logger.info("=" * 80)
        logger.info("PROCESSING UNPROCESSED BALANCE SHEET RECORDS")
        logger.info("=" * 80)
        
        self.db.connect()
        try:
            # Count unprocessed
            unprocessed = self.db.fetch_query("""
                SELECT COUNT(*) FROM transforms.balance_sheet 
                WHERE processed_at IS NULL
            """)[0][0]
            
            if unprocessed == 0:
                logger.info("No unprocessed records")
                return
            
            logger.info(f"Found {unprocessed:,} unprocessed records")
            logger.info("Fetching data...")
            
            # Fetch unprocessed with income statement data
            query = """
                SELECT 
                    t.symbol_id, t.symbol, t.fiscal_date_ending,
                    t.total_assets, t.total_current_assets, t.cash_and_short_term_investments,
                    t.cash_and_cash_equivalents, t.current_net_receivables,
                    t.total_current_liabilities, t.total_liabilities, t.current_debt,
                    t.long_term_debt, t.total_shareholder_equity, t.retained_earnings,
                    t.treasury_stock, t.goodwill, t.intangible_assets,
                    t.property_plant_equipment, t.common_stock_shares_outstanding,
                    t.sector, t.industry,
                    inc.ebit, inc.total_revenue
                FROM transforms.balance_sheet t
                LEFT JOIN raw.income_statement inc 
                    ON t.symbol = inc.symbol 
                    AND t.fiscal_date_ending = inc.fiscal_date_ending
                    AND inc.period_type = 'quarterly'
                WHERE t.processed_at IS NULL
                ORDER BY t.symbol_id, t.fiscal_date_ending
            """
            
            df = pd.read_sql(query, self.db.connection)
            logger.info(f"Loaded {len(df):,} records")
            
            # Compute features
            logger.info("Computing features...")
            df = self._compute_all_features(df)
            
            # Update in batches
            logger.info("Updating database...")
            self._batch_update(df)
            
            logger.info("=" * 80)
            logger.info(f"✅ Processed {len(df):,} records")
            logger.info("=" * 80)
            
        finally:
            self.db.close()

    def _compute_all_features(self, df):
        """Compute all features at once."""
        df = df.copy()
        
        # Basic ratios
        df['fbs_current_ratio'] = self._safe_div(df['total_current_assets'], df['total_current_liabilities'])
        df['fbs_quick_ratio'] = self._safe_div(
            df['cash_and_short_term_investments'] + df['current_net_receivables'],
            df['total_current_liabilities']
        )
        df['fbs_cash_ratio'] = self._safe_div(df['cash_and_cash_equivalents'], df['total_current_liabilities'])
        df['fbs_working_capital'] = df['total_current_assets'] - df['total_current_liabilities']
        
        df['fbs_debt_to_equity'] = self._safe_div(df['total_liabilities'], df['total_shareholder_equity'])
        df['fbs_current_debt_ratio'] = self._safe_div(df['current_debt'], df['total_assets'])
        df['fbs_long_term_debt_ratio'] = self._safe_div(df['long_term_debt'], df['total_assets'])
        df['fbs_debt_to_assets'] = self._safe_div(df['total_liabilities'], df['total_assets'])
        
        df['fbs_tangible_asset_ratio'] = self._safe_div(
            df['total_assets'] - df['goodwill'].fillna(0) - df['intangible_assets'].fillna(0),
            df['total_assets']
        )
        df['fbs_intangibles_share'] = self._safe_div(
            df['goodwill'].fillna(0) + df['intangible_assets'].fillna(0),
            df['total_assets']
        )
        df['fbs_ppe_intensity'] = self._safe_div(df['property_plant_equipment'], df['total_assets'])
        df['fbs_cash_to_assets'] = self._safe_div(df['cash_and_short_term_investments'], df['total_assets'])
        
        df['fbs_book_value_per_share'] = self._safe_div(
            df['total_shareholder_equity'],
            df['common_stock_shares_outstanding']
        )
        df['fbs_retained_earnings_ratio'] = self._safe_div(df['retained_earnings'], df['total_shareholder_equity'])
        df['fbs_treasury_stock_effect'] = self._safe_div(df['treasury_stock'].fillna(0), df['total_shareholder_equity'])
        
        # Risk indicators
        df['fbs_balance_sheet_leverage'] = self._safe_div(df['total_liabilities'], df['total_assets'])
        df['fbs_financial_leverage'] = self._safe_div(df['total_assets'], df['total_shareholder_equity'])
        df['fbs_interest_coverage_proxy'] = self._safe_div(
            df['ebit'].fillna(0),
            df['current_debt'].fillna(0) + df['long_term_debt'].fillna(0)
        )
        df['fbs_asset_turnover'] = self._safe_div(df['total_revenue'].fillna(0), df['total_assets'])
        
        # Growth metrics (QoQ, YoY) - limited set for speed
        df = df.sort_values(['symbol_id', 'fiscal_date_ending'])
        df['fbs_current_ratio_qoq_pct'] = df.groupby('symbol_id')['fbs_current_ratio'].pct_change(1, fill_method=None)
        df['fbs_quick_ratio_qoq_pct'] = df.groupby('symbol_id')['fbs_quick_ratio'].pct_change(1, fill_method=None)
        df['fbs_working_capital_qoq_pct'] = df.groupby('symbol_id')['fbs_working_capital'].pct_change(1, fill_method=None)
        df['fbs_debt_to_equity_qoq_pct'] = df.groupby('symbol_id')['fbs_debt_to_equity'].pct_change(1, fill_method=None)
        
        df['fbs_current_ratio_yoy_pct'] = df.groupby('symbol_id')['fbs_current_ratio'].pct_change(4, fill_method=None)
        df['fbs_quick_ratio_yoy_pct'] = df.groupby('symbol_id')['fbs_quick_ratio'].pct_change(4, fill_method=None)
        
        # Volatility
        df['fbs_current_ratio_volatility'] = df.groupby('symbol_id')['fbs_current_ratio'].rolling(4, min_periods=2).std().reset_index(level=0, drop=True)
        df['fbs_quick_ratio_volatility'] = df.groupby('symbol_id')['fbs_quick_ratio'].rolling(4, min_periods=2).std().reset_index(level=0, drop=True)
        
        # Rankings
        df['fbs_current_ratio_sector_rank'] = df.groupby(['fiscal_date_ending', 'sector'])['fbs_current_ratio'].rank(pct=True)
        df['fbs_quick_ratio_sector_rank'] = df.groupby(['fiscal_date_ending', 'sector'])['fbs_quick_ratio'].rank(pct=True)
        df['fbs_current_ratio_industry_rank'] = df.groupby(['fiscal_date_ending', 'industry'])['fbs_current_ratio'].rank(pct=True)
        df['fbs_quick_ratio_industry_rank'] = df.groupby(['fiscal_date_ending', 'industry'])['fbs_quick_ratio'].rank(pct=True)
        
        # Flags
        df['fbs_liquidity_shock_flag'] = (df['fbs_current_ratio_qoq_pct'] < -0.2).astype(int)
        
        return df

    def _batch_update(self, df):
        """Update transforms table in batches."""
        feature_cols = [c for c in df.columns if c.startswith('fbs_')]
        
        # Replace inf with None
        for col in feature_cols:
            if df[col].dtype in ['float64', 'int64']:
                df[col] = df[col].replace([np.inf, -np.inf], None)
        
        # Build update query
        set_clause = ', '.join([f"{col} = %s" for col in feature_cols])
        update_sql = f"""
            UPDATE transforms.balance_sheet
            SET {set_clause}, processed_at = NOW()
            WHERE symbol_id = %s AND fiscal_date_ending = %s
        """
        
        # Prepare records
        records = []
        for _, row in df.iterrows():
            values = [None if pd.isna(row[col]) else row[col] for col in feature_cols]
            values.extend([row['symbol_id'], row['fiscal_date_ending']])
            records.append(tuple(values))
        
        # Execute batch
        self.db.execute_many(update_sql, records)
        logger.info(f"Updated {len(records):,} records")


def main():
    parser = argparse.ArgumentParser(description='Balance sheet transformer')
    parser.add_argument('--init', action='store_true', help='Initialize table')
    parser.add_argument('--sync', action='store_true', help='Sync new quarters from raw')
    parser.add_argument('--process', action='store_true', help='Process unprocessed records')
    
    args = parser.parse_args()
    
    if not (args.init or args.sync or args.process):
        parser.error("Must specify --init, --sync, or --process")
    
    transformer = BalanceSheetTransformer()
    
    if args.init:
        transformer.initialize()
    
    if args.sync:
        transformer.sync()
    
    if args.process:
        transformer.process()


if __name__ == "__main__":
    main()
