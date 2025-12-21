"""Transform cash flow data - Simplified append-only approach.

Quarterly cash flow data is immutable once published. This transformer:
1. Copies raw.cash_flow → transforms.cash_flow (one-time init)
2. Adds feature columns with NULL values
3. Processes unprocessed records (WHERE feature columns IS NULL)
4. Future runs only process new quarters

Usage:
    # One-time initialization
    python transform_cash_flow.py --init
    
    # Process unprocessed records (run after new data arrives)
    python transform_cash_flow.py --process
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


class CashFlowTransformer:
    """Simplified cash flow transformer with embedded watermark."""

    def __init__(self):
        self.db = PostgresDatabaseManager()

    def _safe_div(self, num, denom, epsilon=1e-6):
        """Safe division avoiding divide by zero."""
        return num / (denom + epsilon)

    def initialize(self):
        """Initialize transforms.cash_flow from raw data."""
        logger.info("Initializing transforms.cash_flow...")
        
        self.db.connect()
        try:
            # Create table from raw
            logger.info("Creating base table from raw.cash_flow...")
            self.db.execute_query("DROP TABLE IF EXISTS transforms.cash_flow CASCADE")
            
            self.db.execute_query("""
                CREATE TABLE transforms.cash_flow AS
                SELECT 
                    cf.*,
                    ov.sector,
                    ov.industry
                FROM raw.cash_flow cf
                LEFT JOIN raw.company_overview ov ON cf.symbol = ov.symbol
                WHERE cf.period_type = 'quarterly'
                  AND cf.symbol_id IS NOT NULL
            """)
            
            # Add primary key
            self.db.execute_query("""
                ALTER TABLE transforms.cash_flow 
                ADD PRIMARY KEY (symbol_id, fiscal_date_ending)
            """)
            
            # Add feature columns (all start as NULL)
            logger.info("Adding feature columns...")
            feature_cols = [
                # Cash flow strength
                'fcf_free_cash_flow', 'fcf_ocf_to_capex_ratio', 'fcf_cash_conversion_ratio',
                'fcf_capex_intensity', 'fcf_operating_cf_margin',
                
                # Investment patterns
                'fcf_investment_intensity', 'fcf_capex_to_investment_ratio',
                
                # Financing strategy
                'fcf_debt_financing_ratio', 'fcf_equity_financing_ratio', 
                'fcf_dividend_payout_ratio', 'fcf_share_buyback_ratio',
                'fcf_net_financing_activity',
                
                # Cash flexibility
                'fcf_cash_change_volatility', 'fcf_operating_cf_volatility',
                
                # Growth & trends
                'fcf_operating_cf_qoq_pct', 'fcf_free_cash_flow_qoq_pct',
                'fcf_operating_cf_yoy_pct', 'fcf_free_cash_flow_yoy_pct',
                'fcf_capex_qoq_pct',
                
                # Rankings
                'fcf_operating_cf_sector_rank', 'fcf_free_cash_flow_sector_rank',
                'fcf_operating_cf_industry_rank', 'fcf_free_cash_flow_industry_rank',
                
                # Flags
                'fcf_negative_free_cash_flow_flag', 'fcf_cash_burn_flag'
            ]
            
            for col in feature_cols:
                col_type = 'INTEGER' if 'flag' in col else 'NUMERIC'
                self.db.execute_query(f"ALTER TABLE transforms.cash_flow ADD COLUMN {col} {col_type}")
            
            # Add processed timestamp
            self.db.execute_query("""
                ALTER TABLE transforms.cash_flow 
                ADD COLUMN processed_at TIMESTAMPTZ
            """)
            
            # Create index on unprocessed
            self.db.execute_query("""
                CREATE INDEX idx_cf_unprocessed 
                ON transforms.cash_flow (symbol_id, fiscal_date_ending) 
                WHERE processed_at IS NULL
            """)
            
            count = self.db.fetch_query("SELECT COUNT(*) FROM transforms.cash_flow")[0][0]
            logger.info(f"✅ Initialized with {count:,} quarterly records")
            
        finally:
            self.db.close()

    def sync(self):
        """Sync new quarters from raw.cash_flow into transforms."""
        logger.info("Syncing new quarters from raw.cash_flow...")
        
        self.db.connect()
        try:
            inserted = self.db.execute_query("""
                INSERT INTO transforms.cash_flow
                SELECT 
                    cf.*,
                    NULL as processed_at
                FROM raw.cash_flow cf
                WHERE cf.period_type = 'quarterly'
                  AND cf.symbol_id IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM transforms.cash_flow t
                      WHERE t.symbol_id = cf.symbol_id 
                        AND t.fiscal_date_ending = cf.fiscal_date_ending
                  )
            """)
            
            logger.info(f"✅ Synced {inserted:,} new quarters")
            
        finally:
            self.db.close()

    def process(self):
        """Process unprocessed records."""
        logger.info("=" * 80)
        logger.info("PROCESSING UNPROCESSED CASH FLOW RECORDS")
        logger.info("=" * 80)
        
        self.db.connect()
        try:
            # Count unprocessed
            unprocessed = self.db.fetch_query("""
                SELECT COUNT(*) FROM transforms.cash_flow 
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
                    t.operating_cashflow,
                    t.capital_expenditures,
                    t.cashflow_from_investment,
                    t.cashflow_from_financing,
                    t.change_in_cash_and_cash_equivalents,
                    t.proceeds_from_issuance_of_long_term_debt,
                    t.proceeds_from_issuance_of_common_stock,
                    t.dividend_payout,
                    t.payments_for_repurchase_of_common_stock,
                    t.sector, t.industry,
                    inc.net_income,
                    inc.total_revenue
                FROM transforms.cash_flow t
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
        
        # Free cash flow (most important cash flow metric)
        df['fcf_free_cash_flow'] = df['operating_cashflow'].fillna(0) - df['capital_expenditures'].fillna(0).abs()
        
        # Cash flow strength & coverage
        df['fcf_ocf_to_capex_ratio'] = self._safe_div(
            df['operating_cashflow'],
            df['capital_expenditures'].abs()
        )
        df['fcf_cash_conversion_ratio'] = self._safe_div(
            df['operating_cashflow'],
            df['net_income'].fillna(0)
        )
        df['fcf_capex_intensity'] = self._safe_div(
            df['capital_expenditures'].abs(),
            df['operating_cashflow']
        )
        df['fcf_operating_cf_margin'] = self._safe_div(
            df['operating_cashflow'],
            df['total_revenue'].fillna(0)
        )
        
        # Investment patterns
        df['fcf_investment_intensity'] = self._safe_div(
            df['cashflow_from_investment'].abs(),
            df['operating_cashflow']
        )
        df['fcf_capex_to_investment_ratio'] = self._safe_div(
            df['capital_expenditures'].abs(),
            df['cashflow_from_investment'].abs()
        )
        
        # Financing strategy
        total_financing = (
            df['proceeds_from_issuance_of_long_term_debt'].fillna(0) +
            df['proceeds_from_issuance_of_common_stock'].fillna(0)
        )
        
        df['fcf_debt_financing_ratio'] = self._safe_div(
            df['proceeds_from_issuance_of_long_term_debt'].fillna(0),
            total_financing
        )
        df['fcf_equity_financing_ratio'] = self._safe_div(
            df['proceeds_from_issuance_of_common_stock'].fillna(0),
            total_financing
        )
        df['fcf_dividend_payout_ratio'] = self._safe_div(
            df['dividend_payout'].fillna(0).abs(),
            df['operating_cashflow']
        )
        df['fcf_share_buyback_ratio'] = self._safe_div(
            df['payments_for_repurchase_of_common_stock'].fillna(0).abs(),
            df['operating_cashflow']
        )
        df['fcf_net_financing_activity'] = df['cashflow_from_financing'].fillna(0)
        
        # Sort for time-based features
        df = df.sort_values(['symbol_id', 'fiscal_date_ending'])
        
        # Growth metrics (QoQ, YoY)
        df['fcf_operating_cf_qoq_pct'] = df.groupby('symbol_id')['operating_cashflow'].pct_change(1, fill_method=None)
        df['fcf_free_cash_flow_qoq_pct'] = df.groupby('symbol_id')['fcf_free_cash_flow'].pct_change(1, fill_method=None)
        df['fcf_operating_cf_yoy_pct'] = df.groupby('symbol_id')['operating_cashflow'].pct_change(4, fill_method=None)
        df['fcf_free_cash_flow_yoy_pct'] = df.groupby('symbol_id')['fcf_free_cash_flow'].pct_change(4, fill_method=None)
        df['fcf_capex_qoq_pct'] = df.groupby('symbol_id')['capital_expenditures'].pct_change(1, fill_method=None)
        
        # Volatility
        df['fcf_cash_change_volatility'] = df.groupby('symbol_id')['change_in_cash_and_cash_equivalents'].rolling(4, min_periods=2).std().reset_index(level=0, drop=True)
        df['fcf_operating_cf_volatility'] = df.groupby('symbol_id')['operating_cashflow'].rolling(4, min_periods=2).std().reset_index(level=0, drop=True)
        
        # Rankings
        df['fcf_operating_cf_sector_rank'] = df.groupby(['fiscal_date_ending', 'sector'])['operating_cashflow'].rank(pct=True)
        df['fcf_free_cash_flow_sector_rank'] = df.groupby(['fiscal_date_ending', 'sector'])['fcf_free_cash_flow'].rank(pct=True)
        df['fcf_operating_cf_industry_rank'] = df.groupby(['fiscal_date_ending', 'industry'])['operating_cashflow'].rank(pct=True)
        df['fcf_free_cash_flow_industry_rank'] = df.groupby(['fiscal_date_ending', 'industry'])['fcf_free_cash_flow'].rank(pct=True)
        
        # Flags
        df['fcf_negative_free_cash_flow_flag'] = (df['fcf_free_cash_flow'] < 0).astype(int)
        df['fcf_cash_burn_flag'] = (
            (df['fcf_free_cash_flow'] < 0) & 
            (df['change_in_cash_and_cash_equivalents'] < 0)
        ).astype(int)
        
        return df

    def _batch_update(self, df):
        """Update transforms table in batches."""
        feature_cols = [c for c in df.columns if c.startswith('fcf_')]
        
        # Replace inf with None
        for col in feature_cols:
            if df[col].dtype in ['float64', 'int64']:
                df[col] = df[col].replace([np.inf, -np.inf], None)
        
        # Build update query
        set_clause = ', '.join([f"{col} = %s" for col in feature_cols])
        update_sql = f"""
            UPDATE transforms.cash_flow
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
    parser = argparse.ArgumentParser(description='Cash flow transformer')
    parser.add_argument('--init', action='store_true', help='Initialize table')
    parser.add_argument('--sync', action='store_true', help='Sync new quarters from raw')
    parser.add_argument('--process', action='store_true', help='Process unprocessed records')
    
    args = parser.parse_args()
    
    if not (args.init or args.sync or args.process):
        parser.error("Must specify --init, --sync, or --process")
    
    transformer = CashFlowTransformer()
    
    if args.init:
        transformer.initialize()
    
    if args.sync:
        transformer.sync()
    
    if args.process:
        transformer.process()


if __name__ == "__main__":
    main()
