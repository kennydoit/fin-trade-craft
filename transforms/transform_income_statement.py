"""Transform income statement data - Simplified append-only approach.

Quarterly income statement data is immutable once published. This transformer:
1. Copies raw.income_statement → transforms.income_statement (one-time init)
2. Adds feature columns with NULL values
3. Processes unprocessed records (WHERE feature columns IS NULL)
4. Future runs only process new quarters

Usage:
    # One-time initialization
    python transform_income_statement.py --init
    
    # Process unprocessed records (run after new data arrives)
    python transform_income_statement.py --process
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


class IncomeStatementTransformer:
    """Simplified income statement transformer with embedded watermark."""

    def __init__(self):
        self.db = PostgresDatabaseManager()

    def _safe_div(self, num, denom, epsilon=1e-6):
        """Safe division avoiding divide by zero."""
        return num / (denom + epsilon)

    def initialize(self):
        """Initialize transforms.income_statement from raw data."""
        logger.info("Initializing transforms.income_statement...")
        
        self.db.connect()
        try:
            # Create table from raw
            logger.info("Creating base table from raw.income_statement...")
            self.db.execute_query("DROP TABLE IF EXISTS transforms.income_statement CASCADE")
            
            self.db.execute_query("""
                CREATE TABLE transforms.income_statement AS
                SELECT 
                    inc.*,
                    ov.sector,
                    ov.industry
                FROM raw.income_statement inc
                LEFT JOIN raw.company_overview ov ON inc.symbol = ov.symbol
                WHERE inc.period_type = 'quarterly'
                  AND inc.symbol_id IS NOT NULL
            """)
            
            # Add primary key
            self.db.execute_query("""
                ALTER TABLE transforms.income_statement 
                ADD PRIMARY KEY (symbol_id, fiscal_date_ending)
            """)
            
            # Add feature columns (all start as NULL)
            logger.info("Adding feature columns...")
            feature_cols = [
                # Core profitability margins
                'fis_gross_margin', 'fis_operating_margin', 'fis_net_margin', 
                'fis_ebit_margin', 'fis_ebitda_margin',
                
                # Expense control
                'fis_sga_ratio', 'fis_rd_ratio', 'fis_opex_ratio',
                
                # Leverage & interest
                'fis_interest_coverage', 'fis_interest_burden', 'fis_financial_leverage_effect',
                
                # Tax efficiency
                'fis_effective_tax_rate', 'fis_tax_burden',
                
                # Quality metrics
                'fis_continuing_ops_ratio', 'fis_comprehensive_income_ratio',
                
                # Cash proxies
                'fis_ebitda_to_revenue', 'fis_depreciation_ratio',
                
                # Growth (QoQ, YoY)
                'fis_revenue_qoq_pct', 'fis_net_income_qoq_pct', 'fis_ebitda_qoq_pct',
                'fis_revenue_yoy_pct', 'fis_net_income_yoy_pct', 'fis_ebitda_yoy_pct',
                
                # Volatility
                'fis_revenue_volatility', 'fis_net_income_volatility', 'fis_margin_volatility',
                
                # Rankings
                'fis_net_margin_sector_rank', 'fis_operating_margin_sector_rank',
                'fis_net_margin_industry_rank', 'fis_operating_margin_industry_rank',
                
                # Flags
                'fis_negative_net_income_flag', 'fis_revenue_decline_flag'
            ]
            
            for col in feature_cols:
                col_type = 'INTEGER' if 'flag' in col else 'NUMERIC'
                self.db.execute_query(f"ALTER TABLE transforms.income_statement ADD COLUMN {col} {col_type}")
            
            # Add processed timestamp
            self.db.execute_query("""
                ALTER TABLE transforms.income_statement 
                ADD COLUMN processed_at TIMESTAMPTZ
            """)
            
            # Create index on unprocessed
            self.db.execute_query("""
                CREATE INDEX idx_is_unprocessed 
                ON transforms.income_statement (symbol_id, fiscal_date_ending) 
                WHERE processed_at IS NULL
            """)
            
            count = self.db.fetch_query("SELECT COUNT(*) FROM transforms.income_statement")[0][0]
            logger.info(f"✅ Initialized with {count:,} quarterly records")
            
        finally:
            self.db.close()

    def sync(self):
        """Sync new quarters from raw.income_statement into transforms."""
        logger.info("Syncing new quarters from raw.income_statement...")
        
        self.db.connect()
        try:
            inserted = self.db.execute_query("""
                INSERT INTO transforms.income_statement
                SELECT 
                    inc.*,
                    NULL as processed_at
                FROM raw.income_statement inc
                WHERE inc.period_type = 'quarterly'
                  AND inc.symbol_id IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM transforms.income_statement t
                      WHERE t.symbol_id = inc.symbol_id 
                        AND t.fiscal_date_ending = inc.fiscal_date_ending
                  )
            """)
            
            logger.info(f"✅ Synced {inserted:,} new quarters")
            
        finally:
            self.db.close()

    def process(self):
        """Process unprocessed records."""
        logger.info("=" * 80)
        logger.info("PROCESSING UNPROCESSED INCOME STATEMENT RECORDS")
        logger.info("=" * 80)
        
        self.db.connect()
        try:
            # Count unprocessed
            unprocessed = self.db.fetch_query("""
                SELECT COUNT(*) FROM transforms.income_statement 
                WHERE processed_at IS NULL
            """)[0][0]
            
            if unprocessed == 0:
                logger.info("No unprocessed records")
                return
            
            logger.info(f"Found {unprocessed:,} unprocessed records")
            logger.info("Fetching data...")
            
            # Fetch unprocessed data
            query = """
                SELECT 
                    symbol_id, symbol, fiscal_date_ending,
                    total_revenue, gross_profit, operating_income, net_income,
                    ebit, ebitda,
                    selling_general_and_administrative, research_and_development,
                    operating_expenses,
                    interest_expense, interest_and_debt_expense, net_interest_income,
                    income_tax_expense, income_before_tax,
                    net_income_from_continuing_operations,
                    comprehensive_income_net_of_tax,
                    depreciation_and_amortization,
                    sector, industry
                FROM transforms.income_statement
                WHERE processed_at IS NULL
                ORDER BY symbol_id, fiscal_date_ending
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
        
        # Core profitability margins
        df['fis_gross_margin'] = self._safe_div(df['gross_profit'], df['total_revenue'])
        df['fis_operating_margin'] = self._safe_div(df['operating_income'], df['total_revenue'])
        df['fis_net_margin'] = self._safe_div(df['net_income'], df['total_revenue'])
        df['fis_ebit_margin'] = self._safe_div(df['ebit'], df['total_revenue'])
        df['fis_ebitda_margin'] = self._safe_div(df['ebitda'], df['total_revenue'])
        
        # Expense control
        df['fis_sga_ratio'] = self._safe_div(
            df['selling_general_and_administrative'].fillna(0),
            df['total_revenue']
        )
        df['fis_rd_ratio'] = self._safe_div(
            df['research_and_development'].fillna(0),
            df['total_revenue']
        )
        df['fis_opex_ratio'] = self._safe_div(df['operating_expenses'].fillna(0), df['total_revenue'])
        
        # Leverage & interest coverage
        df['fis_interest_coverage'] = self._safe_div(
            df['ebit'],
            df['interest_expense'].fillna(0).abs()
        )
        df['fis_interest_burden'] = self._safe_div(
            df['income_before_tax'],
            df['ebit']
        )
        df['fis_financial_leverage_effect'] = self._safe_div(
            df['net_income'],
            df['income_before_tax']
        )
        
        # Tax efficiency
        df['fis_effective_tax_rate'] = self._safe_div(
            df['income_tax_expense'].fillna(0),
            df['income_before_tax']
        )
        df['fis_tax_burden'] = self._safe_div(
            df['net_income'],
            df['income_before_tax']
        )
        
        # Quality metrics
        df['fis_continuing_ops_ratio'] = self._safe_div(
            df['net_income_from_continuing_operations'].fillna(df['net_income']),
            df['net_income']
        )
        df['fis_comprehensive_income_ratio'] = self._safe_div(
            df['comprehensive_income_net_of_tax'].fillna(df['net_income']),
            df['net_income']
        )
        
        # Cash proxies
        df['fis_ebitda_to_revenue'] = self._safe_div(df['ebitda'], df['total_revenue'])
        df['fis_depreciation_ratio'] = self._safe_div(
            df['depreciation_and_amortization'].fillna(0),
            df['total_revenue']
        )
        
        # Sort for time-based features
        df = df.sort_values(['symbol_id', 'fiscal_date_ending'])
        
        # Growth metrics (QoQ, YoY)
        df['fis_revenue_qoq_pct'] = df.groupby('symbol_id')['total_revenue'].pct_change(1, fill_method=None)
        df['fis_net_income_qoq_pct'] = df.groupby('symbol_id')['net_income'].pct_change(1, fill_method=None)
        df['fis_ebitda_qoq_pct'] = df.groupby('symbol_id')['ebitda'].pct_change(1, fill_method=None)
        
        df['fis_revenue_yoy_pct'] = df.groupby('symbol_id')['total_revenue'].pct_change(4, fill_method=None)
        df['fis_net_income_yoy_pct'] = df.groupby('symbol_id')['net_income'].pct_change(4, fill_method=None)
        df['fis_ebitda_yoy_pct'] = df.groupby('symbol_id')['ebitda'].pct_change(4, fill_method=None)
        
        # Volatility (rolling 4 quarters)
        df['fis_revenue_volatility'] = df.groupby('symbol_id')['total_revenue'].rolling(4, min_periods=2).std().reset_index(level=0, drop=True)
        df['fis_net_income_volatility'] = df.groupby('symbol_id')['net_income'].rolling(4, min_periods=2).std().reset_index(level=0, drop=True)
        df['fis_margin_volatility'] = df.groupby('symbol_id')['fis_net_margin'].rolling(4, min_periods=2).std().reset_index(level=0, drop=True)
        
        # Rankings
        df['fis_net_margin_sector_rank'] = df.groupby(['fiscal_date_ending', 'sector'])['fis_net_margin'].rank(pct=True)
        df['fis_operating_margin_sector_rank'] = df.groupby(['fiscal_date_ending', 'sector'])['fis_operating_margin'].rank(pct=True)
        df['fis_net_margin_industry_rank'] = df.groupby(['fiscal_date_ending', 'industry'])['fis_net_margin'].rank(pct=True)
        df['fis_operating_margin_industry_rank'] = df.groupby(['fiscal_date_ending', 'industry'])['fis_operating_margin'].rank(pct=True)
        
        # Flags
        df['fis_negative_net_income_flag'] = (df['net_income'] < 0).astype(int)
        df['fis_revenue_decline_flag'] = (df['fis_revenue_qoq_pct'] < 0).astype(int)
        
        return df

    def _batch_update(self, df):
        """Update transforms table in batches."""
        feature_cols = [c for c in df.columns if c.startswith('fis_')]
        
        # Replace inf with None
        for col in feature_cols:
            if df[col].dtype in ['float64', 'int64']:
                df[col] = df[col].replace([np.inf, -np.inf], None)
        
        # Build update query
        set_clause = ', '.join([f"{col} = %s" for col in feature_cols])
        update_sql = f"""
            UPDATE transforms.income_statement
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
    parser = argparse.ArgumentParser(description='Income statement transformer')
    parser.add_argument('--init', action='store_true', help='Initialize table')
    parser.add_argument('--sync', action='store_true', help='Sync new quarters from raw')
    parser.add_argument('--process', action='store_true', help='Process unprocessed records')
    
    args = parser.parse_args()
    
    if not (args.init or args.sync or args.process):
        parser.error("Must specify --init, --sync, or --process")
    
    transformer = IncomeStatementTransformer()
    
    if args.init:
        transformer.initialize()
    
    if args.sync:
        transformer.sync()
    
    if args.process:
        transformer.process()


if __name__ == "__main__":
    main()
