"""Transform fundamental quality scores - Aggregate scoring from balance sheet, cash flow, and income statement.

This transformer creates quality scores for stocks based on fundamental metrics:
- Balance Sheet Quality: Liquidity, leverage, asset quality
- Cash Flow Quality: FCF generation, cash conversion, sustainability
- Income Statement Quality: Profitability, margins, earnings quality
- Overall Quality: Weighted composite of all three

Scores range from 0-100, with higher scores indicating better fundamental quality.

Usage:
    # One-time initialization
    python transform_fundamental_quality_scores.py --init
    
    # Process unprocessed records (run after fundamental transforms update)
    python transform_fundamental_quality_scores.py --process
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


class FundamentalQualityScorer:
    """Generate quality scores from fundamental transforms."""

    def __init__(self):
        self.db = PostgresDatabaseManager()

    def initialize(self):
        """Initialize transforms.fundamental_quality_scores table."""
        logger.info("Initializing transforms.fundamental_quality_scores...")
        
        self.db.connect()
        try:
            # Drop and create table
            logger.info("Creating quality scores table...")
            self.db.execute_query("DROP TABLE IF EXISTS transforms.fundamental_quality_scores CASCADE")
            
            self.db.execute_query("""
                CREATE TABLE transforms.fundamental_quality_scores (
                    symbol_id BIGINT NOT NULL,
                    symbol VARCHAR(20) NOT NULL,
                    fiscal_date_ending DATE NOT NULL,
                    
                    -- Component scores (0-100 scale)
                    balance_sheet_quality_score NUMERIC,
                    cash_flow_quality_score NUMERIC,
                    income_statement_quality_score NUMERIC,
                    
                    -- Overall composite score
                    overall_quality_score NUMERIC,
                    
                    -- Sub-scores for balance sheet
                    bs_liquidity_score NUMERIC,
                    bs_leverage_score NUMERIC,
                    bs_asset_quality_score NUMERIC,
                    
                    -- Sub-scores for cash flow
                    cf_generation_score NUMERIC,
                    cf_efficiency_score NUMERIC,
                    cf_sustainability_score NUMERIC,
                    
                    -- Sub-scores for income statement
                    is_profitability_score NUMERIC,
                    is_margin_score NUMERIC,
                    is_growth_score NUMERIC,
                    
                    -- Quality flags
                    is_high_quality BOOLEAN,  -- Overall score >= 70
                    is_investment_grade BOOLEAN,  -- All component scores >= 50
                    has_red_flags BOOLEAN,  -- Any critical weakness detected
                    
                    -- Metadata
                    processed_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    
                    PRIMARY KEY (symbol_id, fiscal_date_ending)
                )
            """)
            
            # Create indexes
            self.db.execute_query("""
                CREATE INDEX idx_fqs_unprocessed 
                ON transforms.fundamental_quality_scores (symbol_id, fiscal_date_ending) 
                WHERE processed_at IS NULL
            """)
            
            self.db.execute_query("""
                CREATE INDEX idx_fqs_overall_score 
                ON transforms.fundamental_quality_scores (overall_quality_score DESC)
            """)
            
            self.db.execute_query("""
                CREATE INDEX idx_fqs_high_quality 
                ON transforms.fundamental_quality_scores (is_high_quality, fiscal_date_ending)
            """)
            
            # Initialize with union of symbol-date combinations across all fundamentals
            logger.info("Initializing records from fundamentals (BS ∪ CF ∪ IS)...")
            self.db.execute_query("""
                INSERT INTO transforms.fundamental_quality_scores 
                    (symbol_id, symbol, fiscal_date_ending)
                SELECT DISTINCT symbol_id, symbol, fiscal_date_ending
                FROM (
                    SELECT symbol_id, symbol, fiscal_date_ending
                    FROM transforms.balance_sheet
                    WHERE processed_at IS NOT NULL
                    UNION
                    SELECT symbol_id, symbol, fiscal_date_ending
                    FROM transforms.cash_flow
                    WHERE processed_at IS NOT NULL
                    UNION
                    SELECT symbol_id, symbol, fiscal_date_ending
                    FROM transforms.income_statement
                    WHERE processed_at IS NOT NULL
                ) AS keys
            """)
            
            count = self.db.fetch_query("SELECT COUNT(*) FROM transforms.fundamental_quality_scores")[0][0]
            logger.info(f"✅ Initialized with {count:,} records")
            
        finally:
            self.db.close()

    def process(self):
        """Process unprocessed quality scores."""
        logger.info("=" * 80)
        logger.info("PROCESSING FUNDAMENTAL QUALITY SCORES")
        logger.info("=" * 80)
        
        self.db.connect()
        try:
            # Before processing, insert any newly available (symbol, date) keys from fundamentals
            logger.info("Syncing missing symbol-date keys from fundamentals...")
            inserted = self.db.execute_query("""
                INSERT INTO transforms.fundamental_quality_scores (symbol_id, symbol, fiscal_date_ending)
                SELECT DISTINCT symbol_id, symbol, fiscal_date_ending
                FROM (
                    SELECT symbol_id, symbol, fiscal_date_ending FROM transforms.balance_sheet WHERE processed_at IS NOT NULL
                    UNION
                    SELECT symbol_id, symbol, fiscal_date_ending FROM transforms.cash_flow WHERE processed_at IS NOT NULL
                    UNION
                    SELECT symbol_id, symbol, fiscal_date_ending FROM transforms.income_statement WHERE processed_at IS NOT NULL
                ) s
                ON CONFLICT (symbol_id, fiscal_date_ending) DO NOTHING
                RETURNING 1
            """)
            # execute_query returns rowcount for non-SELECT; if RETURNING used, it returns fetched rows
            if isinstance(inserted, list):
                logger.info(f"Added {len(inserted):,} new key(s)")
            else:
                try:
                    logger.info(f"Added {inserted:,} new key(s)")
                except Exception:
                    logger.info("Key sync completed")

            # Count unprocessed
            unprocessed = self.db.fetch_query("""
                SELECT COUNT(*) FROM transforms.fundamental_quality_scores 
                WHERE processed_at IS NULL
            """)[0][0]
            
            if unprocessed == 0:
                logger.info("No unprocessed records")
                return
            
            logger.info(f"Found {unprocessed:,} unprocessed records")
            logger.info("Fetching data...")
            
            # Fetch all fundamental data for unprocessed scores
            query = """
                SELECT 
                    fqs.symbol_id,
                    fqs.symbol,
                    fqs.fiscal_date_ending,
                    
                    -- Balance sheet metrics
                    bs.fbs_current_ratio,
                    bs.fbs_quick_ratio,
                    bs.fbs_debt_to_equity,
                    bs.fbs_debt_to_assets,
                    bs.fbs_tangible_asset_ratio,
                    bs.fbs_current_ratio_yoy_pct,
                    bs.fbs_liquidity_shock_flag,
                    
                    -- Cash flow metrics
                    cf.fcf_free_cash_flow,
                    cf.fcf_ocf_to_capex_ratio,
                    cf.fcf_cash_conversion_ratio,
                    cf.fcf_operating_cf_margin,
                    cf.fcf_free_cash_flow_yoy_pct,
                    cf.fcf_negative_free_cash_flow_flag,
                    cf.fcf_cash_burn_flag,
                    
                    -- Income statement metrics
                    inc.fis_net_margin,
                    inc.fis_operating_margin,
                    inc.fis_gross_margin,
                    inc.fis_net_income_yoy_pct,
                    inc.fis_revenue_yoy_pct,
                    inc.fis_negative_net_income_flag,
                    inc.fis_revenue_decline_flag
                    
                FROM transforms.fundamental_quality_scores fqs
                LEFT JOIN transforms.balance_sheet bs 
                    ON fqs.symbol_id = bs.symbol_id 
                    AND fqs.fiscal_date_ending = bs.fiscal_date_ending
                LEFT JOIN transforms.cash_flow cf 
                    ON fqs.symbol_id = cf.symbol_id 
                    AND fqs.fiscal_date_ending = cf.fiscal_date_ending
                LEFT JOIN transforms.income_statement inc 
                    ON fqs.symbol_id = inc.symbol_id 
                    AND fqs.fiscal_date_ending = inc.fiscal_date_ending
                WHERE fqs.processed_at IS NULL
                ORDER BY fqs.symbol_id, fqs.fiscal_date_ending
            """
            
            df = pd.read_sql(query, self.db.connection)
            logger.info(f"Loaded {len(df):,} records")
            
            # Compute scores
            logger.info("Computing quality scores...")
            df = self._compute_all_scores(df)
            
            # Update database
            logger.info("Updating database...")
            self._batch_update(df)
            
            logger.info("=" * 80)
            logger.info(f"✅ Processed {len(df):,} records")
            logger.info("=" * 80)
            
        finally:
            self.db.close()

    def _normalize_to_score(self, values, reverse=False, lower_bound=None, upper_bound=None):
        """
        Normalize values to 0-100 score scale.
        
        Args:
            values: Series to normalize
            reverse: If True, lower values get higher scores
            lower_bound: Optional lower percentile bound (default 10th percentile)
            upper_bound: Optional upper percentile bound (default 90th percentile)
        """
        if values.isna().all():
            return pd.Series([50] * len(values))  # Neutral score if no data
        
        # Use percentile bounds to handle outliers
        if lower_bound is None:
            lower_bound = values.quantile(0.10)
        if upper_bound is None:
            upper_bound = values.quantile(0.90)
        
        # Clip to bounds
        clipped = values.clip(lower_bound, upper_bound)
        
        # Normalize to 0-1
        range_val = upper_bound - lower_bound
        if range_val == 0:
            normalized = pd.Series([50] * len(values))
        else:
            normalized = (clipped - lower_bound) / range_val
        
        # Reverse if needed
        if reverse:
            normalized = 1 - normalized
        
        # Scale to 0-100
        return normalized * 100

    def _compute_balance_sheet_score(self, df):
        """Compute balance sheet quality score (0-100)."""
        # Liquidity score (30% weight)
        liquidity_components = []
        if 'fbs_current_ratio' in df.columns:
            # Current ratio: 1.5+ is excellent, <1.0 is poor
            cr_score = self._normalize_to_score(df['fbs_current_ratio'], lower_bound=0.5, upper_bound=2.5)
            liquidity_components.append(cr_score)
        
        if 'fbs_quick_ratio' in df.columns:
            # Quick ratio: 1.0+ is excellent, <0.5 is poor
            qr_score = self._normalize_to_score(df['fbs_quick_ratio'], lower_bound=0.3, upper_bound=1.5)
            liquidity_components.append(qr_score)
        
        if liquidity_components:
            df['bs_liquidity_score'] = pd.concat(liquidity_components, axis=1).mean(axis=1)
        else:
            df['bs_liquidity_score'] = 50
        
        # Leverage score (40% weight) - lower leverage is better
        leverage_components = []
        if 'fbs_debt_to_equity' in df.columns:
            # D/E: <1.0 is excellent, >3.0 is poor
            de_score = self._normalize_to_score(df['fbs_debt_to_equity'], reverse=True, lower_bound=0, upper_bound=3.0)
            leverage_components.append(de_score)
        
        if 'fbs_debt_to_assets' in df.columns:
            # D/A: <0.3 is excellent, >0.7 is poor
            da_score = self._normalize_to_score(df['fbs_debt_to_assets'], reverse=True, lower_bound=0, upper_bound=0.8)
            leverage_components.append(da_score)
        
        if leverage_components:
            df['bs_leverage_score'] = pd.concat(leverage_components, axis=1).mean(axis=1)
        else:
            df['bs_leverage_score'] = 50
        
        # Asset quality score (30% weight)
        asset_components = []
        if 'fbs_tangible_asset_ratio' in df.columns:
            # Tangible assets: >0.7 is excellent
            ta_score = self._normalize_to_score(df['fbs_tangible_asset_ratio'], lower_bound=0.3, upper_bound=0.95)
            asset_components.append(ta_score)
        
        if asset_components:
            df['bs_asset_quality_score'] = pd.concat(asset_components, axis=1).mean(axis=1)
        else:
            df['bs_asset_quality_score'] = 50
        
        # Weighted composite
        df['balance_sheet_quality_score'] = (
            df['bs_liquidity_score'] * 0.30 +
            df['bs_leverage_score'] * 0.40 +
            df['bs_asset_quality_score'] * 0.30
        )
        
        # Penalty for liquidity shock
        if 'fbs_liquidity_shock_flag' in df.columns:
            df.loc[df['fbs_liquidity_shock_flag'] == 1, 'balance_sheet_quality_score'] *= 0.8
        
        return df

    def _compute_cash_flow_score(self, df):
        """Compute cash flow quality score (0-100)."""
        # FCF generation score (40% weight)
        generation_components = []
        if 'fcf_free_cash_flow' in df.columns:
            # Positive FCF is good, scale by magnitude
            fcf_score = self._normalize_to_score(df['fcf_free_cash_flow'], lower_bound=df['fcf_free_cash_flow'].quantile(0.20), upper_bound=df['fcf_free_cash_flow'].quantile(0.80))
            generation_components.append(fcf_score)
        
        if 'fcf_operating_cf_margin' in df.columns:
            # OCF margin: >15% is excellent, <5% is poor
            margin_score = self._normalize_to_score(df['fcf_operating_cf_margin'], lower_bound=0.05, upper_bound=0.25)
            generation_components.append(margin_score)
        
        if generation_components:
            df['cf_generation_score'] = pd.concat(generation_components, axis=1).mean(axis=1)
        else:
            df['cf_generation_score'] = 50
        
        # Efficiency score (30% weight)
        efficiency_components = []
        if 'fcf_ocf_to_capex_ratio' in df.columns:
            # OCF/Capex: >2.0 is excellent, <1.0 is poor
            capex_score = self._normalize_to_score(df['fcf_ocf_to_capex_ratio'], lower_bound=0.5, upper_bound=3.0)
            efficiency_components.append(capex_score)
        
        if 'fcf_cash_conversion_ratio' in df.columns:
            # Cash conversion: >1.0 is good (OCF > net income)
            conversion_score = self._normalize_to_score(df['fcf_cash_conversion_ratio'], lower_bound=0.5, upper_bound=1.5)
            efficiency_components.append(conversion_score)
        
        if efficiency_components:
            df['cf_efficiency_score'] = pd.concat(efficiency_components, axis=1).mean(axis=1)
        else:
            df['cf_efficiency_score'] = 50
        
        # Sustainability score (30% weight)
        sustainability_components = []
        if 'fcf_free_cash_flow_yoy_pct' in df.columns:
            # YoY growth: positive is good
            growth_score = self._normalize_to_score(df['fcf_free_cash_flow_yoy_pct'], lower_bound=-0.20, upper_bound=0.30)
            sustainability_components.append(growth_score)
        
        if sustainability_components:
            df['cf_sustainability_score'] = pd.concat(sustainability_components, axis=1).mean(axis=1)
        else:
            df['cf_sustainability_score'] = 50
        
        # Weighted composite
        df['cash_flow_quality_score'] = (
            df['cf_generation_score'] * 0.40 +
            df['cf_efficiency_score'] * 0.30 +
            df['cf_sustainability_score'] * 0.30
        )
        
        # Penalty for negative FCF or cash burn
        if 'fcf_negative_free_cash_flow_flag' in df.columns:
            df.loc[df['fcf_negative_free_cash_flow_flag'] == 1, 'cash_flow_quality_score'] *= 0.7
        
        if 'fcf_cash_burn_flag' in df.columns:
            df.loc[df['fcf_cash_burn_flag'] == 1, 'cash_flow_quality_score'] *= 0.5
        
        return df

    def _compute_income_statement_score(self, df):
        """Compute income statement quality score (0-100)."""
        # Profitability score (40% weight)
        profitability_components = []
        if 'fis_net_margin' in df.columns:
            # Net margin: >15% is excellent, <5% is poor
            nm_score = self._normalize_to_score(df['fis_net_margin'], lower_bound=0.05, upper_bound=0.25)
            profitability_components.append(nm_score)
        
        if profitability_components:
            df['is_profitability_score'] = pd.concat(profitability_components, axis=1).mean(axis=1)
        else:
            df['is_profitability_score'] = 50
        
        # Margin score (30% weight)
        margin_components = []
        if 'fis_operating_margin' in df.columns:
            # Operating margin: >20% is excellent, <10% is poor
            om_score = self._normalize_to_score(df['fis_operating_margin'], lower_bound=0.05, upper_bound=0.30)
            margin_components.append(om_score)
        
        if 'fis_gross_margin' in df.columns:
            # Gross margin: >40% is excellent, <20% is poor
            gm_score = self._normalize_to_score(df['fis_gross_margin'], lower_bound=0.20, upper_bound=0.60)
            margin_components.append(gm_score)
        
        if margin_components:
            df['is_margin_score'] = pd.concat(margin_components, axis=1).mean(axis=1)
        else:
            df['is_margin_score'] = 50
        
        # Growth score (30% weight)
        growth_components = []
        if 'fis_revenue_yoy_pct' in df.columns:
            # Revenue growth: >10% is excellent, <0% is poor
            rev_growth_score = self._normalize_to_score(df['fis_revenue_yoy_pct'], lower_bound=-0.05, upper_bound=0.20)
            growth_components.append(rev_growth_score)
        
        if 'fis_net_income_yoy_pct' in df.columns:
            # Net income growth is good
            income_growth_score = self._normalize_to_score(df['fis_net_income_yoy_pct'], lower_bound=-0.20, upper_bound=0.30)
            growth_components.append(income_growth_score)
        
        if growth_components:
            df['is_growth_score'] = pd.concat(growth_components, axis=1).mean(axis=1)
        else:
            df['is_growth_score'] = 50
        
        # Weighted composite
        df['income_statement_quality_score'] = (
            df['is_profitability_score'] * 0.40 +
            df['is_margin_score'] * 0.30 +
            df['is_growth_score'] * 0.30
        )
        
        # Penalty for negative income or declining revenue
        if 'fis_negative_net_income_flag' in df.columns:
            df.loc[df['fis_negative_net_income_flag'] == 1, 'income_statement_quality_score'] *= 0.6
        
        if 'fis_revenue_decline_flag' in df.columns:
            df.loc[df['fis_revenue_decline_flag'] == 1, 'income_statement_quality_score'] *= 0.85
        
        return df

    def _compute_all_scores(self, df):
        """Compute all quality scores."""
        df = df.copy()
        
        # Compute component scores
        df = self._compute_balance_sheet_score(df)
        df = self._compute_cash_flow_score(df)
        df = self._compute_income_statement_score(df)
        
        # Overall composite score (equal weight)
        df['overall_quality_score'] = (
            df['balance_sheet_quality_score'].fillna(50) * 0.33 +
            df['cash_flow_quality_score'].fillna(50) * 0.33 +
            df['income_statement_quality_score'].fillna(50) * 0.34
        )
        
        # Clip to 0-100 range
        score_cols = [
            'balance_sheet_quality_score', 'cash_flow_quality_score', 
            'income_statement_quality_score', 'overall_quality_score',
            'bs_liquidity_score', 'bs_leverage_score', 'bs_asset_quality_score',
            'cf_generation_score', 'cf_efficiency_score', 'cf_sustainability_score',
            'is_profitability_score', 'is_margin_score', 'is_growth_score'
        ]
        for col in score_cols:
            if col in df.columns:
                df[col] = df[col].clip(0, 100)
        
        # Quality flags
        df['is_high_quality'] = df['overall_quality_score'] >= 70
        df['is_investment_grade'] = (
            (df['balance_sheet_quality_score'] >= 50) &
            (df['cash_flow_quality_score'] >= 50) &
            (df['income_statement_quality_score'] >= 50)
        )
        
        # Red flags: any critical weakness
        df['has_red_flags'] = False
        if 'fbs_liquidity_shock_flag' in df.columns:
            df['has_red_flags'] |= (df['fbs_liquidity_shock_flag'] == 1)
        if 'fcf_cash_burn_flag' in df.columns:
            df['has_red_flags'] |= (df['fcf_cash_burn_flag'] == 1)
        if 'fis_negative_net_income_flag' in df.columns:
            df['has_red_flags'] |= (df['fis_negative_net_income_flag'] == 1)
        
        return df

    def _batch_update(self, df):
        """Update quality scores table."""
        update_cols = [
            'balance_sheet_quality_score', 'cash_flow_quality_score', 
            'income_statement_quality_score', 'overall_quality_score',
            'bs_liquidity_score', 'bs_leverage_score', 'bs_asset_quality_score',
            'cf_generation_score', 'cf_efficiency_score', 'cf_sustainability_score',
            'is_profitability_score', 'is_margin_score', 'is_growth_score',
            'is_high_quality', 'is_investment_grade', 'has_red_flags'
        ]
        
        # Replace inf with None
        for col in update_cols:
            if col in df.columns and df[col].dtype in ['float64', 'int64']:
                df[col] = df[col].replace([np.inf, -np.inf], None)
        
        # Build update query
        set_clause = ', '.join([f"{col} = %s" for col in update_cols])
        update_sql = f"""
            UPDATE transforms.fundamental_quality_scores
            SET {set_clause}, processed_at = NOW()
            WHERE symbol_id = %s AND fiscal_date_ending = %s
        """
        
        # Prepare records
        records = []
        for _, row in df.iterrows():
            values = [None if pd.isna(row.get(col)) else row.get(col) for col in update_cols]
            values.extend([row['symbol_id'], row['fiscal_date_ending']])
            records.append(tuple(values))
        
        # Execute batch
        self.db.execute_many(update_sql, records)
        logger.info(f"Updated {len(records):,} records")


def main():
    parser = argparse.ArgumentParser(description='Fundamental quality score transformer')
    parser.add_argument('--init', action='store_true', help='Initialize table')
    parser.add_argument('--process', action='store_true', help='Process unprocessed records')
    
    args = parser.parse_args()
    
    if not (args.init or args.process):
        parser.error("Must specify --init or --process")
    
    scorer = FundamentalQualityScorer()
    
    if args.init:
        scorer.initialize()
    
    if args.process:
        scorer.process()


if __name__ == "__main__":
    main()
