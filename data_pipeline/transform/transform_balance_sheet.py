#!/usr/bin/env python3
"""Transform Balance Sheet data into analytical features.

Creates table ``transformed.balance_sheet_features`` containing liquidity,
leverage, asset efficiency, equity strength and market linked features along
with growth and risk indicators.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).parent.parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager


class BalanceSheetTransformer:
    """Create analytical features from balance sheet information."""

    def __init__(self) -> None:
        self.db = PostgresDatabaseManager()

    # ------------------------------------------------------------------
    # Data fetching helpers
    # ------------------------------------------------------------------
    def _fetch_balance_sheet(self) -> pd.DataFrame:
        query = """
            SELECT
                b.symbol_id,
                b.symbol,
                b.fiscal_date_ending,
                b.total_assets,
                b.total_current_assets,
                b.cash_and_short_term_investments,
                b.cash_and_cash_equivalents_at_carrying_value,
                b.current_net_receivables,
                b.total_current_liabilities,
                b.total_liabilities,
                b.current_debt,
                b.long_term_debt,
                b.total_shareholder_equity,
                b.retained_earnings,
                b.treasury_stock,
                b.goodwill,
                b.intangible_assets,
                b.property_plant_equipment,
                b.common_stock_shares_outstanding
            FROM balance_sheet b
            WHERE b.report_type = 'quarterly'
        """
        return self.db.fetch_dataframe(query)

    def _fetch_income_statement(self) -> pd.DataFrame:
        query = """
            SELECT symbol, fiscal_date_ending, ebit, total_revenue
            FROM income_statement
            WHERE report_type = 'quarterly'
        """
        return self.db.fetch_dataframe(query)

    def _fetch_overview(self) -> pd.DataFrame:
        query = """
            SELECT symbol, sector, industry
            FROM extracted.overview
        """
        return self.db.fetch_dataframe(query)

    def _fetch_price_data(self) -> pd.DataFrame:
        query = """
            SELECT symbol, date, adjusted_close
            FROM time_series_daily_adjusted
        """
        return self.db.fetch_dataframe(query)

    # ------------------------------------------------------------------
    # Feature engineering
    # ------------------------------------------------------------------
    def _compute_base_ratios(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        # Liquidity
        df['current_ratio'] = df['total_current_assets'] / df['total_current_liabilities']
        df['quick_ratio'] = (
            df['cash_and_short_term_investments'] + df['current_net_receivables']
        ) / df['total_current_liabilities']
        df['cash_ratio'] = (
            df['cash_and_cash_equivalents_at_carrying_value'] / df['total_current_liabilities']
        )
        df['working_capital'] = df['total_current_assets'] - df['total_current_liabilities']

        # Leverage
        df['debt_to_equity'] = df['total_liabilities'] / df['total_shareholder_equity']
        df['current_debt_ratio'] = df['current_debt'] / df['total_assets']
        df['long_term_debt_ratio'] = df['long_term_debt'] / df['total_assets']
        df['debt_to_assets'] = df['total_liabilities'] / df['total_assets']

        # Asset efficiency
        df['tangible_asset_ratio'] = (
            (df['total_assets'] - df['goodwill'] - df['intangible_assets']) / df['total_assets']
        )
        df['intangibles_share'] = (
            (df['goodwill'] + df['intangible_assets']) / df['total_assets']
        )
        df['ppe_intensity'] = df['property_plant_equipment'] / df['total_assets']
        df['cash_to_assets'] = df['cash_and_short_term_investments'] / df['total_assets']

        # Equity strength
        df['book_value_per_share'] = (
            df['total_shareholder_equity'] / df['common_stock_shares_outstanding']
        )
        df['retained_earnings_ratio'] = (
            df['retained_earnings'] / df['total_shareholder_equity']
        )
        df['treasury_stock_effect'] = (
            df['treasury_stock'] / df['total_shareholder_equity']
        )
        return df

    def _compute_growth_and_momentum(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(['symbol', 'fiscal_date_ending'])
        metrics = [
            'current_ratio',
            'quick_ratio',
            'cash_ratio',
            'working_capital',
            'debt_to_equity',
            'current_debt_ratio',
            'long_term_debt_ratio',
            'debt_to_assets',
            'tangible_asset_ratio',
            'intangibles_share',
            'ppe_intensity',
            'cash_to_assets',
            'book_value_per_share',
            'retained_earnings_ratio',
            'treasury_stock_effect',
        ]
        for col in metrics:
            df[f'{col}_qoq_pct'] = df.groupby('symbol')[col].pct_change(periods=1)
            df[f'{col}_yoy_pct'] = df.groupby('symbol')[col].pct_change(periods=4)
            rolling = df.groupby('symbol')[col].rolling(window=4, min_periods=1)
            df[f'{col}_zscore'] = (
                (rolling.apply(lambda x: x.iloc[-1]) - rolling.mean()) / rolling.std()
            ).reset_index(level=0, drop=True)
        return df

    def _compute_rankings(self, df: pd.DataFrame) -> pd.DataFrame:
        metrics = [
            'current_ratio',
            'quick_ratio',
            'cash_ratio',
            'working_capital',
            'debt_to_equity',
            'current_debt_ratio',
            'long_term_debt_ratio',
            'debt_to_assets',
            'tangible_asset_ratio',
            'intangibles_share',
            'ppe_intensity',
            'cash_to_assets',
            'book_value_per_share',
            'retained_earnings_ratio',
            'treasury_stock_effect',
        ]
        for col in metrics:
            df[f'{col}_sector_rank'] = df.groupby(['fiscal_date_ending', 'sector'])[col].rank(pct=True)
            df[f'{col}_industry_rank'] = df.groupby(['fiscal_date_ending', 'industry'])[col].rank(pct=True)
        return df

    def _merge_price_features(self, bs_df: pd.DataFrame, price_df: pd.DataFrame) -> pd.DataFrame:
        price_df = price_df.sort_values(['symbol', 'date'])
        price_df['return'] = price_df.groupby('symbol')['adjusted_close'].pct_change()
        price_df['volatility_30d'] = price_df.groupby('symbol')['return'].rolling(30).std().reset_index(level=0, drop=True)
        merged = pd.merge_asof(
            bs_df.sort_values('fiscal_date_ending'),
            price_df.sort_values('date'),
            left_on='fiscal_date_ending',
            right_on='date',
            by='symbol',
            direction='backward'
        )
        merged['market_cap'] = merged['common_stock_shares_outstanding'] * merged['adjusted_close']
        merged['price_to_book'] = merged['market_cap'] / merged['total_shareholder_equity']
        merged['debt_to_market_cap'] = merged['total_liabilities'] / merged['market_cap']
        merged['net_cash_per_share'] = (
            (merged['cash_and_short_term_investments'] - merged['total_liabilities'])
            / merged['common_stock_shares_outstanding']
        )
        merged['recent_price_volatility'] = merged['volatility_30d']
        return merged

    def _compute_risk_indicators(self, df: pd.DataFrame, income_df: pd.DataFrame) -> pd.DataFrame:
        df = df.merge(
            income_df[['symbol', 'fiscal_date_ending', 'ebit', 'total_revenue']],
            on=['symbol', 'fiscal_date_ending'],
            how='left'
        )
        df['altman_z_score'] = (
            1.2 * (df['working_capital'] / df['total_assets']) +
            1.4 * (df['retained_earnings'] / df['total_assets']) +
            3.3 * (df['ebit'] / df['total_assets']) +
            0.6 * (df['market_cap'] / df['total_liabilities']) +
            1.0 * (df['total_revenue'] / df['total_assets'])
        )
        df['volatility_adjusted_leverage'] = df['debt_to_equity'] * df['recent_price_volatility']
        df['liquidity_shock_flag'] = (df['current_ratio_qoq_pct'] < -0.2).astype(int)
        return df

    # ------------------------------------------------------------------
    def run(self) -> pd.DataFrame:
        self.db.connect()
        try:
            bs_df = self._fetch_balance_sheet()
            overview_df = self._fetch_overview()
            income_df = self._fetch_income_statement()
            price_df = self._fetch_price_data()

            df = bs_df.merge(overview_df, on='symbol', how='left')
            df = self._compute_base_ratios(df)
            df = self._compute_growth_and_momentum(df)
            df = self._compute_rankings(df)
            df = self._merge_price_features(df, price_df)
            df = self._compute_risk_indicators(df, income_df)

            self._write_output(df)
            return df
        finally:
            self.db.close()

    def _write_output(self, df: pd.DataFrame) -> None:
        self.db.execute_query("CREATE SCHEMA IF NOT EXISTS transformed")
        create_sql = """
            CREATE TABLE IF NOT EXISTS transformed.balance_sheet_features (
                symbol_id BIGINT,
                symbol VARCHAR(20),
                fiscal_date_ending DATE,
                data JSONB,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """
        self.db.execute_query(create_sql)

        feature_cols = [c for c in df.columns if c not in {'symbol_id', 'symbol', 'fiscal_date_ending'}]
        records = []
        for _, row in df.iterrows():
            payload = row[feature_cols].to_json()
            records.append((row['symbol_id'], row['symbol'], row['fiscal_date_ending'], payload))

        insert_sql = """
            INSERT INTO transformed.balance_sheet_features (
                symbol_id, symbol, fiscal_date_ending, data
            ) VALUES (%s, %s, %s, %s)
        """
        self.db.execute_many(insert_sql, records)


if __name__ == "__main__":  # pragma: no cover
    transformer = BalanceSheetTransformer()
    df = transformer.run()
    print(df.head())
