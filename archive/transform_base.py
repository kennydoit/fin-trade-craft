#!/usr/bin/env python3
"""
Foundation for data transformation pipeline
Transforms raw extracted data into business-ready datasets
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

# Add the parent directories to the path so we can import from db
sys.path.append(str(Path(__file__).parent.parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager


class DataTransformer:
    """Base class for data transformation operations"""

    def __init__(self):
        self.db_manager = PostgresDatabaseManager()

    def transform_company_profiles(self):
        """Transform overview data into enriched company profiles"""
        print("🏢 Transforming company profiles...")

        with self.db_manager as db:
            # Extract data from extracted.overview
            extract_query = """
                SELECT symbol, name, description, sector, industry, 
                       market_capitalization, country, exchange,
                       currency, latest_quarter, pe_ratio, peg_ratio,
                       book_value, dividend_per_share, dividend_yield,
                       eps, revenue_per_share_ttm, profit_margin,
                       operating_margin_ttm, return_on_assets_ttm,
                       return_on_equity_ttm, revenue_ttm, gross_profit_ttm,
                       diluted_eps_ttm, quarterly_earnings_growth_yoy,
                       quarterly_revenue_growth_yoy, analyst_target_price,
                       trailing_pe, forward_pe, price_to_sales_ratio_ttm,
                       price_to_book_ratio, ev_to_revenue, ev_to_ebitda,
                       beta, week_52_high, week_52_low, day_50_moving_average,
                       day_200_moving_average, shares_outstanding, 
                       shares_float, shares_short, shares_short_prior_month,
                       short_ratio, short_percent_outstanding, 
                       short_percent_float, percent_insiders, 
                       percent_institutions, forward_annual_dividend_rate,
                       forward_annual_dividend_yield, payout_ratio,
                       dividend_date, ex_dividend_date, last_split_factor,
                       last_split_date
                FROM extracted.overview
                WHERE symbol IS NOT NULL
            """

            raw_data = db.fetch_query(extract_query)

            if not raw_data:
                print("No data found in extracted.overview")
                return

            # Convert to DataFrame for easier manipulation
            columns = [
                'symbol', 'name', 'description', 'sector', 'industry',
                'market_capitalization', 'country', 'exchange', 'currency',
                'latest_quarter', 'pe_ratio', 'peg_ratio', 'book_value',
                'dividend_per_share', 'dividend_yield', 'eps', 'revenue_per_share_ttm',
                'profit_margin', 'operating_margin_ttm', 'return_on_assets_ttm',
                'return_on_equity_ttm', 'revenue_ttm', 'gross_profit_ttm',
                'diluted_eps_ttm', 'quarterly_earnings_growth_yoy',
                'quarterly_revenue_growth_yoy', 'analyst_target_price',
                'trailing_pe', 'forward_pe', 'price_to_sales_ratio_ttm',
                'price_to_book_ratio', 'ev_to_revenue', 'ev_to_ebitda',
                'beta', 'week_52_high', 'week_52_low', 'day_50_moving_average',
                'day_200_moving_average', 'shares_outstanding', 'shares_float',
                'shares_short', 'shares_short_prior_month', 'short_ratio',
                'short_percent_outstanding', 'short_percent_float',
                'percent_insiders', 'percent_institutions',
                'forward_annual_dividend_rate', 'forward_annual_dividend_yield',
                'payout_ratio', 'dividend_date', 'ex_dividend_date',
                'last_split_factor', 'last_split_date'
            ]

            df = pd.DataFrame(raw_data, columns=columns)

            # Clean and transform data
            df['market_cap'] = pd.to_numeric(df['market_capitalization'], errors='coerce')

            # Categorize market cap
            def categorize_market_cap(market_cap):
                if pd.isna(market_cap):
                    return 'Unknown'
                if market_cap >= 200_000_000_000:  # $200B+
                    return 'Mega'
                if market_cap >= 10_000_000_000:   # $10B+
                    return 'Large'
                if market_cap >= 2_000_000_000:    # $2B+
                    return 'Mid'
                if market_cap >= 300_000_000:      # $300M+
                    return 'Small'
                return 'Micro'

            df['market_cap_category'] = df['market_cap'].apply(categorize_market_cap)

            # Prepare data for insertion
            insert_records = []
            for _, row in df.iterrows():
                record = (
                    row['symbol'],
                    row['name'],
                    row['sector'],
                    row['industry'],
                    row['market_cap'],
                    row['market_cap_category'],
                    row['country'],
                    row['exchange'],
                    True,  # is_active
                    datetime.now(),  # created_at
                    datetime.now()   # updated_at
                )
                insert_records.append(record)

            # Insert into transformed.company_profiles
            insert_query = """
                INSERT INTO transformed.company_profiles 
                (symbol, company_name, sector, industry, market_cap, 
                 market_cap_category, country, exchange, is_active, 
                 created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol) DO UPDATE SET
                    company_name = EXCLUDED.company_name,
                    sector = EXCLUDED.sector,
                    industry = EXCLUDED.industry,
                    market_cap = EXCLUDED.market_cap,
                    market_cap_category = EXCLUDED.market_cap_category,
                    country = EXCLUDED.country,
                    exchange = EXCLUDED.exchange,
                    updated_at = EXCLUDED.updated_at
            """

            inserted_count = db.execute_many(insert_query, insert_records)
            print(f"✅ Processed {len(insert_records)} company profiles")

    def transform_stock_performance(self, limit_days=30):
        """Transform daily price data into performance metrics"""
        print(f"📈 Transforming stock performance data (last {limit_days} days)...")

        with self.db_manager as db:
            # Get recent stock data
            cutoff_date = datetime.now().date() - timedelta(days=limit_days)

            extract_query = """
                SELECT symbol, date, open, high, low, close, 
                       adjusted_close, volume, dividend_amount, split_coefficient
                FROM extracted.time_series_daily_adjusted
                WHERE date >= %s 
                AND symbol IS NOT NULL
                ORDER BY symbol, date
            """

            raw_data = db.fetch_query(extract_query, (cutoff_date,))

            if not raw_data:
                print("No recent stock data found")
                return

            df = pd.DataFrame(raw_data, columns=[
                'symbol', 'date', 'open', 'high', 'low', 'close',
                'adjusted_close', 'volume', 'dividend_amount', 'split_coefficient'
            ])

            # Convert data types
            df['date'] = pd.to_datetime(df['date'])
            numeric_cols = ['open', 'high', 'low', 'close', 'adjusted_close', 'volume']
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            # Calculate technical indicators and performance metrics
            transformed_records = []

            for symbol in df['symbol'].unique():
                symbol_data = df[df['symbol'] == symbol].sort_values('date')

                if len(symbol_data) < 20:  # Need at least 20 days for indicators
                    continue

                # Calculate moving averages
                symbol_data['sma_20'] = symbol_data['adjusted_close'].rolling(window=20).mean()
                symbol_data['sma_50'] = symbol_data['adjusted_close'].rolling(window=50).mean()
                symbol_data['sma_200'] = symbol_data['adjusted_close'].rolling(window=200).mean()

                # Calculate daily returns
                symbol_data['daily_return'] = symbol_data['adjusted_close'].pct_change()

                # Calculate 30-day rolling volatility
                symbol_data['volatility_30d'] = symbol_data['daily_return'].rolling(window=30).std()

                # Calculate RSI (simplified version)
                def calculate_rsi(prices, period=14):
                    delta = prices.diff()
                    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
                    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
                    rs = gain / loss
                    rsi = 100 - (100 / (1 + rs))
                    return rsi

                symbol_data['rsi_14'] = calculate_rsi(symbol_data['adjusted_close'])

                # Prepare records for insertion
                for _, row in symbol_data.iterrows():
                    if pd.notna(row['sma_20']):  # Only include rows with calculated indicators
                        record = (
                            row['symbol'],
                            row['date'].date(),
                            row['open'],
                            row['high'],
                            row['low'],
                            row['close'],
                            row['adjusted_close'],
                            int(row['volume']) if pd.notna(row['volume']) else None,
                            row['sma_20'],
                            row['sma_50'] if pd.notna(row['sma_50']) else None,
                            row['sma_200'] if pd.notna(row['sma_200']) else None,
                            row['rsi_14'] if pd.notna(row['rsi_14']) else None,
                            row['daily_return'],
                            row['volatility_30d'] if pd.notna(row['volatility_30d']) else None,
                            datetime.now()
                        )
                        transformed_records.append(record)

            if transformed_records:
                # Insert into transformed.stock_performance
                insert_query = """
                    INSERT INTO transformed.stock_performance 
                    (symbol, date, open_price, high_price, low_price, close_price,
                     adjusted_close, volume, sma_20, sma_50, sma_200, rsi_14,
                     daily_return, volatility_30d, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, date) DO UPDATE SET
                        open_price = EXCLUDED.open_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price,
                        adjusted_close = EXCLUDED.adjusted_close,
                        volume = EXCLUDED.volume,
                        sma_20 = EXCLUDED.sma_20,
                        sma_50 = EXCLUDED.sma_50,
                        sma_200 = EXCLUDED.sma_200,
                        rsi_14 = EXCLUDED.rsi_14,
                        daily_return = EXCLUDED.daily_return,
                        volatility_30d = EXCLUDED.volatility_30d,
                        created_at = EXCLUDED.created_at
                """

                db.execute_many(insert_query, transformed_records)
                print(f"✅ Processed {len(transformed_records)} stock performance records")

    def run_all_transformations(self):
        """Run all transformation processes"""
        print("🔄 Starting data transformation pipeline...")
        print("=" * 60)

        try:
            # Transform company profiles
            self.transform_company_profiles()

            # Transform stock performance
            self.transform_stock_performance()

            print("\n" + "=" * 60)
            print("🎉 Data transformation pipeline completed successfully!")
            print("✅ Company profiles transformed")
            print("✅ Stock performance metrics calculated")
            print("\nTransformed data is now available in 'transformed' schema")
            print("=" * 60)

        except Exception as e:
            print(f"\n❌ Transformation failed: {str(e)}")
            import traceback
            traceback.print_exc()

def main():
    """Main function to run transformations"""
    transformer = DataTransformer()
    transformer.run_all_transformations()

if __name__ == "__main__":
    main()
