"""Create visualization input CSV for top 25 trades from filtered results."""

import sys
import os
import pandas as pd

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.postgres_database_manager import PostgresDatabaseManager

# Load filtered trades
trades_df = pd.read_csv('backtesting/trades_filtered_90pct.csv')

# Sort by success probability and take top 25
top_25 = trades_df.sort_values('success_probability', ascending=False).head(25)

# Get company names from database
db = PostgresDatabaseManager()
db.connect()

symbols = "', '".join(top_25['symbol'].unique())
query = f"""
    SELECT DISTINCT 
        symbol, 
        name,
        sector,
        industry,
        market_capitalization
    FROM raw.company_overview
    WHERE symbol IN ('{symbols}')
"""

names_df = pd.read_sql(query, db.connection)
db.close()

# Merge with top 25
result = top_25.merge(names_df, on='symbol', how='left')

# Select columns for visualizer
output = result[['symbol', 'entry_date', 'name', 'strategy', 'success_probability', 
                 'sector_x', 'industry', 'market_capitalization', 
                 'overall_quality_score']].copy()
output.rename(columns={
    'entry_date': 'date', 
    'strategy': 'trade_strategy',
    'sector_x': 'sector'
}, inplace=True)

# Add signal_strength (we don't have it, so use success_probability * 100)
output['signal_strength'] = output['success_probability'] * 100

# Fill missing names
output['name'] = output['name'].fillna(output['symbol'])
output['industry'] = output['industry'].fillna('Unknown')
output['market_capitalization'] = output['market_capitalization'].fillna(0)

# Save
output.to_csv('backtesting/top_25_signals_for_viz.csv', index=False)
print(f"Created visualization input with {len(output)} trades")
print(f"\nTop 5:")
print(output.head()[['symbol', 'name', 'date', 'success_probability']])
