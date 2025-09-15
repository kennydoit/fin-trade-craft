#!/usr/bin/env python3

from db.postgres_database_manager import PostgresDatabaseManager

try:
    db = PostgresDatabaseManager()
    db.connect()

    # Check existing Treasury Yield data
    result = db.fetch_dataframe("""
    SELECT indicator_name, COUNT(*) as record_count
    FROM source.economic_indicators 
    WHERE indicator_name LIKE '%Treasury Yield%'
    GROUP BY indicator_name
    ORDER BY record_count DESC;
    """)

    print('Existing Treasury Yield data:')
    if result.empty:
        print('  No Treasury Yield data found')
    else:
        for _, row in result.iterrows():
            print(f'  {row["indicator_name"]}: {row["record_count"]:,} records')

    # Check total economic indicators
    total = db.fetch_dataframe("""
    SELECT COUNT(*) as total_records FROM source.economic_indicators;
    """)
    print(f'\nTotal economic indicator records: {total.iloc[0]["total_records"]:,}')

    # Check all indicators with record counts
    all_indicators = db.fetch_dataframe("""
    SELECT indicator_name, COUNT(*) as record_count
    FROM source.economic_indicators 
    GROUP BY indicator_name
    ORDER BY record_count DESC
    LIMIT 10;
    """)
    
    print('\nTop 10 indicators by record count:')
    for _, row in all_indicators.iterrows():
        print(f'  {row["indicator_name"]}: {row["record_count"]:,} records')

    db.close()
    
except Exception as e:
    print(f'Error: {e}')
