#!/usr/bin/env python3

from db.postgres_database_manager import PostgresDatabaseManager

try:
    db = PostgresDatabaseManager()
    db.connect()

    # Check what feature tables were created
    feature_tables = db.fetch_dataframe("""
    SELECT schemaname, tablename, 
           pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
    FROM pg_tables 
    WHERE schemaname = 'transformed' AND tablename LIKE '%_features'
    ORDER BY tablename;
    """)
    
    print('=== FEATURE TABLES CREATED ===')
    for _, row in feature_tables.iterrows():
        print(f'  {row["tablename"]}: {row["size"]}')
        
        # Get record count and feature count for each table
        count_query = f"SELECT COUNT(*) as record_count FROM {row['schemaname']}.{row['tablename']}"
        count_result = db.fetch_dataframe(count_query)
        record_count = count_result.iloc[0]['record_count']
        
        # Get column count (excluding identifier columns)
        col_query = f"""
        SELECT COUNT(*) as feature_count 
        FROM information_schema.columns 
        WHERE table_schema = '{row['schemaname']}' AND table_name = '{row['tablename']}'
        AND column_name NOT IN ('symbol_id', 'symbol', 'fiscal_date_ending', 'created_at', 'updated_at')
        """
        col_result = db.fetch_dataframe(col_query)
        feature_count = col_result.iloc[0]['feature_count']
        
        print(f'    Records: {record_count:,} | Features: {feature_count}')
        
    print('\n=== SAMPLE FROM EACH TABLE ===')
    for _, row in feature_tables.iterrows():
        print(f'\n{row["tablename"]}:')
        sample_query = f"""
        SELECT symbol, fiscal_date_ending 
        FROM {row['schemaname']}.{row['tablename']} 
        ORDER BY fiscal_date_ending DESC 
        LIMIT 3
        """
        sample = db.fetch_dataframe(sample_query)
        for _, sample_row in sample.iterrows():
            print(f'  {sample_row["symbol"]} | {sample_row["fiscal_date_ending"]}')
            
except Exception as e:
    print(f'Error: {e}')
finally:
    if 'db' in locals():
        db.close()
