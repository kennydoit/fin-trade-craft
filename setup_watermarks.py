#!/usr/bin/env python3

from db.postgres_database_manager import PostgresDatabaseManager

try:
    db = PostgresDatabaseManager()
    db.connect()
    
    # Check source schema tables
    source_tables_query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'source' 
    ORDER BY table_name
    """
    
    source_tables = db.execute_query(source_tables_query)
    print('Source schema tables:')
    for table in source_tables:
        print(f'  - {table[0]}')
        
    # Check if extraction_watermarks exists in source
    if ('extraction_watermarks',) not in source_tables:
        print('\n❌ extraction_watermarks table not found in source schema')
        
        # Create the extraction_watermarks table
        create_watermarks_table = """
        CREATE TABLE IF NOT EXISTS source.extraction_watermarks (
            symbol_id TEXT NOT NULL,
            table_name TEXT NOT NULL,
            last_fiscal_date DATE,
            last_extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            consecutive_failures INTEGER DEFAULT 0,
            PRIMARY KEY (symbol_id, table_name)
        )
        """
        
        print('Creating extraction_watermarks table...')
        db.execute_query(create_watermarks_table)
        print('✅ Created extraction_watermarks table')
    else:
        print('\n✅ extraction_watermarks table exists')

    # Now check if ORIC exists in source.extraction_watermarks
    watermark_check = """
    SELECT 
        symbol_id,
        table_name,
        last_fiscal_date,
        last_extracted_at,
        consecutive_failures
    FROM source.extraction_watermarks 
    WHERE symbol_id = 'ORIC' AND table_name = 'balance_sheet'
    """
    
    watermark_result = db.execute_query(watermark_check)
    if watermark_result:
        print('\nORIC watermark data:')
        for row in watermark_result:
            print(f'  Symbol: {row[0]}, Table: {row[1]}')
            print(f'  Last Fiscal: {row[2]}, Last Extracted: {row[3]}')
            print(f'  Failures: {row[4]}')
    else:
        print('\n❌ No ORIC watermark in source.extraction_watermarks')

except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()
finally:
    if 'db' in locals():
        db.close()
