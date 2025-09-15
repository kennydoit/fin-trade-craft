#!/usr/bin/env python3

from db.postgres_database_manager import PostgresDatabaseManager

try:
    db = PostgresDatabaseManager()
    db.connect()
    
    # Get the actual schema of extraction_watermarks
    schema_query = """
    SELECT 
        column_name, 
        data_type, 
        is_nullable
    FROM information_schema.columns 
    WHERE table_schema = 'source' 
        AND table_name = 'extraction_watermarks'
    ORDER BY ordinal_position
    """
    
    schema_result = db.execute_query(schema_query)
    print('extraction_watermarks table schema:')
    for row in schema_result:
        print(f'  - {row[0]} ({row[1]}, nullable: {row[2]})')
        
    print()
    
    # Get sample data with actual column names
    sample_query = """
    SELECT * FROM source.extraction_watermarks LIMIT 5
    """
    
    sample_result = db.execute_query(sample_query)
    if sample_result:
        print('Sample watermark data:')
        for i, row in enumerate(sample_result, 1):
            print(f'  {i}. {row}')
    else:
        print('❌ No data in extraction_watermarks')

except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()
finally:
    if 'db' in locals():
        db.close()
