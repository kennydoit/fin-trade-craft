#!/usr/bin/env python3

from db.postgres_database_manager import PostgresDatabaseManager

db = PostgresDatabaseManager()
db.connect()

# Check company_overview_id column default
result = db.fetch_dataframe("""
SELECT column_name, column_default 
FROM information_schema.columns 
WHERE table_schema = 'source' AND table_name = 'company_overview' 
AND column_name = 'company_overview_id';
""")

print('company_overview_id column info:')
print(result.to_string())

# Check if there's a sequence for this table
sequences = db.fetch_dataframe("""
SELECT sequence_name, data_type, start_value, increment_by
FROM information_schema.sequences 
WHERE sequence_schema = 'source';
""")

print('\nSequences in source schema:')
print(sequences.to_string())

db.close()
