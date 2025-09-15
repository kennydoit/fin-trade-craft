#!/usr/bin/env python3

from db.postgres_database_manager import PostgresDatabaseManager

db = PostgresDatabaseManager()
db.connect()

# Check source_run_id and run_id columns
result = db.fetch_dataframe("""
SELECT column_name, is_nullable, column_default
FROM information_schema.columns 
WHERE table_schema = 'source' AND table_name = 'economic_indicators'
AND column_name IN ('source_run_id', 'run_id')
ORDER BY ordinal_position;
""")

print('source_run_id and run_id columns:')
for _, row in result.iterrows():
    print(f'  {row["column_name"]}: nullable={row["is_nullable"]}, default={row["column_default"]}')

# Also check if we can make source_run_id nullable
print('\nAll NOT NULL columns in economic_indicators:')
not_null_cols = db.fetch_dataframe("""
SELECT column_name, is_nullable, column_default
FROM information_schema.columns 
WHERE table_schema = 'source' AND table_name = 'economic_indicators'
AND is_nullable = 'NO' AND column_default IS NULL
ORDER BY ordinal_position;
""")

for _, row in not_null_cols.iterrows():
    print(f'  {row["column_name"]}: nullable={row["is_nullable"]}, default={row["column_default"]}')

db.close()
