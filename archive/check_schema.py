#!/usr/bin/env python3
"""
Check the company_master table schema to understand the data type issue
"""

import pandas as pd

from db.postgres_database_manager import PostgresDatabaseManager


def main():
    db_manager = PostgresDatabaseManager()

    with db_manager as db:
        print("🔍 Checking company_master table schema...")
        schema_query = """
        SELECT column_name, data_type, is_nullable, column_default 
        FROM information_schema.columns 
        WHERE table_name = 'company_master' AND table_schema = 'transformed' 
        ORDER BY ordinal_position
        """
        schema_data = db.fetch_query(schema_query)
        schema_df = pd.DataFrame(schema_data, columns=['column', 'type', 'nullable', 'default'])

        print(f"Company master table has {len(schema_df)} columns:")
        print(schema_df.to_string())

        # Check for INTEGER columns that might be causing issues
        integer_cols = schema_df[schema_df['type'] == 'integer']['column'].tolist()
        print(f"\n🔍 INTEGER columns: {integer_cols}")

        # Check if the table has any data
        count_query = "SELECT COUNT(*) FROM transformed.company_master"
        count_result = db.fetch_query(count_query)
        print(f"\n📊 Current row count: {count_result[0][0] if count_result else 0}")

if __name__ == "__main__":
    main()
