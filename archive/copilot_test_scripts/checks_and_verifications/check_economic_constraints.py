import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from db.postgres_database_manager import PostgresDatabaseManager

db_manager = PostgresDatabaseManager()
with db_manager as db:
    query = """
    SELECT tc.constraint_name, tc.constraint_type, ccu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
    WHERE tc.table_name = 'economic_indicators' AND tc.table_schema = 'public'
    ORDER BY tc.constraint_name, ccu.column_name
    """
    result = db.fetch_query(query)
    if result:
        print("Constraints on economic_indicators table:")
        for row in result:
            print(f"  {row[0]} ({row[1]}): {row[2]}")
    else:
        print("No constraints found on economic_indicators table")
