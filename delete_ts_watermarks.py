"""Delete and reinitialize time_series_daily_adjusted transformation group."""
from db.postgres_database_manager import PostgresDatabaseManager

db = PostgresDatabaseManager()
db.connect()

# Delete old watermarks
delete_query = """
    DELETE FROM transforms.transformation_watermarks 
    WHERE transformation_group = %s
"""
db.execute_query(delete_query, ('time_series_daily_adjusted',))
print("Deleted old time_series_daily_adjusted watermarks")

db.close()
