#!/usr/bin/env python3
"""Quick check of watermark table status."""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from db.postgres_database_manager import PostgresDatabaseManager

db = PostgresDatabaseManager()
db.connect()

# Check watermark status
query = """
SELECT 
    transformation_group,
    COUNT(*) as total_symbols,
    COUNT(CASE WHEN last_successful_run > NOW() - INTERVAL '48 hours' THEN 1 END) as recent_48h,
    COUNT(CASE WHEN last_successful_run IS NULL OR last_successful_run <= NOW() - INTERVAL '48 hours' THEN 1 END) as stale_48h,
    MAX(last_successful_run) as most_recent_run,
    MIN(last_successful_run) as oldest_run
FROM transforms.transformation_watermarks
WHERE transformation_group = 'time_series_daily_adjusted'
GROUP BY transformation_group
"""

results = db.fetch_query(query)

if results:
    for row in results:
        group, total, recent, stale, most_recent, oldest = row
        print(f"Group: {group}")
        print(f"Total symbols: {total:,}")
        print(f"Recent (< 48h): {recent:,}")
        print(f"Stale (>= 48h): {stale:,}")
        print(f"Most recent run: {most_recent}")
        print(f"Oldest run: {oldest}")
else:
    print("No watermark data found for time_series_daily_adjusted")

db.close()
