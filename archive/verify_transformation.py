#!/usr/bin/env python3
"""
Quick verification of the company_master table
"""

from db.postgres_database_manager import PostgresDatabaseManager


def main():
    db_manager = PostgresDatabaseManager()

    with db_manager as db:
        # Check sample records
        query = """
        SELECT symbol, name, exchange, country, sector, 
               cash_flow_count, time_series_daily_adjusted_count 
        FROM transformed.company_master 
        WHERE country = 'USA' AND sector IS NOT NULL 
        LIMIT 5
        """

        result = db.fetch_query(query)
        print("✅ Sample records from company_master:")
        for r in result:
            print(f"  {r[0]}: {r[1]} | {r[2]} | {r[3]} | {r[4]} | CF:{r[5]} TS:{r[6]}")

        # Check data availability counts
        query2 = """
        SELECT 
            COUNT(*) as total_companies,
            COUNT(CASE WHEN cash_flow_count > 0 THEN 1 END) as have_cash_flow,
            COUNT(CASE WHEN time_series_daily_adjusted_count > 0 THEN 1 END) as have_time_series,
            COUNT(CASE WHEN sector IS NOT NULL THEN 1 END) as have_sector
        FROM transformed.company_master
        """

        result2 = db.fetch_query(query2)
        stats = result2[0]
        print("\n📊 Data availability stats:")
        print(f"  Total companies: {stats[0]}")
        print(f"  Have cash flow data: {stats[1]} ({stats[1]/stats[0]*100:.1f}%)")
        print(f"  Have time series data: {stats[2]} ({stats[2]/stats[0]*100:.1f}%)")
        print(f"  Have sector info: {stats[3]} ({stats[3]/stats[0]*100:.1f}%)")

if __name__ == "__main__":
    main()
