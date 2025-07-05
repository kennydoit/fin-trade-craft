"""
Examples of how to update income statement data for quarterly refreshes.
This script demonstrates different update strategies for maintaining current data.
"""

from extract_income_statement import IncomeStatementExtractor

def quarterly_refresh_example():
    """Example: Quarterly refresh for production use"""
    extractor = IncomeStatementExtractor()
    
    print("=== QUARTERLY REFRESH EXAMPLE ===")
    print("Use this approach every quarter to get latest financial data")
    
    # Update symbols that haven't been refreshed in 90+ days
    # This catches new quarterly reports while avoiding unnecessary API calls
    extractor.run_etl_update(
        exchange_filter='NASDAQ', 
        limit=100,  # Process 100 symbols at a time
        min_age_days=90  # Only update if data is 90+ days old
    )

def latest_data_only_example():
    """Example: Keep only the most recent periods to save storage"""
    extractor = IncomeStatementExtractor()
    
    print("=== LATEST DATA ONLY EXAMPLE ===")
    print("Use this to keep only recent data and save database storage")
    
    # Keep only the latest 4 periods (1 year of quarters)
    # This replaces all historical data with just recent data
    extractor.run_etl_latest_periods(
        exchange_filter='NASDAQ',
        limit=50,  # Process 50 symbols at a time
        periods_back=4  # Keep 4 latest periods
    )

def force_refresh_example():
    """Example: Force refresh all symbols regardless of age"""
    extractor = IncomeStatementExtractor()
    
    print("=== FORCE REFRESH EXAMPLE ===")
    print("Use this to force update all symbols (e.g., after API changes)")
    
    # Update all symbols regardless of when they were last updated
    extractor.run_etl_update(
        exchange_filter='NASDAQ',
        limit=20,  # Small batch for testing
        min_age_days=0  # Update all symbols regardless of age
    )

def monthly_maintenance_example():
    """Example: Monthly maintenance to catch new earnings"""
    extractor = IncomeStatementExtractor()
    
    print("=== MONTHLY MAINTENANCE EXAMPLE ===")
    print("Use this monthly to catch companies that reported early")
    
    # Update symbols older than 30 days
    extractor.run_etl_update(
        exchange_filter='NASDAQ',
        min_age_days=30  # Update if data is 30+ days old
    )

if __name__ == "__main__":
    print("Income Statement Update Examples")
    print("=" * 50)
    print()
    print("Choose an update strategy:")
    print("1. Quarterly refresh (recommended)")
    print("2. Latest data only (storage efficient)")
    print("3. Force refresh all")
    print("4. Monthly maintenance")
    print()
    
    choice = input("Enter your choice (1-4): ").strip()
    
    if choice == "1":
        quarterly_refresh_example()
    elif choice == "2":
        latest_data_only_example()
    elif choice == "3":
        force_refresh_example()
    elif choice == "4":
        monthly_maintenance_example()
    else:
        print("Invalid choice. Please run again and select 1-4.")
