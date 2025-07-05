"""
Example scripts for different economic indicators extraction strategies.

This file demonstrates various approaches to extracting and updating economic indicators data
using the EconomicIndicatorsExtractor class.
"""

import sys
from pathlib import Path

# Add the parent directories to the path so we can import from data_pipeline
sys.path.append(str(Path(__file__).parent.parent.parent))
from data_pipeline.extract.extract_economic_indicators import EconomicIndicatorsExtractor

def example_initial_extraction():
    """Example 1: Initial extraction of economic indicators data."""
    print("="*70)
    print("EXAMPLE 1: Initial Economic Indicators Data Extraction")
    print("="*70)
    
    extractor = EconomicIndicatorsExtractor()
    
    # Start with GDP indicators (quarterly data)
    print("\nStep 1: Extract GDP indicators (quarterly data)")
    gdp_indicators = ['REAL_GDP', 'REAL_GDP_PER_CAPITA']
    extractor.run_etl_batch(gdp_indicators, batch_size=2, force_update=False)
    
    # Then Treasury yields (daily data)
    print("\nStep 2: Extract Treasury yields (daily data)")
    treasury_indicators = ['TREASURY_YIELD_3MONTH', 'TREASURY_YIELD_2YEAR', 'TREASURY_YIELD_5YEAR', 
                          'TREASURY_YIELD_7YEAR', 'TREASURY_YIELD_10YEAR', 'TREASURY_YIELD_30YEAR']
    extractor.run_etl_batch(treasury_indicators, batch_size=3, force_update=False)
    
    # Then Federal Funds Rate
    print("\nStep 3: Extract Federal Funds Rate")
    fed_indicators = ['FEDERAL_FUNDS_RATE']
    extractor.run_etl_batch(fed_indicators, batch_size=1, force_update=False)
    
    # Then inflation indicators (monthly data)
    print("\nStep 4: Extract inflation indicators (monthly data)")
    inflation_indicators = ['CPI', 'INFLATION']
    extractor.run_etl_batch(inflation_indicators, batch_size=2, force_update=False)
    
    # Then labor market indicators (monthly data)
    print("\nStep 5: Extract labor market indicators (monthly data)")
    labor_indicators = ['UNEMPLOYMENT', 'NONFARM_PAYROLL']
    extractor.run_etl_batch(labor_indicators, batch_size=2, force_update=False)
    
    # Finally other economic indicators
    print("\nStep 6: Extract other economic indicators")
    other_indicators = ['RETAIL_SALES', 'DURABLES']
    extractor.run_etl_batch(other_indicators, batch_size=2, force_update=False)
    
    print("\nInitial extraction completed!")
    extractor.get_database_summary()

def example_daily_updates():
    """Example 2: Daily updates for daily indicators."""
    print("="*70)
    print("EXAMPLE 2: Daily Updates for Daily Indicators")
    print("="*70)
    
    extractor = EconomicIndicatorsExtractor()
    
    # Update only daily indicators that haven't been updated in the last day
    print("Updating daily indicators (Treasury yields and Fed funds)...")
    daily_indicators = ['TREASURY_YIELD_10YEAR', 'TREASURY_YIELD_3MONTH', 'TREASURY_YIELD_2YEAR', 
                       'TREASURY_YIELD_5YEAR', 'TREASURY_YIELD_7YEAR', 'TREASURY_YIELD_30YEAR', 
                       'FEDERAL_FUNDS_RATE']
    extractor.run_etl_latest_periods(days_threshold=1, batch_size=3)
    
    extractor.get_database_summary()

def example_monthly_updates():
    """Example 3: Monthly updates for monthly indicators."""
    print("="*70)
    print("EXAMPLE 3: Monthly Updates for Monthly Indicators")
    print("="*70)
    
    extractor = EconomicIndicatorsExtractor()
    
    # Update only monthly indicators that haven't been updated in the last 30 days
    print("Updating monthly indicators (30-day threshold)...")
    monthly_indicators = ['CPI', 'INFLATION', 'RETAIL_SALES', 'DURABLES', 'UNEMPLOYMENT', 'NONFARM_PAYROLL']
    total_inserted, status_summary = extractor.run_etl_batch(monthly_indicators, batch_size=3, force_update=False)
    
    print(f"\nMonthly update results:")
    print(f"Total records inserted: {total_inserted}")
    print(f"Status summary: {status_summary}")
    
    extractor.get_database_summary()

def example_quarterly_updates():
    """Example 4: Quarterly updates for quarterly indicators."""
    print("="*70)
    print("EXAMPLE 4: Quarterly Updates for Quarterly Indicators")
    print("="*70)
    
    extractor = EconomicIndicatorsExtractor()
    
    # Update quarterly indicators that haven't been updated in the last 90 days
    print("Updating quarterly indicators (90-day threshold)...")
    quarterly_indicators = ['REAL_GDP', 'REAL_GDP_PER_CAPITA']
    total_inserted, status_summary = extractor.run_etl_batch(quarterly_indicators, batch_size=2, force_update=False)
    
    print(f"\nQuarterly update results:")
    print(f"Total records inserted: {total_inserted}")
    print(f"Status summary: {status_summary}")
    
    extractor.get_database_summary()

def example_force_refresh():
    """Example 5: Force refresh of specific indicators."""
    print("="*70)
    print("EXAMPLE 5: Force Refresh of Specific Indicators")
    print("="*70)
    
    extractor = EconomicIndicatorsExtractor()
    
    # Force update specific indicators regardless of last update date
    print("Force refreshing priority indicators...")
    priority_indicators = ['FEDERAL_FUNDS_RATE', 'TREASURY_YIELD_10YEAR', 'UNEMPLOYMENT']  # Most important indicators
    extractor.run_etl_update(priority_indicators, batch_size=2)
    
    extractor.get_database_summary()

def example_selective_extraction():
    """Example 6: Selective extraction by indicator type."""
    print("="*70)
    print("EXAMPLE 6: Selective Extraction by Indicator Type")
    print("="*70)
    
    extractor = EconomicIndicatorsExtractor()
    
    # Option A: Only interest rate indicators
    print("Option A: Interest rate indicators only")
    interest_rate_indicators = ['FEDERAL_FUNDS_RATE', 'TREASURY_YIELD_10YEAR', 'TREASURY_YIELD_3MONTH']
    extractor.run_etl_batch(interest_rate_indicators, batch_size=2, force_update=False)
    
    # Option B: Only economic growth indicators
    print("\nOption B: Economic growth indicators only")
    growth_indicators = ['REAL_GDP', 'REAL_GDP_PER_CAPITA', 'RETAIL_SALES']
    extractor.run_etl_batch(growth_indicators, batch_size=2, force_update=False)
    
    # Option C: Only labor market indicators
    print("\nOption C: Labor market indicators only")
    labor_indicators = ['UNEMPLOYMENT', 'NONFARM_PAYROLL']
    extractor.run_etl_batch(labor_indicators, batch_size=2, force_update=False)
    
    # Option D: Only inflation indicators
    print("\nOption D: Inflation indicators only")
    inflation_indicators = ['CPI', 'INFLATION']
    extractor.run_etl_batch(inflation_indicators, batch_size=2, force_update=False)
    
    extractor.get_database_summary()

def example_smart_updates():
    """Example 7: Smart updates based on indicator frequency."""
    print("="*70)
    print("EXAMPLE 7: Smart Updates Based on Indicator Frequency")
    print("="*70)
    
    extractor = EconomicIndicatorsExtractor()
    
    # Daily indicators - check for updates daily
    print("Checking daily indicators for updates...")
    daily_indicators = ['TREASURY_YIELD_10YEAR', 'TREASURY_YIELD_3MONTH', 'TREASURY_YIELD_2YEAR', 
                       'TREASURY_YIELD_5YEAR', 'TREASURY_YIELD_7YEAR', 'TREASURY_YIELD_30YEAR', 
                       'FEDERAL_FUNDS_RATE']
    for indicator in daily_indicators[:3]:  # Test with first 3 to save API calls
        print(f"Updating {indicator}...")
        extractor.extract_and_load_indicator(indicator, force_update=False)
    
    # Monthly indicators - check for updates monthly (30 days)
    print("\nChecking monthly indicators for updates...")
    monthly_indicators = ['CPI', 'INFLATION', 'RETAIL_SALES', 'DURABLES', 'UNEMPLOYMENT', 'NONFARM_PAYROLL']
    
    # Get indicators that need updates (30+ days old)
    indicators_needing_update = extractor.get_indicators_needing_update(days_threshold=30)
    
    if indicators_needing_update:
        print(f"Found {len(indicators_needing_update)} indicators needing updates")
        extractor.run_etl_update(indicators_needing_update[:3], batch_size=2)  # Test with first 3
    else:
        print("No monthly indicators need updates")
    
    # Quarterly indicators - check for updates quarterly (90 days)
    print("\nChecking quarterly indicators for updates...")
    quarterly_indicators = ['REAL_GDP', 'REAL_GDP_PER_CAPITA']
    
    # Get indicators that need updates (90+ days old)
    quarterly_needing_update = extractor.get_indicators_needing_update(days_threshold=90)
    
    if quarterly_needing_update:
        print(f"Found {len(quarterly_needing_update)} quarterly indicators needing updates")
        extractor.run_etl_update(quarterly_needing_update, batch_size=2)
    else:
        print("No quarterly indicators need updates")
    
    extractor.get_database_summary()

def example_production_schedule():
    """Example 8: Production-ready scheduled updates."""
    print("="*70)
    print("EXAMPLE 8: Production-Ready Scheduled Updates")
    print("="*70)
    
    extractor = EconomicIndicatorsExtractor()
    
    print("This example shows how you might schedule updates in production:")
    print("\n1. DAILY SCHEDULE (run every weekday after market close):")
    print("   - Update daily indicators (Treasury yields, Fed funds rate)")
    print("   - These update daily and are most important for financial markets")
    
    # Simulate daily update
    daily_indicators = ['TREASURY_YIELD_10YEAR', 'TREASURY_YIELD_3MONTH', 'FEDERAL_FUNDS_RATE']
    print(f"\nExecuting daily update for critical indicators...")
    extractor.run_etl_latest_periods(days_threshold=1, batch_size=2)
    
    print("\n2. WEEKLY SCHEDULE (run every Monday):")
    print("   - Check all daily indicators for any missed updates")
    print("   - Catch up on any failed daily updates")
    
    # Simulate weekly catch-up
    print(f"\nExecuting weekly catch-up...")
    extractor.run_etl_latest_periods(days_threshold=7, batch_size=3)
    
    print("\n3. MONTHLY SCHEDULE (run first weekday of each month):")
    print("   - Update monthly indicators (CPI, inflation, employment, retail sales)")
    print("   - These typically update monthly with government releases")
    
    # Simulate monthly update
    monthly_indicators = ['CPI', 'INFLATION', 'UNEMPLOYMENT', 'NONFARM_PAYROLL', 'RETAIL_SALES', 'DURABLES']
    print(f"\nExecuting monthly indicators update...")
    extractor.run_etl_latest_periods(days_threshold=30, batch_size=3)
    
    print("\n4. QUARTERLY SCHEDULE (run first weekday after quarter end):")
    print("   - Update quarterly indicators (GDP metrics)")
    print("   - These update quarterly with government GDP releases")
    
    # Simulate quarterly update
    quarterly_indicators = ['REAL_GDP', 'REAL_GDP_PER_CAPITA']
    print(f"\nExecuting quarterly indicators update...")
    extractor.run_etl_latest_periods(days_threshold=90, batch_size=2)
    
    extractor.get_database_summary()

def example_yield_curve_analysis():
    """Example 9: Extract all Treasury yields for yield curve analysis."""
    print("="*70)
    print("EXAMPLE 9: Treasury Yield Curve Data Extraction")
    print("="*70)
    
    extractor = EconomicIndicatorsExtractor()
    
    print("Extracting all Treasury yields for yield curve analysis...")
    print("This includes: 3-month, 2-year, 5-year, 7-year, 10-year, and 30-year yields")
    
    # Extract all Treasury yields
    yield_indicators = ['TREASURY_YIELD_3MONTH', 'TREASURY_YIELD_2YEAR', 'TREASURY_YIELD_5YEAR', 
                       'TREASURY_YIELD_7YEAR', 'TREASURY_YIELD_10YEAR', 'TREASURY_YIELD_30YEAR']
    
    total_inserted, status_summary = extractor.run_etl_batch(yield_indicators, batch_size=3, force_update=False)
    
    print(f"\nYield curve extraction results:")
    print(f"Total records inserted: {total_inserted}")
    print(f"Status summary: {status_summary}")
    print("\nThis data can be used for:")
    print("- Yield curve construction and analysis")
    print("- Interest rate risk modeling")
    print("- Economic forecasting")
    print("- Bond portfolio optimization")
    
    extractor.get_database_summary()

def main():
    """Run the example that you want to test."""
    
    # Choose which example to run:
    
    # Uncomment one of these to run a specific example:
    
    # example_initial_extraction()      # Full initial data load
    # example_daily_updates()           # Daily indicator updates
    # example_monthly_updates()         # Monthly indicator updates
    # example_quarterly_updates()       # Quarterly indicator updates
    # example_force_refresh()           # Force refresh specific indicators
    # example_selective_extraction()    # Extract by indicator type
    # example_smart_updates()           # Smart updates based on frequency
    example_production_schedule()      # Production-ready scheduling example
    # example_yield_curve_analysis()    # Treasury yield curve data
    
    print("\n" + "="*70)
    print("Example completed successfully!")
    print("="*70)

if __name__ == "__main__":
    main()
