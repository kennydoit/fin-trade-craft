"""
Example scripts for different commodities extraction strategies.

This file demonstrates various approaches to extracting and updating commodities data
using the CommoditiesExtractor class.
"""

import sys
from pathlib import Path

# Add the parent directories to the path so we can import from data_pipeline
sys.path.append(str(Path(__file__).parent.parent.parent))
from data_pipeline.extract.extract_commodities import CommoditiesExtractor

def example_initial_extraction():
    """Example 1: Initial extraction of commodities data."""
    print("="*60)
    print("EXAMPLE 1: Initial Commodities Data Extraction")
    print("="*60)
    
    extractor = CommoditiesExtractor()
    
    # Start with energy commodities (daily data)
    print("\nStep 1: Extract energy commodities (daily data)")
    energy_commodities = ['WTI', 'BRENT', 'NATURAL_GAS']
    extractor.run_etl_batch(energy_commodities, batch_size=2, force_update=False)
    
    # Then metals (monthly data)
    print("\nStep 2: Extract metals commodities (monthly data)")
    metals_commodities = ['COPPER', 'ALUMINUM']
    extractor.run_etl_batch(metals_commodities, batch_size=2, force_update=False)
    
    # Then agricultural commodities (monthly data)
    print("\nStep 3: Extract agricultural commodities (monthly data)")
    agriculture_commodities = ['WHEAT', 'CORN', 'COTTON', 'SUGAR', 'COFFEE']
    extractor.run_etl_batch(agriculture_commodities, batch_size=3, force_update=False)
    
    # Finally the global index
    print("\nStep 4: Extract global commodities index")
    index_commodities = ['ALL_COMMODITIES']
    extractor.run_etl_batch(index_commodities, batch_size=1, force_update=False)
    
    print("\nInitial extraction completed!")
    extractor.get_database_summary()

def example_daily_updates():
    """Example 2: Daily updates for energy commodities."""
    print("="*60)
    print("EXAMPLE 2: Daily Updates for Energy Commodities")
    print("="*60)
    
    extractor = CommoditiesExtractor()
    
    # Update only energy commodities that haven't been updated in the last day
    print("Updating energy commodities (daily frequency)...")
    energy_commodities = ['WTI', 'BRENT', 'NATURAL_GAS']
    extractor.run_etl_latest_periods(days_threshold=1, batch_size=2)
    
    extractor.get_database_summary()

def example_monthly_updates():
    """Example 3: Monthly updates for non-energy commodities."""
    print("="*60)
    print("EXAMPLE 3: Monthly Updates for Non-Energy Commodities")
    print("="*60)
    
    extractor = CommoditiesExtractor()
    
    # Update only commodities that haven't been updated in the last 30 days
    print("Updating monthly commodities (30-day threshold)...")
    monthly_commodities = ['COPPER', 'ALUMINUM', 'WHEAT', 'CORN', 'COTTON', 'SUGAR', 'COFFEE', 'ALL_COMMODITIES']
    total_inserted, status_summary = extractor.run_etl_batch(monthly_commodities, batch_size=3, force_update=False)
    
    print(f"\nMonthly update results:")
    print(f"Total records inserted: {total_inserted}")
    print(f"Status summary: {status_summary}")
    
    extractor.get_database_summary()

def example_force_refresh():
    """Example 4: Force refresh of specific commodities."""
    print("="*60)
    print("EXAMPLE 4: Force Refresh of Specific Commodities")
    print("="*60)
    
    extractor = CommoditiesExtractor()
    
    # Force update specific commodities regardless of last update date
    print("Force refreshing priority commodities...")
    priority_commodities = ['WTI', 'BRENT']  # Most important energy commodities
    extractor.run_etl_update(priority_commodities, batch_size=2)
    
    extractor.get_database_summary()

def example_selective_extraction():
    """Example 5: Selective extraction by commodity type."""
    print("="*60)
    print("EXAMPLE 5: Selective Extraction by Commodity Type")
    print("="*60)
    
    extractor = CommoditiesExtractor()
    
    # Option A: Only energy commodities
    print("Option A: Energy commodities only")
    energy_only = ['WTI', 'BRENT', 'NATURAL_GAS']
    extractor.run_etl_batch(energy_only, batch_size=2, force_update=False)
    
    # Option B: Only precious metals (if we had them)
    # For this example, we'll use industrial metals
    print("\nOption B: Industrial metals only")
    metals_only = ['COPPER', 'ALUMINUM']
    extractor.run_etl_batch(metals_only, batch_size=2, force_update=False)
    
    # Option C: Only food commodities
    print("\nOption C: Food commodities only")
    food_only = ['WHEAT', 'CORN', 'SUGAR', 'COFFEE']
    extractor.run_etl_batch(food_only, batch_size=2, force_update=False)
    
    extractor.get_database_summary()

def example_smart_updates():
    """Example 6: Smart updates based on commodity update frequency."""
    print("="*60)
    print("EXAMPLE 6: Smart Updates Based on Commodity Frequency")
    print("="*60)
    
    extractor = CommoditiesExtractor()
    
    # Daily commodities (energy) - check for updates daily
    print("Checking daily commodities for updates...")
    daily_commodities = ['WTI', 'BRENT', 'NATURAL_GAS']
    for commodity in daily_commodities:
        print(f"Updating {commodity}...")
        extractor.extract_and_load_commodity(commodity, force_update=False)
    
    # Monthly commodities - check for updates monthly (30 days)
    print("\nChecking monthly commodities for updates...")
    monthly_commodities = ['COPPER', 'ALUMINUM', 'WHEAT', 'CORN', 'COTTON', 'SUGAR', 'COFFEE', 'ALL_COMMODITIES']
    
    # Get commodities that need updates (30+ days old)
    commodities_needing_update = extractor.get_commodities_needing_update(days_threshold=30)
    
    if commodities_needing_update:
        print(f"Found {len(commodities_needing_update)} commodities needing updates")
        extractor.run_etl_update(commodities_needing_update, batch_size=3)
    else:
        print("No monthly commodities need updates")
    
    extractor.get_database_summary()

def example_production_schedule():
    """Example 7: Production-ready scheduled updates."""
    print("="*60)
    print("EXAMPLE 7: Production-Ready Scheduled Updates")
    print("="*60)
    
    extractor = CommoditiesExtractor()
    
    print("This example shows how you might schedule updates in production:")
    print("\n1. DAILY SCHEDULE (run every day at market close):")
    print("   - Update energy commodities (WTI, BRENT, NATURAL_GAS)")
    print("   - These update daily and are most volatile")
    
    # Simulate daily energy update
    energy_commodities = ['WTI', 'BRENT', 'NATURAL_GAS']
    print(f"\nExecuting daily energy update...")
    extractor.run_etl_latest_periods(days_threshold=1, batch_size=2)
    
    print("\n2. WEEKLY SCHEDULE (run every Monday):")
    print("   - Check all commodities for any missed updates")
    print("   - Catch up on any failed daily updates")
    
    # Simulate weekly catch-up
    print(f"\nExecuting weekly catch-up...")
    extractor.run_etl_latest_periods(days_threshold=7, batch_size=3)
    
    print("\n3. MONTHLY SCHEDULE (run first of each month):")
    print("   - Update monthly commodities (metals, agriculture, indices)")
    print("   - These typically update monthly")
    
    # Simulate monthly update
    monthly_commodities = ['COPPER', 'ALUMINUM', 'WHEAT', 'CORN', 'COTTON', 'SUGAR', 'COFFEE', 'ALL_COMMODITIES']
    print(f"\nExecuting monthly commodities update...")
    extractor.run_etl_latest_periods(days_threshold=30, batch_size=3)
    
    extractor.get_database_summary()

def main():
    """Run the example that you want to test."""
    
    # Choose which example to run:
    
    # Uncomment one of these to run a specific example:
    
    # example_initial_extraction()      # Full initial data load
    # example_daily_updates()           # Daily energy updates
    # example_monthly_updates()         # Monthly non-energy updates
    # example_force_refresh()           # Force refresh specific commodities
    # example_selective_extraction()    # Extract by commodity type
    # example_smart_updates()           # Smart updates based on frequency
    example_production_schedule()      # Production-ready scheduling example
    
    print("\n" + "="*60)
    print("Example completed successfully!")
    print("="*60)

if __name__ == "__main__":
    main()
