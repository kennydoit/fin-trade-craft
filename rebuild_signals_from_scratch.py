"""
Rebuild Trading Signals From Scratch

This script rebuilds the entire trading signals pipeline from scratch:
1. Checks data gaps
2. Runs time series transforms to calculate technical indicators
3. Initializes and populates trading signals table

Usage:
    # Check what needs to be done (dry run)
    python rebuild_signals_from_scratch.py --check-only
    
    # Full rebuild (this will take time!)
    python rebuild_signals_from_scratch.py --full
    
    # Incremental update (faster, only recent data)
    python rebuild_signals_from_scratch.py --incremental
"""

import sys
import subprocess
import argparse
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from db.postgres_database_manager import PostgresDatabaseManager


def check_status():
    """Check current status of all tables."""
    db = PostgresDatabaseManager()
    db.connect()
    
    print("="*80)
    print("CURRENT DATA PIPELINE STATUS")
    print("="*80)
    
    # Check raw data
    result = db.fetch_query('''
        SELECT COUNT(*) as total, 
               COUNT(DISTINCT symbol_id) as symbols,
               MIN(date) as min_date, 
               MAX(date) as max_date
        FROM raw.time_series_daily_adjusted
    ''')
    print(f"\n1. RAW DATA (raw.time_series_daily_adjusted):")
    print(f"   Rows: {result[0][0]:,}")
    print(f"   Symbols: {result[0][1]:,}")
    print(f"   Date range: {result[0][2]} to {result[0][3]}")
    raw_max = result[0][3]
    
    # Check transform data
    result = db.fetch_query('''
        SELECT COUNT(*) as total,
               COUNT(DISTINCT symbol_id) as symbols,
               MIN(date) as min_date,
               MAX(date) as max_date,
               COUNT(ohlcv_ema_8) as with_indicators
        FROM transforms.time_series_daily_adjusted
    ''')
    print(f"\n2. TRANSFORMS (transforms.time_series_daily_adjusted):")
    print(f"   Rows: {result[0][0]:,}")
    print(f"   Symbols: {result[0][1]:,}")
    print(f"   Date range: {result[0][2]} to {result[0][3]}")
    print(f"   Rows with indicators: {result[0][4]:,}")
    transform_max = result[0][3]
    
    # Check signals
    result = db.fetch_query('''
        SELECT COUNT(*) as total,
               COUNT(DISTINCT symbol_id) as symbols,
               MIN(date) as min_date,
               MAX(date) as max_date,
               COUNT(DISTINCT trade_strategy) as strategies
        FROM transforms.trading_signals
    ''')
    print(f"\n3. SIGNALS (transforms.trading_signals):")
    print(f"   Rows: {result[0][0]:,}")
    print(f"   Symbols: {result[0][1]:,}")
    print(f"   Date range: {result[0][2]} to {result[0][3]}")
    print(f"   Strategies: {result[0][4]}")
    
    # Check the gap
    print(f"\n4. GAP ANALYSIS:")
    if raw_max and transform_max:
        gap_days = (raw_max - transform_max).days
        print(f"   Latest raw data:    {raw_max}")
        print(f"   Latest transform:   {transform_max}")
        print(f"   Gap:                {gap_days} days")
        
        if gap_days > 0:
            print(f"\n   WARNING: Transforms are {gap_days} days behind raw data!")
            print(f"   Technical indicators need to be recalculated.")
            status = "NEEDS_UPDATE"
        else:
            print(f"\n   OK: Transforms are up to date with raw data.")
            status = "UP_TO_DATE"
    else:
        status = "ERROR"
    
    db.close()
    print("="*80)
    return status


def run_command(cmd, description):
    """Run a command and report results."""
    print(f"\n{'='*80}")
    print(f"RUNNING: {description}")
    print(f"Command: {' '.join(cmd)}")
    print(f"{'='*80}\n")
    
    try:
        result = subprocess.run(
            cmd,
            cwd=Path(__file__).parent,
            capture_output=False,  # Show output in real-time
            text=True
        )
        
        if result.returncode == 0:
            print(f"\n✓ SUCCESS: {description}")
            return True
        else:
            print(f"\n✗ FAILED: {description} (exit code: {result.returncode})")
            return False
            
    except Exception as e:
        print(f"\n✗ ERROR running {description}: {e}")
        return False


def rebuild_full():
    """Full rebuild - recreate everything from scratch."""
    print("\n" + "="*80)
    print("FULL REBUILD - This will take several hours for 21K+ symbols!")
    print("="*80)
    
    response = input("\nAre you sure you want to do a FULL rebuild? (yes/no): ")
    if response.lower() != 'yes':
        print("Cancelled.")
        return
    
    python_exe = sys.executable
    start_time = datetime.now()
    
    # Step 1: Initialize/rebuild time series transforms
    print("\n\nSTEP 1: Rebuild time series transforms with technical indicators")
    print("This calculates EMA, RSI, MACD, Bollinger Bands, etc. for ALL symbols")
    success = run_command(
        [python_exe, 'transforms/transform_time_series_daily_adjusted.py', '--mode', 'full'],
        'Time Series Transforms (FULL MODE)'
    )
    
    if not success:
        print("\n✗ Time series transform failed. Stopping.")
        return
    
    # Step 2: Initialize trading signals table
    print("\n\nSTEP 2: Initialize trading signals table")
    success = run_command(
        [python_exe, 'transforms/transform_trading_signals.py', '--init'],
        'Initialize Trading Signals Table'
    )
    
    if not success:
        print("\n✗ Trading signals initialization failed. Stopping.")
        return
    
    # Step 3: Generate all trading signals
    print("\n\nSTEP 3: Generate trading signals for all symbols")
    success = run_command(
        [python_exe, 'transforms/transform_trading_signals.py', '--mode', 'full'],
        'Generate Trading Signals (FULL MODE)'
    )
    
    duration = datetime.now() - start_time
    print("\n" + "="*80)
    print("FULL REBUILD COMPLETE")
    print("="*80)
    print(f"Total time: {duration}")
    print("\nChecking final status...")
    check_status()


def rebuild_incremental():
    """Incremental rebuild - only process recent data."""
    print("\n" + "="*80)
    print("INCREMENTAL REBUILD - Only process recent/missing data")
    print("="*80)
    
    python_exe = sys.executable
    start_time = datetime.now()
    
    # Step 1: Update time series transforms incrementally
    print("\n\nSTEP 1: Update time series transforms (incremental)")
    print("This processes only symbols with new/updated raw data")
    success = run_command(
        [python_exe, 'transforms/transform_time_series_daily_adjusted.py', '--mode', 'incremental'],
        'Time Series Transforms (INCREMENTAL)'
    )
    
    if not success:
        print("\n✗ Time series transform failed. Stopping.")
        return
    
    # Step 2: Generate trading signals incrementally
    print("\n\nSTEP 2: Generate trading signals (incremental)")
    success = run_command(
        [python_exe, 'transforms/transform_trading_signals.py', '--mode', 'incremental'],
        'Generate Trading Signals (INCREMENTAL)'
    )
    
    duration = datetime.now() - start_time
    print("\n" + "="*80)
    print("INCREMENTAL REBUILD COMPLETE")
    print("="*80)
    print(f"Total time: {duration}")
    print("\nChecking final status...")
    check_status()


def main():
    parser = argparse.ArgumentParser(
        description='Rebuild trading signals pipeline from scratch',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check current status
  python rebuild_signals_from_scratch.py --check-only
  
  # Incremental update (recommended, faster)
  python rebuild_signals_from_scratch.py --incremental
  
  # Full rebuild (takes hours!)
  python rebuild_signals_from_scratch.py --full
        """
    )
    
    parser.add_argument('--check-only', action='store_true',
                       help='Only check status, do not rebuild')
    parser.add_argument('--full', action='store_true',
                       help='Full rebuild - recreate everything (slow!)')
    parser.add_argument('--incremental', action='store_true',
                       help='Incremental - only process recent data (fast)')
    
    args = parser.parse_args()
    
    # Check status first
    status = check_status()
    
    if args.check_only:
        return
    
    if args.full:
        rebuild_full()
    elif args.incremental:
        rebuild_incremental()
    else:
        print("\n" + "="*80)
        print("RECOMMENDATION")
        print("="*80)
        
        if status == "NEEDS_UPDATE":
            print("\nYour data needs updating. Run one of these:")
            print("\n  RECOMMENDED (faster):")
            print("    python rebuild_signals_from_scratch.py --incremental")
            print("\n  OR full rebuild (takes hours):")
            print("    python rebuild_signals_from_scratch.py --full")
        elif status == "UP_TO_DATE":
            print("\nYour data appears up to date!")
            print("If you still want to rebuild, run:")
            print("  python rebuild_signals_from_scratch.py --incremental")
        
        print("\nUse --help for more options.")


if __name__ == "__main__":
    main()
