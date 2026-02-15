#!/usr/bin/env python3
"""
Daily Transform Pipeline Runner

Executes all transformation scripts in the correct order with proper dependencies,
then scores trading signals and generates visualization charts.

Transform Categories:
1. Fundamentals: Balance Sheet, Cash Flow, Income Statement
2. Fundamental Quality: Aggregated quality scores from fundamentals
3. Insider Transactions: Transaction-level and aggregated
4. Market Data: Time series with technical indicators (uses watermark table)
5. Economic/Commodities: FRED economic indicators and commodities
6. Earnings: Sentiment analysis from earnings call transcripts
7. Signal Scoring & Visualization: Scores signals with ML model and creates charts

Usage:
    # Run all transforms + score signals + generate charts
    python run_daily_transform.py
    
    # Run specific transform groups (still generates charts)
    python run_daily_transform.py --only fundamentals
    python run_daily_transform.py --only insider
    
    # Skip specific groups
    python run_daily_transform.py --skip market
    
    # Dry run (show what would be executed)
    python run_daily_transform.py --dry-run
"""

import sys
import subprocess
import argparse
from pathlib import Path
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent  # Go up one level from transforms/ to project root
sys.path.append(str(PROJECT_ROOT))


class TransformPipeline:
    """Orchestrate daily transformation pipeline."""
    
    # Transform groups in dependency order
    # NOTE: 'market' group (time series) is excluded by default as it processes 21K+ symbols
    # and should only be run when new raw data is available (weekly/manual trigger)
    TRANSFORM_GROUPS = {
        'fundamentals': [
            ('transforms/transform_balance_sheet.py', '--process', 'Balance Sheet'),
            ('transforms/transform_cash_flow.py', '--process', 'Cash Flow'),
            ('transforms/transform_income_statement.py', '--process', 'Income Statement'),
        ],
        'quality': [
            ('transforms/transform_fundamental_quality_scores.py', '--process', 'Fundamental Quality Scores'),
        ],
        'insider': [
            ('transforms/transform_insider_transactions.py', '--process', 'Insider Transactions'),
            ('transforms/transform_insider_transactions_agg.py', '--process', 'Insider Transactions Aggregated'),
        ],
        'market': [
            # WARNING: This processes 21K+ symbols and takes hours
            # Only include when new market data has been extracted
            ('transforms/transform_time_series_daily_adjusted.py', '--mode=incremental --staleness-hours=48 --workers=4', 'Time Series Daily Adjusted'),
        ],
        'economic': [
            ('transforms/transform_economic_indicators.py', '--process', 'Economic Indicators'),
            ('transforms/transform_commodities.py', '--process', 'Commodities'),
        ],
        'earnings': [
            ('transforms/transform_earnings_sentiment_agg.py', '--process', 'Earnings Sentiment Aggregated'),
        ],
    }
    
    def __init__(self, dry_run=False):
        self.dry_run = dry_run
        self.results = []
        self.start_time = None
        self.python_exe = str(PROJECT_ROOT / '.venv' / 'Scripts' / 'python.exe')
    
    def run_transform(self, script_path: str, mode: str, name: str) -> bool:
        """
        Run a single transformation script.
        
        Args:
            script_path: Relative path to transform script
            mode: CLI argument (--process or --incremental)
            name: Display name for logging
            
        Returns:
            True if successful, False otherwise
        """
        full_path = PROJECT_ROOT / script_path
        
        if not full_path.exists():
            logger.error(f"❌ Script not found: {script_path}")
            return False
        
        logger.info(f"\n{'='*80}")
        logger.info(f"🔄 Running: {name}")
        logger.info(f"   Script: {script_path}")
        logger.info(f"   Mode: {mode}")
        logger.info(f"{'='*80}")
        
        if self.dry_run:
            logger.info(f"   [DRY RUN] Would execute: {self.python_exe} {full_path} {mode}")
            return True
        
        try:
            # Split mode string into individual arguments (e.g., "--mode=incremental --workers=4")
            mode_args = mode.split()
            cmd = [self.python_exe, str(full_path)] + mode_args
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=str(PROJECT_ROOT)
            )
            
            # Print output
            if result.stdout:
                print(result.stdout)
            if result.stderr:
                print(result.stderr, file=sys.stderr)
            
            if result.returncode == 0:
                logger.info(f"✅ Completed: {name}")
                return True
            else:
                logger.error(f"❌ Failed: {name} (exit code: {result.returncode})")
                return False
                
        except Exception as e:
            logger.error(f"❌ Exception running {name}: {e}")
            return False
    
    def run_group(self, group_name: str) -> dict:
        """
        Run all transforms in a group.
        
        Returns:
            Dict with group results
        """
        if group_name not in self.TRANSFORM_GROUPS:
            logger.error(f"Unknown transform group: {group_name}")
            return {'group': group_name, 'success': False, 'transforms': []}
        
        logger.info(f"\n{'#'*80}")
        logger.info(f"# Transform Group: {group_name.upper()}")
        logger.info(f"{'#'*80}")
        
        group_results = {
            'group': group_name,
            'transforms': [],
            'success': True
        }
        
        for script_path, mode, name in self.TRANSFORM_GROUPS[group_name]:
            start = datetime.now()
            success = self.run_transform(script_path, mode, name)
            duration = (datetime.now() - start).total_seconds()
            
            transform_result = {
                'name': name,
                'script': script_path,
                'success': success,
                'duration': duration
            }
            
            group_results['transforms'].append(transform_result)
            
            if not success:
                group_results['success'] = False
                logger.warning(f"⚠️  Group '{group_name}' had failures, but continuing...")
        
        return group_results
    
    def run_pipeline(self, only_groups=None, skip_groups=None):
        """
        Run the full transformation pipeline.
        
        Args:
            only_groups: List of group names to run (None = all)
            skip_groups: List of group names to skip (None = none)
        """
        self.start_time = datetime.now()
        
        logger.info(f"\n{'*'*80}")
        logger.info(f"* Daily Transform Pipeline")
        logger.info(f"* Started: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        if self.dry_run:
            logger.info(f"* Mode: DRY RUN")
        logger.info(f"{'*'*80}")
        
        # Determine which groups to run
        groups_to_run = list(self.TRANSFORM_GROUPS.keys())
        
        # Skip market by default (too slow for daily runs)
        if not only_groups and not skip_groups:
            groups_to_run = [g for g in groups_to_run if g != 'market']
            logger.info("Skipping 'market' group by default (use --only market to include)")
        
        if only_groups:
            groups_to_run = [g for g in groups_to_run if g in only_groups]
            logger.info(f"Running only: {', '.join(groups_to_run)}")
        
        if skip_groups:
            groups_to_run = [g for g in groups_to_run if g not in skip_groups]
            logger.info(f"Skipping: {', '.join(skip_groups)}")
        
        # Run each group
        for group_name in groups_to_run:
            group_result = self.run_group(group_name)
            self.results.append(group_result)
        
        # Generate signal charts after all transforms complete
        charts_generated = self.generate_signal_charts()
        
        # Print summary
        self.print_summary()
        
        # Final status
        if charts_generated:
            logger.info(f"\n✅ Signal charts generated successfully! Location: {PROJECT_ROOT / 'backtesting' / 'signal_charts_indicators'}")
        else:
            logger.warning(f"\n⚠️  Signal chart generation had issues, but pipeline completed.")
    
    def generate_signal_charts(self) -> bool:
        """
        Generate signal charts using daily signal scorer and visualization tools.
        
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"\n{'='*80}")
        logger.info(f"🎨 Generating Signal Charts")
        logger.info(f"{'='*80}")
        
        try:
            output_dir = PROJECT_ROOT / 'backtesting' / 'signal_charts_indicators'
            if output_dir.exists():
                before_count = len(list(output_dir.glob('*.png')))
            else:
                before_count = 0

            # Step 1: Score signals
            logger.info("Step 1: Scoring trading signals...")
            score_cmd = [self.python_exe, str(PROJECT_ROOT / 'backtesting' / 'daily_signal_scorer.py')]
            result = subprocess.run(
                score_cmd,
                capture_output=True,
                text=True,
                cwd=str(PROJECT_ROOT)
            )
            
            if result.returncode != 0:
                logger.error(f"❌ Failed to score signals")
                if result.stderr:
                    logger.error(result.stderr)
                return False
            
            # Extract output filename from scorer output or use default
            output_file = None
            if "saved to:" in result.stdout:
                # Try to extract filename from output
                for line in result.stdout.split('\n'):
                    if 'saved to:' in line:
                        output_file = line.split('saved to:')[-1].strip()
                        break
            
            if not output_file:
                # Use default filename pattern
                today = datetime.now().strftime('%Y%m%d')
                output_file = f'backtesting/daily_signals_scored_{today}.csv'
            
            logger.info(f"✅ Signals scored: {output_file}")
            
            # Step 2: Generate visualization charts
            logger.info("Step 2: Creating visualization charts...")
            viz_cmd = [
                self.python_exe, 
                str(PROJECT_ROOT / 'backtesting' / 'visualize_signals_with_indicators.py'),
                '--input', output_file,
                '--top', '25',
                '--output-dir', str(output_dir)
            ]
            result = subprocess.run(
                viz_cmd,
                capture_output=True,
                text=True,
                cwd=str(PROJECT_ROOT)
            )
            
            if result.returncode != 0:
                logger.error(f"❌ Failed to generate charts")
                if result.stderr:
                    logger.error(result.stderr)
                return False
            
            after_count = len(list(output_dir.glob('*.png')))
            generated_count = max(0, after_count - before_count)
            logger.info(f"✅ Charts generated: {generated_count} (output: {output_dir})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Exception generating charts: {e}")
            return False
    
    def print_summary(self):
        """Print pipeline execution summary."""
        end_time = datetime.now()
        total_duration = (end_time - self.start_time).total_seconds()
        
        logger.info(f"\n{'='*80}")
        logger.info(f"PIPELINE SUMMARY")
        logger.info(f"{'='*80}")
        logger.info(f"Completed: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Total Duration: {total_duration:.2f} seconds ({total_duration/60:.2f} minutes)")
        logger.info(f"")
        
        total_transforms = 0
        successful_transforms = 0
        failed_transforms = 0
        
        for group_result in self.results:
            group_name = group_result['group']
            group_success = group_result['success']
            
            status = "✅" if group_success else "❌"
            logger.info(f"{status} Group: {group_name.upper()}")
            
            for transform in group_result['transforms']:
                total_transforms += 1
                if transform['success']:
                    successful_transforms += 1
                    status = "  ✅"
                else:
                    failed_transforms += 1
                    status = "  ❌"
                
                logger.info(f"{status} {transform['name']} ({transform['duration']:.2f}s)")
        
        logger.info(f"")
        logger.info(f"{'='*80}")
        logger.info(f"Total Transform Scripts Run: {total_transforms}")
        logger.info(f"  Successful: {successful_transforms}")
        logger.info(f"  Failed: {failed_transforms}")
        logger.info(f"{'='*80}")
        
        if failed_transforms > 0:
            logger.warning(f"\n⚠️  {failed_transforms} transform(s) failed!")
            sys.exit(1)
        else:
            logger.info(f"\n✅ All transforms completed successfully!")


def main():
    parser = argparse.ArgumentParser(
        description='Run daily transformation pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all transforms
  python run_daily_transform.py
  
  # Run only fundamentals and quality
  python run_daily_transform.py --only fundamentals quality
  
  # Skip market data transforms
  python run_daily_transform.py --skip market
  
  # Dry run to see what would execute
  python run_daily_transform.py --dry-run

Transform Groups:
  fundamentals  - Balance Sheet, Cash Flow, Income Statement
  quality       - Fundamental Quality Scores
  insider       - Insider Transactions (detail + aggregated)
  market        - Time Series Daily Adjusted (uses watermark table)
  economic      - Economic Indicators, Commodities
  earnings      - Earnings Sentiment Aggregated
        """
    )
    
    parser.add_argument(
        '--only',
        nargs='+',
        choices=['fundamentals', 'quality', 'insider', 'market', 'economic', 'earnings'],
        help='Run only specified transform groups'
    )
    
    parser.add_argument(
        '--skip',
        nargs='+',
        choices=['fundamentals', 'quality', 'insider', 'market', 'economic', 'earnings'],
        help='Skip specified transform groups'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be executed without running'
    )
    
    args = parser.parse_args()
    
    if args.only and args.skip:
        parser.error("Cannot use --only and --skip together")
    
    pipeline = TransformPipeline(dry_run=args.dry_run)
    pipeline.run_pipeline(only_groups=args.only, skip_groups=args.skip)


if __name__ == "__main__":
    main()
