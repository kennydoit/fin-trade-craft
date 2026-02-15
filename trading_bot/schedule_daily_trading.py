"""
Daily Trading Bot Scheduler

Runs the automated trading bot on a daily schedule (e.g., 9:35 AM ET market open).
Can be run as:
1. Windows Task Scheduler task
2. Standalone daemon process
3. One-time execution

Usage:
    # Run once immediately
    python schedule_daily_trading.py --once
    
    # Run as daemon (keeps running, executes daily)
    python schedule_daily_trading.py --daemon
    
    # Custom schedule time
    python schedule_daily_trading.py --daemon --time "09:35"
"""

import sys
import time
import logging
import argparse
from pathlib import Path
from datetime import datetime, time as dt_time
import schedule

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from trading_bot.automated_trading_bot import AutomatedTradingBot

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('trading_bot/scheduler.log')
    ]
)
logger = logging.getLogger(__name__)


def run_trading_bot(dry_run=False, **kwargs):
    """Run the trading bot with specified parameters."""
    try:
        logger.info("="*80)
        logger.info(f"SCHEDULED EXECUTION TRIGGERED - {datetime.now()}")
        logger.info("="*80)
        
        bot = AutomatedTradingBot(dry_run=dry_run, **kwargs)
        bot.run()
        
        logger.info("="*80)
        logger.info("SCHEDULED EXECUTION COMPLETED")
        logger.info("="*80 + "\n")
        
    except Exception as e:
        logger.error(f"Error in scheduled execution: {e}", exc_info=True)


def run_daemon(schedule_time: str, dry_run=False, **bot_kwargs):
    """Run as daemon process with scheduled executions."""
    logger.info("="*80)
    logger.info("TRADING BOT SCHEDULER - DAEMON MODE")
    logger.info("="*80)
    logger.info(f"Schedule Time: {schedule_time} ET (Market Open)")
    logger.info(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    logger.info("="*80)
    
    # Schedule daily execution
    schedule.every().monday.at(schedule_time).do(
        run_trading_bot, dry_run=dry_run, **bot_kwargs
    )
    schedule.every().tuesday.at(schedule_time).do(
        run_trading_bot, dry_run=dry_run, **bot_kwargs
    )
    schedule.every().wednesday.at(schedule_time).do(
        run_trading_bot, dry_run=dry_run, **bot_kwargs
    )
    schedule.every().thursday.at(schedule_time).do(
        run_trading_bot, dry_run=dry_run, **bot_kwargs
    )
    schedule.every().friday.at(schedule_time).do(
        run_trading_bot, dry_run=dry_run, **bot_kwargs
    )
    
    logger.info("Scheduler started. Press Ctrl+C to stop.")
    logger.info(f"Next run: {schedule.next_run()}\n")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("\nScheduler stopped by user")


def create_windows_task_scheduler():
    """Generate Windows Task Scheduler command."""
    script_path = Path(__file__).absolute()
    python_exe = sys.executable
    
    # Command for Task Scheduler
    command = f'"{python_exe}" "{script_path}" --once'
    
    print("\n" + "="*80)
    print("WINDOWS TASK SCHEDULER SETUP")
    print("="*80)
    print("\nTo schedule this bot to run daily:")
    print("\n1. Open Task Scheduler (taskschd.msc)")
    print("2. Create Basic Task")
    print("3. Name: 'Automated Trading Bot'")
    print("4. Trigger: Daily at 9:35 AM")
    print("5. Action: Start a program")
    print(f"6. Program/script: {python_exe}")
    print(f"7. Arguments: \"{script_path}\" --once")
    print(f"8. Start in: {script_path.parent}")
    print("\nOr use this PowerShell command to create it:")
    print(f"""
$action = New-ScheduledTaskAction -Execute '{python_exe}' -Argument '"{script_path}" --once' -WorkingDirectory '{script_path.parent}'
$trigger = New-ScheduledTaskTrigger -Daily -At 9:35AM
$principal = New-ScheduledTaskPrincipal -UserId "$env:USERNAME" -LogonType S4U
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -DontStopOnIdleEnd
Register-ScheduledTask -TaskName "Automated Trading Bot" -Action $action -Trigger $trigger -Principal $principal -Settings $settings
""")
    print("="*80 + "\n")


def main():
    """Command-line interface."""
    parser = argparse.ArgumentParser(
        description='Schedule daily trading bot execution',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run once immediately (for testing)
  python schedule_daily_trading.py --once --dry-run
  
  # Run as daemon (keeps running, executes at scheduled time)
  python schedule_daily_trading.py --daemon --time "09:35"
  
  # Generate Windows Task Scheduler setup instructions
  python schedule_daily_trading.py --setup-windows-task
  
  # Live trading with custom parameters
  python schedule_daily_trading.py --once --max-positions 5 --position-size 0.10
        """
    )
    
    # Execution mode
    parser.add_argument('--once', action='store_true',
                       help='Run once immediately and exit')
    parser.add_argument('--daemon', action='store_true',
                       help='Run as daemon with scheduled executions')
    parser.add_argument('--setup-windows-task', action='store_true',
                       help='Show Windows Task Scheduler setup instructions')
    
    # Schedule settings
    parser.add_argument('--time', type=str, default='09:35',
                       help='Daily execution time in HH:MM format (default: 09:35)')
    
    # Bot parameters
    parser.add_argument('--dry-run', action='store_true',
                       help='Test mode - no actual orders placed')
    parser.add_argument('--max-positions', type=int, default=10,
                       help='Maximum concurrent positions (default: 10)')
    parser.add_argument('--position-size', type=float, default=0.05,
                       help='Position size as fraction (default: 0.05 = 5%%)')
    parser.add_argument('--min-probability', type=float, default=0.85,
                       help='Minimum success probability (default: 0.85)')
    parser.add_argument('--stop-loss', type=float, default=0.10,
                       help='Stop loss percentage (default: 0.10 = 10%%)')
    parser.add_argument('--take-profit', type=float, default=0.15,
                       help='Take profit percentage (default: 0.15 = 15%%)')
    
    args = parser.parse_args()
    
    # Bot kwargs
    bot_kwargs = {
        'max_positions': args.max_positions,
        'position_size_pct': args.position_size,
        'min_probability': args.min_probability,
        'stop_loss_pct': args.stop_loss,
        'take_profit_pct': args.take_profit
    }
    
    if args.setup_windows_task:
        create_windows_task_scheduler()
    elif args.once:
        # Run once and exit
        run_trading_bot(dry_run=args.dry_run, **bot_kwargs)
    elif args.daemon:
        # Run as daemon
        run_daemon(args.time, dry_run=args.dry_run, **bot_kwargs)
    else:
        parser.print_help()
        print("\nPlease specify --once, --daemon, or --setup-windows-task")


if __name__ == '__main__':
    main()
