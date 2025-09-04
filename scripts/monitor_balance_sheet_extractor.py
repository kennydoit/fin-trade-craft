"""
Balance Sheet Extractor Monitoring Script
Monitor the health, performance, and data quality of the balance sheet extractor.
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any

# Add the parent directories to the path
sys.path.append(str(Path(__file__).parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager


class BalanceSheetMonitor:
    """Monitor balance sheet extraction health and performance."""
    
    def __init__(self):
        """Initialize monitor."""
        self.db_manager = PostgresDatabaseManager()
    
    def check_recent_activity(self, hours: int = 24) -> Dict[str, Any]:
        """Check recent extraction activity."""
        print(f"📊 Checking activity in last {hours} hours...")
        
        with self.db_manager as db:
            # Check recent API calls
            api_query = """
                SELECT response_status, COUNT(*) as count
                FROM source.api_responses_landing 
                WHERE table_name = 'balance_sheet' 
                  AND created_at > NOW() - INTERVAL '%s hours'
                GROUP BY response_status
                ORDER BY count DESC
            """
            
            api_results = db.fetch_query(api_query, (hours,))
            
            # Check recent data inserts
            data_query = """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT symbol_id) as unique_symbols,
                    MIN(fiscal_date_ending) as earliest_fiscal_date,
                    MAX(fiscal_date_ending) as latest_fiscal_date
                FROM source.balance_sheet 
                WHERE created_at > NOW() - INTERVAL '%s hours'
            """
            
            data_results = db.fetch_query(data_query, (hours,))
            
            # Check watermark updates
            watermark_query = """
                SELECT 
                    COUNT(*) as watermarks_updated,
                    COUNT(CASE WHEN consecutive_failures = 0 THEN 1 END) as successful_updates,
                    COUNT(CASE WHEN consecutive_failures > 0 THEN 1 END) as failed_updates
                FROM source.extraction_watermarks 
                WHERE table_name = 'balance_sheet' 
                  AND updated_at > NOW() - INTERVAL '%s hours'
            """
            
            watermark_results = db.fetch_query(watermark_query, (hours,))
            
            print("   API Activity:")
            for status, count in api_results:
                print(f"      {status}: {count} calls")
            
            if data_results and data_results[0][0] > 0:
                total_records, unique_symbols, earliest, latest = data_results[0]
                print("   Data Activity:")
                print(f"      Records inserted: {total_records}")
                print(f"      Unique symbols: {unique_symbols}")
                print(f"      Fiscal date range: {earliest} to {latest}")
            else:
                print("   Data Activity: No new records")
            
            if watermark_results:
                total_wm, successful_wm, failed_wm = watermark_results[0]
                print("   Watermark Activity:")
                print(f"      Total updates: {total_wm}")
                print(f"      Successful: {successful_wm}")
                print(f"      Failed: {failed_wm}")
            
            return {
                'api_calls': dict(api_results),
                'data_activity': data_results[0] if data_results else None,
                'watermark_activity': watermark_results[0] if watermark_results else None
            }
    
    def check_failed_extractions(self, min_failures: int = 3) -> List[Dict[str, Any]]:
        """Check for symbols with consecutive extraction failures."""
        print(f"🚨 Checking symbols with {min_failures}+ consecutive failures...")
        
        with self.db_manager as db:
            query = """
                SELECT 
                    ew.symbol_id,
                    ls.symbol,
                    ls.exchange,
                    ew.consecutive_failures,
                    ew.last_successful_run,
                    ew.updated_at as last_attempt
                FROM source.extraction_watermarks ew
                JOIN extracted.listing_status ls ON ew.symbol_id = ls.symbol_id
                WHERE ew.table_name = 'balance_sheet'
                  AND ew.consecutive_failures >= %s
                ORDER BY ew.consecutive_failures DESC, ew.updated_at DESC
                LIMIT 20
            """
            
            results = db.fetch_query(query, (min_failures,))
            
            failed_symbols = []
            for row in results:
                symbol_data = {
                    'symbol_id': row[0],
                    'symbol': row[1],
                    'exchange': row[2],
                    'consecutive_failures': row[3],
                    'last_successful_run': row[4],
                    'last_attempt': row[5]
                }
                failed_symbols.append(symbol_data)
                
                last_success = row[4].strftime("%Y-%m-%d") if row[4] else "Never"
                last_attempt = row[5].strftime("%Y-%m-%d %H:%M") if row[5] else "Unknown"
                
                print(f"   {row[1]} ({row[2]}): {row[3]} failures")
                print(f"      Last success: {last_success}")
                print(f"      Last attempt: {last_attempt}")
            
            if not failed_symbols:
                print("   ✅ No symbols with excessive failures")
            
            return failed_symbols
    
    def check_data_quality(self, hours: int = 24) -> Dict[str, Any]:
        """Check data quality of recent extractions."""
        print(f"🔍 Checking data quality for last {hours} hours...")
        
        with self.db_manager as db:
            # Check for null critical fields
            null_check_query = """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN total_assets IS NULL THEN 1 END) as null_assets,
                    COUNT(CASE WHEN total_liabilities IS NULL THEN 1 END) as null_liabilities,
                    COUNT(CASE WHEN total_shareholder_equity IS NULL THEN 1 END) as null_equity,
                    COUNT(CASE WHEN fiscal_date_ending IS NULL THEN 1 END) as null_fiscal_date
                FROM source.balance_sheet 
                WHERE created_at > NOW() - INTERVAL '%s hours'
            """
            
            null_results = db.fetch_query(null_check_query, (hours,))
            
            # Check for negative values
            negative_check_query = """
                SELECT 
                    COUNT(CASE WHEN total_assets < 0 THEN 1 END) as negative_assets,
                    COUNT(CASE WHEN total_shareholder_equity < 0 THEN 1 END) as negative_equity
                FROM source.balance_sheet 
                WHERE created_at > NOW() - INTERVAL '%s hours'
                  AND (total_assets IS NOT NULL OR total_shareholder_equity IS NOT NULL)
            """
            
            negative_results = db.fetch_query(negative_check_query, (hours,))
            
            # Check for future dates
            date_check_query = """
                SELECT 
                    COUNT(CASE WHEN fiscal_date_ending > CURRENT_DATE THEN 1 END) as future_dates,
                    COUNT(CASE WHEN fiscal_date_ending < '2000-01-01' THEN 1 END) as very_old_dates
                FROM source.balance_sheet 
                WHERE created_at > NOW() - INTERVAL '%s hours'
            """
            
            date_results = db.fetch_query(date_check_query, (hours,))
            
            if null_results:
                total, null_assets, null_liab, null_equity, null_date = null_results[0]
                print(f"   Records analyzed: {total}")
                
                if total > 0:
                    print("   Null value analysis:")
                    print(f"      Total assets: {null_assets}/{total} ({null_assets/total*100:.1f}%) null")
                    print(f"      Total liabilities: {null_liab}/{total} ({null_liab/total*100:.1f}%) null")
                    print(f"      Shareholder equity: {null_equity}/{total} ({null_equity/total*100:.1f}%) null")
                    print(f"      Fiscal dates: {null_date}/{total} ({null_date/total*100:.1f}%) null")
                    
                    if negative_results:
                        neg_assets, neg_equity = negative_results[0]
                        print("   Negative value analysis:")
                        print(f"      Negative assets: {neg_assets}")
                        print(f"      Negative equity: {neg_equity}")
                    
                    if date_results:
                        future_dates, old_dates = date_results[0]
                        print("   Date analysis:")
                        print(f"      Future fiscal dates: {future_dates}")
                        print(f"      Very old fiscal dates (pre-2000): {old_dates}")
                        
                    # Quality score
                    issues = null_date + (neg_assets if negative_results else 0) + (future_dates if date_results else 0)
                    quality_score = max(0, 100 - (issues / total * 100)) if total > 0 else 100
                    
                    print(f"   📊 Data Quality Score: {quality_score:.1f}%")
                    
                    return {
                        'total_records': total,
                        'null_analysis': null_results[0],
                        'negative_analysis': negative_results[0] if negative_results else (0, 0),
                        'date_analysis': date_results[0] if date_results else (0, 0),
                        'quality_score': quality_score
                    }
                else:
                    print("   No records to analyze")
                    return {'total_records': 0}
            
            return {}
    
    def check_processing_performance(self, hours: int = 24) -> Dict[str, Any]:
        """Check processing performance metrics."""
        print(f"⚡ Checking performance for last {hours} hours...")
        
        with self.db_manager as db:
            # Check processing rate
            rate_query = """
                SELECT 
                    DATE_TRUNC('hour', created_at) as hour,
                    COUNT(*) as api_calls,
                    COUNT(DISTINCT symbol) as unique_symbols
                FROM source.api_responses_landing 
                WHERE table_name = 'balance_sheet' 
                  AND created_at > NOW() - INTERVAL '%s hours'
                GROUP BY DATE_TRUNC('hour', created_at)
                ORDER BY hour DESC
            """
            
            rate_results = db.fetch_query(rate_query, (hours,))
            
            # Check symbols needing processing
            pending_query = """
                SELECT 
                    COUNT(*) as total_pending,
                    COUNT(CASE WHEN ew.last_successful_run IS NULL THEN 1 END) as never_processed,
                    COUNT(CASE WHEN ew.last_successful_run < NOW() - INTERVAL '7 days' THEN 1 END) as week_old,
                    COUNT(CASE WHEN consecutive_failures >= 3 THEN 1 END) as persistently_failing
                FROM extracted.listing_status ls
                LEFT JOIN source.extraction_watermarks ew ON ew.symbol_id = ls.symbol_id 
                                                           AND ew.table_name = 'balance_sheet'
                WHERE ls.asset_type = 'Stock'
                  AND LOWER(ls.status) = 'active'
                  AND (
                      ew.last_successful_run IS NULL 
                      OR ew.last_successful_run < NOW() - INTERVAL '24 hours'
                  )
                  AND COALESCE(ew.consecutive_failures, 0) < 3
            """
            
            pending_results = db.fetch_query(pending_query)
            
            print("   Processing Rate (by hour):")
            if rate_results:
                for hour, api_calls, unique_symbols in rate_results[:6]:  # Show last 6 hours
                    hour_str = hour.strftime("%Y-%m-%d %H:00")
                    print(f"      {hour_str}: {api_calls} calls, {unique_symbols} symbols")
                
                total_calls = sum(row[1] for row in rate_results)
                total_symbols = sum(row[2] for row in rate_results)
                avg_calls_per_hour = total_calls / min(len(rate_results), hours)
                
                print(f"   Total: {total_calls} calls, {total_symbols} symbols")
                print(f"   Average: {avg_calls_per_hour:.1f} calls/hour")
            else:
                print("      No activity in the specified time period")
            
            if pending_results:
                total_pending, never_processed, week_old, failing = pending_results[0]
                print("   Pending Work:")
                print(f"      Total symbols pending: {total_pending}")
                print(f"      Never processed: {never_processed}")
                print(f"      Over a week old: {week_old}")
                print(f"      Persistently failing: {failing}")
                
                return {
                    'processing_rate': rate_results,
                    'pending_work': pending_results[0],
                    'avg_calls_per_hour': avg_calls_per_hour if rate_results else 0
                }
            
            return {}
    
    def check_api_health(self, hours: int = 24) -> Dict[str, Any]:
        """Check API health and error patterns."""
        print(f"🌐 Checking API health for last {hours} hours...")
        
        with self.db_manager as db:
            # Check API response patterns
            api_health_query = """
                SELECT 
                    response_status,
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
                FROM source.api_responses_landing 
                WHERE table_name = 'balance_sheet' 
                  AND created_at > NOW() - INTERVAL '%s hours'
                GROUP BY response_status
                ORDER BY count DESC
            """
            
            api_results = db.fetch_query(api_health_query, (hours,))
            
            # Check for rate limiting patterns
            rate_limit_query = """
                SELECT 
                    DATE_TRUNC('hour', created_at) as hour,
                    COUNT(CASE WHEN response_status = 'rate_limited' THEN 1 END) as rate_limited,
                    COUNT(*) as total_calls
                FROM source.api_responses_landing 
                WHERE table_name = 'balance_sheet' 
                  AND created_at > NOW() - INTERVAL '%s hours'
                GROUP BY DATE_TRUNC('hour', created_at)
                HAVING COUNT(CASE WHEN response_status = 'rate_limited' THEN 1 END) > 0
                ORDER BY hour DESC
            """
            
            rate_limit_results = db.fetch_query(rate_limit_query, (hours,))
            
            print("   API Response Distribution:")
            if api_results:
                for status, count, percentage in api_results:
                    print(f"      {status}: {count} ({percentage}%)")
                
                # Calculate health score
                total_calls = sum(row[1] for row in api_results)
                success_calls = sum(row[1] for row in api_results if row[0] == 'success')
                error_calls = sum(row[1] for row in api_results if row[0] == 'error')
                
                health_score = (success_calls / total_calls * 100) if total_calls > 0 else 0
                
                print(f"   📊 API Health Score: {health_score:.1f}%")
                
                if rate_limit_results:
                    print("   Rate Limiting Detected:")
                    for hour, rate_limited, total in rate_limit_results[:3]:
                        hour_str = hour.strftime("%Y-%m-%d %H:00")
                        print(f"      {hour_str}: {rate_limited}/{total} calls rate limited")
                else:
                    print("   ✅ No significant rate limiting detected")
                
                return {
                    'response_distribution': api_results,
                    'health_score': health_score,
                    'rate_limiting': rate_limit_results
                }
            else:
                print("      No API calls in the specified time period")
                return {}
    
    def generate_health_report(self, hours: int = 24):
        """Generate comprehensive health report."""
        print("🏥 Balance Sheet Extractor Health Report")
        print("=" * 60)
        print(f"Report Period: Last {hours} hours")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Run all health checks
        try:
            activity = self.check_recent_activity(hours)
            print()
            
            failures = self.check_failed_extractions()
            print()
            
            quality = self.check_data_quality(hours)
            print()
            
            performance = self.check_processing_performance(hours)
            print()
            
            api_health = self.check_api_health(hours)
            print()
            
            # Overall health assessment
            print("📋 Overall Health Assessment")
            print("-" * 30)
            
            issues = []
            recommendations = []
            
            # Check for issues and make recommendations
            if api_health.get('health_score', 100) < 95:
                issues.append(f"API health score low: {api_health.get('health_score', 0):.1f}%")
                recommendations.append("Check API key and rate limiting")
            
            if quality.get('quality_score', 100) < 90:
                issues.append(f"Data quality score low: {quality.get('quality_score', 0):.1f}%")
                recommendations.append("Review data transformation logic")
            
            if len(failures) > 10:
                issues.append(f"High number of failed extractions: {len(failures)}")
                recommendations.append("Investigate failing symbols and reset watermarks if needed")
            
            pending_work = performance.get('pending_work')
            if pending_work and pending_work[0] > 1000:
                issues.append(f"Large backlog: {pending_work[0]} symbols pending")
                recommendations.append("Consider increasing extraction frequency or batch size")
            
            if not issues:
                print("✅ System is healthy!")
                print("   No significant issues detected")
            else:
                print("⚠️ Issues detected:")
                for issue in issues:
                    print(f"   - {issue}")
            
            if recommendations:
                print("\n💡 Recommendations:")
                for rec in recommendations:
                    print(f"   - {rec}")
            
            print("\n🔄 Next Steps:")
            print("   1. Monitor failed extractions and reset watermarks if needed")
            print("   2. Check API usage and ensure rate limits are respected")
            print("   3. Verify data quality meets business requirements")
            print("   4. Scale processing based on pending work volume")
            
        except Exception as e:
            print(f"❌ Health check failed: {e}")
            import traceback
            traceback.print_exc()


def main():
    """Run balance sheet extractor monitoring."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Balance Sheet Extractor Monitor")
    parser.add_argument("--hours", type=int, default=24, 
                       help="Number of hours to look back for analysis (default: 24)")
    parser.add_argument("--failures-only", action="store_true",
                       help="Only check for failed extractions")
    
    args = parser.parse_args()
    
    try:
        monitor = BalanceSheetMonitor()
        
        if args.failures_only:
            monitor.check_failed_extractions()
        else:
            monitor.generate_health_report(args.hours)
        
    except Exception as e:
        print(f"❌ Monitoring failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
