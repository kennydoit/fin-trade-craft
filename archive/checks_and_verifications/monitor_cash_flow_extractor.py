"""
Cash Flow Extractor monitoring and health check tool.
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from db.postgres_database_manager import PostgresDatabaseManager


def monitor_extraction_activity():
    """Monitor recent extraction activity."""
    print("📊 Cash Flow Extraction Activity Monitor")
    print("="*50)
    
    try:
        with PostgresDatabaseManager() as db:
            # Recent API calls
            api_activity = db.fetch_query("""
                SELECT 
                    response_status,
                    COUNT(*) as call_count,
                    COUNT(DISTINCT symbol) as unique_symbols,
                    MAX(fetched_at) as latest_call
                FROM source.api_responses_landing 
                WHERE table_name = 'cash_flow' 
                  AND fetched_at >= NOW() - INTERVAL '24 hours'
                GROUP BY response_status
                ORDER BY call_count DESC
            """)
            
            if api_activity:
                print("\n🔄 API Activity (Last 24 Hours):")
                total_calls = sum(row[1] for row in api_activity)
                for status, calls, symbols, latest in api_activity:
                    print(f"   {status}: {calls:,} calls ({symbols:,} symbols) - Latest: {latest}")
                print(f"   Total API calls: {total_calls:,}")
            else:
                print("\n⚠️ No API activity in the last 24 hours")
            
            # Data extraction summary
            extraction_stats = db.fetch_query("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT symbol_id) as unique_symbols,
                    COUNT(DISTINCT DATE(fiscal_date_ending)) as unique_dates,
                    MIN(fiscal_date_ending) as earliest_date,
                    MAX(fiscal_date_ending) as latest_date,
                    COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '24 hours') as recent_records,
                    COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days') as weekly_records
                FROM source.cash_flow
            """)
            
            if extraction_stats and extraction_stats[0][0] > 0:
                stats = extraction_stats[0]
                print(f"\n📈 Data Extraction Summary:")
                print(f"   Total records: {stats[0]:,}")
                print(f"   Unique symbols: {stats[1]:,}")
                print(f"   Unique dates: {stats[2]:,}")
                print(f"   Date range: {stats[3]} to {stats[4]}")
                print(f"   Records added today: {stats[5]:,}")
                print(f"   Records added this week: {stats[6]:,}")
                
                # Calculate average records per symbol
                if stats[1] > 0:
                    avg_records = stats[0] / stats[1]
                    print(f"   Average records per symbol: {avg_records:.1f}")
            
            # Recent errors
            error_summary = db.fetch_query("""
                SELECT 
                    response_status,
                    COUNT(*) as error_count,
                    array_agg(DISTINCT symbol ORDER BY symbol) as symbols
                FROM source.api_responses_landing 
                WHERE table_name = 'cash_flow' 
                  AND fetched_at >= NOW() - INTERVAL '24 hours'
                  AND response_status IN ('error', 'rate_limited', 'empty')
                GROUP BY response_status
                ORDER BY error_count DESC
            """)
            
            if error_summary:
                print(f"\n⚠️ Recent Issues (Last 24 Hours):")
                for status, count, symbols in error_summary:
                    symbol_list = symbols[:5] if symbols else []
                    symbol_str = ', '.join(symbol_list)
                    if len(symbols) > 5:
                        symbol_str += f" (and {len(symbols)-5} more)"
                    print(f"   {status}: {count:,} occurrences")
                    print(f"      Sample symbols: {symbol_str}")
    
    except Exception as e:
        print(f"❌ Error monitoring extraction activity: {e}")


def monitor_data_quality():
    """Monitor data quality metrics."""
    print("\n📊 Data Quality Monitor")
    print("="*50)
    
    try:
        with PostgresDatabaseManager() as db:
            # Field completeness analysis
            completeness_query = """
                SELECT 
                    COUNT(*) as total_records,
                    -- Operating Activities
                    ROUND(100.0 * COUNT(operating_cashflow) / COUNT(*), 1) as operating_cashflow_pct,
                    ROUND(100.0 * COUNT(depreciation_depletion_and_amortization) / COUNT(*), 1) as depreciation_pct,
                    -- Investing Activities  
                    ROUND(100.0 * COUNT(cashflow_from_investment) / COUNT(*), 1) as investing_cashflow_pct,
                    ROUND(100.0 * COUNT(capital_expenditures) / COUNT(*), 1) as capex_pct,
                    -- Financing Activities
                    ROUND(100.0 * COUNT(cashflow_from_financing) / COUNT(*), 1) as financing_cashflow_pct,
                    ROUND(100.0 * COUNT(dividend_payout) / COUNT(*), 1) as dividend_pct,
                    -- Summary
                    ROUND(100.0 * COUNT(change_in_cash_and_cash_equivalents) / COUNT(*), 1) as cash_change_pct,
                    -- Metadata
                    ROUND(100.0 * COUNT(content_hash) / COUNT(*), 1) as content_hash_pct
                FROM source.cash_flow
            """
            
            completeness = db.fetch_query(completeness_query)
            
            if completeness and completeness[0][0] > 0:
                stats = completeness[0]
                print(f"\n📋 Field Completeness (based on {stats[0]:,} records):")
                print(f"   Operating Activities:")
                print(f"      Operating Cashflow: {stats[1]}%")
                print(f"      Depreciation: {stats[2]}%")
                print(f"   Investing Activities:")
                print(f"      Investing Cashflow: {stats[3]}%")
                print(f"      Capital Expenditures: {stats[4]}%")
                print(f"   Financing Activities:")
                print(f"      Financing Cashflow: {stats[5]}%")
                print(f"      Dividend Payout: {stats[6]}%")
                print(f"   Summary:")
                print(f"      Cash Change: {stats[7]}%")
                print(f"   Metadata:")
                print(f"      Content Hash: {stats[8]}%")
            
            # Recent data quality trends
            trend_query = """
                SELECT 
                    DATE(created_at) as date,
                    COUNT(*) as records_added,
                    COUNT(DISTINCT symbol_id) as symbols_processed,
                    ROUND(100.0 * COUNT(operating_cashflow) / COUNT(*), 1) as operating_cashflow_pct
                FROM source.cash_flow 
                WHERE created_at >= NOW() - INTERVAL '7 days'
                GROUP BY DATE(created_at)
                ORDER BY date DESC
                LIMIT 7
            """
            
            trends = db.fetch_query(trend_query)
            
            if trends:
                print(f"\n📈 Recent Quality Trends (Last 7 days):")
                for date, records, symbols, operating_pct in trends:
                    print(f"   {date}: {records:,} records, {symbols:,} symbols, {operating_pct}% operating cashflow")
    
    except Exception as e:
        print(f"❌ Error monitoring data quality: {e}")


def monitor_watermark_status():
    """Monitor watermark processing status."""
    print("\n📊 Watermark Status Monitor")
    print("="*50)
    
    try:
        with PostgresDatabaseManager() as db:
            # Overall watermark status
            watermark_summary = db.fetch_query("""
                SELECT 
                    COUNT(*) as total_symbols,
                    COUNT(CASE WHEN last_extraction_date IS NOT NULL THEN 1 END) as processed_symbols,
                    COUNT(CASE WHEN last_successful_extraction = TRUE THEN 1 END) as successful_symbols,
                    COUNT(CASE WHEN last_extraction_date >= NOW() - INTERVAL '24 hours' THEN 1 END) as recent_symbols,
                    MAX(last_extraction_date) as latest_extraction,
                    MIN(last_extraction_date) as earliest_extraction
                FROM source.extraction_watermarks 
                WHERE table_name = 'cash_flow'
            """)
            
            if watermark_summary:
                stats = watermark_summary[0]
                print(f"\n🏷️ Watermark Summary:")
                print(f"   Total symbols tracked: {stats[0]:,}")
                print(f"   Symbols processed: {stats[1]:,}")
                print(f"   Successful extractions: {stats[2]:,}")
                print(f"   Processed in last 24h: {stats[3]:,}")
                print(f"   Latest extraction: {stats[4]}")
                print(f"   Earliest extraction: {stats[5]}")
                
                if stats[1] > 0:
                    success_rate = (stats[2] / stats[1]) * 100
                    print(f"   Success rate: {success_rate:.1f}%")
            
            # Symbols needing attention
            attention_query = """
                SELECT 
                    symbol,
                    last_extraction_date,
                    last_successful_extraction,
                    CASE 
                        WHEN last_extraction_date IS NULL THEN 'Never processed'
                        WHEN last_extraction_date < NOW() - INTERVAL '7 days' THEN 'Stale (>7 days)'
                        WHEN last_successful_extraction = FALSE THEN 'Recent failure'
                        ELSE 'OK'
                    END as status
                FROM source.extraction_watermarks 
                WHERE table_name = 'cash_flow'
                  AND (
                    last_extraction_date IS NULL 
                    OR last_extraction_date < NOW() - INTERVAL '7 days'
                    OR last_successful_extraction = FALSE
                  )
                ORDER BY 
                  CASE 
                    WHEN last_extraction_date IS NULL THEN 1
                    WHEN last_successful_extraction = FALSE THEN 2  
                    ELSE 3
                  END,
                  last_extraction_date ASC
                LIMIT 10
            """
            
            attention_symbols = db.fetch_query(attention_query)
            
            if attention_symbols:
                print(f"\n⚠️ Symbols Needing Attention (Top 10):")
                for symbol, last_date, success, status in attention_symbols:
                    date_str = str(last_date) if last_date else "Never"
                    print(f"   {symbol}: {status} (Last: {date_str})")
    
    except Exception as e:
        print(f"❌ Error monitoring watermark status: {e}")


def monitor_performance_metrics():
    """Monitor performance and timing metrics."""
    print("\n📊 Performance Monitor")
    print("="*50)
    
    try:
        with PostgresDatabaseManager() as db:
            # Processing volume trends
            volume_trends = db.fetch_query("""
                SELECT 
                    DATE(fetched_at) as date,
                    COUNT(*) as api_calls,
                    COUNT(DISTINCT symbol) as unique_symbols,
                    COUNT(*) FILTER (WHERE response_status = 'success') as successful_calls,
                    ROUND(100.0 * COUNT(*) FILTER (WHERE response_status = 'success') / COUNT(*), 1) as success_rate
                FROM source.api_responses_landing 
                WHERE table_name = 'cash_flow' 
                  AND fetched_at >= NOW() - INTERVAL '7 days'
                GROUP BY DATE(fetched_at)
                ORDER BY date DESC
                LIMIT 7
            """)
            
            if volume_trends:
                print(f"\n📈 Processing Volume Trends (Last 7 days):")
                for date, calls, symbols, successful, success_rate in volume_trends:
                    print(f"   {date}: {calls:,} calls, {symbols:,} symbols, {success_rate}% success")
            
            # Response time analysis (based on creation timestamps)
            timing_analysis = db.fetch_query("""
                SELECT 
                    response_status,
                    COUNT(*) as count,
                    EXTRACT(EPOCH FROM (MAX(fetched_at) - MIN(fetched_at))) / 60 as duration_minutes
                FROM source.api_responses_landing 
                WHERE table_name = 'cash_flow' 
                  AND fetched_at >= NOW() - INTERVAL '24 hours'
                GROUP BY response_status
                ORDER BY count DESC
            """)
            
            if timing_analysis:
                print(f"\n⏱️ Response Distribution (Last 24 hours):")
                for status, count, duration in timing_analysis:
                    print(f"   {status}: {count:,} responses over {duration:.1f} minutes")
    
    except Exception as e:
        print(f"❌ Error monitoring performance metrics: {e}")


def generate_health_report():
    """Generate overall health assessment."""
    print("\n📊 Health Assessment")
    print("="*50)
    
    try:
        with PostgresDatabaseManager() as db:
            # Critical health indicators
            health_checks = []
            
            # Check 1: Recent activity
            recent_activity = db.fetch_query("""
                SELECT COUNT(*) FROM source.api_responses_landing 
                WHERE table_name = 'cash_flow' 
                  AND fetched_at >= NOW() - INTERVAL '24 hours'
            """)
            
            recent_count = recent_activity[0][0] if recent_activity else 0
            health_checks.append(("Recent Activity", recent_count > 0, f"{recent_count:,} API calls in 24h"))
            
            # Check 2: Success rate
            success_rate_query = db.fetch_query("""
                SELECT 
                    COUNT(*) FILTER (WHERE response_status = 'success') * 100.0 / COUNT(*) as success_rate
                FROM source.api_responses_landing 
                WHERE table_name = 'cash_flow' 
                  AND fetched_at >= NOW() - INTERVAL '24 hours'
            """)
            
            success_rate = success_rate_query[0][0] if success_rate_query and success_rate_query[0][0] else 0
            health_checks.append(("Success Rate", success_rate >= 70, f"{success_rate:.1f}%"))
            
            # Check 3: Data completeness
            completeness_query = db.fetch_query("""
                SELECT 
                    COUNT(*) FILTER (WHERE content_hash IS NOT NULL) * 100.0 / COUNT(*) as completeness
                FROM source.cash_flow
            """)
            
            completeness = completeness_query[0][0] if completeness_query and completeness_query[0][0] else 0
            health_checks.append(("Data Integrity", completeness >= 95, f"{completeness:.1f}% have content hash"))
            
            # Check 4: Recent data additions
            recent_data = db.fetch_query("""
                SELECT COUNT(*) FROM source.cash_flow 
                WHERE created_at >= NOW() - INTERVAL '7 days'
            """)
            
            recent_data_count = recent_data[0][0] if recent_data else 0
            health_checks.append(("Recent Data", recent_data_count > 0, f"{recent_data_count:,} records added this week"))
            
            # Display health status
            print(f"\n🏥 System Health Indicators:")
            healthy_count = 0
            
            for check_name, is_healthy, details in health_checks:
                status = "✅ HEALTHY" if is_healthy else "⚠️ ATTENTION"
                print(f"   {check_name}: {status} ({details})")
                if is_healthy:
                    healthy_count += 1
            
            overall_health = (healthy_count / len(health_checks)) * 100
            print(f"\n🎯 Overall Health Score: {overall_health:.0f}% ({healthy_count}/{len(health_checks)} checks passed)")
            
            if overall_health >= 90:
                print("💚 System is healthy and operating normally")
            elif overall_health >= 70:
                print("💛 System is functional but may need attention")
            else:
                print("❤️ System needs immediate attention")
    
    except Exception as e:
        print(f"❌ Error generating health report: {e}")


def run_monitoring_dashboard():
    """Run complete monitoring dashboard."""
    print("🚀 Cash Flow Extractor Monitoring Dashboard")
    print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    monitoring_functions = [
        monitor_extraction_activity,
        monitor_data_quality, 
        monitor_watermark_status,
        monitor_performance_metrics,
        generate_health_report
    ]
    
    for monitor_func in monitoring_functions:
        try:
            monitor_func()
        except Exception as e:
            print(f"❌ Error in {monitor_func.__name__}: {e}")
    
    print("\n" + "="*60)
    print("📝 Monitoring completed")
    print("💡 For detailed analysis, check individual database tables:")
    print("   • source.cash_flow - Main data table")
    print("   • source.api_responses_landing - API response log")
    print("   • source.extraction_watermarks - Processing status")


if __name__ == "__main__":
    run_monitoring_dashboard()
