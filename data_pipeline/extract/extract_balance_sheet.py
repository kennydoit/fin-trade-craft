"""
Balance Sheet Extractor using incremental ETL architecture.
Uses source schema, watermarks, and deterministic processing.
"""

import os
import sys
import time
import json
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

import requests
from dotenv import load_dotenv

# Add the parent directories to the path so we can import from db and utils
sys.path.append(str(Path(__file__).parent.parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager
from utils.incremental_etl import DateUtils, ContentHasher, WatermarkManager, RunIdGenerator

# API configuration
STOCK_API_FUNCTION = "BALANCE_SHEET"
API_DELAY_SECONDS = 0.8  # Alpha Vantage rate limiting

# Schema-driven field mapping configuration
BALANCE_SHEET_FIELDS = {
    # Asset fields
    'total_assets': 'totalAssets',
    'total_current_assets': 'totalCurrentAssets',
    'cash_and_cash_equivalents_at_carrying_value': 'cashAndCashEquivalentsAtCarryingValue',
    'cash_and_short_term_investments': 'cashAndShortTermInvestments',
    'inventory': 'inventory',
    'current_net_receivables': 'currentNetReceivables',
    'total_non_current_assets': 'totalNonCurrentAssets',
    'property_plant_equipment': 'propertyPlantEquipment',
    'accumulated_depreciation_amortization_ppe': 'accumulatedDepreciationAmortizationPPE',
    'intangible_assets': 'intangibleAssets',
    'intangible_assets_excluding_goodwill': 'intangibleAssetsExcludingGoodwill',
    'goodwill': 'goodwill',
    'investments': 'investments',
    'long_term_investments': 'longTermInvestments',
    'short_term_investments': 'shortTermInvestments',
    'other_current_assets': 'otherCurrentAssets',
    'other_non_current_assets': 'otherNonCurrentAssets',
    
    # Liability fields
    'total_liabilities': 'totalLiabilities',
    'total_current_liabilities': 'totalCurrentLiabilities',
    'current_accounts_payable': 'currentAccountsPayable',
    'deferred_revenue': 'deferredRevenue',
    'current_debt': 'currentDebt',
    'short_term_debt': 'shortTermDebt',
    'total_non_current_liabilities': 'totalNonCurrentLiabilities',
    'capital_lease_obligations': 'capitalLeaseObligations',
    'long_term_debt': 'longTermDebt',
    'current_long_term_debt': 'currentLongTermDebt',
    'long_term_debt_noncurrent': 'longTermDebtNoncurrent',
    'short_long_term_debt_total': 'shortLongTermDebtTotal',
    'other_current_liabilities': 'otherCurrentLiabilities',
    'other_non_current_liabilities': 'otherNonCurrentLiabilities',
    
    # Equity fields
    'total_shareholder_equity': 'totalShareholderEquity',
    'treasury_stock': 'treasuryStock',
    'retained_earnings': 'retainedEarnings',
    'common_stock': 'commonStock',
    'common_stock_shares_outstanding': 'commonStockSharesOutstanding',
}


class BalanceSheetExtractor:
    """Balance sheet extractor with incremental processing."""
    
    def __init__(self):
        """Initialize the extractor."""
        load_dotenv()
        self.api_key = os.getenv("ALPHAVANTAGE_API_KEY")
        if not self.api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY not found in environment variables")
        
        self.table_name = "balance_sheet"
        self.schema_name = "source"
        self.db_manager = None
        self.watermark_manager = None
    
    def _get_db_manager(self):
        """Get database manager with context management."""
        if not self.db_manager:
            self.db_manager = PostgresDatabaseManager()
        return self.db_manager
    
    def _initialize_watermark_manager(self, db):
        """Initialize watermark manager with database connection."""
        if not self.watermark_manager:
            self.watermark_manager = WatermarkManager(db)
        return self.watermark_manager
    
    def _ensure_schema_exists(self, db):
        """Ensure the source schema and tables exist."""
        try:
            # Read and execute the source schema
            schema_file = Path(__file__).parent.parent.parent / "db" / "schema" / "source_schema.sql"
            if schema_file.exists():
                with open(schema_file, 'r') as f:
                    schema_sql = f.read()
                db.execute_script(schema_sql)
                print(f"✅ Source schema initialized")
            else:
                print(f"⚠️ Schema file not found: {schema_file}")
        except Exception as e:
            print(f"⚠️ Schema initialization error: {e}")
    
    def _fetch_api_data(self, symbol: str) -> tuple[Dict[str, Any], str]:
        """
        Fetch balance sheet data from Alpha Vantage API.
        
        Args:
            symbol: Stock symbol to fetch
            
        Returns:
            Tuple of (api_response, status)
        """
        url = f"https://www.alphavantage.co/query"
        params = {
            "function": STOCK_API_FUNCTION,
            "symbol": symbol,
            "apikey": self.api_key
        }
        
        try:
            print(f"Fetching data from: {url}?function={STOCK_API_FUNCTION}&symbol={symbol}&apikey={self.api_key}")
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API errors
            if "Error Message" in data:
                return data, "error"
            elif "Note" in data:
                return data, "rate_limited"
            elif not data or len(data) == 0:
                return data, "empty"
            else:
                return data, "success"
                
        except Exception as e:
            print(f"API request failed for {symbol}: {e}")
            return {"error": str(e)}, "error"
    
    def _store_landing_record(self, db, symbol: str, symbol_id: int, 
                            api_response: Dict[str, Any], status: str, run_id: str) -> str:
        """
        Store raw API response in landing table.
        
        Args:
            db: Database manager
            symbol: Stock symbol
            symbol_id: Symbol ID
            api_response: Raw API response
            status: Response status
            run_id: Unique run ID
            
        Returns:
            Content hash of the response
        """
        content_hash = ContentHasher.calculate_api_response_hash(api_response)
        
        insert_query = """
            INSERT INTO source.api_responses_landing 
            (table_name, symbol, symbol_id, api_function, api_response, 
             content_hash, source_run_id, response_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        db.execute_query(insert_query, (
            self.table_name, symbol, symbol_id, STOCK_API_FUNCTION,
            json.dumps(api_response), content_hash, run_id, status
        ))
        
        return content_hash
    
    def _transform_data(self, symbol: str, symbol_id: int, api_response: Dict[str, Any], 
                       run_id: str) -> List[Dict[str, Any]]:
        """
        Transform API response to standardized records.
        
        Args:
            symbol: Stock symbol
            symbol_id: Symbol ID
            api_response: Raw API response
            run_id: Unique run ID
            
        Returns:
            List of transformed records
        """
        records = []
        
        # Process quarterly reports
        quarterly_reports = api_response.get("quarterlyReports", [])
        for report in quarterly_reports:
            record = self._transform_single_report(
                symbol, symbol_id, report, "quarterly", run_id
            )
            if record:
                records.append(record)
        
        # Process annual reports
        annual_reports = api_response.get("annualReports", [])
        for report in annual_reports:
            record = self._transform_single_report(
                symbol, symbol_id, report, "annual", run_id
            )
            if record:
                records.append(record)
        
        return records
    
    def _transform_single_report(self, symbol: str, symbol_id: int, 
                                report: Dict[str, Any], report_type: str, 
                                run_id: str) -> Optional[Dict[str, Any]]:
        """
        Transform a single balance sheet report.
        
        Args:
            symbol: Stock symbol
            symbol_id: Symbol ID
            report: Single report data
            report_type: 'quarterly' or 'annual'
            run_id: Unique run ID
            
        Returns:
            Transformed record or None if invalid
        """
        try:
            # Initialize record with all fields as None
            record = {field: None for field in BALANCE_SHEET_FIELDS.keys()}
            
            # Set known values
            record.update({
                "symbol_id": symbol_id,
                "symbol": symbol,
                "report_type": report_type,
                "api_response_status": "pass",
                "source_run_id": run_id,
                "fetched_at": datetime.now()
            })
            
            # Helper function to convert API values
            def convert_value(value):
                if value is None or value == "None" or value == "":
                    return None
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return None
            
            # Map API fields to database fields using schema-driven approach
            for db_field, api_field in BALANCE_SHEET_FIELDS.items():
                if db_field in ["symbol_id", "symbol", "report_type"]:
                    # These are already set
                    continue
                elif api_field == 'fiscalDateEnding':
                    # Use deterministic date parsing
                    record[db_field] = DateUtils.parse_fiscal_date(report.get(api_field))
                elif api_field in report:
                    record[db_field] = convert_value(report.get(api_field))
            
            # Handle special fields not in the main mapping
            record["fiscal_date_ending"] = DateUtils.parse_fiscal_date(report.get("fiscalDateEnding"))
            record["reported_currency"] = report.get("reportedCurrency")
            
            # Validate required fields
            if not record["fiscal_date_ending"]:
                print(f"Missing fiscal_date_ending for {symbol} {report_type} report")
                return None
            
            # Calculate content hash for change detection
            record["content_hash"] = ContentHasher.calculate_business_content_hash(record)
            
            return record
            
        except Exception as e:
            print(f"Error transforming report for {symbol}: {e}")
            return None
    
    def _content_has_changed(self, db, symbol_id: int, content_hash: str) -> bool:
        """
        Check if content has changed based on hash comparison.
        
        Args:
            db: Database manager
            symbol_id: Symbol ID
            content_hash: New content hash
            
        Returns:
            True if content has changed or is new
        """
        query = """
            SELECT COUNT(*) FROM source.balance_sheet 
            WHERE symbol_id = %s AND content_hash = %s
        """
        
        result = db.fetch_query(query, (symbol_id, content_hash))
        return result[0][0] == 0 if result else True  # True if no matching hash found
    
    def _upsert_records(self, db, records: List[Dict[str, Any]]) -> int:
        """
        Upsert records into the balance sheet table.
        
        Args:
            db: Database manager
            records: List of records to upsert
            
        Returns:
            Number of rows affected
        """
        if not records:
            return 0
        
        # Get column names (excluding auto-generated ones)
        columns = [col for col in records[0].keys() 
                  if col not in ['balance_sheet_id', 'created_at', 'updated_at']]
        
        # Build upsert query
        placeholders = ", ".join(["%s" for _ in columns])
        update_columns = [col for col in columns 
                         if col not in ['symbol_id', 'fiscal_date_ending', 'report_type']]
        update_set = [f"{col} = EXCLUDED.{col}" for col in update_columns]
        update_set.append("updated_at = NOW()")
        
        upsert_query = f"""
            INSERT INTO source.balance_sheet ({', '.join(columns)}, created_at, updated_at) 
            VALUES ({placeholders}, NOW(), NOW())
            ON CONFLICT (symbol_id, fiscal_date_ending, report_type) 
            DO UPDATE SET {', '.join(update_set)}
        """
        
        # Prepare record values
        record_values = []
        for record in records:
            values = [record[col] for col in columns]
            record_values.append(tuple(values))
        
        # Execute upsert
        with db.connection.cursor() as cursor:
            cursor.executemany(upsert_query, record_values)
            db.connection.commit()
            return cursor.rowcount
    
    def extract_symbol(self, symbol: str, symbol_id: int, db) -> Dict[str, Any]:
        """
        Extract balance sheet data for a single symbol.
        
        Args:
            symbol: Stock symbol
            symbol_id: Symbol ID
            db: Database manager (passed from caller)
            
        Returns:
            Processing result summary
        """
        run_id = RunIdGenerator.generate()
        
        watermark_mgr = self._initialize_watermark_manager(db)
        
        # Fetch API data
        api_response, status = self._fetch_api_data(symbol)
        
        # Store in landing table (always)
        content_hash = self._store_landing_record(
            db, symbol, symbol_id, api_response, status, run_id
        )
        
        # Process if successful
        if status == "success":
            # Check if content has changed
            if not self._content_has_changed(db, symbol_id, content_hash):
                print(f"No changes detected for {symbol}, skipping transformation")
                watermark_mgr.update_watermark(self.table_name, symbol_id, success=True)
                return {
                    "symbol": symbol,
                    "status": "no_changes",
                    "records_processed": 0,
                    "run_id": run_id
                }
            
            # Transform data
            records = self._transform_data(symbol, symbol_id, api_response, run_id)
            
            if records:
                # Upsert records
                rows_affected = self._upsert_records(db, records)
                
                # Update watermark with latest fiscal date
                latest_fiscal_date = max(
                    r['fiscal_date_ending'] for r in records 
                    if r['fiscal_date_ending']
                )
                watermark_mgr.update_watermark(
                    self.table_name, symbol_id, latest_fiscal_date, success=True
                )
                
                return {
                    "symbol": symbol,
                    "status": "success",
                    "records_processed": len(records),
                    "rows_affected": rows_affected,
                    "latest_fiscal_date": latest_fiscal_date,
                    "run_id": run_id
                }
            else:
                # No valid records
                watermark_mgr.update_watermark(self.table_name, symbol_id, success=False)
                return {
                    "symbol": symbol,
                    "status": "no_valid_records",
                    "records_processed": 0,
                    "run_id": run_id
                }
        else:
            # API failure
            watermark_mgr.update_watermark(self.table_name, symbol_id, success=False)
            return {
                "symbol": symbol,
                "status": "api_failure",
                "error": status,
                "records_processed": 0,
                "run_id": run_id
            }
    
    def run_incremental_extraction(self, limit: Optional[int] = None, 
                                 staleness_hours: int = 24) -> Dict[str, Any]:
        """
        Run incremental extraction for symbols that need processing.
        
        Args:
            limit: Maximum number of symbols to process
            staleness_hours: Hours before data is considered stale
            
        Returns:
            Processing summary
        """
        print(f"🚀 Starting incremental balance sheet extraction...")
        print(f"Configuration: limit={limit}, staleness_hours={staleness_hours}")
        
        with self._get_db_manager() as db:
            # Ensure schema exists
            self._ensure_schema_exists(db)
            
            watermark_mgr = self._initialize_watermark_manager(db)
            
            # Get symbols needing processing
            symbols_to_process = watermark_mgr.get_symbols_needing_processing(
                self.table_name, staleness_hours=staleness_hours, limit=limit
            )
            
            print(f"Found {len(symbols_to_process)} symbols needing processing")
            
            if not symbols_to_process:
                print("✅ No symbols need processing")
                return {
                    "symbols_processed": 0,
                    "successful": 0,
                    "failed": 0,
                    "no_changes": 0,
                    "total_records": 0
                }
            
            # Process symbols
            results = {
                "symbols_processed": 0,
                "successful": 0,
                "failed": 0,
                "no_changes": 0,
                "total_records": 0,
                "details": []
            }
            
            for i, symbol_data in enumerate(symbols_to_process, 1):
                symbol_id = symbol_data["symbol_id"]
                symbol = symbol_data["symbol"]
                
                print(f"Processing {symbol} (ID: {symbol_id}) [{i}/{len(symbols_to_process)}]")
                
                # Extract symbol data
                result = self.extract_symbol(symbol, symbol_id, db)
                results["details"].append(result)
                results["symbols_processed"] += 1
                
                if result["status"] == "success":
                    results["successful"] += 1
                    results["total_records"] += result["records_processed"]
                    print(f"✅ {symbol}: {result['records_processed']} records processed")
                elif result["status"] == "no_changes":
                    results["no_changes"] += 1
                    print(f"⚪ {symbol}: No changes detected")
                else:
                    results["failed"] += 1
                    print(f"❌ {symbol}: {result['status']}")
                
                # Rate limiting
                if i < len(symbols_to_process):
                    time.sleep(API_DELAY_SECONDS)
            
            print(f"\n🎯 Incremental extraction completed:")
            print(f"  Symbols processed: {results['symbols_processed']}")
            print(f"  Successful: {results['successful']}")
            print(f"  No changes: {results['no_changes']}")
            print(f"  Failed: {results['failed']}")
            print(f"  Total records: {results['total_records']}")
            
            return results


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description="Balance Sheet Extractor")
    parser.add_argument("--limit", type=int, help="Maximum number of symbols to process")
    parser.add_argument("--staleness-hours", type=int, default=24, 
                       help="Hours before data is considered stale (default: 24)")
    
    args = parser.parse_args()
    
    extractor = BalanceSheetExtractor()
    result = extractor.run_incremental_extraction(
        limit=args.limit,
        staleness_hours=args.staleness_hours
    )
    
    # Exit with appropriate code
    if result["failed"] > 0 and result["successful"] == 0:
        sys.exit(1)  # All failed
    else:
        sys.exit(0)  # Some or all successful


if __name__ == "__main__":
    main()

# -----------------------------------------------------------------------------
# Example commands (PowerShell):
# 
# Basic incremental extraction (process up to 50 symbols, 24-hour staleness):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_balance_sheet.py --limit 50
#
# Process only 10 symbols (useful for testing):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_balance_sheet.py --limit 10
#
# Aggressive refresh (1-hour staleness, process 25 symbols):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_balance_sheet.py --limit 25 --staleness-hours 1
#
# Weekly batch processing (7-day staleness, no limit):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_balance_sheet.py --staleness-hours 168
#
# Large batch processing (process 100 symbols, 24-hour staleness):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_balance_sheet.py --limit 100 --staleness-hours 24
#
# Force refresh of recent data (6-hour staleness, 50 symbols):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_balance_sheet.py --limit 50 --staleness-hours 6
#
# Process unprocessed symbols only (very long staleness period):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_balance_sheet.py --limit 25 --staleness-hours 8760
# -----------------------------------------------------------------------------
