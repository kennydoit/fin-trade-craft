"""
Time Series Daily Adjusted Extractor using incremental ETL architecture.
Uses source schema, watermarks, and deterministic processing.
"""

import os
import sys
import time
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
from io import StringIO

import requests
import pandas as pd
from dotenv import load_dotenv

# Add the parent directories to the path so we can import from db and utils
sys.path.append(str(Path(__file__).parent.parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager
from utils.incremental_etl import DateUtils, ContentHasher, WatermarkManager, RunIdGenerator

# API configuration
STOCK_API_FUNCTION = "TIME_SERIES_DAILY_ADJUSTED"
API_DELAY_SECONDS = 0.8  # Alpha Vantage rate limiting
DATATYPE = "csv"
OUTPUTSIZE = "full"  # Use full to get complete historical data (20+ years)

# Schema-driven field mapping configuration
TIME_SERIES_FIELDS = {
    'symbol_id': 'symbol_id',
    'symbol': 'symbol',
    'date': 'timestamp',
    'open': 'open',
    'high': 'high',
    'low': 'low',
    'close': 'close',
    'adjusted_close': 'adjusted_close',
    'volume': 'volume',
    'dividend_amount': 'dividend_amount',
    'split_coefficient': 'split_coefficient',
}


class TimeSeriesDailyAdjustedExtractor:
    """Time series daily adjusted extractor with incremental processing."""
    
    def __init__(self):
        """Initialize the extractor."""
        load_dotenv()
        self.api_key = os.getenv("ALPHAVANTAGE_API_KEY")
        if not self.api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY not found in environment variables")
        
        self.table_name = "time_series_daily_adjusted"
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
    
    def _determine_output_size(self, symbol_id: int, db) -> str:
        """
        Determine whether to use 'compact' or 'full' output size based on watermark age.
        
        Args:
            symbol_id: Symbol ID to check watermark for
            db: Database manager
            
        Returns:
            'compact' for recent updates (< 100 trading days), 'full' for initial loads
        """
        try:
            # Check if we have a watermark for this symbol
            query = """
                SELECT last_fiscal_date 
                FROM source.extraction_watermarks 
                WHERE table_name = %s AND symbol_id = %s
                ORDER BY updated_at DESC 
                LIMIT 1
            """
            
            result = db.fetch_query(query, (self.table_name, symbol_id))
            
            if not result or not result[0][0]:
                # No watermark exists - this is an initial load
                print(f"No watermark found for symbol_id {symbol_id}, using 'full' mode")
                return "full"
            
            last_date = result[0][0]
            current_date = datetime.now().date()
            days_since_last = (current_date - last_date).days
            
            # Use compact if less than 75 days (buffer below 100 trading days)
            # Assuming ~250 trading days per year, 75 calendar days ≈ 50-55 trading days
            if days_since_last < 75:
                print(f"Last update {days_since_last} days ago for symbol_id {symbol_id}, using 'compact' mode")
                return "compact"
            else:
                print(f"Last update {days_since_last} days ago for symbol_id {symbol_id}, using 'full' mode")
                return "full"
                
        except Exception as e:
            print(f"Error checking watermark for symbol_id {symbol_id}: {e}, defaulting to 'full'")
            return "full"
    
    def _fetch_api_data(self, symbol: str, symbol_id: int, db) -> tuple[pd.DataFrame, str]:
        """
        Fetch time series data from Alpha Vantage API.
        Uses dynamic output size based on watermark age.
        
        Args:
            symbol: Stock symbol to fetch
            symbol_id: Symbol ID for watermark lookup
            db: Database manager for watermark queries
            
        Returns:
            Tuple of (dataframe, status)
        """
        # Determine output size based on watermark age
        outputsize = self._determine_output_size(symbol_id, db)
        
        url = f"https://www.alphavantage.co/query"
        params = {
            "function": STOCK_API_FUNCTION,
            "symbol": symbol,
            "datatype": DATATYPE,
            "outputsize": outputsize,
            "apikey": self.api_key
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            # Check if we got CSV data
            if (response.headers.get("content-type", "").startswith("text/csv") 
                or "timestamp" in response.text.lower()):
                
                # Read CSV data
                df = pd.read_csv(StringIO(response.text))
                
                if df.empty or "timestamp" not in df.columns:
                    print(f"Empty or invalid CSV response for {symbol}")
                    return pd.DataFrame(), "empty"
                
                print(f"Successfully fetched {len(df)} records for {symbol} (mode: {outputsize})")
                return df, "success"
            else:
                # Might be JSON error response
                try:
                    data = response.json()
                    if "Error Message" in data:
                        return pd.DataFrame(), "error"
                    elif "Note" in data:
                        return pd.DataFrame(), "rate_limited"
                    else:
                        return pd.DataFrame(), "empty"
                except:
                    return pd.DataFrame(), "error"
                
        except Exception as e:
            print(f"API request failed for {symbol}: {e}")
            return pd.DataFrame(), "error"
    
    def _store_landing_record(self, db, symbol: str, symbol_id: int, 
                            df: pd.DataFrame, status: str, run_id: str) -> str:
        """
        Store raw API response in landing table.
        
        Args:
            db: Database manager
            symbol: Stock symbol
            symbol_id: Symbol ID
            df: DataFrame with API response
            status: Response status
            run_id: Unique run ID
            
        Returns:
            Content hash of the response
        """
        # Convert DataFrame to dict for storage
        if not df.empty:
            api_response = {
                "data": df.to_dict("records"),
                "columns": list(df.columns),
                "row_count": len(df)
            }
        else:
            api_response = {"data": [], "columns": [], "row_count": 0}
        
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
    
    def _transform_data(self, symbol: str, symbol_id: int, df: pd.DataFrame, 
                       run_id: str) -> List[Dict[str, Any]]:
        """
        Transform API response to standardized records.
        
        Args:
            symbol: Stock symbol
            symbol_id: Symbol ID
            df: DataFrame with API response
            run_id: Unique run ID
            
        Returns:
            List of transformed records
        """
        if df.empty:
            return []
        
        records = []
        
        try:
            # Rename columns to match our schema
            df_transformed = df.copy()
            
            # Convert timestamp to date
            if 'timestamp' in df_transformed.columns:
                df_transformed['date'] = pd.to_datetime(df_transformed['timestamp']).dt.date
            
            # Helper function to convert API values
            def convert_value(value):
                if pd.isna(value) or value == "None" or value == "":
                    return None
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return None
            
            # Process each row
            for _, row in df_transformed.iterrows():
                # Initialize record with all fields as None
                record = {field: None for field in TIME_SERIES_FIELDS.keys()}
                
                # Set known values
                record.update({
                    "symbol_id": symbol_id,
                    "symbol": symbol,
                    "api_response_status": "pass",
                    "source_run_id": run_id,
                    "fetched_at": datetime.now()
                })
                
                # Map API fields to database fields using schema-driven approach
                for db_field, api_field in TIME_SERIES_FIELDS.items():
                    if db_field in ["symbol_id", "symbol"]:
                        # These are already set
                        continue
                    elif api_field == 'timestamp':
                        # Use the converted date
                        record[db_field] = row.get('date')
                    elif api_field in row.index:
                        record[db_field] = convert_value(row[api_field])
                
                # Validate required fields
                if not record["date"]:
                    print(f"Missing date for {symbol} record")
                    continue
                
                # Calculate content hash for change detection
                record["content_hash"] = ContentHasher.calculate_business_content_hash(record)
                
                records.append(record)
            
            print(f"Transformed {len(records)} records for {symbol}")
            return records
            
        except Exception as e:
            print(f"Error transforming data for {symbol}: {e}")
            return []
    
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
            SELECT COUNT(*) FROM source.time_series_daily_adjusted 
            WHERE symbol_id = %s AND content_hash = %s
        """
        
        result = db.fetch_query(query, (symbol_id, content_hash))
        return result[0][0] == 0 if result else True  # True if no matching hash found
    
    def _upsert_records(self, db, records: List[Dict[str, Any]]) -> int:
        """
        Upsert records into the time series table.
        
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
                  if col not in ['time_series_id', 'created_at', 'updated_at']]
        
        # Build upsert query
        placeholders = ", ".join(["%s" for _ in columns])
        update_columns = [col for col in columns 
                         if col not in ['symbol_id', 'date']]
        update_set = [f"{col} = EXCLUDED.{col}" for col in update_columns]
        update_set.append("updated_at = NOW()")
        
        upsert_query = f"""
            INSERT INTO source.time_series_daily_adjusted ({', '.join(columns)}, created_at, updated_at) 
            VALUES ({placeholders}, NOW(), NOW())
            ON CONFLICT (symbol_id, date) 
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
        Extract time series data for a single symbol.
        
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
        df, status = self._fetch_api_data(symbol, symbol_id, db)
        
        # Store in landing table (always)
        content_hash = self._store_landing_record(
            db, symbol, symbol_id, df, status, run_id
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
            records = self._transform_data(symbol, symbol_id, df, run_id)
            
            if records:
                # Upsert records
                rows_affected = self._upsert_records(db, records)
                
                # Update watermark with latest date
                latest_date = max(
                    r['date'] for r in records 
                    if r['date']
                )
                watermark_mgr.update_watermark(
                    self.table_name, symbol_id, latest_date, success=True
                )
                
                return {
                    "symbol": symbol,
                    "status": "success",
                    "records_processed": len(records),
                    "rows_affected": rows_affected,
                    "latest_date": latest_date,
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
                                 staleness_hours: int = 24,
                                 exchange_filter: Optional[List[str]] = None,
                                 asset_type_filter: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Run incremental extraction for symbols that need processing.
        
        Args:
            limit: Maximum number of symbols to process
            staleness_hours: Hours before data is considered stale
            exchange_filter: Filter by exchanges (e.g., ['NYSE', 'NASDAQ'])
            asset_type_filter: Filter by asset types (e.g., ['Stock', 'ETF'])
            
        Returns:
            Processing summary
        """
        print(f"🚀 Starting incremental time series extraction...")
        print(f"Configuration: limit={limit}, staleness_hours={staleness_hours}")
        print(f"Exchange filter: {exchange_filter}")
        print(f"Asset type filter: {asset_type_filter}")
        
        with self._get_db_manager() as db:
            # Ensure schema exists
            self._ensure_schema_exists(db)
            
            watermark_mgr = self._initialize_watermark_manager(db)
            
            # Get symbols needing processing with filtering
            symbols_to_process = watermark_mgr.get_symbols_needing_processing_with_filters(
                self.table_name, 
                staleness_hours=staleness_hours, 
                limit=limit,
                exchange_filter=exchange_filter,
                asset_type_filter=asset_type_filter
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
    parser = argparse.ArgumentParser(description="Time Series Daily Adjusted Extractor")
    parser.add_argument("--limit", type=int, help="Maximum number of symbols to process")
    parser.add_argument("--staleness-hours", type=int, default=24, 
                       help="Hours before data is considered stale (default: 24)")
    parser.add_argument("--exchanges", nargs="+", help="Filter by exchanges (e.g., NYSE NASDAQ)")
    parser.add_argument("--asset-types", nargs="+", help="Filter by asset types (e.g., Stock ETF)")
    
    args = parser.parse_args()
    
    extractor = TimeSeriesDailyAdjustedExtractor()
    result = extractor.run_incremental_extraction(
        limit=args.limit,
        staleness_hours=args.staleness_hours,
        exchange_filter=args.exchanges,
        asset_type_filter=args.asset_types
    )
    
    # Exit with appropriate code
    if result["failed"] > 0 and result["successful"] == 0:
        sys.exit(1)  # All failed
    else:
        sys.exit(0)  # Some or all successful


if __name__ == "__main__":
    main()


# Sample usage calls:
# python extract_time_series_daily_adjusted.py --limit 5
# python extract_time_series_daily_adjusted.py --limit 25 --staleness-hours 12
# python extract_time_series_daily_adjusted.py --exchanges NYSE NASDAQ --asset-types Stock

# -----------------------------------------------------------------------------
# Example commands (PowerShell):
# 
# Basic incremental extraction (process up to 50 symbols, 24-hour staleness):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_time_series_daily_adjusted.py --limit 50
#
# Process only 10 symbols (useful for testing):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_time_series_daily_adjusted.py --limit 10
#
# Aggressive refresh (1-hour staleness, process 25 symbols):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_time_series_daily_adjusted.py --limit 25 --staleness-hours 1
#
# Daily batch processing (6-hour staleness, no limit):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_time_series_daily_adjusted.py --staleness-hours 6
#
# Large batch processing (process 100 symbols, 24-hour staleness):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_time_series_daily_adjusted.py --limit 100 --staleness-hours 24
#
# Force refresh of recent data (1-hour staleness, 50 symbols):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_time_series_daily_adjusted.py --limit 50 --staleness-hours 1
#
# Extract only NYSE stocks:
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_time_series_daily_adjusted.py --limit 25 --exchanges NYSE --asset-types Stock
#
# Extract ETFs from major exchanges:
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_time_series_daily_adjusted.py --limit 50 --exchanges NYSE NASDAQ "NYSE ARCA" --asset-types ETF
# -----------------------------------------------------------------------------
