#!/usr/bin/env python3
"""
Transformation Watermark Manager

Manages watermarks for transformation groups, tracking which symbols need processing
and when they were last successfully transformed.
"""

import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import pandas as pd

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager


class TransformationWatermarkManager:
    """Manage transformation watermarks for incremental processing."""
    
    def __init__(self):
        self.db = PostgresDatabaseManager()
    
    def create_watermark_table(self):
        """Create the transforms.transformation_watermarks table."""
        create_table_sql = """
            -- Create transforms schema if it doesn't exist
            CREATE SCHEMA IF NOT EXISTS transforms;
            
            -- Drop existing table if it exists
            DROP TABLE IF EXISTS transforms.transformation_watermarks CASCADE;
            
            -- Create watermark table
            CREATE TABLE transforms.transformation_watermarks (
                watermark_id            SERIAL PRIMARY KEY,
                symbol_id               INTEGER NOT NULL,
                symbol                  VARCHAR(20) NOT NULL,
                transformation_group    VARCHAR(100) NOT NULL,
                
                -- Symbol metadata from raw.pg_etl_watermarks
                listing_status          VARCHAR(20),
                ipo_date                DATE,
                delisting_date          DATE,
                exchange                VARCHAR(20),
                
                -- Date tracking
                first_date_processed    DATE,
                last_date_processed     DATE,
                
                -- Status tracking
                transformation_eligible BOOLEAN DEFAULT true,
                last_run_status         VARCHAR(20),  -- 'success', 'partial', 'failed'
                consecutive_failures    INTEGER DEFAULT 0,
                
                -- Audit fields
                created_at              TIMESTAMP DEFAULT NOW(),
                updated_at              TIMESTAMP DEFAULT NOW(),
                last_successful_run     TIMESTAMP,
                
                -- Constraints
                UNIQUE(symbol_id, transformation_group)
            );
            
            -- Create indexes
            CREATE INDEX idx_watermark_group 
                ON transforms.transformation_watermarks(transformation_group);
            CREATE INDEX idx_watermark_eligible 
                ON transforms.transformation_watermarks(transformation_eligible);
            CREATE INDEX idx_watermark_symbol_group 
                ON transforms.transformation_watermarks(symbol_id, transformation_group);
            CREATE INDEX idx_watermark_symbol 
                ON transforms.transformation_watermarks(symbol);
            CREATE INDEX idx_watermark_status 
                ON transforms.transformation_watermarks(last_run_status);
            CREATE INDEX idx_watermark_last_run 
                ON transforms.transformation_watermarks(last_successful_run);
            
            -- Create trigger for updated_at
            CREATE OR REPLACE FUNCTION transforms.update_transformation_watermarks_updated_at()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            
            DROP TRIGGER IF EXISTS trg_transformation_watermarks_updated_at 
                ON transforms.transformation_watermarks;
            
            CREATE TRIGGER trg_transformation_watermarks_updated_at
                BEFORE UPDATE ON transforms.transformation_watermarks
                FOR EACH ROW
                EXECUTE FUNCTION transforms.update_transformation_watermarks_updated_at();
        """
        
        with self.db as db:
            db.execute_query(create_table_sql)
            print("✓ Created transforms.transformation_watermarks table")
    
    def initialize_transformation_group(self, transformation_group: str):
        """
        Initialize a transformation group by creating watermark records for all eligible symbols.
        Pulls symbols from raw.etl_watermarks.
        
        Args:
            transformation_group: Name of the transformation group (e.g., 'insider_transactions')
        """
        print(f"🔄 Initializing transformation group: {transformation_group}")
        
        # Get all unique symbols from raw.etl_watermarks
        query = """
            SELECT DISTINCT
                symbol_id,
                symbol,
                status,
                ipo_date,
                delisting_date,
                exchange
            FROM raw.etl_watermarks
            WHERE status IN ('Active', 'Delisted')
              AND asset_type IN ('Stock', 'ETF')
            ORDER BY symbol_id
        """
        
        with self.db as db:
            print("📊 Fetching symbols from raw.etl_watermarks...")
            symbols = db.fetch_query(query)
            
            if not symbols:
                print("⚠️  No symbols found in raw.pg_etl_watermarks")
                return
            
            print(f"   Found {len(symbols):,} unique symbols")
            
            # Insert watermark records for each symbol
            insert_query = """
                INSERT INTO transforms.transformation_watermarks (
                    symbol_id,
                    symbol,
                    transformation_group,
                    listing_status,
                    ipo_date,
                    delisting_date,
                    exchange,
                    transformation_eligible,
                    consecutive_failures
                ) VALUES (
                    %(symbol_id)s,
                    %(symbol)s,
                    %(transformation_group)s,
                    %(listing_status)s,
                    %(ipo_date)s,
                    %(delisting_date)s,
                    %(exchange)s,
                    true,
                    0
                )
                ON CONFLICT (symbol_id, transformation_group) DO NOTHING
            """
            
            records = [
                {
                    'symbol_id': row[0],
                    'symbol': row[1],
                    'transformation_group': transformation_group,
                    'listing_status': row[2],
                    'ipo_date': row[3],
                    'delisting_date': row[4],
                    'exchange': row[5]
                }
                for row in symbols
            ]
            
            cursor = db.connection.cursor()
            inserted_count = 0
            
            for record in records:
                try:
                    cursor.execute(insert_query, record)
                    if cursor.rowcount > 0:
                        inserted_count += 1
                except Exception as e:
                    print(f"⚠️  Error inserting {record['symbol']}: {e}")
                    db.connection.rollback()
                    continue
            
            db.connection.commit()
            cursor.close()
            
            print(f"✅ Initialized {inserted_count:,} new watermark records for '{transformation_group}'")
            
            # Show summary
            self.show_group_summary(transformation_group)
    
    def get_symbols_needing_transformation(
        self,
        transformation_group: str,
        staleness_hours: int = 24,
        limit: Optional[int] = None
    ) -> list[dict[str, Any]]:
        """
        Get symbols that need transformation processing.
        
        Args:
            transformation_group: Name of the transformation group
            staleness_hours: Hours before data is considered stale
            limit: Maximum number of symbols to return
            
        Returns:
            List of symbol data needing processing
        """
        query = """
            SELECT DISTINCT
                w.symbol_id,
                w.symbol,
                w.first_date_processed,
                w.last_date_processed,
                w.last_successful_run,
                w.consecutive_failures,
                p.ipo_date,
                p.delisting_date,
                p.exchange
            FROM transforms.transformation_watermarks w
            JOIN raw.etl_watermarks p ON w.symbol_id = p.symbol_id
            WHERE w.transformation_group = %(transformation_group)s
              AND w.transformation_eligible = true
              AND w.consecutive_failures < 3
              AND (
                  w.last_successful_run IS NULL 
                  OR w.last_successful_run < NOW() - INTERVAL '1 hour' * %(staleness_hours)s
              )
              AND (
                  p.status = 'Active'
                  OR (p.status = 'Delisted' AND (w.last_date_processed IS NULL OR w.last_date_processed < p.delisting_date))
              )
              AND p.asset_type IN ('Stock', 'ETF')
            ORDER BY 
                w.last_successful_run ASC NULLS FIRST,
                w.symbol_id ASC
        """
        
        params = {
            'transformation_group': transformation_group,
            'staleness_hours': staleness_hours
        }
        
        if limit:
            query += " LIMIT %(limit)s"
            params['limit'] = limit
        
        with self.db as db:
            results = db.fetch_query(query, params)
            
            return [
                {
                    'symbol_id': row[0],
                    'symbol': row[1],
                    'first_date_processed': row[2],
                    'last_date_processed': row[3],
                    'last_successful_run': row[4],
                    'consecutive_failures': row[5],
                    'ipo_date': row[6],
                    'delisting_date': row[7],
                    'exchange': row[8]
                }
                for row in results
            ]
    
    def update_watermark(
        self,
        symbol_id: int,
        transformation_group: str,
        success: bool = True,
        first_date: Optional[date] = None,
        last_date: Optional[date] = None
    ):
        """
        Update watermark after transformation processing.
        
        Args:
            symbol_id: Symbol ID
            transformation_group: Name of the transformation group
            success: Whether the transformation was successful
            first_date: First date processed (for new transforms)
            last_date: Last date processed
        """
        if success:
            update_query = """
                UPDATE transforms.transformation_watermarks
                SET 
                    first_date_processed = COALESCE(first_date_processed, %(first_date)s),
                    last_date_processed = COALESCE(%(last_date)s, last_date_processed),
                    last_run_status = 'success',
                    consecutive_failures = 0,
                    last_successful_run = NOW()
                WHERE symbol_id = %(symbol_id)s
                  AND transformation_group = %(transformation_group)s
            """
        else:
            update_query = """
                UPDATE transforms.transformation_watermarks
                SET 
                    last_run_status = 'failed',
                    consecutive_failures = consecutive_failures + 1,
                    transformation_eligible = CASE 
                        WHEN consecutive_failures + 1 >= 3 THEN false 
                        ELSE transformation_eligible 
                    END
                WHERE symbol_id = %(symbol_id)s
                  AND transformation_group = %(transformation_group)s
            """
        
        params = {
            'symbol_id': symbol_id,
            'transformation_group': transformation_group,
            'first_date': first_date,
            'last_date': last_date
        }
        
        with self.db as db:
            db.execute_query(update_query, params)
    
    def show_group_summary(self, transformation_group: str):
        """Show summary statistics for a transformation group."""
        stats_query = """
            SELECT 
                COUNT(*) as total_symbols,
                COUNT(*) FILTER (WHERE transformation_eligible = true) as eligible_symbols,
                COUNT(*) FILTER (WHERE last_successful_run IS NOT NULL) as processed_symbols,
                COUNT(*) FILTER (WHERE last_successful_run IS NULL) as unprocessed_symbols,
                COUNT(*) FILTER (WHERE consecutive_failures > 0) as failed_symbols,
                COUNT(*) FILTER (WHERE consecutive_failures >= 3) as disabled_symbols,
                MIN(last_date_processed) as earliest_date,
                MAX(last_date_processed) as latest_date,
                MIN(last_successful_run) as first_run,
                MAX(last_successful_run) as last_run
            FROM transforms.transformation_watermarks
            WHERE transformation_group = %(transformation_group)s
        """
        
        with self.db as db:
            result = db.fetch_query(stats_query, {'transformation_group': transformation_group})
            
            if result:
                stats = result[0]
                print(f"\n📈 Summary for '{transformation_group}':")
                print(f"   Total symbols: {stats[0]:,}")
                print(f"   Eligible: {stats[1]:,}")
                print(f"   Processed: {stats[2]:,}")
                print(f"   Unprocessed: {stats[3]:,}")
                print(f"   Failed (retry): {stats[4]:,}")
                print(f"   Disabled (3+ failures): {stats[5]:,}")
                if stats[6]:
                    print(f"   Date range: {stats[6]} to {stats[7]}")
                if stats[8]:
                    print(f"   Run range: {stats[8]} to {stats[9]}")
    
    def list_transformation_groups(self):
        """List all transformation groups and their status."""
        query = """
            SELECT 
                transformation_group,
                COUNT(*) as total_symbols,
                COUNT(*) FILTER (WHERE transformation_eligible = true) as eligible_symbols,
                COUNT(*) FILTER (WHERE last_successful_run IS NOT NULL) as processed_symbols,
                MAX(last_successful_run) as last_run
            FROM transforms.transformation_watermarks
            GROUP BY transformation_group
            ORDER BY transformation_group
        """
        
        with self.db as db:
            results = db.fetch_query(query)
            
            print("\n📋 Transformation Groups:")
            print("=" * 100)
            for row in results:
                group, total, eligible, processed, last_run = row
                print(f"{group}:")
                print(f"  Total: {total:,} | Eligible: {eligible:,} | Processed: {processed:,}")
                print(f"  Last run: {last_run if last_run else 'Never'}")
                print()


def main():
    """Main entry point for watermark management."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Manage transformation watermarks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create the watermark table
  python transforms/transformation_watermark_manager.py --create-table
  
  # Initialize a transformation group with all symbols from raw.pg_etl_watermarks
  python transforms/transformation_watermark_manager.py --init-group insider_transactions
  
  # Initialize multiple groups
  python transforms/transformation_watermark_manager.py --init-group time_series_technical_indicators
  
  # List all transformation groups
  python transforms/transformation_watermark_manager.py --list-groups
  
  # Show summary for a specific group
  python transforms/transformation_watermark_manager.py --summary insider_transactions
        """
    )
    
    parser.add_argument(
        "--create-table",
        action="store_true",
        help="Create the transformation_watermarks table"
    )
    
    parser.add_argument(
        "--init-group",
        type=str,
        help="Initialize a transformation group with symbols from raw.pg_etl_watermarks"
    )
    
    parser.add_argument(
        "--list-groups",
        action="store_true",
        help="List all transformation groups and their status"
    )
    
    parser.add_argument(
        "--summary",
        type=str,
        help="Show summary for a specific transformation group"
    )
    
    args = parser.parse_args()
    
    try:
        manager = TransformationWatermarkManager()
        
        if args.create_table:
            manager.create_watermark_table()
        
        if args.init_group:
            manager.initialize_transformation_group(args.init_group)
        
        if args.list_groups:
            manager.list_transformation_groups()
        
        if args.summary:
            manager.show_group_summary(args.summary)
        
        if not any([args.create_table, args.init_group, args.list_groups, args.summary]):
            parser.print_help()
        
    except Exception as e:
        print(f"\n❌ Operation failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
