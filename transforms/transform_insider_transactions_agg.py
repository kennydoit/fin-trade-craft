#!/usr/bin/env python3
"""Aggregate Insider Transactions for ML modeling.

Creates transforms.insider_transactions_agg table with aggregated metrics
by symbol and transaction_date, ready for joining to modeling datasets.

Uses self-watermarking pattern: processed_at timestamp for incremental updates.

Usage:
    # Initialize table structure and populate from all raw data
    python transform_insider_transactions_agg.py --init
    
    # Process only new unprocessed transactions
    python transform_insider_transactions_agg.py --process
"""

import sys
import logging
import argparse
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class InsiderTransactionsAggregator:
    """Aggregate insider transactions using SQL."""
    
    def __init__(self):
        self.db = PostgresDatabaseManager()
    
    def initialize(self):
        """Initialize transforms.insider_transactions_agg table structure and populate."""
        logger.info("Initializing transforms.insider_transactions_agg...")
        
        self.db.connect()
        try:
            # Drop existing table
            self.db.execute_query("DROP TABLE IF EXISTS transforms.insider_transactions_agg")
            
            logger.info("Creating aggregated insider transactions table via SQL...")
            
            # Create and populate - using simpler approach with DOUBLE PRECISION for values
            self.db.execute_query("""
                CREATE TABLE transforms.insider_transactions_agg AS
                WITH classified AS (
                    SELECT 
                        symbol_id,
                        symbol,
                        transaction_date,
                        transaction_type,
                        shares::BIGINT,
                        price_per_share,
                        CASE 
                            WHEN insider_title ILIKE ANY(ARRAY['%CEO%', '%President%', '%Chair%']) THEN 3
                            WHEN insider_title ILIKE ANY(ARRAY['%CFO%', '%COO%', '%CTO%', '%CIO%', '%CMO%', '%EVP%', '%SVP%', '%Chief%', '%Executive Vice%']) THEN 2
                            WHEN insider_title ILIKE ANY(ARRAY['%Director%', '%Vice President%', '%Secretary%', '%Treasurer%', '%Controller%', '%VP%']) THEN 1
                            ELSE 0
                        END AS tier,
                        (insider_title ILIKE '%10%%Owner%' OR insider_title ILIKE '%Ten Percent%Owner%') AS is_owner_10pct,
                        (shares * price_per_share)::DOUBLE PRECISION AS transaction_value
                    FROM raw.insider_transactions
                    WHERE symbol IS NOT NULL
                      AND transaction_date IS NOT NULL
                      AND shares IS NOT NULL
                )
                SELECT 
                    symbol_id,
                    symbol,
                    transaction_date,
                    
                    -- Disposition (D) - Share counts
                    COALESCE(SUM(CASE WHEN transaction_type = 'D' THEN shares ELSE 0 END), 0)::BIGINT AS total_shares_d,
                    COALESCE(SUM(CASE WHEN transaction_type = 'D' AND tier = 0 THEN shares ELSE 0 END), 0)::BIGINT AS total_shares_d_tier_0,
                    COALESCE(SUM(CASE WHEN transaction_type = 'D' AND tier = 1 THEN shares ELSE 0 END), 0)::BIGINT AS total_shares_d_tier_1,
                    COALESCE(SUM(CASE WHEN transaction_type = 'D' AND tier = 2 THEN shares ELSE 0 END), 0)::BIGINT AS total_shares_d_tier_2,
                    COALESCE(SUM(CASE WHEN transaction_type = 'D' AND tier = 3 THEN shares ELSE 0 END), 0)::BIGINT AS total_shares_d_tier_3,
                    COALESCE(SUM(CASE WHEN transaction_type = 'D' AND is_owner_10pct THEN shares ELSE 0 END), 0)::BIGINT AS total_shares_d_10pct_owner,
                    
                    -- Disposition (D) - Transaction values
                    COALESCE(SUM(CASE WHEN transaction_type = 'D' THEN transaction_value ELSE 0 END), 0)::DOUBLE PRECISION AS total_value_d,
                    COALESCE(SUM(CASE WHEN transaction_type = 'D' AND tier = 0 THEN transaction_value ELSE 0 END), 0)::DOUBLE PRECISION AS total_value_d_tier_0,
                    COALESCE(SUM(CASE WHEN transaction_type = 'D' AND tier = 1 THEN transaction_value ELSE 0 END), 0)::DOUBLE PRECISION AS total_value_d_tier_1,
                    COALESCE(SUM(CASE WHEN transaction_type = 'D' AND tier = 2 THEN transaction_value ELSE 0 END), 0)::DOUBLE PRECISION AS total_value_d_tier_2,
                    COALESCE(SUM(CASE WHEN transaction_type = 'D' AND tier = 3 THEN transaction_value ELSE 0 END), 0)::DOUBLE PRECISION AS total_value_d_tier_3,
                    COALESCE(SUM(CASE WHEN transaction_type = 'D' AND is_owner_10pct THEN transaction_value ELSE 0 END), 0)::DOUBLE PRECISION AS total_value_d_10pct_owner,
                    
                    -- Acquisition (A) - Share counts
                    COALESCE(SUM(CASE WHEN transaction_type = 'A' THEN shares ELSE 0 END), 0)::BIGINT AS total_shares_a,
                    COALESCE(SUM(CASE WHEN transaction_type = 'A' AND tier = 0 THEN shares ELSE 0 END), 0)::BIGINT AS total_shares_a_tier_0,
                    COALESCE(SUM(CASE WHEN transaction_type = 'A' AND tier = 1 THEN shares ELSE 0 END), 0)::BIGINT AS total_shares_a_tier_1,
                    COALESCE(SUM(CASE WHEN transaction_type = 'A' AND tier = 2 THEN shares ELSE 0 END), 0)::BIGINT AS total_shares_a_tier_2,
                    COALESCE(SUM(CASE WHEN transaction_type = 'A' AND tier = 3 THEN shares ELSE 0 END), 0)::BIGINT AS total_shares_a_tier_3,
                    COALESCE(SUM(CASE WHEN transaction_type = 'A' AND is_owner_10pct THEN shares ELSE 0 END), 0)::BIGINT AS total_shares_a_10pct_owner,
                    
                    -- Acquisition (A) - Transaction values
                    COALESCE(SUM(CASE WHEN transaction_type = 'A' THEN transaction_value ELSE 0 END), 0)::DOUBLE PRECISION AS total_value_a,
                    COALESCE(SUM(CASE WHEN transaction_type = 'A' AND tier = 0 THEN transaction_value ELSE 0 END), 0)::DOUBLE PRECISION AS total_value_a_tier_0,
                    COALESCE(SUM(CASE WHEN transaction_type = 'A' AND tier = 1 THEN transaction_value ELSE 0 END), 0)::DOUBLE PRECISION AS total_value_a_tier_1,
                    COALESCE(SUM(CASE WHEN transaction_type = 'A' AND tier = 2 THEN transaction_value ELSE 0 END), 0)::DOUBLE PRECISION AS total_value_a_tier_2,
                    COALESCE(SUM(CASE WHEN transaction_type = 'A' AND tier = 3 THEN transaction_value ELSE 0 END), 0)::DOUBLE PRECISION AS total_value_a_tier_3,
                    COALESCE(SUM(CASE WHEN transaction_type = 'A' AND is_owner_10pct THEN transaction_value ELSE 0 END), 0)::DOUBLE PRECISION AS total_value_a_10pct_owner,
                    
                    -- Average prices
                    AVG(CASE WHEN transaction_type = 'D' THEN price_per_share ELSE NULL END) AS avg_price_d,
                    AVG(CASE WHEN transaction_type = 'A' THEN price_per_share ELSE NULL END) AS avg_price_a,
                    
                    -- Transaction counts
                    COALESCE(COUNT(CASE WHEN transaction_type = 'D' THEN 1 END), 0)::BIGINT AS transaction_count_d,
                    COALESCE(COUNT(CASE WHEN transaction_type = 'A' THEN 1 END), 0)::BIGINT AS transaction_count_a,
                    COALESCE(COUNT(CASE WHEN transaction_type = 'D' AND is_owner_10pct THEN 1 END), 0)::BIGINT AS transaction_count_d_10pct,
                    COALESCE(COUNT(CASE WHEN transaction_type = 'A' AND is_owner_10pct THEN 1 END), 0)::BIGINT AS transaction_count_a_10pct
                    
                FROM classified
                GROUP BY symbol_id, symbol, transaction_date
            """)
            
            logger.info("Adding primary key and indexes...")
            
            # Add processed_at column for watermarking
            self.db.execute_query("""
                ALTER TABLE transforms.insider_transactions_agg
                ADD COLUMN processed_at TIMESTAMPTZ DEFAULT NOW()
            """)
            
            # Add primary key
            self.db.execute_query("""
                ALTER TABLE transforms.insider_transactions_agg 
                ADD PRIMARY KEY (symbol_id, transaction_date)
            """)
            
            # Add indexes
            self.db.execute_query("""
                CREATE INDEX idx_insider_agg_symbol_id 
                ON transforms.insider_transactions_agg (symbol_id)
            """)
            
            self.db.execute_query("""
                CREATE INDEX idx_insider_agg_symbol 
                ON transforms.insider_transactions_agg (symbol)
            """)
            
            self.db.execute_query("""
                CREATE INDEX idx_insider_agg_date 
                ON transforms.insider_transactions_agg (transaction_date DESC)
            """)
            
            self.db.execute_query("""
                CREATE INDEX idx_insider_agg_processed 
                ON transforms.insider_transactions_agg (processed_at)
            """)
            
            # Get record count
            count = self.db.fetch_query("SELECT COUNT(*) FROM transforms.insider_transactions_agg")[0][0]
            logger.info(f"✅ Created transforms.insider_transactions_agg with {count:,} records")
            logger.info("   Ready for joining to modeling datasets!")
            
        finally:
            self.db.close()
    
    def process_unprocessed(self):
        """Process only new unprocessed transactions incrementally."""
        logger.info("Processing unprocessed insider transactions...")
        
        self.db.connect()
        try:
            # Find new symbol-dates from raw that don't exist in transforms or need updates
            logger.info("Finding new transactions to process...")
            
            unprocessed_count = self.db.fetch_query("""
                SELECT COUNT(DISTINCT (symbol_id, transaction_date))
                FROM raw.insider_transactions r
                WHERE NOT EXISTS (
                    SELECT 1 
                    FROM transforms.insider_transactions_agg t
                    WHERE t.symbol_id = r.symbol_id 
                      AND t.transaction_date = r.transaction_date
                      AND t.processed_at IS NOT NULL
                )
                AND r.symbol IS NOT NULL
                AND r.transaction_date IS NOT NULL
                AND r.shares IS NOT NULL
            """)[0][0]
            
            if unprocessed_count == 0:
                logger.info("No new transactions to process")
                return
            
            logger.info(f"Found {unprocessed_count:,} new symbol-dates to aggregate")
            
            # Aggregate new transactions and upsert
            logger.info("Aggregating new transactions...")
            self.db.execute_query("""
                WITH new_symbol_dates AS (
                    SELECT DISTINCT symbol_id, transaction_date
                    FROM raw.insider_transactions r
                    WHERE NOT EXISTS (
                        SELECT 1 
                        FROM transforms.insider_transactions_agg t
                        WHERE t.symbol_id = r.symbol_id 
                          AND t.transaction_date = r.transaction_date
                          AND t.processed_at IS NOT NULL
                    )
                    AND r.symbol IS NOT NULL
                    AND r.transaction_date IS NOT NULL
                    AND r.shares IS NOT NULL
                ),
                classified AS (
                    SELECT 
                        r.symbol_id,
                        r.symbol,
                        r.transaction_date,
                        r.transaction_type,
                        r.shares::BIGINT,
                        r.price_per_share,
                        CASE 
                            WHEN r.insider_title ILIKE ANY(ARRAY['%CEO%', '%President%', '%Chair%']) THEN 3
                            WHEN r.insider_title ILIKE ANY(ARRAY['%CFO%', '%COO%', '%CTO%', '%CIO%', '%CMO%', '%EVP%', '%SVP%', '%Chief%', '%Executive Vice%']) THEN 2
                            WHEN r.insider_title ILIKE ANY(ARRAY['%Director%', '%Vice President%', '%Secretary%', '%Treasurer%', '%Controller%', '%VP%']) THEN 1
                            ELSE 0
                        END AS tier,
                        (r.insider_title ILIKE '%10%%Owner%' OR r.insider_title ILIKE '%Ten Percent%Owner%') AS is_owner_10pct,
                        (r.shares * r.price_per_share)::DOUBLE PRECISION AS transaction_value
                    FROM raw.insider_transactions r
                    INNER JOIN new_symbol_dates nsd
                        ON r.symbol_id = nsd.symbol_id
                        AND r.transaction_date = nsd.transaction_date
                    WHERE r.symbol IS NOT NULL
                      AND r.transaction_date IS NOT NULL
                      AND r.shares IS NOT NULL
                ),
                aggregated AS (
                    SELECT 
                        symbol_id,
                        symbol,
                        transaction_date,
                        
                        -- Disposition (D) - Share counts
                        COALESCE(SUM(CASE WHEN transaction_type = 'D' THEN shares ELSE 0 END), 0)::BIGINT AS total_shares_d,
                        COALESCE(SUM(CASE WHEN transaction_type = 'D' AND tier = 0 THEN shares ELSE 0 END), 0)::BIGINT AS total_shares_d_tier_0,
                        COALESCE(SUM(CASE WHEN transaction_type = 'D' AND tier = 1 THEN shares ELSE 0 END), 0)::BIGINT AS total_shares_d_tier_1,
                        COALESCE(SUM(CASE WHEN transaction_type = 'D' AND tier = 2 THEN shares ELSE 0 END), 0)::BIGINT AS total_shares_d_tier_2,
                        COALESCE(SUM(CASE WHEN transaction_type = 'D' AND tier = 3 THEN shares ELSE 0 END), 0)::BIGINT AS total_shares_d_tier_3,
                        COALESCE(SUM(CASE WHEN transaction_type = 'D' AND is_owner_10pct THEN shares ELSE 0 END), 0)::BIGINT AS total_shares_d_10pct_owner,
                        
                        -- Disposition (D) - Transaction values
                        COALESCE(SUM(CASE WHEN transaction_type = 'D' THEN transaction_value ELSE 0 END), 0)::DOUBLE PRECISION AS total_value_d,
                        COALESCE(SUM(CASE WHEN transaction_type = 'D' AND tier = 0 THEN transaction_value ELSE 0 END), 0)::DOUBLE PRECISION AS total_value_d_tier_0,
                        COALESCE(SUM(CASE WHEN transaction_type = 'D' AND tier = 1 THEN transaction_value ELSE 0 END), 0)::DOUBLE PRECISION AS total_value_d_tier_1,
                        COALESCE(SUM(CASE WHEN transaction_type = 'D' AND tier = 2 THEN transaction_value ELSE 0 END), 0)::DOUBLE PRECISION AS total_value_d_tier_2,
                        COALESCE(SUM(CASE WHEN transaction_type = 'D' AND tier = 3 THEN transaction_value ELSE 0 END), 0)::DOUBLE PRECISION AS total_value_d_tier_3,
                        COALESCE(SUM(CASE WHEN transaction_type = 'D' AND is_owner_10pct THEN transaction_value ELSE 0 END), 0)::DOUBLE PRECISION AS total_value_d_10pct_owner,
                        
                        -- Acquisition (A) - Share counts
                        COALESCE(SUM(CASE WHEN transaction_type = 'A' THEN shares ELSE 0 END), 0)::BIGINT AS total_shares_a,
                        COALESCE(SUM(CASE WHEN transaction_type = 'A' AND tier = 0 THEN shares ELSE 0 END), 0)::BIGINT AS total_shares_a_tier_0,
                        COALESCE(SUM(CASE WHEN transaction_type = 'A' AND tier = 1 THEN shares ELSE 0 END), 0)::BIGINT AS total_shares_a_tier_1,
                        COALESCE(SUM(CASE WHEN transaction_type = 'A' AND tier = 2 THEN shares ELSE 0 END), 0)::BIGINT AS total_shares_a_tier_2,
                        COALESCE(SUM(CASE WHEN transaction_type = 'A' AND tier = 3 THEN shares ELSE 0 END), 0)::BIGINT AS total_shares_a_tier_3,
                        COALESCE(SUM(CASE WHEN transaction_type = 'A' AND is_owner_10pct THEN shares ELSE 0 END), 0)::BIGINT AS total_shares_a_10pct_owner,
                        
                        -- Acquisition (A) - Transaction values
                        COALESCE(SUM(CASE WHEN transaction_type = 'A' THEN transaction_value ELSE 0 END), 0)::DOUBLE PRECISION AS total_value_a,
                        COALESCE(SUM(CASE WHEN transaction_type = 'A' AND tier = 0 THEN transaction_value ELSE 0 END), 0)::DOUBLE PRECISION AS total_value_a_tier_0,
                        COALESCE(SUM(CASE WHEN transaction_type = 'A' AND tier = 1 THEN transaction_value ELSE 0 END), 0)::DOUBLE PRECISION AS total_value_a_tier_1,
                        COALESCE(SUM(CASE WHEN transaction_type = 'A' AND tier = 2 THEN transaction_value ELSE 0 END), 0)::DOUBLE PRECISION AS total_value_a_tier_2,
                        COALESCE(SUM(CASE WHEN transaction_type = 'A' AND tier = 3 THEN transaction_value ELSE 0 END), 0)::DOUBLE PRECISION AS total_value_a_tier_3,
                        COALESCE(SUM(CASE WHEN transaction_type = 'A' AND is_owner_10pct THEN transaction_value ELSE 0 END), 0)::DOUBLE PRECISION AS total_value_a_10pct_owner,
                        
                        -- Average prices
                        AVG(CASE WHEN transaction_type = 'D' THEN price_per_share ELSE NULL END) AS avg_price_d,
                        AVG(CASE WHEN transaction_type = 'A' THEN price_per_share ELSE NULL END) AS avg_price_a,
                        
                        -- Transaction counts
                        COALESCE(COUNT(CASE WHEN transaction_type = 'D' THEN 1 END), 0)::BIGINT AS transaction_count_d,
                        COALESCE(COUNT(CASE WHEN transaction_type = 'A' THEN 1 END), 0)::BIGINT AS transaction_count_a,
                        COALESCE(COUNT(CASE WHEN transaction_type = 'D' AND is_owner_10pct THEN 1 END), 0)::BIGINT AS transaction_count_d_10pct,
                        COALESCE(COUNT(CASE WHEN transaction_type = 'A' AND is_owner_10pct THEN 1 END), 0)::BIGINT AS transaction_count_a_10pct
                        
                    FROM classified
                    GROUP BY symbol_id, symbol, transaction_date
                )
                INSERT INTO transforms.insider_transactions_agg
                SELECT *, NOW() as processed_at
                FROM aggregated
                ON CONFLICT (symbol_id, transaction_date) 
                DO UPDATE SET
                    symbol = EXCLUDED.symbol,
                    total_shares_d = EXCLUDED.total_shares_d,
                    total_shares_d_tier_0 = EXCLUDED.total_shares_d_tier_0,
                    total_shares_d_tier_1 = EXCLUDED.total_shares_d_tier_1,
                    total_shares_d_tier_2 = EXCLUDED.total_shares_d_tier_2,
                    total_shares_d_tier_3 = EXCLUDED.total_shares_d_tier_3,
                    total_shares_d_10pct_owner = EXCLUDED.total_shares_d_10pct_owner,
                    total_value_d = EXCLUDED.total_value_d,
                    total_value_d_tier_0 = EXCLUDED.total_value_d_tier_0,
                    total_value_d_tier_1 = EXCLUDED.total_value_d_tier_1,
                    total_value_d_tier_2 = EXCLUDED.total_value_d_tier_2,
                    total_value_d_tier_3 = EXCLUDED.total_value_d_tier_3,
                    total_value_d_10pct_owner = EXCLUDED.total_value_d_10pct_owner,
                    total_shares_a = EXCLUDED.total_shares_a,
                    total_shares_a_tier_0 = EXCLUDED.total_shares_a_tier_0,
                    total_shares_a_tier_1 = EXCLUDED.total_shares_a_tier_1,
                    total_shares_a_tier_2 = EXCLUDED.total_shares_a_tier_2,
                    total_shares_a_tier_3 = EXCLUDED.total_shares_a_tier_3,
                    total_shares_a_10pct_owner = EXCLUDED.total_shares_a_10pct_owner,
                    total_value_a = EXCLUDED.total_value_a,
                    total_value_a_tier_0 = EXCLUDED.total_value_a_tier_0,
                    total_value_a_tier_1 = EXCLUDED.total_value_a_tier_1,
                    total_value_a_tier_2 = EXCLUDED.total_value_a_tier_2,
                    total_value_a_tier_3 = EXCLUDED.total_value_a_tier_3,
                    total_value_a_10pct_owner = EXCLUDED.total_value_a_10pct_owner,
                    avg_price_d = EXCLUDED.avg_price_d,
                    avg_price_a = EXCLUDED.avg_price_a,
                    transaction_count_d = EXCLUDED.transaction_count_d,
                    transaction_count_a = EXCLUDED.transaction_count_a,
                    transaction_count_d_10pct = EXCLUDED.transaction_count_d_10pct,
                    transaction_count_a_10pct = EXCLUDED.transaction_count_a_10pct,
                    processed_at = NOW()
            """)
            
            # Get counts
            total_count = self.db.fetch_query("SELECT COUNT(*) FROM transforms.insider_transactions_agg")[0][0]
            processed_count = self.db.fetch_query("SELECT COUNT(*) FROM transforms.insider_transactions_agg WHERE processed_at IS NOT NULL")[0][0]
            
            logger.info(f"✅ Processed {unprocessed_count:,} new symbol-dates")
            logger.info(f"   Total records: {total_count:,} ({processed_count:,} processed)")
            
        finally:
            self.db.close()


def main():
    parser = argparse.ArgumentParser(description='Aggregate insider transactions for ML modeling')
    parser.add_argument('--init', action='store_true', help='Initialize table structure and populate from all data')
    parser.add_argument('--process', action='store_true', help='Process only new unprocessed transactions')
    
    args = parser.parse_args()
    
    if not args.init and not args.process:
        parser.error("Must specify --init or --process")
    
    aggregator = InsiderTransactionsAggregator()
    
    if args.init:
        aggregator.initialize()
    elif args.process:
        aggregator.process_unprocessed()


if __name__ == "__main__":
    main()
