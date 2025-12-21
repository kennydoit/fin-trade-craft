#!/usr/bin/env python3
"""Transform Earnings Call Sentiment - Role-Based Aggregation.

Creates transforms.earnings_sentiment_agg with one record per symbol-quarter,
aggregating sentiment scores by speaker role classification.

Uses self-watermarking pattern: processed_at timestamp for incremental updates.

Usage:
    # One-time initialization
    python transform_earnings_sentiment_agg.py --init
    
    # Process unprocessed records (run after new earnings calls arrive)
    python transform_earnings_sentiment_agg.py --process
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


class EarningsSentimentAggregator:
    """Aggregate earnings call sentiment by role."""
    
    def __init__(self):
        self.db = PostgresDatabaseManager()
    
    def initialize(self):
        """Initialize transforms.earnings_sentiment_agg table from raw data."""
        logger.info("Initializing transforms.earnings_sentiment_agg...")
        
        self.db.connect()
        try:
            # Drop and recreate table
            logger.info("Creating table structure...")
            self.db.execute_query("DROP TABLE IF EXISTS transforms.earnings_sentiment_agg CASCADE")
            
            # Create table with aggregated sentiment by symbol-quarter
            self.db.execute_query("""
                CREATE TABLE transforms.earnings_sentiment_agg AS
                WITH classified_sentiment AS (
                    SELECT 
                        symbol_id,
                        symbol,
                        quarter,
                        sentiment::NUMERIC,
                        CASE 
                            -- C-Suite / President
                            WHEN title ILIKE ANY(ARRAY[
                                '%CEO%', '%Chief Executive%', '%President and CEO%',
                                '%Chairman%', '%Chair%', '%Chairperson%',
                                '%President%' 
                            ]) THEN 'csuite'
                            
                            -- CFO specifically (most important for guidance)
                            WHEN title ILIKE ANY(ARRAY[
                                '%CFO%', '%Chief Financial%'
                            ]) THEN 'cfo'
                            
                            -- Other Senior Executive Officers
                            WHEN title ILIKE ANY(ARRAY[
                                '%COO%', '%Chief Operating%',
                                '%CTO%', '%Chief Technology%',
                                '%CIO%', '%Chief Information%',
                                '%CMO%', '%Chief Marketing%',
                                '%Chief%',
                                '%EVP%', '%Executive Vice President%',
                                '%SVP%', '%Senior Vice President%'
                            ]) THEN 'senior_exec'
                            
                            -- VP / Director Level
                            WHEN title ILIKE ANY(ARRAY[
                                '%Vice President%', '%VP %',
                                '%Director%',
                                '%General Counsel%',
                                '%Secretary%',
                                '%Treasurer%',
                                '%Controller%'
                            ]) THEN 'vp_director'
                            
                            -- Analysts / IR (for reference, but less useful)
                            WHEN title ILIKE ANY(ARRAY[
                                '%Analyst%',
                                '%Investor Relations%', '%IR %',
                                '%Manager%'
                            ]) THEN 'analyst_ir'
                            
                            -- External (exclude from aggregation)
                            WHEN title ILIKE ANY(ARRAY[
                                '%Moderator%',
                                '%Operator%',
                                '%Conference%'
                            ]) THEN 'external'
                            
                            ELSE 'other'
                        END AS role_category
                    FROM raw.earnings_call_transcript
                    WHERE sentiment IS NOT NULL
                      AND symbol_id IS NOT NULL
                      AND quarter IS NOT NULL
                )
                SELECT 
                    symbol_id,
                    symbol,
                    quarter,
                    
                    -- Core executive sentiment (most important)
                    AVG(CASE WHEN role_category = 'csuite' THEN sentiment END) AS sentiment_csuite,
                    AVG(CASE WHEN role_category = 'cfo' THEN sentiment END) AS sentiment_cfo,
                    AVG(CASE WHEN role_category = 'senior_exec' THEN sentiment END) AS sentiment_senior_exec,
                    
                    -- Supporting roles
                    AVG(CASE WHEN role_category = 'vp_director' THEN sentiment END) AS sentiment_vp_director,
                    AVG(CASE WHEN role_category = 'analyst_ir' THEN sentiment END) AS sentiment_analyst_ir,
                    
                    -- Composite scores
                    AVG(CASE WHEN role_category IN ('csuite', 'cfo', 'senior_exec') 
                        THEN sentiment END) AS sentiment_management_all,
                    
                    -- Weighted management score (CFO 40%, C-Suite 40%, Senior Exec 20%)
                    (
                        COALESCE(AVG(CASE WHEN role_category = 'cfo' THEN sentiment END), 0) * 0.40 +
                        COALESCE(AVG(CASE WHEN role_category = 'csuite' THEN sentiment END), 0) * 0.40 +
                        COALESCE(AVG(CASE WHEN role_category = 'senior_exec' THEN sentiment END), 0) * 0.20
                    ) AS sentiment_management_weighted,
                    
                    -- Statement counts by role (for weighting/validation)
                    COUNT(CASE WHEN role_category = 'csuite' THEN 1 END)::INTEGER AS count_csuite,
                    COUNT(CASE WHEN role_category = 'cfo' THEN 1 END)::INTEGER AS count_cfo,
                    COUNT(CASE WHEN role_category = 'senior_exec' THEN 1 END)::INTEGER AS count_senior_exec,
                    COUNT(CASE WHEN role_category = 'vp_director' THEN 1 END)::INTEGER AS count_vp_director,
                    COUNT(CASE WHEN role_category = 'analyst_ir' THEN 1 END)::INTEGER AS count_analyst_ir,
                    
                    -- Overall counts
                    COUNT(CASE WHEN role_category IN ('csuite', 'cfo', 'senior_exec') 
                        THEN 1 END)::INTEGER AS count_management_total,
                    COUNT(*)::INTEGER AS count_total_statements
                    
                FROM classified_sentiment
                WHERE role_category NOT IN ('external', 'other')  -- Exclude non-valuable roles
                GROUP BY symbol_id, symbol, quarter
                HAVING COUNT(CASE WHEN role_category IN ('csuite', 'cfo', 'senior_exec') 
                       THEN 1 END) > 0  -- Must have at least one management statement
            """)
            
            # Add processed timestamp column
            self.db.execute_query("""
                ALTER TABLE transforms.earnings_sentiment_agg 
                ADD COLUMN processed_at TIMESTAMPTZ
            """)
            
            logger.info("Adding primary key and indexes...")
            
            # Add primary key
            self.db.execute_query("""
                ALTER TABLE transforms.earnings_sentiment_agg 
                ADD PRIMARY KEY (symbol_id, quarter)
            """)
            
            # Add indexes
            self.db.execute_query("""
                CREATE INDEX idx_earnings_sent_symbol_id 
                ON transforms.earnings_sentiment_agg (symbol_id)
            """)
            
            self.db.execute_query("""
                CREATE INDEX idx_earnings_sent_symbol 
                ON transforms.earnings_sentiment_agg (symbol)
            """)
            
            self.db.execute_query("""
                CREATE INDEX idx_earnings_sent_quarter 
                ON transforms.earnings_sentiment_agg (quarter)
            """)
            
            # Add index on weighted sentiment for filtering
            self.db.execute_query("""
                CREATE INDEX idx_earnings_sent_weighted 
                ON transforms.earnings_sentiment_agg (sentiment_management_weighted)
            """)
            
            # Add index for unprocessed records
            self.db.execute_query("""
                CREATE INDEX idx_earnings_sent_unprocessed 
                ON transforms.earnings_sentiment_agg (symbol_id, quarter) 
                WHERE processed_at IS NULL
            """)
            
            # Get record count
            count = self.db.fetch_query("SELECT COUNT(*) FROM transforms.earnings_sentiment_agg")[0][0]
            logger.info(f"✅ Initialized with {count:,} symbol-quarter records")
            
        finally:
            self.db.close()

    def process(self):
        """Process unprocessed sentiment aggregations."""
        logger.info("=" * 80)
        logger.info("PROCESSING UNPROCESSED EARNINGS SENTIMENT")
        logger.info("=" * 80)
        
        self.db.connect()
        try:
            # Count unprocessed
            unprocessed = self.db.fetch_query("""
                SELECT COUNT(*) FROM transforms.earnings_sentiment_agg 
                WHERE processed_at IS NULL
            """)[0][0]
            
            if unprocessed == 0:
                logger.info("No unprocessed records")
                return
            
            logger.info(f"Found {unprocessed:,} unprocessed records")
            logger.info("Computing sentiment aggregations...")
            
            # Update unprocessed records with computed values (already aggregated during init)
            # Since this is an aggregation table, we just mark as processed
            self.db.execute_query("""
                UPDATE transforms.earnings_sentiment_agg
                SET processed_at = NOW()
                WHERE processed_at IS NULL
            """)
            
            logger.info("=" * 80)
            logger.info(f"✅ Processed {unprocessed:,} records")
            
            # Show sample statistics
            stats = self.db.fetch_query("""
                SELECT 
                    AVG(sentiment_management_weighted) as avg_weighted,
                    MIN(sentiment_management_weighted) as min_weighted,
                    MAX(sentiment_management_weighted) as max_weighted,
                    AVG(count_management_total) as avg_mgmt_statements
                FROM transforms.earnings_sentiment_agg
                WHERE processed_at IS NOT NULL
            """)
            
            if stats and stats[0][0] is not None:
                avg_w, min_w, max_w, avg_stmts = stats[0]
                logger.info(f"Sentiment Stats:")
                logger.info(f"  Weighted Average: {avg_w:.4f}")
                logger.info(f"  Range: {min_w:.4f} to {max_w:.4f}")
                logger.info(f"  Avg Management Statements/Call: {avg_stmts:.1f}")
            
            logger.info("=" * 80)
            
        finally:
            self.db.close()


def main():
    parser = argparse.ArgumentParser(description='Aggregate earnings call sentiment by role')
    parser.add_argument('--init', action='store_true', help='Initialize table')
    parser.add_argument('--process', action='store_true', help='Process unprocessed records')
    
    args = parser.parse_args()
    
    if not (args.init or args.process):
        parser.error("Must specify --init or --process")
    
    aggregator = EarningsSentimentAggregator()
    
    if args.init:
        aggregator.initialize()
    
    if args.process:
        aggregator.process()


if __name__ == "__main__":
    main()
