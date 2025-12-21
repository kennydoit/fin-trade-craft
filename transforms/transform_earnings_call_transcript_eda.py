#!/usr/bin/env python3
"""EDA for Earnings Call Transcript Titles - Classification Analysis.

Analyzes the distribution of speaker titles in earnings call transcripts
and classifies them using patterns similar to insider transactions.

Usage:
    python transform_earnings_call_transcript_eda.py
"""

import sys
import logging
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


class EarningsCallTitleEDA:
    """Analyze and classify earnings call transcript speaker titles."""
    
    def __init__(self):
        self.db = PostgresDatabaseManager()
    
    def analyze_titles(self):
        """Analyze and classify earnings call transcript titles."""
        logger.info("=" * 80)
        logger.info("EARNINGS CALL TRANSCRIPT TITLE ANALYSIS")
        logger.info("=" * 80)
        
        self.db.connect()
        try:
            logger.info("Classifying titles using SQL...")
            
            # Use SQL to classify and aggregate titles
            results = self.db.fetch_query("""
                WITH classified_titles AS (
                    SELECT 
                        title,
                        CASE 
                            -- Tier 3: C-Suite / Top Executives
                            WHEN title ILIKE ANY(ARRAY[
                                '%CEO%', '%Chief Executive%', '%President and CEO%',
                                '%Chairman%', '%Chair%', '%Chairperson%',
                                '%President%' 
                            ]) THEN 'C-Suite / President'
                            
                            -- Tier 2: Senior Executive Officers
                            WHEN title ILIKE ANY(ARRAY[
                                '%CFO%', '%Chief Financial%',
                                '%COO%', '%Chief Operating%',
                                '%CTO%', '%Chief Technology%',
                                '%CIO%', '%Chief Information%',
                                '%CMO%', '%Chief Marketing%',
                                '%Chief%',
                                '%EVP%', '%Executive Vice President%',
                                '%SVP%', '%Senior Vice President%'
                            ]) THEN 'Senior Executive Officer'
                            
                            -- Tier 1: VP / Director Level
                            WHEN title ILIKE ANY(ARRAY[
                                '%Vice President%', '%VP %',
                                '%Director%',
                                '%General Counsel%',
                                '%Secretary%',
                                '%Treasurer%',
                                '%Controller%'
                            ]) THEN 'VP / Director'
                            
                            -- Analysts / IR
                            WHEN title ILIKE ANY(ARRAY[
                                '%Analyst%',
                                '%Investor Relations%', '%IR %',
                                '%Manager%'
                            ]) THEN 'Analyst / IR / Manager'
                            
                            -- External / Other
                            WHEN title ILIKE ANY(ARRAY[
                                '%Moderator%',
                                '%Operator%',
                                '%Conference%'
                            ]) THEN 'External (Moderator/Operator)'
                            
                            -- Unclassified
                            ELSE 'Needs Classification'
                        END AS aggregate_title
                    FROM raw.earnings_call_transcript
                    WHERE title IS NOT NULL AND title != ''
                )
                SELECT 
                    aggregate_title,
                    COUNT(*) as count,
                    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
                FROM classified_titles
                GROUP BY aggregate_title
                ORDER BY count DESC
            """)
            
            logger.info("=" * 80)
            logger.info("CLASSIFICATION RESULTS")
            logger.info("=" * 80)
            
            total = sum(r[1] for r in results)
            
            print(f"\n{'Aggregate Title':<40} {'Count':>15} {'Percentage':>12}")
            print("=" * 70)
            
            for row in results:
                aggregate_title, count, percentage = row
                print(f"{aggregate_title:<40} {count:>15,} {percentage:>11.2f}%")
            
            print("=" * 70)
            print(f"{'TOTAL':<40} {total:>15,} {100.0:>11.2f}%")
            print()
            
            # Get some sample unclassified titles
            logger.info("Fetching sample unclassified titles...")
            unclassified_samples = self.db.fetch_query("""
                SELECT DISTINCT title, COUNT(*) as cnt
                FROM raw.earnings_call_transcript
                WHERE title IS NOT NULL 
                  AND title != ''
                  AND NOT (
                      title ILIKE ANY(ARRAY[
                          '%CEO%', '%Chief Executive%', '%President and CEO%',
                          '%Chairman%', '%Chair%', '%Chairperson%', '%President%',
                          '%CFO%', '%Chief Financial%', '%COO%', '%Chief Operating%',
                          '%CTO%', '%Chief Technology%', '%CIO%', '%Chief Information%',
                          '%CMO%', '%Chief Marketing%', '%Chief%',
                          '%EVP%', '%Executive Vice President%',
                          '%SVP%', '%Senior Vice President%',
                          '%Vice President%', '%VP %', '%Director%',
                          '%General Counsel%', '%Secretary%', '%Treasurer%', '%Controller%',
                          '%Analyst%', '%Investor Relations%', '%IR %', '%Manager%',
                          '%Moderator%', '%Operator%', '%Conference%'
                      ])
                  )
                GROUP BY title
                ORDER BY cnt DESC
                LIMIT 30
            """)
            
            if unclassified_samples:
                logger.info("=" * 80)
                logger.info("SAMPLE UNCLASSIFIED TITLES (Top 30 by frequency)")
                logger.info("=" * 80)
                print(f"\n{'Title':<60} {'Count':>10}")
                print("=" * 70)
                for title, cnt in unclassified_samples:
                    # Truncate long titles
                    display_title = title[:57] + "..." if len(title) > 60 else title
                    print(f"{display_title:<60} {cnt:>10,}")
                print()
            
            logger.info("=" * 80)
            logger.info("✅ Analysis complete!")
            logger.info("=" * 80)
            
        finally:
            self.db.close()


def main():
    eda = EarningsCallTitleEDA()
    eda.analyze_titles()


if __name__ == "__main__":
    main()
