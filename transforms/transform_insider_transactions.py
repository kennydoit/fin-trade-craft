#!/usr/bin/env python3
"""
Transform Insider Transactions from raw schema to transforms schema.

This script reads from raw.insider_transactions and creates a transformed version
in transforms.insider_transactions with tiered classification of insider roles.

Uses self-watermarking pattern: processed_at timestamp for incremental updates.

Usage:
    # Initialize table structure and populate from all raw data
    python transform_insider_transactions.py --init
    
    # Process only new unprocessed transactions
    python transform_insider_transactions.py --process
"""

import argparse
import re
import sys
from pathlib import Path
from typing import Any

import pandas as pd

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager


# ========== Title Normalization & Tiering ==========

# Tier 3: Top executives (CEO, President, Chair)
TIER3_PATTERNS = [
    r"\bCEO\b",
    r"Chief\s+Executive\b",
    r"\bPresident\b",
    r"\bChair\b",
    r"Executive\s+Chair"
]

# Tier 2: C-Suite and Executive VPs
TIER2_PATTERNS = [
    r"\bCFO\b",
    r"\bCOO\b",
    r"\bCTO\b",
    r"\bCIO\b",
    r"\bCMO\b",
    r"\bEVP\b",
    r"\bSVP\b",
    r"Chief\s+\w+\s+Officer",  # any Chief <X> Officer
    r"Executive\s+Vice\s+President",
    r"Senior\s+Vice\s+President"
]

# Tier 1: Directors, VPs, and other officers
TIER1_PATTERNS = [
    r"\bDirector\b",
    r"Vice\s+President\b",
    r"\bSecretary\b",
    r"\bTreasurer\b",
    r"Assistant\s+Secretary",
    r"Associate\s+VP",
    r"\bController\b"
]

# 10% Owner patterns
OWNER_PATTERNS = [
    r"10%\s*Owner",
    r"Ten\s*Percent\s*Owner"
]

# Canonical role labels for standardization
ROLE_LABELS = [
    ("CEO", [r"\bCEO\b", r"Chief\s+Executive\b"]),
    ("President", [r"\bPresident\b"]),
    ("Chair", [r"\bChair\b", r"Executive\s+Chair"]),
    ("CFO", [r"\bCFO\b", r"Chief\s+Financial\b"]),
    ("COO", [r"\bCOO\b", r"Chief\s+Operating\b"]),
    ("CTO", [r"\bCTO\b", r"Chief\s+Technology\b"]),
    ("CIO", [r"\bCIO\b", r"Chief\s+Information\b"]),
    ("EVP", [r"\bEVP\b", r"Executive\s+Vice\s+President"]),
    ("SVP", [r"\bSVP\b", r"Senior\s+Vice\s+President"]),
    ("Director", [r"\bDirector\b"]),
    ("VP", [r"Vice\s+President\b"]),
    ("Secretary", [r"\bSecretary\b"]),
    ("Treasurer", [r"\bTreasurer\b"]),
    ("Controller", [r"\bController\b"]),
]


def regex_any(patterns: list[str], text: str) -> bool:
    """Check if any pattern matches the text (case-insensitive)."""
    return any(re.search(p, text, flags=re.IGNORECASE) for p in patterns)


def find_roles(text: str) -> list[str]:
    """Extract standardized role labels from title text."""
    roles = []
    for label, patterns in ROLE_LABELS:
        if regex_any(patterns, text):
            roles.append(label)
    # Deduplicate while preserving order
    seen = set()
    return [r for r in roles if not (r in seen or seen.add(r))]


def determine_tier(text: str) -> int:
    """Determine seniority tier from title text (0-3, higher = more senior)."""
    tier = 0
    if regex_any(TIER3_PATTERNS, text):
        tier = max(tier, 3)
    if regex_any(TIER2_PATTERNS, text):
        tier = max(tier, 2)
    if regex_any(TIER1_PATTERNS, text):
        tier = max(tier, 1)
    return tier


def is_owner(text: str) -> bool:
    """Check if title indicates 10% ownership."""
    return regex_any(OWNER_PATTERNS, text)


def clean_title(raw_title: str | None) -> str:
    """Clean and normalize title text."""
    if raw_title is None:
        return ""
    # Normalize whitespace
    return re.sub(r"\s+", " ", raw_title.strip())


def normalize_title(raw_title: str | None) -> dict[str, Any]:
    """
    Parse and normalize an executive title into structured components.
    
    Returns:
        dict with keys:
            - executive_title_raw: original title
            - executive_title_clean: normalized title
            - standardized_roles: list of role labels
            - seniority_tier: 0-3 (higher = more senior)
            - is_owner_10pct: boolean (10% owner flag)
    """
    original = "" if raw_title is None else str(raw_title)
    clean = clean_title(original)
    
    roles = find_roles(clean)
    tier = determine_tier(clean)
    owner = is_owner(clean)
    
    return {
        "executive_title_raw": original,
        "executive_title_clean": clean,
        "standardized_roles": roles,
        "seniority_tier": tier,
        "is_owner_10pct": owner
    }


# ========== Database Operations ==========

class InsiderTransactionsTransformer:
    """Transform insider transactions with tiered role classification."""
    
    def __init__(self):
        self.db = PostgresDatabaseManager()
    
    def ensure_transforms_schema(self, db):
        """Ensure the transforms schema exists."""
        create_schema_sql = """
            CREATE SCHEMA IF NOT EXISTS transforms;
        """
        db.execute_query(create_schema_sql)
        print("✓ Ensured transforms schema exists")
    
    def create_transformed_table(self, db):
        """Create transforms.insider_transactions table with tiered structure."""
        create_table_sql = """
            DROP TABLE IF EXISTS transforms.insider_transactions CASCADE;
            
            CREATE TABLE transforms.insider_transactions (
                transaction_id          SERIAL PRIMARY KEY,
                symbol_id               INTEGER NOT NULL,
                symbol                  VARCHAR(20) NOT NULL,
                transaction_date        DATE NOT NULL,
                executive               VARCHAR(255) NOT NULL,
                executive_title_raw     VARCHAR(255),
                executive_title_clean   VARCHAR(255),
                standardized_roles      TEXT[],  -- Array of standardized role labels
                seniority_tier          INTEGER,  -- 0, 1, 2, or 3
                is_owner_10pct          BOOLEAN,
                transaction_type        VARCHAR(100),  -- Transaction type from raw data
                shares                  DECIMAL(20,4),
                share_price             DECIMAL(20,4),
                transaction_value       DECIMAL(20,4),  -- shares * share_price
                created_at              TIMESTAMP DEFAULT NOW(),
                updated_at              TIMESTAMP DEFAULT NOW(),
                processed_at            TIMESTAMPTZ
            );
            
            -- Create indexes
            CREATE INDEX IF NOT EXISTS idx_transforms_insider_symbol_id 
                ON transforms.insider_transactions(symbol_id);
            CREATE INDEX IF NOT EXISTS idx_transforms_insider_symbol 
                ON transforms.insider_transactions(symbol);
            CREATE INDEX IF NOT EXISTS idx_transforms_insider_date 
                ON transforms.insider_transactions(transaction_date);
            CREATE INDEX IF NOT EXISTS idx_transforms_insider_tier 
                ON transforms.insider_transactions(seniority_tier);
            CREATE INDEX IF NOT EXISTS idx_transforms_insider_type 
                ON transforms.insider_transactions(transaction_type);
            CREATE INDEX IF NOT EXISTS idx_transforms_insider_owner 
                ON transforms.insider_transactions(is_owner_10pct);
            CREATE INDEX IF NOT EXISTS idx_transforms_insider_processed 
                ON transforms.insider_transactions(processed_at);
        """
        
        db.execute_query(create_table_sql)
        print("✓ Created transforms.insider_transactions table")
    
    def transform_and_load(self, db, batch_size: int = 10000, symbols: list = None):
        """
        Read raw insider transactions, transform with tiered roles, and load to transforms schema.
        
        Args:
            db: Database connection
            batch_size: Number of records to process at once
            symbols: Optional list of symbol_ids to process (for incremental updates)
        """
        print("🔄 Starting insider transactions transformation...")
        
        # Build query with optional symbol filter
        symbol_filter = ""
        if symbols:
            symbol_ids = [s['symbol_id'] for s in symbols]
            symbol_filter = f"WHERE symbol_id IN ({','.join(map(str, symbol_ids))})"
            print(f"   Processing {len(symbols)} symbols incrementally")
        
        # Read from raw schema
        query = f"""
            SELECT 
                symbol_id,
                symbol,
                transaction_date,
                insider_name,
                insider_title,
                transaction_type,
                shares,
                price_per_share
            FROM raw.insider_transactions
            {symbol_filter}
            ORDER BY symbol_id, transaction_date
        """
        
        print("📊 Reading raw insider transactions...")
        df = pd.read_sql(query, db.connection)
        
        if df.empty:
            print("⚠️  No data found in raw.insider_transactions")
            return
        
        print(f"   Found {len(df):,} transactions")
        
        # Convert symbol_id to integer (it may be read as float/numeric)
        df['symbol_id'] = df['symbol_id'].astype('Int64')
        
        # Handle NaN values and extreme values in numeric columns
        # Replace NaN with None for proper NULL handling
        df['shares'] = df['shares'].replace([float('inf'), float('-inf')], None)
        df['price_per_share'] = df['price_per_share'].replace([float('inf'), float('-inf')], None)
        
        # Check for extremely large values that would cause overflow
        max_allowed = 10**16 - 1  # Max value for DECIMAL(20,4)
        
        # Cap or null out extreme values
        df.loc[df['shares'].abs() > max_allowed, 'shares'] = None
        df.loc[df['price_per_share'].abs() > max_allowed, 'price_per_share'] = None
        
        # Apply title normalization
        print("🔍 Normalizing executive titles...")
        normalized = df['insider_title'].apply(normalize_title)
        
        # Expand normalized data into columns
        df['executive_title_raw'] = normalized.apply(lambda x: x['executive_title_raw'])
        df['executive_title_clean'] = normalized.apply(lambda x: x['executive_title_clean'])
        df['standardized_roles'] = normalized.apply(lambda x: x['standardized_roles'])
        df['seniority_tier'] = normalized.apply(lambda x: x['seniority_tier'])
        df['is_owner_10pct'] = normalized.apply(lambda x: bool(x['is_owner_10pct']))
        
        # Calculate transaction value (handle None values)
        df['transaction_value'] = df.apply(
            lambda row: row['shares'] * row['price_per_share'] 
            if pd.notna(row['shares']) and pd.notna(row['price_per_share']) 
            else None, 
            axis=1
        )
        
        # Cap transaction_value to prevent overflow
        df.loc[df['transaction_value'].notna() & (df['transaction_value'].abs() > max_allowed), 'transaction_value'] = None
        
        # Rename columns to match target schema
        df = df.rename(columns={
            'insider_name': 'executive',
            'price_per_share': 'share_price'
        })
        
        # Drop the original insider_title column (now have _raw and _clean)
        df = df.drop(columns=['insider_title'])
        
        # Insert into transforms table in batches
        print(f"💾 Loading transformed data to transforms.insider_transactions...")
        
        total_inserted = 0
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            
            # Convert to records for insertion
            records = batch.to_dict('records')
            
            # Prepare insert query
            insert_query = """
                INSERT INTO transforms.insider_transactions (
                    symbol_id, symbol, transaction_date, executive,
                    executive_title_raw, executive_title_clean,
                    standardized_roles, seniority_tier, is_owner_10pct,
                    transaction_type,
                    shares, share_price, transaction_value
                ) VALUES (
                    %(symbol_id)s, %(symbol)s, %(transaction_date)s, %(executive)s,
                    %(executive_title_raw)s, %(executive_title_clean)s,
                    %(standardized_roles)s, %(seniority_tier)s, %(is_owner_10pct)s,
                    %(transaction_type)s,
                    %(shares)s, %(share_price)s, %(transaction_value)s
                )
            """
            
            cursor = db.connection.cursor()
            successful_inserts = 0
            for record in records:
                try:
                    cursor.execute(insert_query, record)
                    db.connection.commit()  # Commit each record individually
                    successful_inserts += 1
                except Exception as e:
                    # Rollback the failed transaction
                    db.connection.rollback()
                    print(f"⚠️  Error inserting record: {e}")
                    print(f"   Record: symbol={record.get('symbol')}, date={record.get('transaction_date')}")
                    print(f"   shares={record.get('shares')}, price={record.get('share_price')}, value={record.get('transaction_value')}")
                    # Skip this record and continue
                    continue
            
            cursor.close()
            
            total_inserted += successful_inserts
            print(f"   Inserted {total_inserted:,} / {len(df):,} records")
        
        print(f"✅ Transformation complete: {total_inserted:,} records loaded")
        
        # Print summary statistics
        self.print_summary_stats(db)
        
        return total_inserted
    
    def initialize(self):
        """Initialize transforms.insider_transactions table structure and populate from all raw data."""
        print("Initializing insider transactions transform...")
        
        self.db.connect()
        try:
            # Ensure schema exists
            self.ensure_transforms_schema(self.db)
            
            # Create table
            self.create_transformed_table(self.db)
            
            # Transform and load all data
            self.transform_and_load(self.db)
            
            # Mark all as processed
            print("\nMarking all records as processed...")
            self.db.execute_query("""
                UPDATE transforms.insider_transactions
                SET processed_at = NOW()
                WHERE processed_at IS NULL
            """)
            
            count = self.db.fetch_query("SELECT COUNT(*) FROM transforms.insider_transactions")[0][0]
            print(f"Initialized with {count:,} records (all marked processed)")
            
        finally:
            self.db.close()
    
    def process_unprocessed(self):
        """Process only new unprocessed transactions incrementally."""
        print("Processing new insider transactions...")
        
        self.db.connect()
        try:
            # Find new transactions from raw that don't exist in transforms
            print("Finding new transactions to process...")
            
            unprocessed_count = self.db.fetch_query("""
                SELECT COUNT(*)
                FROM raw.insider_transactions r
                WHERE NOT EXISTS (
                    SELECT 1 
                    FROM transforms.insider_transactions t
                    WHERE t.symbol_id = r.symbol_id 
                      AND t.transaction_date = r.transaction_date
                      AND t.executive = r.insider_name
                      AND t.processed_at IS NOT NULL
                )
            """)[0][0]
            
            if unprocessed_count == 0:
                print("No new transactions to process")
                return
            
            print(f"Found {unprocessed_count:,} new transactions to transform")
            
            # Get new transactions and transform them
            query = """
                SELECT 
                    r.symbol_id,
                    r.symbol,
                    r.transaction_date,
                    r.insider_name,
                    r.insider_title,
                    r.transaction_type,
                    r.shares,
                    r.price_per_share
                FROM raw.insider_transactions r
                WHERE NOT EXISTS (
                    SELECT 1 
                    FROM transforms.insider_transactions t
                    WHERE t.symbol_id = r.symbol_id 
                      AND t.transaction_date = r.transaction_date
                      AND t.executive = r.insider_name
                      AND t.processed_at IS NOT NULL
                )
                ORDER BY r.symbol_id, r.transaction_date
            """
            
            print("Reading new raw insider transactions...")
            df = pd.read_sql(query, self.db.connection)
            
            if df.empty:
                print("No new data found")
                return
            
            print(f"   Found {len(df):,} new transactions")
            
            # Convert symbol_id to integer
            df['symbol_id'] = df['symbol_id'].astype('Int64')
            
            # Handle NaN and extreme values
            df['shares'] = df['shares'].replace([float('inf'), float('-inf')], None)
            df['price_per_share'] = df['price_per_share'].replace([float('inf'), float('-inf')], None)
            
            max_allowed = 10**16 - 1
            df.loc[df['shares'].abs() > max_allowed, 'shares'] = None
            df.loc[df['price_per_share'].abs() > max_allowed, 'price_per_share'] = None
            
            # Apply title normalization
            print("Normalizing executive titles...")
            normalized = df['insider_title'].apply(normalize_title)
            
            df['executive_title_raw'] = normalized.apply(lambda x: x['executive_title_raw'])
            df['executive_title_clean'] = normalized.apply(lambda x: x['executive_title_clean'])
            df['standardized_roles'] = normalized.apply(lambda x: x['standardized_roles'])
            df['seniority_tier'] = normalized.apply(lambda x: x['seniority_tier'])
            df['is_owner_10pct'] = normalized.apply(lambda x: bool(x['is_owner_10pct']))
            
            # Calculate transaction value
            df['transaction_value'] = df.apply(
                lambda row: row['shares'] * row['price_per_share'] 
                if pd.notna(row['shares']) and pd.notna(row['price_per_share']) 
                else None, 
                axis=1
            )
            
            df.loc[df['transaction_value'].notna() & (df['transaction_value'].abs() > max_allowed), 'transaction_value'] = None
            
            # Rename columns
            df = df.rename(columns={
                'insider_name': 'executive',
                'price_per_share': 'share_price'
            })
            
            df = df.drop(columns=['insider_title'])
            
            # Insert new records
            print(f"Loading transformed data...")
            
            records = df.to_dict('records')
            insert_query = """
                INSERT INTO transforms.insider_transactions (
                    symbol_id, symbol, transaction_date, executive,
                    executive_title_raw, executive_title_clean,
                    standardized_roles, seniority_tier, is_owner_10pct,
                    transaction_type,
                    shares, share_price, transaction_value,
                    processed_at
                ) VALUES (
                    %(symbol_id)s, %(symbol)s, %(transaction_date)s, %(executive)s,
                    %(executive_title_raw)s, %(executive_title_clean)s,
                    %(standardized_roles)s, %(seniority_tier)s, %(is_owner_10pct)s,
                    %(transaction_type)s,
                    %(shares)s, %(share_price)s, %(transaction_value)s,
                    NOW()
                )
            """
            
            cursor = self.db.connection.cursor()
            successful_inserts = 0
            for record in records:
                try:
                    cursor.execute(insert_query, record)
                    self.db.connection.commit()
                    successful_inserts += 1
                except Exception as e:
                    self.db.connection.rollback()
                    print(f"Error inserting record: {e}")
                    continue
            
            cursor.close()
            
            print(f"Processed {successful_inserts:,} new transactions")
            
            # Get totals
            total_count = self.db.fetch_query("SELECT COUNT(*) FROM transforms.insider_transactions")[0][0]
            processed_count = self.db.fetch_query("SELECT COUNT(*) FROM transforms.insider_transactions WHERE processed_at IS NOT NULL")[0][0]
            
            print(f"   Total records: {total_count:,} ({processed_count:,} processed)")
            
        finally:
            self.db.close()
    
    def print_summary_stats(self, db):
        """Print summary statistics about the transformed data."""
        stats_query = """
            SELECT 
                seniority_tier,
                is_owner_10pct,
                transaction_type,
                COUNT(*) as transaction_count,
                COUNT(DISTINCT symbol_id) as symbol_count,
                SUM(transaction_value) as total_value,
                AVG(transaction_value) as avg_value
            FROM transforms.insider_transactions
            GROUP BY seniority_tier, is_owner_10pct, transaction_type
            ORDER BY seniority_tier DESC, is_owner_10pct DESC, transaction_type
        """
        
        print("\n📈 Summary Statistics by Tier:")
        print("=" * 100)
        
        results = db.fetch_query(stats_query)
        
        tier_labels = {0: "Unclassified", 1: "Tier 1", 2: "Tier 2", 3: "Tier 3"}
        
        for row in results:
            tier, is_owner, trans_type, count, symbols, total_val, avg_val = row
            tier_label = tier_labels.get(tier, f"Tier {tier}")
            owner_label = " (10% Owner)" if is_owner else ""
            
            print(f"{tier_label}{owner_label} - {trans_type}:")
            print(f"  Transactions: {count:,}")
            print(f"  Symbols: {symbols:,}")
            print(f"  Total Value: ${total_val:,.2f}" if total_val else "  Total Value: N/A")
            print(f"  Avg Value: ${avg_val:,.2f}" if avg_val else "  Avg Value: N/A")
            print()


def main():
    """Main entry point for the transformation script."""
    parser = argparse.ArgumentParser(
        description="Transform insider transactions with tiered role classification"
    )
    parser.add_argument(
        "--init",
        action='store_true',
        help="Initialize table structure and populate from all data"
    )
    parser.add_argument(
        "--process",
        action='store_true',
        help="Process only new unprocessed transactions"
    )
    
    args = parser.parse_args()
    
    if not args.init and not args.process:
        parser.error("Must specify --init or --process")
    
    try:
        transformer = InsiderTransactionsTransformer()
        
        if args.init:
            transformer.initialize()
        elif args.process:
            transformer.process_unprocessed()
        
    except Exception as e:
        print(f"\nTransformation failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()


# ============================================================================
# USAGE EXAMPLES
# ============================================================================
#
# 1. Initialize table structure and populate from all raw data:
#    python transforms/transform_insider_transactions.py --init
#
# 2. Process only new unprocessed transactions:
#    python transforms/transform_insider_transactions.py --process
#
# ============================================================================
# OUTPUT TABLE STRUCTURE
# ============================================================================
#
# transforms.insider_transactions columns:
#   - transaction_id: Auto-incrementing primary key
#   - symbol_id: Reference to company
#   - symbol: Stock ticker
#   - transaction_date: Date of the transaction
#   - executive: Name of the insider
#   - executive_title_raw: Original title from raw data
#   - executive_title_clean: Normalized title
#   - standardized_roles: Array of role labels (e.g., ['CEO', 'Director'])
#   - seniority_tier: 0-3 classification (3=highest, 0=unclassified)
#   - is_owner_10pct: Boolean flag for 10% owners
#   - security_type: Type of security transacted
#   - acquisition_or_disposal: 'A' or 'D'
#   - shares: Number of shares
#   - share_price: Price per share
#   - transaction_value: Calculated (shares * share_price)
#   - api_response_status: Status from raw data
#   - created_at, updated_at: Timestamps
#
# ============================================================================
# TIER CLASSIFICATION
# ============================================================================
#
# Tier 3 (Highest): CEO, President, Chairman
# Tier 2: CFO, COO, CTO, EVP, SVP, other C-suite
# Tier 1: Directors, VPs, Secretary, Treasurer
# Tier 0: Unclassified/ambiguous titles
#
# 10% Owner: Flagged separately from tier classification
#
# ============================================================================
