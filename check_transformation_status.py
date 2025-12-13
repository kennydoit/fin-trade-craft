"""Check transformation progress and watermark status."""
from db.postgres_database_manager import PostgresDatabaseManager
from dotenv import load_dotenv
import sys

load_dotenv()

def main():
    transformation_group = sys.argv[1] if len(sys.argv) > 1 else 'insider_transactions'
    
    with PostgresDatabaseManager() as db:
        # Overall summary
        summary = db.fetch_query("""
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE last_successful_run IS NOT NULL) as completed,
                COUNT(*) FILTER (WHERE last_successful_run IS NULL) as pending,
                COUNT(*) FILTER (WHERE last_run_status = 'failed') as failed,
                COUNT(*) FILTER (WHERE transformation_eligible = false) as disabled
            FROM transforms.transformation_watermarks
            WHERE transformation_group = %(group)s
        """, {'group': transformation_group})[0]
        
        total, completed, pending, failed, disabled = summary
        pct_complete = (completed / total * 100) if total > 0 else 0
        
        print(f"\n📊 Transformation Status: {transformation_group}")
        print("=" * 70)
        print(f"Total Symbols:        {total:>8,}")
        print(f"Completed:            {completed:>8,}  ({pct_complete:>5.1f}%)")
        print(f"Pending:              {pending:>8,}  ({(pending/total*100):>5.1f}%)")
        print(f"Failed (retry):       {failed:>8,}")
        print(f"Disabled (3+ fails):  {disabled:>8,}")
        
        # Recently completed
        recent = db.fetch_query("""
            SELECT symbol, last_successful_run
            FROM transforms.transformation_watermarks
            WHERE transformation_group = %(group)s
              AND last_successful_run IS NOT NULL
            ORDER BY last_successful_run DESC
            LIMIT 5
        """, {'group': transformation_group})
        
        if recent:
            print(f"\n✅ Recently Completed ({len(recent)} shown):")
            for symbol, timestamp in recent:
                print(f"   {symbol:<10} {timestamp}")
        
        # Next to process
        next_pending = db.fetch_query("""
            SELECT symbol, created_at
            FROM transforms.transformation_watermarks
            WHERE transformation_group = %(group)s
              AND last_successful_run IS NULL
              AND transformation_eligible = true
            ORDER BY symbol_id
            LIMIT 10
        """, {'group': transformation_group})
        
        if next_pending:
            print(f"\n⏳ Next to Process ({len(next_pending)} shown):")
            for symbol, created in next_pending:
                print(f"   {symbol}")

if __name__ == "__main__":
    main()
