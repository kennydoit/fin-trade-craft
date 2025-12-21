"""Verify commodities and economic indicators use self-watermarking."""
from db.postgres_database_manager import PostgresDatabaseManager

db = PostgresDatabaseManager()
db.connect()

# Check commodities
print("COMMODITIES:")
comm_cols = db.fetch_query("""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_schema = 'transforms' 
      AND table_name = 'commodities'
      AND column_name = 'processed_at'
""")
print(f"  Has processed_at column: {len(comm_cols) > 0}")

comm_counts = db.fetch_query("""
    SELECT 
        COUNT(*) as total,
        COUNT(processed_at) as processed,
        COUNT(*) - COUNT(processed_at) as unprocessed
    FROM transforms.commodities
""")
if comm_counts:
    total, processed, unprocessed = comm_counts[0]
    print(f"  Total: {total:,}, Processed: {processed:,}, Unprocessed: {unprocessed:,}")

# Check economic indicators
print("\nECONOMIC INDICATORS:")
econ_cols = db.fetch_query("""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_schema = 'transforms' 
      AND table_name = 'economic_indicators'
      AND column_name = 'processed_at'
""")
print(f"  Has processed_at column: {len(econ_cols) > 0}")

econ_counts = db.fetch_query("""
    SELECT 
        COUNT(*) as total,
        COUNT(processed_at) as processed,
        COUNT(*) - COUNT(processed_at) as unprocessed
    FROM transforms.economic_indicators
""")
if econ_counts:
    total, processed, unprocessed = econ_counts[0]
    print(f"  Total: {total:,}, Processed: {processed:,}, Unprocessed: {unprocessed:,}")

db.close()
