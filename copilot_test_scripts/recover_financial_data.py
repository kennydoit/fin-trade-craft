#!/usr/bin/env python3
"""
Recover financial data from backup by mapping old symbol_ids to new ones.
This avoids having to re-extract all the financial data via API calls.
"""

import re
import tempfile
from pathlib import Path
from db.postgres_database_manager import PostgresDatabaseManager

def extract_backup_symbols(backup_file_path):
    """Extract symbol mappings from the backup file."""
    print("📄 Extracting symbol mappings from backup file...")
    
    backup_symbols = {}
    
    with open(backup_file_path, 'r', encoding='utf-8') as f:
        in_listing_status = False
        
        for line in f:
            # Look for the start of listing_status data
            if line.startswith('COPY public.listing_status'):
                in_listing_status = True
                continue
                
            # Look for the end of listing_status data
            if in_listing_status and line.strip() == '\\.':
                break
                
            # Parse listing_status data lines
            if in_listing_status and line.strip() and not line.startswith('--'):
                # Split on tabs, extract symbol_id (first) and symbol (second)
                parts = line.strip().split('\t')
                if len(parts) >= 2:
                    try:
                        symbol_id = int(parts[0])
                        symbol = parts[1]
                        backup_symbols[symbol] = symbol_id
                    except (ValueError, IndexError):
                        continue
    
    print(f"  Found {len(backup_symbols)} symbols in backup")
    return backup_symbols

def get_current_symbol_mapping():
    """Get current symbol_id mappings from database."""
    print("🗃️ Getting current symbol mappings from database...")
    
    db_manager = PostgresDatabaseManager()
    with db_manager as db:
        result = db.fetch_query("SELECT symbol_id, symbol FROM extracted.listing_status")
        current_symbols = {row[1]: row[0] for row in result}
    
    print(f"  Found {len(current_symbols)} symbols in current database")
    return current_symbols

def create_symbol_id_mapping(backup_symbols, current_symbols):
    """Create mapping from old symbol_ids to new symbol_ids."""
    print("🔗 Creating symbol_id mapping...")
    
    # Find symbols that exist in both backup and current database
    common_symbols = set(backup_symbols.keys()) & set(current_symbols.keys())
    
    # Create mapping: old_symbol_id -> new_symbol_id
    symbol_id_mapping = {}
    for symbol in common_symbols:
        old_id = backup_symbols[symbol]
        new_id = current_symbols[symbol]
        symbol_id_mapping[old_id] = new_id
    
    print(f"  Mapped {len(symbol_id_mapping)} symbols")
    print(f"  Sample mappings:")
    for i, (old_id, new_id) in enumerate(list(symbol_id_mapping.items())[:5]):
        symbol = next(s for s, id in backup_symbols.items() if id == old_id)
        print(f"    {symbol}: {old_id} -> {new_id}")
    
    return symbol_id_mapping

def create_corrected_backup(backup_file_path, symbol_id_mapping, output_file_path):
    """Create a corrected version of the backup file with updated symbol_ids."""
    print("🔧 Creating corrected backup file...")
    
    financial_tables = {
        'balance_sheet', 'cash_flow', 'earnings_call_transcripts', 
        'income_statement', 'insider_transactions', 'overview', 
        'time_series_daily_adjusted'
    }
    
    with open(backup_file_path, 'r', encoding='utf-8') as input_file, \
         open(output_file_path, 'w', encoding='utf-8') as output_file:
        
        current_table = None
        skip_table = False
        
        for line in input_file:
            # Detect which table we're in
            copy_match = re.match(r'COPY public\.(\w+)', line)
            if copy_match:
                current_table = copy_match.group(1)
                skip_table = current_table not in financial_tables
                
                if skip_table:
                    continue  # Skip non-financial tables
                
                print(f"  Processing table: {current_table}")
                output_file.write(line)
                continue
            
            # Skip lines for tables we don't want
            if skip_table:
                continue
                
            # End of COPY section
            if line.strip() == '\\.':
                if current_table in financial_tables:
                    output_file.write(line)
                current_table = None
                skip_table = False
                continue
            
            # Process data lines for financial tables
            if current_table in financial_tables and line.strip() and not line.startswith('--'):
                parts = line.strip().split('\t')
                if len(parts) >= 2:
                    try:
                        # First column is always symbol_id in our financial tables
                        old_symbol_id = int(parts[0])
                        if old_symbol_id in symbol_id_mapping:
                            # Replace with new symbol_id
                            parts[0] = str(symbol_id_mapping[old_symbol_id])
                            corrected_line = '\t'.join(parts) + '\n'
                            output_file.write(corrected_line)
                        # Skip lines with unmapped symbol_ids (don't write them)
                    except (ValueError, IndexError):
                        # Skip malformed lines
                        continue
                continue
            
            # Copy other lines as-is (schema definitions, etc.)
            if not skip_table:
                output_file.write(line)

def restore_financial_data(corrected_backup_path):
    """Restore the financial data from the corrected backup."""
    print("📥 Restoring financial data...")
    
    # Clear existing financial data first
    db_manager = PostgresDatabaseManager()
    with db_manager as db:
        financial_tables = [
            'balance_sheet', 'cash_flow', 'earnings_call_transcripts',
            'income_statement', 'insider_transactions', 'overview',
            'time_series_daily_adjusted'
        ]
        
        print("  Clearing existing financial data...")
        for table in financial_tables:
            db.execute_query(f"DELETE FROM extracted.{table}")
        
        print("  Restored financial data tables are now empty")

def main():
    backup_file_path = "E:\\Backups\\database_backups\\fin_trade_craft_backup_20250722_095155.sql"
    
    if not Path(backup_file_path).exists():
        print(f"❌ Backup file not found: {backup_file_path}")
        return
    
    try:
        # Step 1: Extract symbol mappings from backup
        backup_symbols = extract_backup_symbols(backup_file_path)
        
        # Step 2: Get current symbol mappings
        current_symbols = get_current_symbol_mapping()
        
        # Step 3: Create symbol_id mapping
        symbol_id_mapping = create_symbol_id_mapping(backup_symbols, current_symbols)
        
        if not symbol_id_mapping:
            print("❌ No common symbols found between backup and current database")
            return
        
        # Step 4: Create corrected backup file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as temp_file:
            corrected_backup_path = temp_file.name
        
        create_corrected_backup(backup_file_path, symbol_id_mapping, corrected_backup_path)
        
        print(f"✅ Created corrected backup file: {corrected_backup_path}")
        print(f"📊 Next step: Restore from corrected backup using psql")
        print(f"   Command: psql -h localhost -p 5432 -U postgres -d fin_trade_craft -f \"{corrected_backup_path}\"")
        
        # Clear existing financial data
        restore_financial_data(corrected_backup_path)
        
    except Exception as e:
        print(f"❌ Error during recovery: {e}")
        raise

if __name__ == "__main__":
    main()
