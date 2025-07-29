#!/usr/bin/env python3
"""
Recover insider_transactions table from backup
"""

import re
from db.postgres_database_manager import PostgresDatabaseManager

def get_symbol_mappings():
    """Get mapping between old backup symbol_ids and new current symbol_ids"""
    db_manager = PostgresDatabaseManager()
    
    print("🗃️ Getting current symbol mappings...")
    with db_manager as db:
        current_symbols = db.fetch_query("""
            SELECT symbol_id, symbol 
            FROM extracted.listing_status 
            ORDER BY symbol_id
        """)
    
    symbol_to_current_id = {symbol: symbol_id for symbol_id, symbol in current_symbols}
    print(f"  Found {len(current_symbols)} symbols in current database")
    
    return symbol_to_current_id

def extract_insider_transactions_from_backup(backup_file_path, symbol_mappings):
    """Extract insider_transactions data from backup file"""
    print("📄 Extracting insider_transactions from backup...")
    
    # Read backup file and extract insider_transactions data
    with open(backup_file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Find insider_transactions COPY block
    copy_pattern = r'COPY extracted\.insider_transactions.*?\n(.*?)\n\\.'
    match = re.search(copy_pattern, content, re.DOTALL)
    
    if not match:
        print("  No insider_transactions data found in backup")
        return []
    
    copy_data = match.group(1)
    lines = copy_data.strip().split('\n')
    
    print(f"  Found {len(lines)} insider_transactions records in backup")
    
    # Parse the data and map symbol_ids
    mapped_records = []
    for line in lines:
        if not line.strip():
            continue
            
        # Split the line (tab-separated values)
        fields = line.split('\t')
        if len(fields) < 2:
            continue
            
        old_symbol_id = int(fields[0])
        symbol = fields[1]
        
        # Map to new symbol_id
        if symbol in symbol_mappings:
            new_symbol_id = symbol_mappings[symbol]
            # Replace the old symbol_id with new one
            fields[0] = str(new_symbol_id)
            mapped_records.append('\t'.join(fields))
    
    print(f"  Successfully mapped {len(mapped_records)} insider_transactions records")
    return mapped_records

def create_insert_statements(records):
    """Create INSERT statements for insider_transactions"""
    if not records:
        return []
    
    print("🔧 Creating INSERT SQL for insider_transactions...")
    
    # Get column names for insider_transactions table
    db_manager = PostgresDatabaseManager()
    with db_manager as db:
        schema_result = db.fetch_query("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'extracted' 
            AND table_name = 'insider_transactions'
            ORDER BY ordinal_position
        """)
    
    if not schema_result:
        print("  Could not get schema for insider_transactions table")
        return []
    
    columns = [row[0] for row in schema_result]
    print(f"  Table has {len(columns)} columns: {', '.join(columns)}")
    
    # Create INSERT statements in batches
    batch_size = 1000
    insert_statements = []
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        
        values_list = []
        for record in batch:
            fields = record.split('\t')
            
            # Process each field according to its data type
            processed_fields = []
            for j, field in enumerate(fields):
                if j >= len(columns):
                    break
                    
                if field == '\\N' or field == '':
                    processed_fields.append('NULL')
                elif schema_result[j][1] in ['integer', 'bigint']:
                    processed_fields.append(field)
                elif schema_result[j][1] in ['numeric', 'double precision']:
                    processed_fields.append(field if field != '\\N' else 'NULL')
                elif schema_result[j][1] in ['date', 'timestamp without time zone']:
                    processed_fields.append(f"'{field}'" if field != '\\N' else 'NULL')
                else:  # text, varchar, etc.
                    # Escape single quotes
                    escaped = field.replace("'", "''")
                    processed_fields.append(f"'{escaped}'")
            
            values_list.append(f"({', '.join(processed_fields)})")
        
        column_list = ', '.join(columns)
        values_str = ',\n    '.join(values_list)
        
        insert_sql = f"""INSERT INTO extracted.insider_transactions ({column_list})
VALUES
    {values_str};"""
        
        insert_statements.append(insert_sql)
    
    print(f"📊 Generated {len(insert_statements)} INSERT statements for insider_transactions")
    return insert_statements

def execute_insert_statements(insert_statements):
    """Execute INSERT statements"""
    if not insert_statements:
        print("  No INSERT statements to execute")
        return
    
    print("🔄 Executing INSERT statements for insider_transactions...")
    
    db_manager = PostgresDatabaseManager()
    with db_manager as db:
        success_count = 0
        for i, statement in enumerate(insert_statements, 1):
            try:
                db.execute_query(statement)
                success_count += 1
                print(f"  Executed {i}/{len(insert_statements)} statements for insider_transactions")
            except Exception as e:
                print(f"  Error executing statement {i}: {e}")
                print(f"  Statement preview: {statement[:200]}...")
                break
    
    if success_count == len(insert_statements):
        print(f"✅ insider_transactions recovery completed! ({success_count}/{len(insert_statements)} statements successful)")
    else:
        print(f"⚠️ insider_transactions recovery partially completed ({success_count}/{len(insert_statements)} statements successful)")

def main():
    backup_file_path = "E:/Backups/database_backups/fin_trade_craft_backup_20250722_095155.sql"
    
    try:
        # Get symbol mappings
        symbol_mappings = get_symbol_mappings()
        
        # Extract insider_transactions data from backup
        records = extract_insider_transactions_from_backup(backup_file_path, symbol_mappings)
        
        if records:
            # Create INSERT statements
            insert_statements = create_insert_statements(records)
            
            # Execute INSERT statements
            execute_insert_statements(insert_statements)
        else:
            print("❌ No insider_transactions data to recover")
            
    except Exception as e:
        print(f"❌ Error during insider_transactions recovery: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
