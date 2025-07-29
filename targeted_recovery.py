#!/usr/bin/env python3
"""
Create targeted SQL to restore financial data with corrected symbol_ids.
This approach is more reliable than trying to process the entire backup file.
"""

import re
import tempfile
from pathlib import Path
from db.postgres_database_manager import PostgresDatabaseManager

def get_symbol_mapping():
    """Get current symbol_id mappings from database."""
    print("🗃️ Getting current symbol mappings...")
    
    db_manager = PostgresDatabaseManager()
    with db_manager as db:
        result = db.fetch_query("SELECT symbol_id, symbol FROM extracted.listing_status")
        symbol_to_id = {row[1]: row[0] for row in result}
    
    print(f"  Found {len(symbol_to_id)} symbols in current database")
    return symbol_to_id

def extract_financial_data_with_symbols(backup_file_path, symbol_mapping):
    """Extract financial data from backup with symbol names for mapping."""
    print("📄 Extracting financial data from backup...")
    
    financial_tables = [
        'balance_sheet', 'cash_flow', 'earnings_call_transcripts',
        'income_statement', 'insider_transactions', 'overview',
        'time_series_daily_adjusted'
    ]
    
    # First pass: get listing_status data from backup to create old_id -> symbol mapping
    print("  Pass 1: Getting symbol mappings from backup...")
    backup_id_to_symbol = {}
    
    with open(backup_file_path, 'r', encoding='utf-8') as f:
        in_listing_status = False
        
        for line in f:
            if line.startswith('COPY public.listing_status'):
                in_listing_status = True
                continue
            if in_listing_status and line.strip() == '\\.':
                break
            if in_listing_status and line.strip() and not line.startswith('--'):
                parts = line.strip().split('\t')
                if len(parts) >= 2:
                    try:
                        symbol_id = int(parts[0])
                        symbol = parts[1]
                        backup_id_to_symbol[symbol_id] = symbol
                    except (ValueError, IndexError):
                        continue
    
    print(f"  Found {len(backup_id_to_symbol)} symbols in backup")
    
    # Second pass: extract financial data for mappable symbols
    print("  Pass 2: Extracting financial data...")
    financial_data = {table: [] for table in financial_tables}
    
    with open(backup_file_path, 'r', encoding='utf-8') as f:
        current_table = None
        
        for line in f:
            # Detect table starts
            copy_match = re.match(r'COPY public\.(\w+)', line)
            if copy_match:
                current_table = copy_match.group(1)
                continue
            
            # End of COPY section
            if line.strip() == '\\.':
                current_table = None
                continue
            
            # Process financial table data
            if current_table in financial_tables and line.strip() and not line.startswith('--'):
                parts = line.strip().split('\t')
                if len(parts) >= 2:
                    try:
                        old_symbol_id = int(parts[0])
                        # Check if we can map this symbol
                        if old_symbol_id in backup_id_to_symbol:
                            symbol = backup_id_to_symbol[old_symbol_id]
                            if symbol in symbol_mapping:
                                # Replace symbol_id with new one
                                parts[0] = str(symbol_mapping[symbol])
                                financial_data[current_table].append(parts)
                    except (ValueError, IndexError):
                        continue
    
    # Report what we found
    for table, data in financial_data.items():
        print(f"    {table}: {len(data)} records")
    
    return financial_data

def create_insert_sql(financial_data, target_tables=None):
    """Create INSERT SQL statements for the financial data."""
    print("🔧 Creating INSERT SQL...")
    
    # Define processing order
    if target_tables is None:
        target_tables = [
            'time_series_daily_adjusted',
            'earnings_call_transcripts', 
            'balance_sheet',
            'cash_flow',
            'income_statement'
        ]
    
    # Table column definitions (in order) - corrected to match actual schema
    table_columns = {
        'time_series_daily_adjusted': [
            'symbol_id', 'symbol', 'date', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume', 
            'dividend_amount', 'split_coefficient', 'created_at', 'updated_at'
        ],
        'earnings_call_transcripts': [
            'transcript_id', 'symbol_id', 'symbol', 'quarter', 'speaker', 'title', 'content', 'content_hash', 
            'sentiment', 'api_response_status', 'created_at', 'updated_at'
        ],
        'balance_sheet': [
            'symbol_id', 'symbol', 'fiscal_date_ending', 'report_type', 'reported_currency', 'total_assets', 
            'total_current_assets', 'cash_and_cash_equivalents_at_carrying_value', 'cash_and_short_term_investments', 
            'inventory', 'current_net_receivables', 'total_non_current_assets', 'property_plant_equipment', 
            'accumulated_depreciation_amortization_ppe', 'intangible_assets', 'intangible_assets_excluding_goodwill', 
            'goodwill', 'investments', 'long_term_investments', 'short_term_investments', 'other_current_assets', 
            'other_non_current_assets', 'total_liabilities', 'total_current_liabilities', 'current_accounts_payable', 
            'deferred_revenue', 'current_debt', 'short_term_debt', 'total_non_current_liabilities', 
            'capital_lease_obligations', 'long_term_debt', 'current_long_term_debt', 'long_term_debt_noncurrent', 
            'short_long_term_debt_total', 'other_current_liabilities', 'other_non_current_liabilities', 
            'total_shareholder_equity', 'treasury_stock', 'retained_earnings', 'common_stock', 
            'common_stock_shares_outstanding', 'api_response_status', 'created_at', 'updated_at'
        ],
        'cash_flow': [
            'symbol_id', 'symbol', 'fiscal_date_ending', 'report_type', 'reported_currency', 'operating_cashflow', 
            'payments_for_operating_activities', 'proceeds_from_operating_activities', 'change_in_operating_liabilities', 
            'change_in_operating_assets', 'depreciation_depletion_and_amortization', 'capital_expenditures', 
            'change_in_receivables', 'change_in_inventory', 'profit_loss', 'cashflow_from_investment', 
            'cashflow_from_financing', 'proceeds_from_repayments_of_short_term_debt', 'payments_for_repurchase_of_common_stock', 
            'payments_for_repurchase_of_equity', 'payments_for_repurchase_of_preferred_stock', 'dividend_payout', 
            'dividend_payout_common_stock', 'dividend_payout_preferred_stock', 'proceeds_from_issuance_of_common_stock', 
            'proceeds_from_issuance_of_long_term_debt_and_capital_securities', 'proceeds_from_issuance_of_preferred_stock', 
            'proceeds_from_repurchase_of_equity', 'proceeds_from_sale_of_treasury_stock', 'change_in_cash_and_cash_equivalents', 
            'change_in_exchange_rate', 'net_income', 'api_response_status', 'created_at', 'updated_at'
        ],
        'income_statement': [
            'symbol_id', 'symbol', 'fiscal_date_ending', 'report_type', 'reported_currency', 'gross_profit', 
            'total_revenue', 'cost_of_revenue', 'cost_of_goods_and_services_sold', 'operating_income', 
            'selling_general_and_administrative', 'research_and_development', 'operating_expenses', 
            'investment_income_net', 'net_interest_income', 'interest_income', 'interest_expense', 
            'non_interest_income', 'other_non_operating_income', 'depreciation', 'depreciation_and_amortization', 
            'income_before_tax', 'income_tax_expense', 'interest_and_debt_expense', 'net_income_from_continuing_operations', 
            'comprehensive_income_net_of_tax', 'ebit', 'ebitda', 'net_income', 'api_response_status', 'created_at', 'updated_at'
        ],
        'overview': [
            'overview_id', 'symbol_id', 'symbol', 'assettype', 'name', 'description', 'cik', 'exchange',
            'currency', 'country', 'sector', 'industry', 'address', 'officialsite', 'fiscalyearend', 
            'status', 'created_at', 'updated_at'
        ]
    }
    
    sql_statements = []
    
    for table in target_tables:
        data = financial_data.get(table, [])
        if not data:
            print(f"  Skipping {table} - no data")
            continue
            
        print(f"  Creating INSERT for {table} ({len(data)} records)")
        
        if table in table_columns:
            columns = table_columns[table]
            column_list = ', '.join(columns)
            
            # Adjust batch size based on table size
            if table == 'time_series_daily_adjusted':
                batch_size = 5000  # Smaller batches for large table
            else:
                batch_size = 1000
            
            # Create batched INSERT statements
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                values_list = []
                
                for row in batch:
                    # Convert NULL values and escape quotes
                    escaped_values = []
                    for val in row:
                        if val == '\\N' or val == 'NULL' or val == '':
                            escaped_values.append('NULL')
                        else:
                            # Escape single quotes and handle special characters
                            escaped_val = str(val).replace("'", "''").replace('\x00', '')
                            escaped_values.append(f"'{escaped_val}'")
                    values_list.append(f"({', '.join(escaped_values)})")
                
                insert_sql = f"""
INSERT INTO extracted.{table} ({column_list})
VALUES {', '.join(values_list)};
"""
                sql_statements.append((table, insert_sql))
        else:
            print(f"  Warning: No column definition for {table}")
    
    return sql_statements

def main():
    backup_file_path = "E:\\Backups\\database_backups\\fin_trade_craft_backup_20250722_095155.sql"
    
    # Define target tables in order of priority
    target_tables = [
        'time_series_daily_adjusted',
        'earnings_call_transcripts', 
        'balance_sheet',
        'cash_flow',
        'income_statement'
    ]
    
    if not Path(backup_file_path).exists():
        print(f"❌ Backup file not found: {backup_file_path}")
        return
    
    try:
        # Get current symbol mappings
        symbol_mapping = get_symbol_mapping()
        
        # Extract financial data with symbol mapping
        financial_data = extract_financial_data_with_symbols(backup_file_path, symbol_mapping)
        
        # Process tables one by one to manage memory
        for table in target_tables:
            if table not in financial_data or not financial_data[table]:
                print(f"⏭️ Skipping {table} - no data found")
                continue
                
            print(f"\n🔄 Processing {table} ({len(financial_data[table])} records)...")
            
            # Create INSERT SQL for just this table
            single_table_data = {table: financial_data[table]}
            sql_statements = create_insert_sql(single_table_data, [table])
            
            if not sql_statements:
                print(f"❌ No SQL statements generated for {table}")
                continue
            
            print(f"📊 Generated {len(sql_statements)} INSERT statements for {table}")
            
            # Execute the SQL
            print(f"🔄 Executing INSERT statements for {table}...")
            db_manager = PostgresDatabaseManager()
            with db_manager as db:
                success_count = 0
                for i, (table_name, sql) in enumerate(sql_statements):
                    try:
                        db.execute_query(sql)
                        success_count += 1
                        if i % 10 == 0 or i == len(sql_statements) - 1:
                            print(f"  Executed {i+1}/{len(sql_statements)} statements for {table}")
                    except Exception as e:
                        print(f"  Error in statement {i+1}: {str(e)[:100]}...")
                        continue
                
                print(f"✅ {table} recovery completed! ({success_count}/{len(sql_statements)} statements successful)")
            
            # Clear this table from memory to save space
            financial_data[table] = []
        
        print("\n🎉 All table recovery completed!")
        
    except Exception as e:
        print(f"❌ Error during recovery: {e}")
        raise

if __name__ == "__main__":
    main()
