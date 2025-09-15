"""
Batch update script to integrate adaptive rate limiter into fundamentals extractors.
This script updates balance_sheet, cash_flow, and income_statement extractors.
"""

import re
from pathlib import Path

def update_extractor(file_path: str, extractor_name: str, api_function: str):
    """Update an extractor file with adaptive rate limiter integration."""
    
    print(f"Updating {extractor_name} extractor...")
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    # 1. Add adaptive rate limiter to _fetch_api_data method
    content = re.sub(
        r'(url = f"https://www\.alphavantage\.co/query"\s+params = \{[^}]+\})\s+(try:)',
        r'\1\n        \n        # Adaptive rate limiting - smart delay based on elapsed time and processing overhead\n        self.rate_limiter.pre_api_call()\n        \n        \2',
        content
    )
    
    # 2. Update extract_symbol method - add processing timer
    content = re.sub(
        r'(def extract_symbol\(self, symbol: str, symbol_id: int, db\) -> Dict\[str, Any\]:\s+"""[^"]*?"""\s+)run_id = RunIdGenerator\.generate\(\)',
        r'\1run_id = RunIdGenerator.generate()\n        \n        # Start processing timer for adaptive rate limiter\n        self.rate_limiter.start_processing()',
        content,
        flags=re.DOTALL
    )
    
    # 3. Update extract_symbol return statements to collect results
    # Replace all direct returns with result assignment
    content = re.sub(
        r'(\s+)return \{\s+"symbol": symbol,\s+"status": "success",([^}]+)\}',
        r'\1result = {\n\1    "symbol": symbol,\n\1    "status": "success",\2}\n\1\n\1# Notify rate limiter about processing result (enables optimization)\n\1self.rate_limiter.post_api_call(result["status"])\n\1\n\1return result',
        content
    )
    
    content = re.sub(
        r'(\s+)return \{\s+"symbol": symbol,\s+"status": "no_valid_records",([^}]+)\}',
        r'\1result = {\n\1    "symbol": symbol,\n\1    "status": "no_valid_records",\2}',
        content
    )
    
    content = re.sub(
        r'(\s+)return \{\s+"symbol": symbol,\s+"status": "api_failure",([^}]+)\}',
        r'\1result = {\n\1    "symbol": symbol,\n\1    "status": "api_failure",\2}',
        content
    )
    
    # 4. Remove old rate limiting and add performance reporting
    content = re.sub(
        r'# Rate limiting\s+if i < len\(symbols_to_process\):\s+time\.sleep\(API_DELAY_SECONDS\)',
        r'# Show periodic performance updates\n                if i % 10 == 0 or i == len(symbols_to_process):\n                    self.rate_limiter.print_performance_summary()',
        content
    )
    
    # 5. Add final performance summary
    content = re.sub(
        r'(print\(f"  Total records: \{results\[\'total_records\'\]\}"\))\s+(return results)',
        r'\1\n            \n            # Final performance summary\n            print("\\n" + "="*60)\n            self.rate_limiter.print_performance_summary()\n            print("="*60)\n            \n            \2',
        content
    )
    
    # 6. Update docstrings and descriptions
    content = re.sub(
        r'Uses source schema, watermarks, and deterministic processing\.',
        r'Uses source schema, watermarks, and adaptive rate limiting for optimal performance.',
        content
    )
    
    content = re.sub(
        r'extractor with incremental processing\.',
        r'extractor with adaptive rate limiting and incremental processing.',
        content
    )
    
    content = re.sub(
        r'Starting incremental .* extraction\.\.\.',
        rf'Starting incremental {extractor_name.lower()} extraction with adaptive rate limiting...',
        content
    )
    
    # Write the updated content back to file
    with open(file_path, 'w') as f:
        f.write(content)
    
    print(f"✅ Updated {extractor_name} extractor successfully")

# Update all three extractors
extractors = [
    ("c:\\Users\\Kenrm\\repositories\\fin-trade-craft\\data_pipeline\\extract\\extract_balance_sheet.py", "Balance Sheet", "BALANCE_SHEET"),
    ("c:\\Users\\Kenrm\\repositories\\fin-trade-craft\\data_pipeline\\extract\\extract_cash_flow.py", "Cash Flow", "CASH_FLOW"),
    ("c:\\Users\\Kenrm\\repositories\\fin-trade-craft\\data_pipeline\\extract\\extract_income_statement.py", "Income Statement", "INCOME_STATEMENT")
]

if __name__ == "__main__":
    for file_path, name, api_func in extractors:
        update_extractor(file_path, name, api_func)
    
    print("🎯 All fundamentals extractors updated with adaptive rate limiting!")
