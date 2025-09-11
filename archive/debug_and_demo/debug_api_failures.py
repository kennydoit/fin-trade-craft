import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from data_pipeline.extract.extract_balance_sheet import BalanceSheetExtractor

# Test the failed symbols directly
ext = BalanceSheetExtractor()
failed_symbols = ['KDNY', 'KMLI', 'LBBB']

print("Testing failed symbols:")
for symbol in failed_symbols:
    resp, status = ext._fetch_api_data(symbol)
    print(f'{symbol}: {status}')
    if status != 'success':
        print(f'  Response keys: {list(resp.keys()) if isinstance(resp, dict) else "Not a dict"}')
        if isinstance(resp, dict) and len(resp) > 0:
            print(f'  Response preview: {str(resp)[:200]}...')

print("\nTesting major symbols:")
major_symbols = ['AAPL', 'MSFT', 'GOOGL', 'META']
for symbol in major_symbols:
    resp, status = ext._fetch_api_data(symbol)
    print(f'{symbol}: {status}')
    if status == 'success' and isinstance(resp, dict):
        annual_count = len(resp.get('annualReports', []))
        quarterly_count = len(resp.get('quarterlyReports', []))
        print(f'  Annual: {annual_count}, Quarterly: {quarterly_count}')
