import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from data_pipeline.extract.extract_balance_sheet import BalanceSheetExtractor

# Test with a known good symbol
ext = BalanceSheetExtractor()
resp, status = ext._fetch_api_data('AAPL')
print(f'AAPL Status: {status}')
if isinstance(resp, dict):
    print(f'Keys: {list(resp.keys())}')
    if 'annualReports' in resp:
        print(f'Annual reports: {len(resp["annualReports"])}')
    if 'quarterlyReports' in resp:
        print(f'Quarterly reports: {len(resp["quarterlyReports"])}')
else:
    print(f'Response: {resp}')

# Test with the failed symbols
print('\n' + '='*50)
for symbol in ['JUGG', 'JVSA']:
    resp, status = ext._fetch_api_data(symbol)
    print(f'{symbol} Status: {status}')
    if status != 'success':
        print(f'  Response: {resp}')
