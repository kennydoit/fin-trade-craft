#!/usr/bin/env python3

from db.postgres_database_manager import PostgresDatabaseManager

try:
    db = PostgresDatabaseManager()
    db.connect()
    
    print('=== CLASS A STOCKS AND FUNDAMENTALS ANALYSIS ===')
    
    # Look for Class A stocks (typically end with .A or -A)
    class_a_query = """
    SELECT symbol, name, asset_type, status, exchange
    FROM source.listing_status 
    WHERE (symbol LIKE '%.A' OR symbol LIKE '%-A' OR symbol ~ '[A-Z]+A$')
      AND asset_type = 'Stock'
      AND LOWER(status) = 'active'
    ORDER BY symbol 
    LIMIT 20
    """
    
    class_a_stocks = db.execute_query(class_a_query)
    if class_a_stocks:
        print(f'Found {len(class_a_stocks)} potential Class A stocks:')
        for i, row in enumerate(class_a_stocks[:10], 1):
            print(f'  {i}. {row[0]} - {row[1][:50]}{"..." if len(row[1]) > 50 else ""} ({row[2]}, {row[4]})')
    
    # Check if any Class A stocks have balance sheet data
    print('\n=== CHECKING CLASS A STOCKS FOR FUNDAMENTAL DATA ===')
    
    if class_a_stocks:
        # Take first few Class A stocks and check for fundamentals
        test_symbols = [row[0] for row in class_a_stocks[:5]]
        
        for symbol in test_symbols:
            # Check for balance sheet data
            balance_query = """
            SELECT COUNT(*), MAX(fiscal_date_ending) 
            FROM source.balance_sheet b
            JOIN source.listing_status ls ON b.symbol_id = ls.symbol_id
            WHERE ls.symbol = %s
            """
            
            balance_result = db.execute_query(balance_query, (symbol,))
            if balance_result and balance_result[0][0] > 0:
                count, latest_date = balance_result[0]
                print(f'  ✅ {symbol}: {count} balance sheet records, latest: {latest_date}')
            else:
                print(f'  ❌ {symbol}: No balance sheet data')
    
    # Check some well-known Class A examples
    print('\n=== WELL-KNOWN CLASS A EXAMPLES ===')
    known_class_a = ['BRK.A', 'GOOGL', 'GOOG', 'META']  # Berkshire A, Alphabet A, etc.
    
    for symbol in known_class_a:
        check_query = """
        SELECT ls.symbol, ls.name, 
               (SELECT COUNT(*) FROM source.balance_sheet b WHERE b.symbol_id = ls.symbol_id) as balance_count,
               (SELECT MAX(fiscal_date_ending) FROM source.balance_sheet b WHERE b.symbol_id = ls.symbol_id) as latest_balance
        FROM source.listing_status ls
        WHERE ls.symbol = %s
        """
        
        result = db.execute_query(check_query, (symbol,))
        if result and result[0]:
            symbol, name, balance_count, latest_balance = result[0]
            status = "✅ Has fundamentals" if balance_count > 0 else "❌ No fundamentals"
            print(f'  {symbol}: {status} ({balance_count} records, latest: {latest_balance})')
        else:
            print(f'  {symbol}: Not found in database')
    
    print('\n=== SUMMARY ===')
    print('Class A stocks generally DO have fundamentals because:')
    print('  • They represent ownership in the same underlying company')  
    print('  • Financial statements consolidate all share classes')
    print('  • APIs like Alpha Vantage typically provide the same fundamental data')
    print('  • Only voting rights and dividend policies usually differ between classes')
    
    print('\nHowever, some considerations:')
    print('  • Some data providers may not have complete coverage for all share classes')
    print('  • New or less liquid Class A shares might have limited data availability')
    print('  • Our extraction filters might exclude some due to symbol patterns')

except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()
finally:
    if 'db' in locals():
        db.close()
