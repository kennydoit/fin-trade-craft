#!/usr/bin/env python3

from db.postgres_database_manager import PostgresDatabaseManager

try:
    db = PostgresDatabaseManager()
    db.connect()
    
    print('=== ANALYZING PRIORITY 1 SYMBOLS ===')
    
    # Get symbols with quarterly gaps to understand the ranking
    priority_analysis_query = """
    WITH quarterly_analysis AS (
        SELECT 
            ls.symbol_id, 
            ls.symbol,
            ew.last_fiscal_date,
            ew.last_successful_run,
            CASE 
                WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '45 days'
                THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '1 day')::date
                WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' + INTERVAL '45 days'
                THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' - INTERVAL '1 day')::date
                ELSE (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '6 months' - INTERVAL '1 day')::date
            END as expected_latest_quarter,
            -- Priority calculation
            CASE 
                WHEN ew.last_fiscal_date IS NULL THEN 0 -- Never processed
                WHEN ew.last_fiscal_date < (
                    CASE 
                        WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '45 days'
                        THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '1 day')::date
                        WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' + INTERVAL '45 days'
                        THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' - INTERVAL '1 day')::date
                        ELSE (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '6 months' - INTERVAL '1 day')::date
                    END
                ) THEN 1 -- Has quarterly gap
                ELSE 2 -- Time-stale only
            END as priority
        FROM source.listing_status ls
        LEFT JOIN source.extraction_watermarks ew ON ew.symbol_id = ls.symbol_id 
                                                   AND ew.table_name = 'balance_sheet'
        WHERE ls.asset_type = 'Stock'
          AND LOWER(ls.status) = 'active'
          AND ls.symbol NOT LIKE '%WS%'
          AND ls.symbol NOT LIKE '%R'
          AND ls.symbol NOT LIKE '%.R%'
          AND ls.symbol NOT LIKE '%P%'
          AND ls.symbol NOT LIKE '%U'
          AND COALESCE(ew.consecutive_failures, 0) < 3
    )
    SELECT priority, COUNT(*) as count,
           MIN(last_successful_run) as earliest_run,
           MAX(last_successful_run) as latest_run
    FROM quarterly_analysis
    WHERE (last_fiscal_date IS NULL OR 
           last_fiscal_date < expected_latest_quarter OR
           last_successful_run < NOW() - INTERVAL '24 hours')
    GROUP BY priority
    ORDER BY priority
    """
    
    result = db.execute_query(priority_analysis_query)
    if result:
        print('Priority distribution:')
        for row in result:
            print(f'  Priority {row[0]}: {row[1]} symbols')
            if row[2] and row[3]:
                print(f'    Run dates: {row[2]} to {row[3]}')
        print()
    
    # Now check ORIC's position among priority 1 symbols
    print('=== ORIC POSITION AMONG PRIORITY 1 SYMBOLS ===')
    
    priority_1_ranking_query = """
    WITH quarterly_analysis AS (
        SELECT 
            ls.symbol_id, 
            ls.symbol,
            ew.last_fiscal_date,
            ew.last_successful_run,
            CASE 
                WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '45 days'
                THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '1 day')::date
                WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' + INTERVAL '45 days'
                THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' - INTERVAL '1 day')::date
                ELSE (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '6 months' - INTERVAL '1 day')::date
            END as expected_latest_quarter,
            -- Priority calculation
            CASE 
                WHEN ew.last_fiscal_date IS NULL THEN 0
                WHEN ew.last_fiscal_date < (
                    CASE 
                        WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '45 days'
                        THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '1 day')::date
                        WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' + INTERVAL '45 days'
                        THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' - INTERVAL '1 day')::date
                        ELSE (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '6 months' - INTERVAL '1 day')::date
                    END
                ) THEN 1
                ELSE 2
            END as priority
        FROM source.listing_status ls
        LEFT JOIN source.extraction_watermarks ew ON ew.symbol_id = ls.symbol_id 
                                                   AND ew.table_name = 'balance_sheet'
        WHERE ls.asset_type = 'Stock'
          AND LOWER(ls.status) = 'active'
          AND ls.symbol NOT LIKE '%WS%'
          AND ls.symbol NOT LIKE '%R'
          AND ls.symbol NOT LIKE '%.R%'
          AND ls.symbol NOT LIKE '%P%'
          AND ls.symbol NOT LIKE '%U'
          AND COALESCE(ew.consecutive_failures, 0) < 3
    ),
    ranked_priority_1 AS (
        SELECT symbol, last_fiscal_date, last_successful_run,
               ROW_NUMBER() OVER (
                   ORDER BY COALESCE(last_successful_run, '1900-01-01'::timestamp) ASC,
                            LENGTH(symbol) ASC,
                            symbol ASC
               ) as rank_within_priority_1
        FROM quarterly_analysis
        WHERE priority = 1
          AND (last_successful_run < NOW() - INTERVAL '24 hours' OR last_successful_run IS NULL)
    )
    SELECT rank_within_priority_1, symbol, last_fiscal_date, last_successful_run
    FROM ranked_priority_1
    WHERE symbol = 'ORIC' OR rank_within_priority_1 <= 10 OR rank_within_priority_1 % 100 = 0
    ORDER BY rank_within_priority_1
    """
    
    ranking_result = db.execute_query(priority_1_ranking_query)
    if ranking_result:
        print('Priority 1 symbols ranking:')
        for row in ranking_result:
            marker = '🎯' if row[1] == 'ORIC' else '  '
            print(f'{marker} #{row[0]}: {row[1]} - Last Fiscal: {row[2]}, Last Run: {row[3]}')
    
except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()
finally:
    if 'db' in locals():
        db.close()
