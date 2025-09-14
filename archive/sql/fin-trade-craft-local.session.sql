-- BALANCE SHEET DISCREPANCY ANALYSIS
-- Compare extracted vs source schema data

-- 1. Count symbols in each schema
SELECT 'extracted' as schema_name, COUNT(DISTINCT symbol_id) as unique_symbols
FROM extracted.balance_sheet
UNION ALL
SELECT 'source' as schema_name, COUNT(DISTINCT symbol_id) as unique_symbols  
FROM source.balance_sheet;

-- 2. Symbols in extracted but NOT in source
SELECT eb.symbol_id, ls.symbol, COUNT(*) as records_in_extracted
FROM extracted.balance_sheet eb
JOIN extracted.listing_status ls ON eb.symbol_id = ls.symbol_id
WHERE eb.symbol_id NOT IN (
    SELECT DISTINCT symbol_id 
    FROM source.balance_sheet 
    WHERE symbol_id IS NOT NULL
)
GROUP BY eb.symbol_id, ls.symbol
ORDER BY records_in_extracted DESC
LIMIT 20;

-- 3. Symbols in source but NOT in extracted  
SELECT sb.symbol_id, ls.symbol, COUNT(*) as records_in_source
FROM source.balance_sheet sb
JOIN extracted.listing_status ls ON sb.symbol_id = ls.symbol_id
WHERE sb.symbol_id NOT IN (
    SELECT DISTINCT symbol_id 
    FROM extracted.balance_sheet 
    WHERE symbol_id IS NOT NULL
)
GROUP BY sb.symbol_id, ls.symbol
ORDER BY records_in_source DESC
LIMIT 20;

-- 4. Check watermark status for missing symbols
SELECT 
    ew.symbol_id,
    ls.symbol,
    ew.last_successful_run,
    ew.consecutive_failures,
    CASE 
        WHEN ew.symbol_id IN (SELECT DISTINCT symbol_id FROM extracted.balance_sheet) THEN 'In Extracted'
        ELSE 'Not in Extracted'
    END as extracted_status,
    CASE 
        WHEN ew.symbol_id IN (SELECT DISTINCT symbol_id FROM source.balance_sheet) THEN 'In Source'
        ELSE 'Not in Source'
    END as source_status
FROM source.extraction_watermarks ew
JOIN extracted.listing_status ls ON ew.symbol_id = ls.symbol_id
WHERE ew.table_name = 'balance_sheet'
    AND ew.last_successful_run IS NOT NULL
ORDER BY ew.last_successful_run DESC
LIMIT 30;

-- 5. Date ranges comparison
SELECT 
    'extracted' as schema_name,
    MIN(fiscal_date_ending) as earliest_date,
    MAX(fiscal_date_ending) as latest_date,
    COUNT(*) as total_records
FROM extracted.balance_sheet
UNION ALL
SELECT 
    'source' as schema_name,
    MIN(fiscal_date_ending) as earliest_date,
    MAX(fiscal_date_ending) as latest_date,
    COUNT(*) as total_records
FROM source.balance_sheet;