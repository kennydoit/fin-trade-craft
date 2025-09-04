-- Quick Database Reference for GitHub Copilot
-- Common queries for fin-trade-craft database analysis

-- === SYMBOL UNIVERSE OVERVIEW ===
-- Total symbols by asset type
SELECT asset_type, COUNT(*) as count
FROM transformed.symbol_universes 
GROUP BY asset_type 
ORDER BY count DESC;

-- Active stocks in listing_status
SELECT COUNT(*) as total_active_stocks
FROM extracted.listing_status 
WHERE asset_type = 'Stock' 
AND LOWER(status) = 'active';

-- === BALANCE SHEET EXTRACTION STATUS ===
-- Symbols processed for balance sheet
SELECT COUNT(DISTINCT symbol_id) as symbols_with_balance_sheet
FROM source.balance_sheet;

-- Watermark status for balance sheet
SELECT 
    COUNT(*) as total_watermarks,
    COUNT(CASE WHEN last_successful_run IS NOT NULL THEN 1 END) as successful_runs,
    COUNT(CASE WHEN consecutive_failures > 0 THEN 1 END) as failed_symbols
FROM source.extraction_watermarks 
WHERE table_name = 'balance_sheet';

-- Recent balance sheet processing (last 24h)
SELECT COUNT(*) as processed_last_24h
FROM source.extraction_watermarks 
WHERE table_name = 'balance_sheet' 
AND last_successful_run > NOW() - INTERVAL '24 hours';

-- === SYMBOLS NEEDING PROCESSING ===
-- Next symbols to process (similar to WatermarkManager logic)
SELECT 
    ls.symbol_id, 
    ls.symbol, 
    ew.last_successful_run,
    ew.consecutive_failures,
    CASE 
        WHEN ew.last_successful_run IS NULL THEN 'NEW'
        WHEN ew.last_successful_run < NOW() - INTERVAL '24 hours' THEN 'STALE'
        ELSE 'CURRENT'
    END as status
FROM extracted.listing_status ls
LEFT JOIN source.extraction_watermarks ew 
    ON ew.symbol_id = ls.symbol_id 
    AND ew.table_name = 'balance_sheet'
WHERE ls.asset_type = 'Stock'
    AND LOWER(ls.status) = 'active'
    AND ls.symbol NOT LIKE '%WS%'   -- Exclude warrants
    AND ls.symbol NOT LIKE '%R'     -- Exclude rights  
    AND ls.symbol NOT LIKE '%P%'    -- Exclude preferred shares
    AND ls.symbol NOT LIKE '%U'     -- Exclude units (SPACs)
    AND (
        ew.last_successful_run IS NULL  -- Never processed
        OR ew.last_successful_run < NOW() - INTERVAL '24 hours'  -- Stale
    )
    AND COALESCE(ew.consecutive_failures, 0) < 3  -- Not permanently failed
ORDER BY 
    CASE WHEN ew.last_successful_run IS NULL THEN 0 ELSE 1 END,
    COALESCE(ew.last_successful_run, '1900-01-01'::timestamp) ASC,
    LENGTH(ls.symbol) ASC,
    ls.symbol ASC
LIMIT 10;

-- === ECONOMIC INDICATORS STATUS ===
-- Economic indicators recent data
SELECT 
    function_name,
    COUNT(*) as record_count,
    MAX(date) as latest_date,
    (NOW()::date - MAX(date)) as days_old
FROM source.economic_indicators
GROUP BY function_name
ORDER BY latest_date DESC;

-- === TABLE SIZES ===
-- Get sizes of major tables
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables 
WHERE schemaname IN ('source', 'extracted', 'transformed')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
LIMIT 20;
