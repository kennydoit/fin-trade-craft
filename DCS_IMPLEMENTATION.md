# Data Coverage Score (DCS) & Tiered Universe System

## Overview

This implementation adds intelligent symbol prioritization based on ChatGPT-5's recommendations for optimizing stock data extraction. The system categorizes symbols into three tiers and uses a Data Coverage Score to prioritize extraction resources.

## Key Components

### 1. Universe Management (`utils/universe_management.py`)

**Data Coverage Score (DCS)** - Weighted composite score:
- 35% Fundamentals completeness (recent quarters)
- 20% Insider transaction timeliness 
- 15% Earnings transcript availability
- 15% News velocity (set to 1.0 for now)
- 15% OHLCV data freshness

**Three-Tier Universe System:**
- **Core (400-800 symbols)**: High liquidity, DCS ≥ 0.8, 8+ quarters fundamentals
- **Extended (3-5k symbols)**: DCS ≥ 0.6, 4+ quarters fundamentals  
- **Long Tail (10k+ symbols)**: Everything else, minimal data requirements

### 2. Enhanced Watermark Manager (`utils/incremental_etl.py`)

Added `get_symbols_needing_processing_with_dcs()` method that:
- Prioritizes symbols by DCS score + staleness + tier
- Applies same filtering logic as standard method
- Falls back gracefully if DCS data unavailable

### 3. Updated Extractors

Enhanced `extract_balance_sheet.py` (template for others) with:
- `--use-dcs` flag to enable DCS prioritization
- `--min-dcs` threshold parameter (default: 0.4)
- Graceful fallback to standard method if DCS fails

## Usage Examples

### Universe Management

```bash
# Refresh universe classification for all symbols
python -m utils.universe_management --refresh

# Show top core tier symbols
python -m utils.universe_management --show-core

# Show extraction priority for balance sheet
python -m utils.universe_management --priority balance_sheet
```

### DCS-Prioritized Extraction

```bash
# Balance sheet with DCS prioritization (high-quality symbols only)
python data_pipeline/extract/extract_balance_sheet.py --limit 50 --use-dcs --min-dcs 0.8

# Process 25 symbols with minimum DCS of 0.6
python data_pipeline/extract/extract_balance_sheet.py --limit 25 --use-dcs --min-dcs 0.6

# Standard extraction (fallback)
python data_pipeline/extract/extract_balance_sheet.py --limit 50
```

## Database Schema

### New Table: `source.symbol_universe`
```sql
CREATE TABLE source.symbol_universe (
    symbol_id BIGINT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    current_tier VARCHAR(20) NOT NULL,  -- 'core', 'extended', 'long_tail'
    dcs DECIMAL(5,3) NOT NULL,          -- Data Coverage Score 0-1
    fundamentals_completeness DECIMAL(5,3),
    insider_timeliness DECIMAL(5,3), 
    transcript_availability DECIMAL(5,3),
    news_velocity DECIMAL(5,3),
    ohlcv_freshness DECIMAL(5,3),
    avg_daily_dollar_volume DECIMAL(20,2),
    quarters_of_fundamentals INTEGER,
    liquidity_score DECIMAL(5,3),
    last_updated TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

## Optimization Benefits

### API Call Reduction Strategy
- **Core tier**: Daily extraction (full data)
- **Extended tier**: Every 2-3 days (reduced frequency)
- **Long tail**: Weekly extraction (minimal data)

### Estimated Savings
Based on 4,000+ symbol universe:
- Without optimization: ~16,000 API calls/day
- With tiered approach: ~6,000-8,000 API calls/day
- **50-60% reduction** in API costs and processing time

### Quality Improvements
- Focus resources on high-quality, tradeable symbols
- Avoid wasting API calls on warrants, delisted stocks, etc.
- Prioritize symbols with complete data for better model training

## Implementation Status

✅ **Completed:**
- Universe management system with DCS calculation
- Tiered symbol classification
- Enhanced watermark manager with DCS support
- Updated balance sheet extractor as template
- Database schema and indexing

🔄 **Next Steps:**
- Update remaining extractors (cash_flow, income_statement, insider_transactions)
- Set up monthly universe classification refresh
- Monitor performance improvements and fine-tune DCS weights
- Implement ETF look-through features as mentioned in strategy

## Commands Summary

```bash
# Initialize universe (run once)
python -m utils.universe_management --refresh --limit 1000

# Daily core extraction (high-priority symbols)
python data_pipeline/extract/extract_balance_sheet.py --limit 100 --use-dcs --min-dcs 0.8

# Weekly extended extraction (broader universe)  
python data_pipeline/extract/extract_balance_sheet.py --limit 500 --use-dcs --min-dcs 0.6

# Monthly universe refresh (update classifications)
python -m utils.universe_management --refresh
```

This system provides the foundation for intelligent, cost-effective data extraction that focuses resources on the most valuable symbols while maintaining broad market coverage for model training.
