-- Modern source schema for incremental ETL architecture
-- This schema contains cleaned, standardized data from external APIs

-- Create source schema
CREATE SCHEMA IF NOT EXISTS source;

-- Watermarks table for incremental processing
CREATE TABLE IF NOT EXISTS source.extraction_watermarks (
    table_name VARCHAR(50) NOT NULL,           -- Table being tracked
    symbol_id BIGINT NOT NULL,                 -- Symbol being tracked
    last_fiscal_date DATE,                     -- Latest fiscal period processed
    last_successful_run TIMESTAMPTZ,           -- When we last successfully got data
    consecutive_failures INTEGER DEFAULT 0,    -- Track API failures for circuit breaking
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (table_name, symbol_id),
    FOREIGN KEY (symbol_id) REFERENCES extracted.listing_status(symbol_id) ON DELETE CASCADE
);

-- Landing table for raw API responses (audit trail)
CREATE TABLE IF NOT EXISTS source.api_responses_landing (
    landing_id SERIAL PRIMARY KEY,
    table_name VARCHAR(50) NOT NULL,           -- Target table name
    symbol VARCHAR(20) NOT NULL,               -- Symbol processed
    symbol_id BIGINT NOT NULL,                 -- Symbol ID
    api_function VARCHAR(50) NOT NULL,         -- Alpha Vantage function name
    api_response JSONB NOT NULL,               -- Raw API response
    content_hash VARCHAR(32) NOT NULL,         -- MD5 hash of business content
    source_run_id UUID NOT NULL,               -- Run identifier
    response_status VARCHAR(20) NOT NULL,      -- 'success', 'empty', 'error'
    fetched_at TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (symbol_id) REFERENCES extracted.listing_status(symbol_id) ON DELETE CASCADE
);

-- Cash flow table with modern schema
CREATE TABLE IF NOT EXISTS source.cash_flow (
    cash_flow_id SERIAL PRIMARY KEY,
    symbol_id BIGINT NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    fiscal_date_ending DATE NOT NULL,          -- Deterministically parsed date
    report_type VARCHAR(20) NOT NULL,          -- 'quarterly' or 'annual'
    reported_currency VARCHAR(10),
    
    -- Business fields (financial data)
    operating_cashflow DECIMAL(20,2),
    payments_for_operating_activities DECIMAL(20,2),
    proceeds_from_operating_activities DECIMAL(20,2),
    change_in_operating_liabilities DECIMAL(20,2),
    change_in_operating_assets DECIMAL(20,2),
    depreciation_depletion_and_amortization DECIMAL(20,2),
    capital_expenditures DECIMAL(20,2),
    change_in_receivables DECIMAL(20,2),
    change_in_inventory DECIMAL(20,2),
    profit_loss DECIMAL(20,2),
    cashflow_from_investment DECIMAL(20,2),
    cashflow_from_financing DECIMAL(20,2),
    proceeds_from_repayments_of_short_term_debt DECIMAL(20,2),
    payments_for_repurchase_of_common_stock DECIMAL(20,2),
    payments_for_repurchase_of_equity DECIMAL(20,2),
    payments_for_repurchase_of_preferred_stock DECIMAL(20,2),
    dividend_payout DECIMAL(20,2),
    dividend_payout_common_stock DECIMAL(20,2),
    dividend_payout_preferred_stock DECIMAL(20,2),
    proceeds_from_issuance_of_common_stock DECIMAL(20,2),
    proceeds_from_issuance_of_long_term_debt_and_capital_securities_net DECIMAL(20,2),
    proceeds_from_issuance_of_preferred_stock DECIMAL(20,2),
    proceeds_from_repurchase_of_equity DECIMAL(20,2),
    proceeds_from_sale_of_treasury_stock DECIMAL(20,2),
    change_in_cash_and_cash_equivalents DECIMAL(20,2),
    
    -- ETL metadata
    api_response_status VARCHAR(20) NOT NULL,  -- 'pass', 'fail', 'empty'
    content_hash VARCHAR(32) NOT NULL,         -- For change detection
    source_run_id UUID NOT NULL,               -- Links to landing table
    fetched_at TIMESTAMPTZ NOT NULL,           -- When API was called
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Natural key constraint for idempotent upserts
    UNIQUE(symbol_id, fiscal_date_ending, report_type),
    
    FOREIGN KEY (symbol_id) REFERENCES extracted.listing_status(symbol_id) ON DELETE CASCADE
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_source_cash_flow_symbol_date ON source.cash_flow(symbol_id, fiscal_date_ending);
CREATE INDEX IF NOT EXISTS idx_source_cash_flow_fetched_at ON source.cash_flow(fetched_at);
CREATE INDEX IF NOT EXISTS idx_source_cash_flow_content_hash ON source.cash_flow(content_hash);

-- Income statement table with modern schema
CREATE TABLE IF NOT EXISTS source.income_statement (
    income_statement_id SERIAL PRIMARY KEY,
    symbol_id BIGINT NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    fiscal_date_ending DATE NOT NULL,          -- Deterministically parsed date
    report_type VARCHAR(20) NOT NULL,          -- 'quarterly' or 'annual'
    reported_currency VARCHAR(10),
    
    -- Business fields (financial data)
    gross_profit DECIMAL(20,2),
    total_revenue DECIMAL(20,2),
    cost_of_revenue DECIMAL(20,2),
    cost_of_goods_and_services_sold DECIMAL(20,2),
    operating_income DECIMAL(20,2),
    selling_general_and_administrative DECIMAL(20,2),
    research_and_development DECIMAL(20,2),
    operating_expenses DECIMAL(20,2),
    investment_income_net DECIMAL(20,2),
    net_interest_income DECIMAL(20,2),
    interest_income DECIMAL(20,2),
    interest_expense DECIMAL(20,2),
    non_interest_income DECIMAL(20,2),
    other_non_operating_income DECIMAL(20,2),
    depreciation DECIMAL(20,2),
    depreciation_and_amortization DECIMAL(20,2),
    income_before_tax DECIMAL(20,2),
    income_tax_expense DECIMAL(20,2),
    interest_and_debt_expense DECIMAL(20,2),
    net_income_from_continuing_operations DECIMAL(20,2),
    comprehensive_income_net_of_tax DECIMAL(20,2),
    ebit DECIMAL(20,2),
    ebitda DECIMAL(20,2),
    net_income DECIMAL(20,2),
    
    -- ETL metadata
    api_response_status VARCHAR(20) NOT NULL,  -- 'pass', 'fail', 'empty'
    content_hash VARCHAR(32) NOT NULL,         -- For change detection
    source_run_id UUID NOT NULL,               -- Links to landing table
    fetched_at TIMESTAMPTZ NOT NULL,           -- When API was called
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Natural key constraint for idempotent upserts
    UNIQUE(symbol_id, fiscal_date_ending, report_type),
    
    FOREIGN KEY (symbol_id) REFERENCES extracted.listing_status(symbol_id) ON DELETE CASCADE
);

-- Indexes for income statement
CREATE INDEX IF NOT EXISTS idx_source_income_statement_symbol_date ON source.income_statement(symbol_id, fiscal_date_ending);
CREATE INDEX IF NOT EXISTS idx_source_income_statement_fetched_at ON source.income_statement(fetched_at);
CREATE INDEX IF NOT EXISTS idx_source_income_statement_content_hash ON source.income_statement(content_hash);

-- Balance sheet table with modern schema
CREATE TABLE IF NOT EXISTS source.balance_sheet (
    balance_sheet_id SERIAL PRIMARY KEY,
    symbol_id BIGINT NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    fiscal_date_ending DATE NOT NULL,          -- Deterministically parsed date
    report_type VARCHAR(20) NOT NULL,          -- 'quarterly' or 'annual'
    reported_currency VARCHAR(10),
    
    -- Asset fields
    total_assets DECIMAL(28,2),
    total_current_assets DECIMAL(28,2),
    cash_and_cash_equivalents_at_carrying_value DECIMAL(28,2),
    cash_and_short_term_investments DECIMAL(28,2),
    inventory DECIMAL(28,2),
    current_net_receivables DECIMAL(28,2),
    total_non_current_assets DECIMAL(28,2),
    property_plant_equipment DECIMAL(28,2),
    accumulated_depreciation_amortization_ppe DECIMAL(28,2),
    intangible_assets DECIMAL(28,2),
    intangible_assets_excluding_goodwill DECIMAL(28,2),
    goodwill DECIMAL(28,2),
    investments DECIMAL(28,2),
    long_term_investments DECIMAL(28,2),
    short_term_investments DECIMAL(28,2),
    other_current_assets DECIMAL(28,2),
    other_non_current_assets DECIMAL(28,2),
    
    -- Liability fields
    total_liabilities DECIMAL(28,2),
    total_current_liabilities DECIMAL(28,2),
    current_accounts_payable DECIMAL(28,2),
    deferred_revenue DECIMAL(28,2),
    current_debt DECIMAL(28,2),
    short_term_debt DECIMAL(28,2),
    total_non_current_liabilities DECIMAL(28,2),
    capital_lease_obligations DECIMAL(28,2),
    long_term_debt DECIMAL(28,2),
    current_long_term_debt DECIMAL(28,2),
    long_term_debt_noncurrent DECIMAL(28,2),
    short_long_term_debt_total DECIMAL(28,2),
    other_current_liabilities DECIMAL(28,2),
    other_non_current_liabilities DECIMAL(28,2),
    
    -- Equity fields
    total_shareholder_equity DECIMAL(28,2),
    treasury_stock DECIMAL(28,2),
    retained_earnings DECIMAL(28,2),
    common_stock DECIMAL(28,2),
    common_stock_shares_outstanding DECIMAL(28,2),
    
    -- ETL metadata
    api_response_status VARCHAR(20) NOT NULL,  -- 'pass', 'fail', 'empty'
    content_hash VARCHAR(32) NOT NULL,         -- For change detection
    source_run_id UUID NOT NULL,               -- Links to landing table
    fetched_at TIMESTAMPTZ NOT NULL,           -- When API was called
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Natural key constraint for idempotent upserts
    UNIQUE(symbol_id, fiscal_date_ending, report_type),
    
    FOREIGN KEY (symbol_id) REFERENCES extracted.listing_status(symbol_id) ON DELETE CASCADE
);

-- Indexes for balance sheet
CREATE INDEX IF NOT EXISTS idx_source_balance_sheet_symbol_date ON source.balance_sheet(symbol_id, fiscal_date_ending);
CREATE INDEX IF NOT EXISTS idx_source_balance_sheet_fetched_at ON source.balance_sheet(fetched_at);
CREATE INDEX IF NOT EXISTS idx_source_balance_sheet_content_hash ON source.balance_sheet(content_hash);

CREATE INDEX IF NOT EXISTS idx_source_watermarks_table_symbol ON source.extraction_watermarks(table_name, symbol_id);
CREATE INDEX IF NOT EXISTS idx_source_watermarks_last_run ON source.extraction_watermarks(last_successful_run);

CREATE INDEX IF NOT EXISTS idx_source_landing_symbol_fetched ON source.api_responses_landing(symbol_id, fetched_at);
CREATE INDEX IF NOT EXISTS idx_source_landing_content_hash ON source.api_responses_landing(content_hash);

-- Comments for documentation
COMMENT ON SCHEMA source IS 'Clean, standardized data from external APIs with incremental processing support';
COMMENT ON TABLE source.extraction_watermarks IS 'Tracks processing progress for incremental ETL';
COMMENT ON TABLE source.api_responses_landing IS 'Audit trail of raw API responses';
COMMENT ON TABLE source.cash_flow IS 'Clean cash flow data with deterministic natural keys';
COMMENT ON TABLE source.balance_sheet IS 'Clean balance sheet data with deterministic natural keys';
