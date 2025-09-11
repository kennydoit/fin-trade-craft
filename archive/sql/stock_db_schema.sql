

-- Table for storing symbol metadata
CREATE TABLE IF NOT EXISTS listing_status (
    symbol_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL UNIQUE,
    name            TEXT,
    exchange        TEXT,
    asset_type      TEXT,
    ipo_date        DATE,
    delisting_date  DATE,
    status          TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for storing company overview information
CREATE TABLE IF NOT EXISTS overview (
    overview_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol_id       INTEGER NOT NULL,
    symbol          TEXT NOT NULL,    
    assettype       TEXT,
    name            TEXT,   
    description     TEXT,   
    cik             TEXT,   
    exchange        TEXT,   
    currency        TEXT,   
    country         TEXT,   
    sector          TEXT,   
    industry        TEXT,   
    address         TEXT,   
    officialsite    TEXT,   
    fiscalyearend   TEXT,
    status          TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (symbol_id) REFERENCES listing_status(symbol_id),
    UNIQUE(symbol_id)  -- Ensure one overview record per symbol
);


-- Table for storing time series adjusted data
CREATE TABLE IF NOT EXISTS time_series_daily_adjusted (
    symbol_id         INTEGER NOT NULL,
    symbol            TEXT NOT NULL,
    date              DATE NOT NULL,
    open              REAL,
    high              REAL,
    low               REAL,
    close             REAL,
    adjusted_close    REAL,
    volume            INTEGER,
    dividend_amount   REAL,
    split_coefficient REAL,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (symbol_id) REFERENCES listing_status(symbol_id),
    UNIQUE(symbol_id, date)  -- Ensure one overview record per symbol and date
);

-- Table for storing income statement data
CREATE TABLE IF NOT EXISTS income_statement (
    symbol_id                           INTEGER NOT NULL,
    symbol                              TEXT NOT NULL,
    fiscal_date_ending                  DATE,  -- Allow NULL for empty/error records
    report_type                         TEXT NOT NULL CHECK (report_type IN ('annual', 'quarterly')),
    reported_currency                   TEXT,
    gross_profit                        BIGINT,
    total_revenue                       BIGINT,
    cost_of_revenue                     BIGINT,
    cost_of_goods_and_services_sold     BIGINT,
    operating_income                    BIGINT,
    selling_general_and_administrative  BIGINT,
    research_and_development            BIGINT,
    operating_expenses                  BIGINT,
    investment_income_net               BIGINT,
    net_interest_income                 BIGINT,
    interest_income                     BIGINT,
    interest_expense                    BIGINT,
    non_interest_income                 BIGINT,
    other_non_operating_income          BIGINT,
    depreciation                        BIGINT,
    depreciation_and_amortization       BIGINT,
    income_before_tax                   BIGINT,
    income_tax_expense                  BIGINT,
    interest_and_debt_expense           BIGINT,
    net_income_from_continuing_operations BIGINT,
    comprehensive_income_net_of_tax     BIGINT,
    ebit                                BIGINT,
    ebitda                              BIGINT,
    net_income                          BIGINT,
    api_response_status                 TEXT,
    created_at                          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at                          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (symbol_id) REFERENCES listing_status(symbol_id)
    -- Note: For empty/error records, fiscal_date_ending is NULL
    -- For data records, unique constraint is on (symbol_id, report_type, fiscal_date_ending)
);


CREATE TABLE IF NOT EXISTS balance_sheet (
    symbol_id                               INTEGER NOT NULL,
    symbol                                  TEXT NOT NULL,
    fiscal_date_ending                      DATE,  -- Allow NULL for empty/error records
    report_type                             TEXT NOT NULL CHECK (report_type IN ('annual', 'quarterly')),
    reported_currency                       TEXT,
    total_assets                            BIGINT,
    total_current_assets                    BIGINT,
    cash_and_cash_equivalents_at_carrying_value BIGINT,
    cash_and_short_term_investments         BIGINT,
    inventory                               BIGINT,
    current_net_receivables                 BIGINT,
    total_non_current_assets                BIGINT,
    property_plant_equipment                BIGINT,
    accumulated_depreciation_amortization_ppe BIGINT,
    intangible_assets                       BIGINT,
    intangible_assets_excluding_goodwill    BIGINT,
    goodwill                                BIGINT,
    investments                             BIGINT,
    long_term_investments                   BIGINT,
    short_term_investments                  BIGINT,
    other_current_assets                    BIGINT,
    other_non_current_assets                BIGINT,
    total_liabilities                       BIGINT,
    total_current_liabilities               BIGINT,
    current_accounts_payable                BIGINT,
    deferred_revenue                        BIGINT,
    current_debt                            BIGINT,
    short_term_debt                         BIGINT,
    total_non_current_liabilities           BIGINT,
    capital_lease_obligations               BIGINT,
    long_term_debt                          BIGINT,
    current_long_term_debt                  BIGINT,
    long_term_debt_noncurrent               BIGINT,
    short_long_term_debt_total              BIGINT,
    other_current_liabilities               BIGINT,
    other_non_current_liabilities           BIGINT,
    total_shareholder_equity                BIGINT,
    treasury_stock                          BIGINT,
    retained_earnings                       BIGINT,
    common_stock                            BIGINT,
    common_stock_shares_outstanding         BIGINT,
    api_response_status                     TEXT,
    created_at                              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at                              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (symbol_id) REFERENCES listing_status(symbol_id)
    -- Note: For empty/error records, fiscal_date_ending is NULL
    -- For data records, unique constraint is on (symbol_id, report_type, fiscal_date_ending)
);


CREATE TABLE IF NOT EXISTS cash_flow (
    symbol_id                                       INTEGER NOT NULL,
    symbol                                          TEXT NOT NULL,
    fiscal_date_ending                              DATE,  -- Allow NULL for empty/error records
    report_type                                     TEXT NOT NULL CHECK (report_type IN ('annual', 'quarterly')),
    reported_currency                               TEXT,
    operating_cashflow                              BIGINT,
    payments_for_operating_activities               BIGINT,
    proceeds_from_operating_activities              BIGINT,
    change_in_operating_liabilities                 BIGINT,
    change_in_operating_assets                      BIGINT,
    depreciation_depletion_and_amortization         BIGINT,
    capital_expenditures                            BIGINT,
    change_in_receivables                           BIGINT,
    change_in_inventory                             BIGINT,
    profit_loss                                     BIGINT,
    cashflow_from_investment                        BIGINT,
    cashflow_from_financing                         BIGINT,
    proceeds_from_repayments_of_short_term_debt     BIGINT,
    payments_for_repurchase_of_common_stock         BIGINT,
    payments_for_repurchase_of_equity               BIGINT,
    payments_for_repurchase_of_preferred_stock      BIGINT,
    dividend_payout                                 BIGINT,
    dividend_payout_common_stock                    BIGINT,
    dividend_payout_preferred_stock                 BIGINT,
    proceeds_from_issuance_of_common_stock          BIGINT,
    proceeds_from_issuance_of_long_term_debt_and_capital_securities_net BIGINT,
    proceeds_from_issuance_of_preferred_stock       BIGINT,
    proceeds_from_repurchase_of_equity              BIGINT,
    proceeds_from_sale_of_treasury_stock            BIGINT,
    change_in_cash_and_cash_equivalents             BIGINT,
    change_in_exchange_rate                         BIGINT,
    net_income                                      BIGINT,
    api_response_status                             TEXT,
    created_at                                      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at                                      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (symbol_id) REFERENCES listing_status(symbol_id)
    -- Note: For empty/error records, fiscal_date_ending is NULL
    -- For data records, unique constraint is on (symbol_id, report_type, fiscal_date_ending)
);

CREATE TABLE IF NOT EXISTS commodities (
    commodity_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    commodity_name        TEXT NOT NULL,
    function_name         TEXT NOT NULL,  -- API function name (WTI, BRENT, NATURAL_GAS, etc.)
    date                  DATE,           -- Allow NULL for empty/error records
    interval              TEXT NOT NULL CHECK (interval IN ('daily', 'monthly')),
    unit                  TEXT,
    value                 REAL,
    name                  TEXT,           -- Full name from API response
    api_response_status   TEXT,           -- 'data', 'empty', 'error', 'pass'
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(commodity_name, date, interval)  -- Ensure one record per commodity, date, interval
    -- Note: For empty/error records, date is NULL
    -- For data records, unique constraint is on (commodity_name, date, interval)
);

CREATE TABLE IF NOT EXISTS economic_indicators (
    economic_indicator_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    economic_indicator_name        TEXT NOT NULL,
    function_name         TEXT NOT NULL,  -- API function name (REAL_GDP, TREASURY_YIELD, etc.)
    maturity              TEXT,           -- For Treasury yields (3month, 2year, 5year, 7year, 10year, 30year)
    date                  DATE,           -- Allow NULL for empty/error records
    interval              TEXT NOT NULL CHECK (interval IN ('daily', 'monthly', 'quarterly')),
    unit                  TEXT,
    value                 REAL,
    name                  TEXT,           -- Full name from API response
    api_response_status   TEXT,           -- 'data', 'empty', 'error', 'pass'
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(economic_indicator_name, function_name, maturity, date, interval)  -- Ensure one record per indicator, maturity, date, interval
    -- Note: For empty/error records, date is NULL
    -- For data records, unique constraint includes maturity for Treasury yields
);