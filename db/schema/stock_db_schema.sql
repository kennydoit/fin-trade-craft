

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