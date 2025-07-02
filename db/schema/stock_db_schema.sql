

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