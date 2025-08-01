# fin-trade-craft
#### Monorepository to house ETL of stock market data, perform feature engineering, modeling, forecasting, and portfolio optimization.

#### Remote repository: https://github.com/kennydoit/fin-trade-craft.git
<br>

# Environment
- Environment and libraries are all to be managed using UV. 
- Libraries can be added via ```uv pip install``` or ```pip install``` as long as they are also added to pyproject.toml
- Codebase runs on Windows
<br><br>

# Copilot References
- Prompts for Copilot to use for script builds stored in *prompts/*
- All code generated by Copilot to test or scripts to be stored in *copilot_test_scripts/*
- Copilot to minimize the number of diagnostic scripts by submitting quoted code directly to the terminal
<br><br>
  
# Partners
- Database is PostgreSQL 
- Stock API is Alpha Vantage: alphavantage.co
---
<div style="background:white; display:inline-block; padding:5px;">
  <img src="assets\images\pgsql.png" alt="Logo" height="75">
</div>
<div style="background:white; display:inline-block; padding:5px;">
  <img src="assets\images\avwht.png" alt="Logo" height="75">
</div>

<br>

# Directory Structure

```graphql
fin-trade-craft/
├── README.md                       # Guide on how repository works and how Copilot works with codebase
├── pyproject.toml                  # Dependencies 
│
├── archive/                        # This folder may contain outdated instances of scripts and program files. 
│
├── config/                         # Not in use
│
├── copilot_test_scripts/           # Folder for copilot to store all testing scripts
│
├── db/                             # Database-related logic and assets
│   └── schema/                     # DDL scripts, SQL schema definitions
│
├── data_pipeline/                  # Data ingestion and transformation
│   ├── extract/                    # Strictly for Extracting and Loading raw data from AV API
│   │
│   └── transform/
│   
├── features/                       # Feature engineering
│
├── models/                         # Machine learning models
|
├── backtesting/
│ 
├── prompts/                        # Folder containing markdown files with instructions for Copilot
│
├── tests/                          # Unit tests
│  
└── utils/                          # Shared helper functions
```
<br>


# Usage

## Database Monitoring

Monitor database health and statistics with the comprehensive database monitor utility:

```bash
# Quick status overview
python utils/database_monitor.py --quick

# Data freshness report  
python utils/database_monitor.py --freshness

# Detailed analysis for specific table
python utils/database_monitor.py --table commodities

# Full comprehensive report (default)
python utils/database_monitor.py --full
```

The database monitor provides:
- Row counts for all tables
- Data freshness analysis with color-coded status
- Storage usage statistics
- API response status breakdowns
- Null value analysis for key columns
- Last update timestamps

## ETL Data Extraction

Run individual extractors to collect data from Alpha Vantage:

```bash
# Extract commodity data (oil, gas, metals, agriculture)
python data_pipeline/extract/extract_commodities.py

# Extract company overview data
python data_pipeline/extract/extract_overview.py

# Extract financial statements
python data_pipeline/extract/extract_balance_sheet.py
python data_pipeline/extract/extract_cash_flow.py
python data_pipeline/extract/extract_income_statement.py

# Extract stock price data
python data_pipeline/extract/extract_time_series_daily_adjusted.py
```

All extractors use PostgreSQL for data storage and include built-in rate limiting, duplicate prevention, and error handling.

## Code Maintenance

### Linting and Formatting

This project uses **Ruff** for fast Python linting and **Black** for code formatting to maintain consistent code quality.

#### Install Development Tools

```bash
# Install ruff and black for code maintenance
uv add --dev ruff black

# Or using pip if preferred
pip install ruff black
```

#### Linting with Ruff

```bash
# Lint all Python files in the project
ruff check .

# Lint specific directory
ruff check data_pipeline/

# Lint specific file
ruff check data_pipeline/extract/extract_commodities.py

# Auto-fix linting issues where possible
ruff check . --fix

# Show detailed output with rule explanations
ruff check . --show-source
```

#### Formatting with Black

```bash
# Format all Python files in the project
black .

# Format specific directory
black data_pipeline/

# Format specific file
black data_pipeline/extract/extract_commodities.py

# Check formatting without making changes (dry run)
black . --check

# Show diff of what would be changed
black . --diff
```

#### Combined Workflow

```bash
# Run both linting and formatting in sequence
ruff check . --fix && black .

# Check code quality without making changes
ruff check . && black . --check
```

#### VS Code Integration

For automatic formatting and linting in VS Code, add these settings to `.vscode/settings.json`:

```json
{
    "python.linting.enabled": true,
    "python.linting.ruffEnabled": true,
    "python.formatting.provider": "black",
    "editor.formatOnSave": true,
    "editor.codeActionsOnSave": {
        "source.organizeImports": true
    }
}
```

#### Pre-commit Hooks (Optional)

Install pre-commit hooks to automatically run linting and formatting:

```bash
# Install pre-commit
uv add --dev pre-commit

# Set up pre-commit hooks
pre-commit install

# Run hooks manually on all files
pre-commit run --all-files