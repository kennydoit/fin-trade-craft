# fin-trade-craft Financial Data Engineering Repository

Financial data monorepository for ETL, feature engineering, modeling, forecasting, and portfolio optimization using Alpha Vantage API and PostgreSQL.

Always reference these instructions first and fallback to search or bash commands only when you encounter unexpected information that does not match the info here.

## Working Effectively

### Bootstrap and Environment Setup
- Install Python dependencies:
  - `pip install exchange_calendars numpy pandas pandas_ta psycopg2-binary pyyaml python-dotenv requests ta tabulate urllib3 ruff black` -- takes 5-8 minutes. NEVER CANCEL. Set timeout to 10+ minutes.
  - Preferred: Use UV package manager if available: `uv add [packages]`
- Environment runs on Windows primarily but works on Linux
- Python version: 3.12+ (project.toml specifies 3.13+)

### Required Environment Variables
Set these environment variables in .env file or system environment:
```bash
# Alpha Vantage API (required for data extraction)
ALPHAVANTAGE_API_KEY=your_api_key_here

# PostgreSQL Database (required for database operations)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password_here
POSTGRES_DATABASE=fin_trade_craft
```

### Code Quality and Linting
- `ruff check .` -- takes <1 second. Fast Python linting.
- `ruff check --fix .` -- auto-fix issues where possible
- `black .` -- takes ~2 seconds. Format all Python files.
- `black --check .` -- check formatting without changes
- Combined workflow: `ruff check --fix . && black .`
- **CURRENT STATE**: 744 linting issues remain to be fixed (as of last check)
- ALWAYS run `ruff check --fix . && black .` before committing changes or CI will fail

### Testing and Validation
- No formal test suite in `tests/` directory
- Uses `copilot_test_scripts/` for testing and validation scripts
- Test database operations: Scripts will fail gracefully without proper DB credentials
- Test extractors: Scripts will fail with "ALPHAVANTAGE_API_KEY not found" without API key
- **VALIDATION SCENARIOS**: After making changes, test:
  1. Import functionality: `python -c "import sys; sys.path.append('.'); from data_pipeline.extract.extract_commodities import CommoditiesExtractor; print('Import successful')"`
  2. Database utilities: `python utils/database_monitor.py --help` (should show help without errors)
  3. Linting: `ruff check . && black --check .`

## Running the Application

### Database Monitoring
Monitor database health and statistics:
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
**NOTE**: Requires PostgreSQL environment variables to be set.

### Data Extraction (ETL)
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
**NOTE**: All extractors require ALPHAVANTAGE_API_KEY environment variable.

### Manual Validation Scenarios
When making changes to the codebase:
1. **Import Testing**: Verify modules import without errors
2. **Database Connectivity**: Test utilities can connect (with proper credentials)
3. **API Integration**: Verify extractors handle API key requirements correctly
4. **Code Quality**: Always run linting and formatting before committing

## Common Tasks

### Environment and Dependencies
- **Dependencies**: Install with pip or UV as shown above
- **Environment**: Primarily Windows, works on Linux
- **Virtual Environment**: Uses `.venv` directory (Windows: `.venv\Scripts\activate`)

### Build Process
- **No traditional "build"** - this is a Python data pipeline
- **"Build" consists of**: Installing dependencies and running linting
- **Time expectations**: Dependency install ~8 minutes, linting <5 seconds

### Database Operations
- **Database**: PostgreSQL with safety guidelines to prevent data loss
- **Safety**: NEVER use `TRUNCATE CASCADE` - use `DELETE FROM table` instead
- **Monitoring**: Use database_monitor.py utility for health checks

### Project Structure
```
fin-trade-craft/
├── README.md                     # Main documentation
├── pyproject.toml               # Dependencies and tool configuration
├── .vscode/                     # VS Code settings with Python configuration
├── data_pipeline/               # ETL data processing
│   ├── extract/                 # API data extraction scripts
│   └── transform/               # Data transformation logic
├── db/                          # Database management
│   ├── postgres_database_manager.py
│   └── schema/                  # SQL schema definitions
├── utils/                       # Helper utilities
│   ├── database_monitor.py      # Database health monitoring
│   └── database_safety.py       # Safety operations
├── scripts/                     # Development scripts
│   ├── lint.py                  # Linting script (expects UV)
│   └── fix.py                   # Auto-fix script (expects UV)
├── copilot_test_scripts/        # Testing and validation scripts
└── archive/                     # Deprecated/archived code
```

### Common Command Outputs

#### Repository Root Contents
```
.git/
.gitignore
.python-version
.vscode/
README.md
activate.bat                    # Windows venv activation
activate_env.bat               # Alternative venv activation
pyproject.toml                 # Project configuration
scripts/                       # Development scripts
data_pipeline/                 # ETL pipeline
db/                           # Database management
utils/                        # Utilities
copilot_test_scripts/         # Test scripts
archive/                      # Archived code
```

#### Key Configuration Files
- **pyproject.toml**: Dependencies, ruff/black configuration
- **.vscode/settings.json**: Python interpreter, code runner settings
- **DATABASE_SAFETY_GUIDELINES.md**: Critical safety rules for database operations

### Alpha Vantage API Integration
The repository integrates with Alpha Vantage API for financial data:

| Data Type | Frequency | Size | API Usage |
|-----------|-----------|------|-----------|
| OHLCV | Daily | Large | High |
| Commodities | Daily | Small | Low |
| Economic Indicators | Daily | Small | Low |
| News Stories | Varied | Large | Very High |
| Insider Transactions | Varied | Large | High |
| Earnings Transcripts | Quarterly | Large | Extremely High (~10 calls/symbol) |
| Financial Statements | Quarterly | Medium | High |

### Critical Reminders
- **NEVER CANCEL** dependency installation - takes 5-8 minutes, set timeout to 10+ minutes
- **ALWAYS** run linting before committing: `ruff check --fix . && black .`
- **Database Safety**: Use DELETE, never TRUNCATE CASCADE
- **Environment Variables**: Check ALPHAVANTAGE_API_KEY and PostgreSQL vars before running scripts
- **Import Testing**: Test imports after code changes to catch import errors early
- **Code in Archive**: Files in `archive/` may be outdated and should not be modified

### Debugging Tips
- **Import Errors**: Check sys.path and module structure
- **Database Errors**: Verify environment variables are set correctly
- **API Errors**: Confirm ALPHAVANTAGE_API_KEY is valid and set
- **Linting Issues**: Run `ruff check --show-source` for detailed error explanations
- **VS Code**: Configured for Python development with auto-formatting and linting