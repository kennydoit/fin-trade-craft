# Database Ordered Views Reference

## 🎯 Overview
All tables now have corresponding `_ordered` views that provide automatic alphabetical sorting by symbol, with additional date sorting for time-series data.

## 📋 Available Ordered Views

### **Core Reference Tables**
1. **`extracted.listing_status_ordered`**
   - **Sort:** Symbol (alphabetical)
   - **Use:** Browse all symbols A→Z
   - **Example:** `SELECT * FROM extracted.listing_status_ordered`

2. **`extracted.overview_ordered`** 
   - **Sort:** Symbol (alphabetical)
   - **Use:** Company overview data A→Z
   - **Example:** `SELECT * FROM extracted.overview_ordered`

### **Time-Series Data Tables**
3. **`extracted.time_series_ordered`**
   - **Sort:** Symbol (alphabetical) → Date (earliest first)
   - **Use:** Stock price data grouped by symbol, chronological progression
   - **Example:** `SELECT * FROM extracted.time_series_ordered WHERE symbol = 'AAPL'`

### **Financial Statement Tables**
4. **`extracted.income_statement_ordered`**
   - **Sort:** Symbol (alphabetical) → Report Type (Annual, Quarterly) → Fiscal Date (earliest first)
   - **Use:** Income statements grouped by symbol and report type, chronological progression
   - **Example:** `SELECT * FROM extracted.income_statement_ordered WHERE symbol = 'MSFT'`

5. **`extracted.balance_sheet_ordered`**
   - **Sort:** Symbol (alphabetical) → Report Type (Annual, Quarterly) → Fiscal Date (earliest first)
   - **Use:** Balance sheets grouped by symbol and report type, chronological progression
   - **Example:** `SELECT * FROM extracted.balance_sheet_ordered WHERE symbol = 'GOOGL'`

6. **`extracted.cash_flow_ordered`**
   - **Sort:** Symbol (alphabetical) → Report Type (Annual, Quarterly) → Fiscal Date (earliest first)
   - **Use:** Cash flow statements grouped by symbol and report type, chronological progression
   - **Example:** `SELECT * FROM extracted.cash_flow_ordered WHERE symbol = 'TSLA'`

### **Event-Based Tables**
7. **`extracted.insider_transactions_ordered`**
   - **Sort:** Symbol (alphabetical) → Transaction Date (earliest first)
   - **Use:** Insider trading activity by symbol, chronological progression
   - **Example:** `SELECT * FROM extracted.insider_transactions_ordered WHERE symbol = 'NVDA'`

8. **`extracted.earnings_transcripts_ordered`**
   - **Sort:** Symbol (alphabetical) → Updated Date (earliest first)
   - **Use:** Earnings call transcripts by symbol, chronological progression
   - **Example:** `SELECT * FROM extracted.earnings_transcripts_ordered WHERE symbol = 'AAPL'`

## 🎉 Benefits

### **Automatic Alphabetical Ordering**
- **No manual sorting needed** - views open in perfect A→Z order
- **Consistent experience** across all tables
- **Bookmark-friendly** - save these views as your defaults

### **Time-Series Optimization**
- **Symbol grouping** - all data for each symbol appears together
- **Chronological order** - earliest data appears first, showing progression over time
- **Perfect for analysis** - ideal for tracking trends, calculating returns, and time-series modeling

### **Database Performance**
- **Indexed sorting** - uses optimized symbol_id indexes
- **View caching** - PostgreSQL optimizes repeated queries
- **Consistent schema** - all views follow the same pattern

## 💡 Usage Tips

### **In Database Clients (pgAdmin, DBeaver, etc.)**
```sql
-- Instead of the base table:
SELECT * FROM extracted.time_series_daily_adjusted ORDER BY symbol, date ASC;

-- Use the ordered view:
SELECT * FROM extracted.time_series_ordered;
```

### **For Specific Symbol Analysis**
```sql
-- Get AAPL time series (chronological order, earliest first)
SELECT * FROM extracted.time_series_ordered WHERE symbol = 'AAPL';

-- Get AAPL financial statements (grouped by report type, chronological within each)  
SELECT * FROM extracted.income_statement_ordered WHERE symbol = 'AAPL';

-- Get AAPL annual reports only (chronological order)
SELECT * FROM extracted.income_statement_ordered 
WHERE symbol = 'AAPL' AND report_type = 'annual';
```

### **For Cross-Symbol Comparison**
```sql
-- Compare multiple symbols (auto-sorted alphabetically)
SELECT * FROM extracted.time_series_ordered 
WHERE symbol IN ('AAPL', 'MSFT', 'GOOGL') 
AND date >= '2025-01-01';
```

## 🚀 Migration Success Summary

- ✅ **54,690,685 records** successfully migrated
- ✅ **20,627 unique symbols** with calculated symbol IDs
- ✅ **Perfect alphabetical ordering** via symbol_id
- ✅ **8 ordered views** for automatic sorting
- ✅ **Time-series optimization** with date sorting
- ✅ **Zero data loss** - all relationships preserved

## 📊 Symbol ID Algorithm

The migration uses a **Base-27 calculation** that ensures:
- **A** = 15,348,907
- **AA** = 15,880,348  
- **AAA** = 15,900,031
- **AAPL** = 16,204,024
- **MSFT** = 197,765,848

**Result:** `ORDER BY symbol_id` = Perfect alphabetical order A→Z

## 🎯 Next Steps

1. **Bookmark** the `_ordered` views in your database client
2. **Set as defaults** in your analysis workflows  
3. **Use in reports** for consistent symbol ordering
4. **Enjoy automatic alphabetical sorting!** 🎉
