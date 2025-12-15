"""
Verify Data Ordering in Time Series Transformations

This script checks if data is properly ordered before backward-looking calculations.
"""
from pathlib import Path

# Read the transformation file
transform_file = Path("transforms/transform_time_series_daily_adjusted.py")
content = transform_file.read_text()

print("=" * 80)
print("DATA ORDERING VERIFICATION")
print("=" * 80)

print("\n1. INITIAL DATA FETCH (SQL Query)")
print("-" * 80)
if "ORDER BY date" in content:
    print("✅ SQL query includes ORDER BY date clause")
    # Find the query
    start = content.find("SELECT symbol_id, symbol, date")
    end = content.find("ORDER BY date", start) + len("ORDER BY date")
    query_section = content[start:end]
    print("\nQuery excerpt:")
    print(query_section[:200] + "...")
else:
    print("❌ SQL query does NOT order by date")

print("\n2. FEATURE CREATION FUNCTIONS")
print("-" * 80)

# Check each feature function
functions = [
    "create_trend_features",
    "create_momentum_features", 
    "create_volatility_features",
    "create_volume_features",
    "create_target_variables"
]

for func_name in functions:
    # Find the function
    func_start = content.find(f"def {func_name}(self, df):")
    if func_start == -1:
        print(f"\n{func_name}:")
        print(f"  ❓ Function not found")
        continue
    
    # Look for sort_values within the function
    func_end = content.find("\n    def ", func_start + 1)
    if func_end == -1:
        func_end = len(content)
    
    func_content = content[func_start:func_end]
    
    print(f"\n{func_name}:")
    if ".sort_values('date')" in func_content:
        print(f"  ✅ Sorts data by date before calculations")
        # Find the line
        sort_line_start = func_content.find(".sort_values('date')")
        context_start = max(0, sort_line_start - 100)
        context = func_content[context_start:sort_line_start + 50]
        print(f"  Code: ...{context.split(chr(10))[-2]}")
    else:
        print(f"  ❌ Does NOT sort data before calculations")

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

sort_count = content.count(".sort_values('date')")
print(f"\nTotal .sort_values('date') calls found: {sort_count}")

if "ORDER BY date" in content and sort_count >= 5:
    print("\n✅ PASS: Data is properly ordered")
    print("   - SQL query orders by date")
    print(f"   - {sort_count} sort operations in feature functions")
    print("\n📊 Backward-looking calculations (SMA, EMA, RSI, etc.) are SAFE")
else:
    print("\n⚠️ WARNING: Potential ordering issues detected")

print("\n" + "=" * 80)
