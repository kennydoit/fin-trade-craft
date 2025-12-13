"""Debug JHX transformation."""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from transforms.transform_time_series_daily_adjusted import TimeSeriesDailyAdjustedTransformer
import pandas as pd

transformer = TimeSeriesDailyAdjustedTransformer()
transformer.db.connect()

symbol_id = 372856
symbol = 'JHX'

# Check raw data
query = "SELECT COUNT(*) as cnt FROM raw.time_series_daily_adjusted WHERE symbol_id = %s"
df = pd.read_sql(query, transformer.db.connection, params=(str(symbol_id),))
print(f"JHX raw records: {df.iloc[0]['cnt']}")

# Try transformation
result = transformer.transform_symbol(symbol_id, symbol)
if result is not None:
    print(f"Transformed records: {len(result)}")
else:
    print("Transformation returned None")

transformer.db.close()
