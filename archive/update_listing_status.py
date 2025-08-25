"""Update listing status data from Alpha Vantage without resetting indexes.

This module fetches the latest listing status information and applies
incremental updates to the existing ``listing_status`` table. The
primary key ``symbol_id`` remains unchanged so that related tables keep
their references intact.

Update rules:
- Insert any newly discovered symbols.
- Skip symbols whose data has not changed.
- Update records when attributes like name or delisting date change.
- Update ``created_at`` and ``updated_at`` timestamps on inserted or
  modified rows.

The implementation reuses the extraction and transformation logic from
``extract_listing_status.py`` and only overrides the loading behaviour
with update semantics.
"""

import sys
from pathlib import Path

import pandas as pd

# Allow imports from project root
sys.path.append(str(Path(__file__).parent.parent.parent))
from data_pipeline.extract.extract_listing_status import (
    ListingStatusExtractor,  # noqa: E402
)


class ListingStatusUpdater(ListingStatusExtractor):
    """Incrementally update the ``listing_status`` table."""

    def __init__(self):
        super().__init__()

    def load_data(self, df: pd.DataFrame) -> None:
        """Update existing listing status records and insert new ones."""
        print("Updating listing_status table...")

        with self.db_manager as db:
            if not db.table_exists("listing_status", schema_name="extracted"):
                raise Exception("Table extracted.listing_status does not exist")

            existing_df = db.fetch_dataframe(
                """
                SELECT symbol_id, symbol, name, exchange, asset_type,
                       ipo_date, delisting_date, status,
                       created_at, updated_at
                FROM extracted.listing_status
                """
            )
            existing_df = existing_df.set_index("symbol")

            columns_to_compare = [
                "name",
                "exchange",
                "asset_type",
                "ipo_date",
                "delisting_date",
                "status",
            ]

            new_records: list[dict[str, object]] = []
            update_records: list[dict[str, object]] = []

            for _, row in df.iterrows():
                symbol = row["symbol"]
                current_values = row[columns_to_compare].to_dict()

                for key, value in current_values.items():
                    if pd.isna(value):
                        current_values[key] = None

                if symbol not in existing_df.index:
                    record = {
                        "symbol": symbol,
                        **current_values,
                        "created_at": row["created_at"],
                        "updated_at": row["updated_at"],
                    }
                    new_records.append(record)
                else:
                    existing_row = existing_df.loc[symbol]
                    changed = False
                    for col in columns_to_compare:
                        existing_val = existing_row[col]
                        if pd.isna(existing_val):
                            existing_val = None
                        if existing_val != current_values[col]:
                            changed = True
                            break
                    if changed:
                        record = {
                            "symbol": symbol,
                            **current_values,
                            "created_at": row["created_at"],
                            "updated_at": row["updated_at"],
                        }
                        update_records.append(record)

            if new_records:
                insert_cols = [
                    "symbol",
                    "name",
                    "exchange",
                    "asset_type",
                    "ipo_date",
                    "delisting_date",
                    "status",
                    "created_at",
                    "updated_at",
                ]
                placeholders = ", ".join(["%s"] * len(insert_cols))
                insert_query = f"""
                    INSERT INTO extracted.listing_status ({', '.join(insert_cols)})
                    VALUES ({placeholders})
                """
                insert_params = [
                    [rec[col] for col in insert_cols] for rec in new_records
                ]
                db.execute_many(insert_query, insert_params)
                print(f"Inserted {len(new_records)} new symbols")
            else:
                print("No new symbols to insert")

            if update_records:
                update_query = """
                    UPDATE extracted.listing_status
                    SET name = %s,
                        exchange = %s,
                        asset_type = %s,
                        ipo_date = %s,
                        delisting_date = %s,
                        status = %s,
                        created_at = %s,
                        updated_at = %s
                    WHERE symbol = %s
                """
                update_params = []
                for rec in update_records:
                    update_params.append(
                        [
                            rec["name"],
                            rec["exchange"],
                            rec["asset_type"],
                            rec.get("ipo_date"),
                            rec.get("delisting_date"),
                            rec["status"],
                            rec["created_at"],
                            rec["updated_at"],
                            rec["symbol"],
                        ]
                    )
                db.execute_many(update_query, update_params)
                print(f"Updated {len(update_records)} existing symbols")
            else:
                print("No existing symbols required updates")

    def run_update(self) -> None:
        """Run the incremental update process."""
        print("Starting listing status update...")
        try:
            raw_data = self.extract_data()
            transformed_data = self.transform_data(raw_data)
            self.load_data(transformed_data)
            print("Update completed successfully!")
        except Exception as exc:
            print(f"Update failed: {exc}")
            raise


def main() -> None:
    updater = ListingStatusUpdater()
    updater.run_update()


if __name__ == "__main__":
    main()
