#!/usr/bin/env python3
"""Load symbol universes into the transformed schema.

This module creates and appends records to the ``transformed.symbol_universes``
table. It accepts input data either as a CSV file path, a SQL query string, or a
:class:`pandas.DataFrame`. Two additional columns are automatically generated
for each load:

* ``universe_id`` – a random UUID shared across all rows in the load
* ``load_date_time`` – the current timestamp in UTC

The required input columns are ``symbol``, ``exchange`` and ``asset_type``. A
universe name must also be supplied. By default the function appends rows to the
existing table. When the ``start_fresh`` flag is set, the table is dropped and
recreated before loading the new records.

The table schema is created as::

    transformed.symbol_universes(
        universe_id       UUID,
        universe_name     VARCHAR,
        symbol            VARCHAR,
        exchange          VARCHAR,
        asset_type        VARCHAR,
        load_date_time    TIMESTAMPTZ,
        symbol_id         BIGINT,
        symbol_universe_id BIGSERIAL PRIMARY KEY
    )

The first six columns appear in the order requested and are followed by helpful
identifiers such as ``symbol_id`` (from ``listing_status``) and a row
identifier ``symbol_universe_id``.
"""

from __future__ import annotations

import argparse
import sys
import uuid
from collections.abc import Iterable, Sequence
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# Allow imports from the repository root
sys.path.append(str(Path(__file__).parent.parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager


def _ensure_table(db: PostgresDatabaseManager) -> None:
    """Create ``transformed.symbol_universes`` if it does not exist."""
    if not db.table_exists("symbol_universes", "transformed"):
        db.execute_query("CREATE SCHEMA IF NOT EXISTS transformed")
        create_sql = """
            CREATE TABLE transformed.symbol_universes (
                universe_id UUID NOT NULL,
                universe_name VARCHAR NOT NULL,
                symbol VARCHAR NOT NULL,
                exchange VARCHAR NOT NULL,
                asset_type VARCHAR NOT NULL,
                load_date_time TIMESTAMPTZ NOT NULL,
                symbol_id BIGINT,
                symbol_universe_id BIGSERIAL PRIMARY KEY
            )
        """
        db.execute_query(create_sql)


def _recreate_table(db: PostgresDatabaseManager) -> None:
    """Drop and recreate ``transformed.symbol_universes``."""
    db.execute_query("DROP TABLE IF EXISTS transformed.symbol_universes")
    _ensure_table(db)


def load_symbol_universe(
    data: pd.DataFrame | str | Path,
    universe_name: str,
    *,
    start_fresh: bool = False,
) -> uuid.UUID:
    """Load a symbol universe into the database.

    Parameters
    ----------
    data:
        Either a DataFrame, path to a CSV file, or a SQL query string
        containing three columns: ``symbol``, ``exchange`` and ``asset_type``.
    universe_name:
        Name of the universe to apply to all rows.
    start_fresh:
        When ``True`` the ``transformed.symbol_universes`` table will be
        dropped and recreated before loading the new data.

    Returns
    -------
    uuid.UUID
        The generated ``universe_id`` applied to the loaded rows.
    """

    if isinstance(data, (str | Path)):
        if Path(str(data)).exists():
            df = pd.read_csv(str(data))
        else:
            db_tmp = PostgresDatabaseManager()
            db_tmp.connect()
            try:
                df = db_tmp.fetch_dataframe(str(data))
            finally:
                db_tmp.close()
    elif isinstance(data, pd.DataFrame):
        df = data.copy()
    else:
        raise TypeError(
            "data must be a pandas DataFrame, path to CSV, or SQL query string"
        )

    required_cols = {"symbol", "exchange", "asset_type"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Input data missing required columns: {missing}")

    universe_id = uuid.uuid4()
    load_time = datetime.now(timezone.utc)

    db = PostgresDatabaseManager()
    db.connect()
    try:
        if start_fresh:
            _recreate_table(db)
        else:
            _ensure_table(db)

        insert_rows: list[Sequence[object]] = []
        for _, row in df.iterrows():
            symbol = row["symbol"]
            symbol_id = db.get_symbol_id(symbol)
            insert_rows.append(
                (
                    str(universe_id),
                    universe_name,
                    symbol,
                    row["exchange"],
                    row["asset_type"],
                    load_time,
                    symbol_id,
                )
            )

        insert_sql = """
            INSERT INTO transformed.symbol_universes (
                universe_id,
                universe_name,
                symbol,
                exchange,
                asset_type,
                load_date_time,
                symbol_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        db.execute_many(insert_sql, insert_rows)
        print(  # noqa: T201
            f"Loaded {len(insert_rows)} rows into transformed.symbol_universes with universe_id {universe_id}"
        )
        return universe_id
    finally:
        db.close()


def _parse_args(argv: Iterable[str]) -> tuple[Path | None, str | None, str, bool]:
    """Parse command line arguments."""

    parser = argparse.ArgumentParser(
        description="Load symbol universe data into transformed.symbol_universes",
    )
    src_group = parser.add_mutually_exclusive_group(required=True)
    src_group.add_argument("--csv", type=Path, help="CSV file containing symbol data")
    src_group.add_argument(
        "--sql",
        help="SQL query returning columns symbol, exchange and asset_type",
    )
    parser.add_argument("--universe-name", required=True, help="Name of the universe")
    parser.add_argument(
        "--start-fresh",
        action="store_true",
        help="Drop and recreate table before loading",
    )
    args = parser.parse_args(list(argv))
    return args.csv, args.sql, args.universe_name, args.start_fresh


def main(argv: Iterable[str] | None = None) -> None:
    """Command line entry point."""
    csv_path, sql_query, universe_name, start_fresh = _parse_args(
        argv or sys.argv[1:]
    )
    if csv_path is not None:
        load_symbol_universe(csv_path, universe_name, start_fresh=start_fresh)
    else:
        load_symbol_universe(sql_query, universe_name, start_fresh=start_fresh)


def test_load_ipo_universe() -> None:
    """Test function to load the IPO universe CSV file."""
    csv_path = Path(__file__).parent.parent / "symbol_universes" / "ipo_before_2020_all_fundamentals_GT500_OCHLV_500MM_to_1B_Net_Income.csv"
    universe_name = "IPO_Before_2020_All_Fundamentals_GT500_OCHLV_500MM_to_1B_Net_Income"
    
    print(f"Loading CSV file: {csv_path}")
    print(f"Universe name: {universe_name}")
    
    try:
        universe_id = load_symbol_universe(
            data=csv_path,
            universe_name=universe_name,
            start_fresh=False  # Set to True if you want to recreate the table
        )
        print(f"✅ Successfully loaded universe with ID: {universe_id}")
    except Exception as e:
        print(f"❌ Error loading universe: {e}")
        raise


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    # Uncomment the line below to test loading the IPO universe using a CSV file
    # test_load_ipo_universe()

    main()
