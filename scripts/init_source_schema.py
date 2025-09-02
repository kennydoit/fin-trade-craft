"""Initialize (create/recreate) the source schema from SQL file and verify."""
from pathlib import Path
import sys

# Ensure project root is on sys.path for db imports
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from db.postgres_database_manager import PostgresDatabaseManager

SCHEMA_FILE = Path(__file__).resolve().parents[1] / "db" / "schema" / "source_schema.sql"


def main():
    print(f"Using schema file: {SCHEMA_FILE}")
    if not SCHEMA_FILE.exists():
        print("ERROR: source_schema.sql not found")
        return 1

    sql = SCHEMA_FILE.read_text(encoding="utf-8")

    with PostgresDatabaseManager() as db:
        try:
            db.execute_script(sql)
            print("✅ Source schema SQL executed")
        except Exception as e:
            print(f"❌ Failed to execute schema SQL: {e}")
            return 1

    with PostgresDatabaseManager() as db:
        schemas = db.fetch_query("SELECT schema_name FROM information_schema.schemata WHERE schema_name='source'")
        if not schemas:
            print("❌ Source schema not found after execution")
            return 1
        tables = db.fetch_query("SELECT table_name FROM information_schema.tables WHERE table_schema='source' ORDER BY table_name")
        print("Source schema tables:")
        for (t,) in tables:
            print(f"  - {t}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
