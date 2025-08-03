"""
Update all extractors to use PostgreSQL instead of SQLite.
This script will modify the extractor files to use PostgresDatabaseManager.
"""

import re
from pathlib import Path


def update_extractor_file(file_path):
    """Update a single extractor file to use PostgreSQL."""

    print(f"Updating {file_path}...")

    with open(file_path) as f:
        content = f.read()

    # Track if any changes were made
    original_content = content

    # Update imports
    content = re.sub(
        r"from db\.database_manager import DatabaseManager",
        "from db.postgres_database_manager import PostgresDatabaseManager",
        content,
    )

    # Update class initialization - remove db_path parameter
    content = re.sub(
        r'def __init__\(self, db_path="[^"]*"\):', "def __init__(self):", content
    )

    # Update DatabaseManager instantiation
    content = re.sub(
        r"self\.db_manager = DatabaseManager\([^)]*\)",
        "self.db_manager = PostgresDatabaseManager()",
        content,
    )

    # Update schema path
    content = re.sub(
        r'/ "db" / "schema" / "stock_db_schema\.sql"',
        '/ "db" / "schema" / "postgres_stock_db_schema.sql"',
        content,
    )

    # Update SQLite-specific INSERT OR REPLACE with PostgreSQL upsert pattern
    # This is more complex as we need to identify the table name and conflict columns

    # Find INSERT OR REPLACE patterns
    insert_or_replace_pattern = (
        r"INSERT OR REPLACE INTO (\w+) \([^)]+\)\s*VALUES \([^)]+\)"
    )

    if "INSERT OR REPLACE" in content:
        print(f"  Found INSERT OR REPLACE pattern in {file_path}")
        # For now, we'll handle this case by case since each table has different conflict columns

        # Replace the common pattern with a comment for manual review
        content = re.sub(
            r'# Prepare insert query\s*columns = list\(df\.columns\)\s*placeholders = \', \'\.join\(\[\'\?\' for _ in columns\]\)\s*insert_query = f"""\s*INSERT OR REPLACE INTO (\w+) \(\{.*?\}\)\s*VALUES \(\{.*?\}\)\s*"""',
            lambda m: f"""# Use PostgreSQL upsert functionality
            # Convert dataframe to list of records for upsert
            for index, row in df.iterrows():
                data_dict = row.to_dict()
                # Remove timestamp columns for upsert - they'll be handled by the database
                data_dict.pop('created_at', None)
                data_dict.pop('updated_at', None)
                
                # TODO: Define appropriate conflict columns for {m.group(1)}
                db.upsert_data('{m.group(1)}', data_dict, ['symbol_id'])  # Update conflict columns as needed""",
            content,
            flags=re.DOTALL,
        )

        # Also replace the execute_many pattern
        content = re.sub(
            r'# Convert dataframe to list of tuples for bulk insert\s*records = df\.to_records\(index=False\)\.tolist\(\)\s*# Execute bulk insert\s*rows_affected = db\.execute_many\(insert_query, records\)\s*print\(f"Successfully loaded \{rows_affected\} records into.*?"',
            """
            print(f"Successfully loaded {len(df)} records into the table")""",
            content,
            flags=re.DOTALL,
        )

    # Update placeholder syntax from ? to %s
    content = re.sub(r"\?", "%s", content)

    # Save the updated file only if changes were made
    if content != original_content:
        with open(file_path, "w") as f:
            f.write(content)
        print(f"  ✅ Updated {file_path}")
        return True
    print(f"  ⏭️  No changes needed for {file_path}")
    return False


def update_all_extractors():
    """Update all extractor files to use PostgreSQL."""

    print("Updating all extractors to use PostgreSQL...\n")

    # Find all extractor files
    extract_dir = Path("data_pipeline/extract")
    extractor_files = list(extract_dir.glob("extract_*.py"))

    updated_files = []
    skipped_files = []

    for file_path in extractor_files:
        if file_path.name == "extract_listing_status.py":
            print(f"⏭️  Skipping {file_path} (already updated)")
            continue

        if update_extractor_file(file_path):
            updated_files.append(file_path)
        else:
            skipped_files.append(file_path)

    print("\n📊 Summary:")
    print(f"  Updated: {len(updated_files)} files")
    print(f"  Skipped: {len(skipped_files)} files")

    if updated_files:
        print("\n✅ Updated files:")
        for file_path in updated_files:
            print(f"  - {file_path}")

    if skipped_files:
        print("\n⏭️  Skipped files:")
        for file_path in skipped_files:
            print(f"  - {file_path}")

    print("\n⚠️  Note: Some extractors may require manual review for:")
    print("  - Specific conflict column definitions")
    print("  - Complex INSERT/UPDATE patterns")
    print("  - Table-specific logic")


if __name__ == "__main__":
    update_all_extractors()
