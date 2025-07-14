import os
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv


class PostgresDatabaseManager:
    """A class to manage PostgreSQL database connections and queries."""

    def __init__(self, db_config=None):
        """Initialize with database configuration.

        Args:
            db_config (dict, optional): Database configuration dict with keys:
                host, port, user, password, database
                If None, will load from environment variables.
        """
        load_dotenv()

        if db_config:
            self.config = db_config
        else:
            # Load from environment variables
            self.config = {
                "host": os.getenv("POSTGRES_HOST", "localhost"),
                "port": os.getenv("POSTGRES_PORT", "5432"),
                "user": os.getenv("POSTGRES_USER", "postgres"),
                "password": os.getenv("POSTGRES_PASSWORD"),
                "database": os.getenv("POSTGRES_DATABASE", "fin_trade_craft"),
            }

        if not self.config["password"]:
            raise ValueError("PostgreSQL password must be provided")

        self.connection = None

    def connect(self):
        """Connect to the PostgreSQL database."""
        try:
            self.connection = psycopg2.connect(
                host=self.config["host"],
                port=self.config["port"],
                user=self.config["user"],
                password=self.config["password"],
                database=self.config["database"],
            )
            # Set autocommit to False for transaction control
            self.connection.autocommit = False
            print(
                f"Connected to PostgreSQL database '{self.config['database']}' at {self.config['host']}:{self.config['port']}"
            )
        except psycopg2.Error as e:
            raise Exception(f"Failed to connect to PostgreSQL: {e}")

    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            print("PostgreSQL database connection closed.")

    def execute_query(self, query, params=None):
        """Execute a query against the database."""
        if not self.connection:
            raise Exception("Database connection is not established.")

        cursor = self.connection.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            self.connection.commit()
            print(f"Executed query: {query[:50]}...")

            # For SELECT queries, return results
            if query.strip().upper().startswith("SELECT"):
                return cursor.fetchall()
            return cursor.rowcount

        except psycopg2.Error as e:
            self.connection.rollback()
            raise Exception(f"Query execution failed: {e}")
        finally:
            cursor.close()

    def execute_many(self, query, params_list):
        """Execute a query with multiple parameter sets."""
        if not self.connection:
            raise Exception("Database connection is not established.")

        cursor = self.connection.cursor()
        try:
            cursor.executemany(query, params_list)
            self.connection.commit()
            print(
                f"Executed batch query: {query[:50]}... with {len(params_list)} records"
            )
            return cursor.rowcount

        except psycopg2.Error as e:
            self.connection.rollback()
            raise Exception(f"Batch query execution failed: {e}")
        finally:
            cursor.close()

    def fetch_query(self, query, params=None):
        """Execute a SELECT query and return results."""
        if not self.connection:
            raise Exception("Database connection is not established.")

        cursor = self.connection.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            return cursor.fetchall()

        except psycopg2.Error as e:
            raise Exception(f"Query fetch failed: {e}")
        finally:
            cursor.close()

    def fetch_dataframe(self, query, params=None):
        """Execute a SELECT query and return results as a pandas DataFrame."""
        if not self.connection:
            raise Exception("Database connection is not established.")

        try:
            return pd.read_sql_query(query, self.connection, params=params)
        except Exception as e:
            raise Exception(f"DataFrame query failed: {e}")

    def initialize_schema(self, schema_file_path):
        """Initialize the database schema from a SQL file."""
        if not self.connection:
            self.connect()

        schema_path = Path(schema_file_path)
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")

        with open(schema_path) as file:
            schema_sql = file.read()

        cursor = self.connection.cursor()
        try:
            # Execute the entire schema as one transaction
            cursor.execute(schema_sql)
            self.connection.commit()
            print(f"Database schema initialized from {schema_path}")

        except psycopg2.Error as e:
            self.connection.rollback()
            raise Exception(f"Schema initialization failed: {e}")
        finally:
            cursor.close()

    def table_exists(self, table_name):
        """Check if a table exists in the database."""
        if not self.connection:
            raise Exception("Database connection is not established.")

        cursor = self.connection.cursor()
        try:
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                )
            """,
                (table_name,),
            )

            return cursor.fetchone()[0]

        except psycopg2.Error as e:
            raise Exception(f"Table existence check failed: {e}")
        finally:
            cursor.close()

    def get_table_info(self, table_name):
        """Get column information for a table."""
        if not self.connection:
            raise Exception("Database connection is not established.")

        cursor = self.connection.cursor()
        try:
            cursor.execute(
                """
                SELECT 
                    column_name, 
                    data_type, 
                    is_nullable, 
                    column_default
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = %s
                ORDER BY ordinal_position
            """,
                (table_name,),
            )

            return cursor.fetchall()

        except psycopg2.Error as e:
            raise Exception(f"Table info retrieval failed: {e}")
        finally:
            cursor.close()

    def get_symbol_id(self, symbol):
        """Get symbol_id for a given symbol, or insert if not exists."""
        if not self.connection:
            raise Exception("Database connection is not established.")

        cursor = self.connection.cursor()
        try:
            # First, try to get existing symbol_id
            cursor.execute(
                "SELECT symbol_id FROM listing_status WHERE symbol = %s", (symbol,)
            )
            result = cursor.fetchone()

            if result:
                return result[0]

            # If not found, insert new symbol
            cursor.execute(
                """
                INSERT INTO listing_status (symbol, status, created_at, updated_at) 
                VALUES (%s, 'active', NOW(), NOW()) 
                RETURNING symbol_id
            """,
                (symbol,),
            )

            symbol_id = cursor.fetchone()[0]
            self.connection.commit()

            print(f"Created new symbol_id {symbol_id} for symbol {symbol}")
            return symbol_id

        except psycopg2.Error as e:
            self.connection.rollback()
            raise Exception(f"Symbol ID retrieval/creation failed: {e}")
        finally:
            cursor.close()

    def upsert_data(self, table_name, data_dict, conflict_columns):
        """Insert or update data using PostgreSQL UPSERT (INSERT ... ON CONFLICT)."""
        if not self.connection:
            raise Exception("Database connection is not established.")

        if not data_dict:
            return 0

        cursor = self.connection.cursor()
        try:
            # Build the UPSERT query
            columns = list(data_dict.keys())
            placeholders = ["%s" for _ in columns]
            values = list(data_dict.values())

            # Handle updated_at column
            update_columns = [
                col
                for col in columns
                if col not in conflict_columns and col != "created_at"
            ]
            if "updated_at" not in update_columns:
                update_columns.append("updated_at")

            update_set = []
            for col in update_columns:
                if col == "updated_at":
                    update_set.append(f"{col} = NOW()")
                else:
                    update_set.append(f"{col} = EXCLUDED.{col}")

            conflict_cols = ", ".join(conflict_columns)
            update_clause = ", ".join(update_set)

            query = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
                ON CONFLICT ({conflict_cols})
                DO UPDATE SET {update_clause}
            """

            cursor.execute(query, values)
            self.connection.commit()

            return cursor.rowcount

        except psycopg2.Error as e:
            self.connection.rollback()
            raise Exception(f"Upsert operation failed: {e}")
        finally:
            cursor.close()

    def __enter__(self):
        """Context manager entry."""
        if not self.connection:
            self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# Backward compatibility alias - this allows existing code to work
# by simply changing the import
DatabaseManager = PostgresDatabaseManager
