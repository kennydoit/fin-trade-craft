"""
Test PostgreSQL connection and create database if needed.
"""

import os

import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def test_postgres_connection():
    """Test PostgreSQL connection and create database."""

    # Load environment variables
    load_dotenv()

    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD")
    database = os.getenv("POSTGRES_DATABASE", "fin_trade_craft")

    if not password:
        print("Error: POSTGRES_PASSWORD not found in environment variables")
        return False

    try:
        # First, connect to the default 'postgres' database to create our database
        print(f"Connecting to PostgreSQL server at {host}:{port}")
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database="postgres",  # Connect to default database first
        )

        # Set autocommit mode to create database
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        # Check if our database exists
        cursor.execute(
            "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (database,)
        )
        exists = cursor.fetchone()

        if not exists:
            print(f"Creating database '{database}'...")
            cursor.execute(f'CREATE DATABASE "{database}"')
            print(f"Database '{database}' created successfully!")
        else:
            print(f"Database '{database}' already exists.")

        cursor.close()
        conn.close()

        # Now test connection to our new database
        print(f"Testing connection to database '{database}'...")
        conn = psycopg2.connect(
            host=host, port=port, user=user, password=password, database=database
        )

        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()
        print(f"PostgreSQL version: {version[0]}")

        cursor.close()
        conn.close()

        print("✅ PostgreSQL connection successful!")
        return True

    except psycopg2.Error as e:
        print(f"❌ PostgreSQL connection failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False


if __name__ == "__main__":
    test_postgres_connection()
