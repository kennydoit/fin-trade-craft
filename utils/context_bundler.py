#!/usr/bin/env python3
"""
PostgreSQL → Context Bundle Exporter
------------------------------------

Generates a portable bundle from a PostgreSQL schema:
- schema.sql (via pg_dump)
- data_dictionary.md (information_schema + pg_catalog)
- samples/*.csv and *.parquet (random samples per table)
- erd/erd.dot and erd.png (if graphviz/dot available)

Usage (example):
  python export_context_bundle.py \
    --db-url "postgresql+psycopg://USER:PASS@HOST:5432/fin_trade_craft" \
    --schema extracted \
    --output ./context_bundle \
    --sample-rows 200 \
    --pg-dump-path pg_dump \
    --skip-parquet false
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import psycopg2

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False


class ContextBundleExporter:
    """PostgreSQL context bundle exporter for ChatGPT projects."""
    
    def __init__(self, db_url, schema_name, output_dir, sample_rows=200, 
                 pg_dump_path="pg_dump", skip_parquet=False):
        """Initialize the exporter.
        
        Args:
            db_url: PostgreSQL connection URL
            schema_name: Schema name to export
            output_dir: Output directory for bundle
            sample_rows: Number of sample rows per table
            pg_dump_path: Path to pg_dump executable
            skip_parquet: Skip parquet file generation
        """
        self.db_url = db_url
        self.schema_name = schema_name
        self.output_dir = Path(output_dir)
        self.sample_rows = sample_rows
        self.pg_dump_path = pg_dump_path
        self.skip_parquet = skip_parquet and PARQUET_AVAILABLE
        
        # Parse database URL
        parsed = urlparse(db_url)
        self.db_config = {
            'host': parsed.hostname or 'localhost',
            'port': parsed.port or 5432,
            'user': parsed.username,
            'password': parsed.password,
            'database': parsed.path.lstrip('/') if parsed.path else 'postgres'
        }
        
        # Create output directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        (self.output_dir / 'samples').mkdir(exist_ok=True)
        (self.output_dir / 'erd').mkdir(exist_ok=True)
        
    def _get_connection(self):
        """Get database connection."""
        return psycopg2.connect(**self.db_config)
    
    def export_schema(self):
        """Export schema using pg_dump."""
        schema_file = self.output_dir / 'schema.sql'
        
        cmd = [
            self.pg_dump_path,
            f"--host={self.db_config['host']}",
            f"--port={self.db_config['port']}",
            f"--username={self.db_config['user']}",
            f"--schema={self.schema_name}",
            '--schema-only',
            '--no-owner',
            '--no-privileges',
            self.db_config['database']
        ]
        
        env = os.environ.copy()
        if self.db_config['password']:
            env['PGPASSWORD'] = self.db_config['password']
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, env=env)
            if result.returncode != 0:
                print(f"pg_dump error: {result.stderr}")
                return False
            
            with open(schema_file, 'w') as f:
                f.write(result.stdout)
            
            print(f"✓ Schema exported to {schema_file}")
            return True
            
        except Exception as e:
            print(f"Error running pg_dump: {e}")
            return False
    
    def generate_data_dictionary(self):
        """Generate data dictionary from information_schema."""
        dict_file = self.output_dir / 'data_dictionary.md'
        
        with self._get_connection() as conn:
            # Get tables in schema
            tables_query = """
            SELECT table_name, table_type
            FROM information_schema.tables 
            WHERE table_schema = %s
            ORDER BY table_name;
            """
            
            tables_df = pd.read_sql(tables_query, conn, params=[self.schema_name])
            
            # Get columns information
            columns_query = """
            SELECT 
                table_name,
                column_name,
                ordinal_position,
                is_nullable,
                data_type,
                character_maximum_length,
                column_default,
                is_identity,
                identity_generation
            FROM information_schema.columns
            WHERE table_schema = %s
            ORDER BY table_name, ordinal_position;
            """
            
            columns_df = pd.read_sql(columns_query, conn, params=[self.schema_name])
            
            # Get foreign keys
            fk_query = """
            SELECT 
                tc.table_name,
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' 
                AND tc.table_schema = %s;
            """
            
            fk_df = pd.read_sql(fk_query, conn, params=[self.schema_name])
            
            # Get indexes
            indexes_query = """
            SELECT 
                schemaname,
                tablename,
                indexname,
                indexdef
            FROM pg_indexes 
            WHERE schemaname = %s
            ORDER BY tablename, indexname;
            """
            
            indexes_df = pd.read_sql(indexes_query, conn, params=[self.schema_name])
        
        # Generate markdown
        with open(dict_file, 'w') as f:
            f.write(f"# Data Dictionary - {self.schema_name} Schema\n\n")
            f.write(f"Generated for database: {self.db_config['database']}\n\n")
            
            # Tables overview
            f.write("## Tables Overview\n\n")
            f.write("| Table Name | Type | Columns |\n")
            f.write("|------------|------|----------|\n")
            
            for _, table in tables_df.iterrows():
                col_count = len(columns_df[columns_df['table_name'] == table['table_name']])
                f.write(f"| {table['table_name']} | {table['table_type']} | {col_count} |\n")
            
            f.write("\n")
            
            # Detailed table descriptions
            for _, table in tables_df.iterrows():
                table_name = table['table_name']
                f.write(f"## Table: {table_name}\n\n")
                
                # Columns
                table_columns = columns_df[columns_df['table_name'] == table_name]
                f.write("### Columns\n\n")
                f.write("| Column | Type | Nullable | Default | Notes |\n")
                f.write("|--------|------|----------|---------|-------|\n")
                
                for _, col in table_columns.iterrows():
                    col_type = col['data_type']
                    if col['character_maximum_length']:
                        col_type += f"({col['character_maximum_length']})"
                    
                    nullable = "Yes" if col['is_nullable'] == 'YES' else "No"
                    default = col['column_default'] or ""
                    notes = ""
                    if col['is_identity'] == 'YES':
                        notes = "Identity/Auto-increment"
                    
                    f.write(f"| {col['column_name']} | {col_type} | {nullable} | {default} | {notes} |\n")
                
                # Foreign keys
                table_fks = fk_df[fk_df['table_name'] == table_name]
                if not table_fks.empty:
                    f.write("\n### Foreign Keys\n\n")
                    f.write("| Column | References |\n")
                    f.write("|--------|------------|\n")
                    for _, fk in table_fks.iterrows():
                        f.write(f"| {fk['column_name']} | {fk['foreign_table_name']}.{fk['foreign_column_name']} |\n")
                
                # Indexes
                table_indexes = indexes_df[indexes_df['tablename'] == table_name]
                if not table_indexes.empty:
                    f.write("\n### Indexes\n\n")
                    f.write("| Index Name | Definition |\n")
                    f.write("|------------|------------|\n")
                    for _, idx in table_indexes.iterrows():
                        f.write(f"| {idx['indexname']} | {idx['indexdef']} |\n")
                
                f.write("\n---\n\n")
        
        print(f"✓ Data dictionary generated: {dict_file}")
    
    def export_samples(self):
        """Export sample data for each table."""
        with self._get_connection() as conn:
            # Get all tables in schema
            tables_query = """
            SELECT table_name
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            ORDER BY table_name;
            """
            
            tables_df = pd.read_sql(tables_query, conn, params=[self.schema_name])
            
            for _, table in tables_df.iterrows():
                table_name = table['table_name']
                
                try:
                    # Get sample data
                    sample_query = f"""
                    SELECT * FROM {self.schema_name}.{table_name}
                    ORDER BY RANDOM()
                    LIMIT %s;
                    """
                    
                    df = pd.read_sql(sample_query, conn, params=[self.sample_rows])
                    
                    if df.empty:
                        print(f"⚠ Table {table_name} is empty")
                        continue
                    
                    # Export CSV
                    csv_file = self.output_dir / 'samples' / f"{table_name}.csv"
                    df.to_csv(csv_file, index=False)
                    print(f"✓ CSV sample exported: {csv_file}")
                    
                    # Export Parquet if available and not skipped
                    if PARQUET_AVAILABLE and not self.skip_parquet:
                        parquet_file = self.output_dir / 'samples' / f"{table_name}.parquet"
                        df.to_parquet(parquet_file, index=False)
                        print(f"✓ Parquet sample exported: {parquet_file}")
                    
                except Exception as e:
                    print(f"⚠ Error sampling table {table_name}: {e}")
    
    def generate_erd(self):
        """Generate ERD using graphviz if available."""
        # Check if dot is available
        try:
            subprocess.run(['dot', '-V'], capture_output=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("⚠ Graphviz/dot not available, skipping ERD generation")
            return
        
        dot_file = self.output_dir / 'erd' / 'erd.dot'
        png_file = self.output_dir / 'erd' / 'erd.png'
        
        with self._get_connection() as conn:
            # Get tables and columns
            tables_query = """
            SELECT 
                t.table_name,
                c.column_name,
                c.data_type,
                c.is_nullable,
                CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key
            FROM information_schema.tables t
            JOIN information_schema.columns c ON t.table_name = c.table_name
            LEFT JOIN (
                SELECT kcu.table_name, kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                WHERE tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_schema = %s
            ) pk ON c.table_name = pk.table_name AND c.column_name = pk.column_name
            WHERE t.table_schema = %s AND t.table_type = 'BASE TABLE'
            ORDER BY t.table_name, c.ordinal_position;
            """
            
            columns_df = pd.read_sql(tables_query, conn, params=[self.schema_name, self.schema_name])
            
            # Get foreign keys
            fk_query = """
            SELECT 
                tc.table_name as from_table,
                kcu.column_name as from_column,
                ccu.table_name as to_table,
                ccu.column_name as to_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage ccu
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY' 
                AND tc.table_schema = %s;
            """
            
            fk_df = pd.read_sql(fk_query, conn, params=[self.schema_name])
        
        # Generate DOT file
        with open(dot_file, 'w') as f:
            f.write('digraph erd {\n')
            f.write('  rankdir=TB;\n')
            f.write('  node [shape=record];\n\n')
            
            # Generate table nodes
            tables = columns_df['table_name'].unique()
            for table in tables:
                table_cols = columns_df[columns_df['table_name'] == table]
                
                f.write(f'  {table} [label="{{')
                f.write(f'{table}|')
                
                col_parts = []
                for _, col in table_cols.iterrows():
                    col_str = col['column_name']
                    if col['is_primary_key']:
                        col_str = f"+ {col_str}"
                    col_str += f" : {col['data_type']}"
                    col_parts.append(col_str)
                
                f.write('\\l'.join(col_parts))
                f.write('\\l}"];\n')
            
            f.write('\n')
            
            # Generate relationships
            for _, fk in fk_df.iterrows():
                f.write(f'  {fk["from_table"]} -> {fk["to_table"]};\n')
            
            f.write('}\n')
        
        print(f"✓ ERD DOT file generated: {dot_file}")
        
        # Generate PNG
        try:
            subprocess.run([
                'dot', '-Tpng', str(dot_file), '-o', str(png_file)
            ], check=True)
            print(f"✓ ERD PNG generated: {png_file}")
        except subprocess.CalledProcessError as e:
            print(f"⚠ Error generating PNG: {e}")
    
    def export_all(self):
        """Export complete context bundle."""
        print(f"Exporting context bundle to: {self.output_dir}")
        print(f"Schema: {self.schema_name}")
        print(f"Sample rows per table: {self.sample_rows}")
        print()
        
        # Export schema
        if not self.export_schema():
            print("⚠ Schema export failed")
        
        # Generate data dictionary
        try:
            self.generate_data_dictionary()
        except Exception as e:
            print(f"⚠ Data dictionary generation failed: {e}")
        
        # Export samples
        try:
            self.export_samples()
        except Exception as e:
            print(f"⚠ Sample export failed: {e}")
        
        # Generate ERD
        try:
            self.generate_erd()
        except Exception as e:
            print(f"⚠ ERD generation failed: {e}")
        
        print(f"\n✓ Context bundle export completed!")
        print(f"Output directory: {self.output_dir.absolute()}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Export PostgreSQL schema as a context bundle for ChatGPT",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--db-url',
        required=True,
        help='PostgreSQL connection URL (postgresql+psycopg://USER:PASS@HOST:PORT/DB)'
    )
    
    parser.add_argument(
        '--schema',
        default='extracted',
        help='Schema name to export (default: extracted)'
    )
    
    parser.add_argument(
        '--output',
        default='./context_bundle',
        help='Output directory (default: ./context_bundle)'
    )
    
    parser.add_argument(
        '--sample-rows',
        type=int,
        default=200,
        help='Number of sample rows per table (default: 200)'
    )
    
    parser.add_argument(
        '--pg-dump-path',
        default='pg_dump',
        help='Path to pg_dump executable (default: pg_dump)'
    )
    
    parser.add_argument(
        '--skip-parquet',
        action='store_true',
        help='Skip parquet file generation'
    )
    
    args = parser.parse_args()
    
    # Check if pyarrow is available
    if not PARQUET_AVAILABLE and not args.skip_parquet:
        print("⚠ PyArrow not available, parquet files will be skipped")
        print("  Install with: pip install pyarrow")
    
    exporter = ContextBundleExporter(
        db_url=args.db_url,
        schema_name=args.schema,
        output_dir=args.output,
        sample_rows=args.sample_rows,
        pg_dump_path=args.pg_dump_path,
        skip_parquet=args.skip_parquet
    )
    
    exporter.export_all()


if __name__ == '__main__':
    main()