"""
Fix Remaining Database Tables

This script adds missing columns to tables that exist but are incomplete.

Usage:
    python3 scripts/fix_remaining_tables.py
"""
import os
import sys
from pathlib import Path

# Add parent directory to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def get_database_connection():
    """Get a raw database connection"""
    import psycopg2
    from urllib.parse import urlparse

    # Get database URL from environment
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL environment variable not set")

    # Parse the URL
    parsed = urlparse(db_url)

    return psycopg2.connect(
        host=parsed.hostname,
        port=parsed.port or 5432,
        database=parsed.path.lstrip('/'),
        user=parsed.username,
        password=parsed.password
    )


def column_exists(cursor, schema, table, column):
    """Check if a column exists"""
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.columns
            WHERE table_schema = %s
            AND table_name = %s
            AND column_name = %s
        );
    """, (schema, table, column))
    return cursor.fetchone()[0]


def add_column_if_missing(cursor, schema, table, column, definition):
    """Add a column if it doesn't exist"""
    if not column_exists(cursor, schema, table, column):
        try:
            cursor.execute(f"ALTER TABLE {schema}.{table} ADD COLUMN {column} {definition};")
            print(f"   ✅ Added column '{column}' to {table}")
            return True
        except Exception as e:
            print(f"   ❌ Failed to add column '{column}' to {table}: {e}")
            return False
    else:
        print(f"   ℹ️  Column '{column}' already exists in {table}")
        return True


def main():
    print("=" * 70)
    print("  🔧 Fixing Remaining Tables")
    print("=" * 70)

    try:
        # Connect to database
        print("\n📊 Connecting to database...")
        conn = get_database_connection()
        cursor = conn.cursor()
        print("✅ Connected successfully")

        # Get schema name
        schema = os.getenv("OTC_SCHEMA", "otc_analysis")

        # Fix clusters table
        print("\n1️⃣  Fixing clusters table...")
        if not column_exists(cursor, schema, 'clusters', 'id'):
            # Need to recreate with id as primary key
            print("   ⚠️  Need to recreate table with id column...")
            cursor.execute(f"DROP TABLE IF EXISTS {schema}.clusters CASCADE;")
            cursor.execute(f"""
                CREATE TABLE {schema}.clusters (
                    id SERIAL PRIMARY KEY,
                    cluster_id VARCHAR(255) UNIQUE NOT NULL,
                    label VARCHAR(255),
                    cluster_type VARCHAR(50),
                    member_count INTEGER DEFAULT 0,
                    total_volume NUMERIC(36, 18),
                    risk_score FLOAT,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                );
                CREATE INDEX idx_cluster_id ON {schema}.clusters(cluster_id);
            """)
            print("   ✅ Clusters table recreated with id column")
        else:
            print("   ✅ Clusters table already has id column")

        # Fix scan_jobs table
        print("\n2️⃣  Fixing scan_jobs table...")

        # Check current structure
        cursor.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
            AND table_name = 'scan_jobs'
            ORDER BY ordinal_position;
        """)
        current_columns = [row[0] for row in cursor.fetchall()]
        print(f"   Current columns: {', '.join(current_columns)}")

        # Add missing columns
        add_column_if_missing(cursor, schema, 'scan_jobs', 'job_id', 'VARCHAR(255) UNIQUE')
        add_column_if_missing(cursor, schema, 'scan_jobs', 'token_address', 'VARCHAR(255)')
        add_column_if_missing(cursor, schema, 'scan_jobs', 'chain', 'VARCHAR(20)')
        add_column_if_missing(cursor, schema, 'scan_jobs', 'status', 'VARCHAR(50)')
        add_column_if_missing(cursor, schema, 'scan_jobs', 'started_at', 'TIMESTAMP')
        add_column_if_missing(cursor, schema, 'scan_jobs', 'completed_at', 'TIMESTAMP')
        add_column_if_missing(cursor, schema, 'scan_jobs', 'error_message', 'TEXT')

        # Fix scan_results table
        print("\n3️⃣  Fixing scan_results table...")

        # Check current structure
        cursor.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
            AND table_name = 'scan_results'
            ORDER BY ordinal_position;
        """)
        current_columns = [row[0] for row in cursor.fetchall()]
        print(f"   Current columns: {', '.join(current_columns)}")

        # Add missing columns
        add_column_if_missing(cursor, schema, 'scan_results', 'token_score', 'FLOAT')
        add_column_if_missing(cursor, schema, 'scan_results', 'metrics', 'JSON')
        add_column_if_missing(cursor, schema, 'scan_results', 'risk_flags', 'JSON')
        add_column_if_missing(cursor, schema, 'scan_results', 'scan_date', 'TIMESTAMP')

        # Commit all changes
        conn.commit()
        print("\n✅ All changes committed")

        # Verify the fixes
        print("\n" + "=" * 70)
        print("  Verification")
        print("=" * 70)

        tables_to_verify = ['clusters', 'scan_jobs', 'scan_results']
        for table in tables_to_verify:
            cursor.execute(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = '{schema}'
                AND table_name = '{table}'
                ORDER BY ordinal_position;
            """)
            columns = cursor.fetchall()
            print(f"\n📋 {table}:")
            for col_name, data_type in columns:
                print(f"   ✓ {col_name:<25} {data_type}")

        cursor.close()
        conn.close()

        print("\n" + "=" * 70)
        print("  ✅ All Tables Fixed!")
        print("=" * 70)
        print("\n💡 Next steps:")
        print("   1. Run verification: python3 scripts/verify_database_schema.py")
        print("   2. Restart your backend application")
        print("   3. Test all endpoints\n")

        return True

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
