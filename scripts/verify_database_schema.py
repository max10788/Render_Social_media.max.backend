"""
Verify Database Schema

This script verifies that all required tables and columns exist
and match the SQLAlchemy models.

Usage:
    python3 scripts/verify_database_schema.py
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


def check_table(cursor, schema, table_name, required_columns):
    """Check if a table exists and has required columns"""
    # Check table existence
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = %s
            AND table_name = %s
        );
    """, (schema, table_name))

    table_exists = cursor.fetchone()[0]

    if not table_exists:
        return False, []

    # Get all columns
    cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s
        AND table_name = %s
        ORDER BY ordinal_position;
    """, (schema, table_name))

    columns = cursor.fetchall()
    column_names = [col[0] for col in columns]

    # Check required columns
    missing_columns = [col for col in required_columns if col not in column_names]

    return True, missing_columns


def main():
    print("=" * 70)
    print("  🔍 Database Schema Verification")
    print("=" * 70)

    try:
        # Connect to database
        print("\n📊 Connecting to database...")
        conn = get_database_connection()
        cursor = conn.cursor()
        print("✅ Connected successfully")

        # Get schema name
        schema = os.getenv("OTC_SCHEMA", "otc_analysis")
        print(f"📋 Schema: {schema}\n")

        # Define required tables and their critical columns
        tables_to_check = {
            'custom_analyses': ['id', 'token_address', 'chain', 'total_score', 'analysis_date'],
            'transactions': ['id', 'tx_hash', 'chain', 'from_address', 'timestamp'],
            'addresses': ['id', 'address', 'chain'],
            'tokens': ['id', 'address', 'chain', 'symbol'],
            'clusters': ['id', 'cluster_id'],
            'scan_jobs': ['id', 'job_id', 'token_address'],
            'scan_results': ['id', 'token_id', 'token_score'],
            'wallet_analyses': ['id', 'token_id', 'wallet_address'],
        }

        print("=" * 70)
        print("  Checking Tables")
        print("=" * 70)

        all_good = True
        summary = []

        for table_name, required_cols in tables_to_check.items():
            print(f"\n📋 {table_name}...")

            exists, missing = check_table(cursor, schema, table_name, required_cols)

            if not exists:
                print(f"   ❌ Table does NOT exist!")
                summary.append((table_name, "MISSING", []))
                all_good = False
            elif missing:
                print(f"   ⚠️  Table exists but missing columns: {', '.join(missing)}")
                summary.append((table_name, "INCOMPLETE", missing))
                all_good = False
            else:
                print(f"   ✅ Table exists with all required columns")
                summary.append((table_name, "OK", []))

        # List all tables in schema
        print("\n" + "=" * 70)
        print("  All Tables in Schema")
        print("=" * 70)

        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            ORDER BY table_name;
        """, (schema,))

        all_tables = cursor.fetchall()
        print(f"\n📊 Total tables: {len(all_tables)}\n")
        for (table,) in all_tables:
            status = "✓"
            for check_table_name, check_status, _ in summary:
                if check_table_name == table:
                    if check_status == "OK":
                        status = "✅"
                    elif check_status == "INCOMPLETE":
                        status = "⚠️ "
                    else:
                        status = "❌"
                    break
            print(f"   {status} {table}")

        # Summary
        print("\n" + "=" * 70)
        print("  Summary")
        print("=" * 70)

        ok_count = sum(1 for _, status, _ in summary if status == "OK")
        incomplete_count = sum(1 for _, status, _ in summary if status == "INCOMPLETE")
        missing_count = sum(1 for _, status, _ in summary if status == "MISSING")

        print(f"\n✅ OK:         {ok_count}/{len(tables_to_check)}")
        print(f"⚠️  Incomplete: {incomplete_count}/{len(tables_to_check)}")
        print(f"❌ Missing:    {missing_count}/{len(tables_to_check)}")

        if all_good:
            print("\n" + "=" * 70)
            print("  ✅ All Required Tables and Columns Present!")
            print("=" * 70)
            print("\n💡 Your database schema is correct!")
            print("   All endpoints should now work properly.\n")
        else:
            print("\n" + "=" * 70)
            print("  ⚠️  Some Issues Found")
            print("=" * 70)
            print("\n💡 Run the fix scripts to resolve these issues:")
            print("   - scripts/fix_database_schema.py")
            print("   - scripts/fix_transactions_table.py\n")

        cursor.close()
        conn.close()

        return all_good

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
