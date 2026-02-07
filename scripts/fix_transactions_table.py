"""
Fix Transactions Table

This script recreates the transactions table with the proper id column.

WARNING: This will delete all existing transaction data!
Only use this in development/testing environments.

Usage:
    python3 scripts/fix_transactions_table.py
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


def main():
    print("=" * 70)
    print("  🔧 Fixing Transactions Table")
    print("=" * 70)

    try:
        # Connect to database
        print("\n📊 Connecting to database...")
        conn = get_database_connection()
        print("✅ Connected successfully")

        # Get schema name from environment
        schema = os.getenv("OTC_SCHEMA", "otc_analysis")

        cursor = conn.cursor()

        # Check if table has data
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {schema}.transactions;")
            count = cursor.fetchone()[0]
            print(f"\n📊 Current transaction records: {count}")

            if count > 0:
                print("\n⚠️  WARNING: The table contains data!")
                response = input("Do you want to proceed and DELETE all transaction data? (yes/no): ")
                if response.lower() != 'yes':
                    print("❌ Operation cancelled by user")
                    return False
        except Exception as e:
            print(f"⚠️  Could not check existing data: {e}")

        # Drop the old table
        print(f"\n🗑️  Dropping old transactions table...")
        cursor.execute(f"DROP TABLE IF EXISTS {schema}.transactions CASCADE;")
        conn.commit()
        print("✅ Old table dropped")

        # Create the new table with proper structure
        print(f"\n📋 Creating new transactions table with id column...")
        cursor.execute(f"""
            CREATE TABLE {schema}.transactions (
                id SERIAL PRIMARY KEY,
                tx_hash VARCHAR(255) UNIQUE NOT NULL,
                chain VARCHAR(20) NOT NULL,
                block_number INTEGER,
                from_address VARCHAR(255) NOT NULL,
                to_address VARCHAR(255),
                value NUMERIC(36, 18),
                gas_used INTEGER,
                gas_price NUMERIC(36, 18),
                fee NUMERIC(36, 18),
                token_address VARCHAR(255),
                token_amount NUMERIC(36, 18),
                timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                status VARCHAR(20) DEFAULT 'success',
                method VARCHAR(100),
                transaction_metadata JSON
            );
        """)
        conn.commit()
        print("✅ Table created with id column")

        # Create indexes
        print("\n📑 Creating indexes...")
        indexes = [
            ("idx_transaction_hash", "tx_hash"),
            ("idx_transaction_from", "from_address"),
            ("idx_transaction_to", "to_address"),
            ("idx_transaction_token", "token_address"),
            ("idx_transaction_timestamp", "timestamp"),
            ("idx_transaction_chain", "chain"),
        ]

        for idx_name, column in indexes:
            try:
                cursor.execute(f"CREATE INDEX {idx_name} ON {schema}.transactions({column});")
                print(f"   ✓ {idx_name}")
            except Exception as e:
                print(f"   ⚠️  {idx_name}: {e}")

        # Create composite index
        try:
            cursor.execute(f"CREATE INDEX idx_transaction_chain_block ON {schema}.transactions(chain, block_number);")
            print(f"   ✓ idx_transaction_chain_block")
        except Exception as e:
            print(f"   ⚠️  idx_transaction_chain_block: {e}")

        # Create composite index for addresses
        try:
            cursor.execute(f"CREATE INDEX idx_transaction_addresses ON {schema}.transactions(from_address, to_address);")
            print(f"   ✓ idx_transaction_addresses")
        except Exception as e:
            print(f"   ⚠️  idx_transaction_addresses: {e}")

        conn.commit()

        # Verify the table structure
        print("\n🔍 Verifying table structure...")
        cursor.execute(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
            AND table_name = 'transactions'
            ORDER BY ordinal_position;
        """)

        columns = cursor.fetchall()
        print(f"\n📋 Columns in transactions table:")
        for col_name, data_type, nullable in columns:
            nullable_str = "NULL" if nullable == "YES" else "NOT NULL"
            print(f"   ✓ {col_name:<25} {data_type:<20} {nullable_str}")

        # Check for id column specifically
        cursor.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
            AND table_name = 'transactions'
            AND column_name = 'id';
        """)

        if cursor.fetchone():
            print("\n✅ ✅ ✅ ID COLUMN EXISTS! ✅ ✅ ✅")
        else:
            print("\n❌ ID column still missing!")
            return False

        cursor.close()
        conn.close()

        print("\n" + "=" * 70)
        print("  ✅ Transactions Table Fixed!")
        print("=" * 70)
        print("\n💡 Next steps:")
        print("   1. Restart your backend application")
        print("   2. Test transaction endpoints")
        print("   3. All transaction errors should now be resolved\n")

        return True

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
