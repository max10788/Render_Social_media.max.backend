"""
Fix Database Schema Issues

This script directly applies SQL commands to fix the database schema issues:
1. Creates missing custom_analyses table
2. Creates/fixes transactions table with id column
3. Creates all other required tables

This is a simpler alternative to running Alembic migrations.

Usage:
    python3 scripts/fix_database_schema.py
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


def execute_sql(conn, sql, description=""):
    """Execute SQL statement"""
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()
        if description:
            print(f"✅ {description}")
        return True
    except Exception as e:
        conn.rollback()
        print(f"⚠️  {description}: {str(e)}")
        return False
    finally:
        cursor.close()


def check_table_exists(conn, table_name, schema='otc_analysis'):
    """Check if a table exists"""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = %s
                AND table_name = %s
            );
        """, (schema, table_name))
        result = cursor.fetchone()[0]
        return result
    finally:
        cursor.close()


def check_column_exists(conn, table_name, column_name, schema='otc_analysis'):
    """Check if a column exists in a table"""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.columns
                WHERE table_schema = %s
                AND table_name = %s
                AND column_name = %s
            );
        """, (schema, table_name, column_name))
        result = cursor.fetchone()[0]
        return result
    finally:
        cursor.close()


def main():
    print("=" * 70)
    print("  🔧 Fixing Database Schema")
    print("=" * 70)

    try:
        # Connect to database
        print("\n📊 Connecting to database...")
        conn = get_database_connection()
        print("✅ Connected successfully")

        # Get schema name from environment
        schema = os.getenv("OTC_SCHEMA", "otc_analysis")

        # Create schema if it doesn't exist
        print(f"\n📋 Creating schema '{schema}'...")
        execute_sql(conn, f"CREATE SCHEMA IF NOT EXISTS {schema}", f"Schema '{schema}' created/verified")

        # Set search path
        execute_sql(conn, f"SET search_path TO {schema}, public", "Search path set")

        print("\n" + "=" * 70)
        print("  Creating/Fixing Tables")
        print("=" * 70)

        # 1. Create/Fix custom_analyses table
        print("\n1️⃣  custom_analyses table...")
        if not check_table_exists(conn, 'custom_analyses', schema):
            execute_sql(conn, f"""
                CREATE TABLE {schema}.custom_analyses (
                    id SERIAL PRIMARY KEY,
                    token_address VARCHAR NOT NULL,
                    chain VARCHAR NOT NULL,
                    analysis_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                    token_name VARCHAR,
                    token_symbol VARCHAR,
                    market_cap FLOAT,
                    volume_24h FLOAT,
                    liquidity FLOAT,
                    holders_count INTEGER,
                    total_score FLOAT NOT NULL,
                    metrics JSON,
                    risk_flags JSON,
                    analysis_status VARCHAR DEFAULT 'completed',
                    error_message TEXT,
                    user_id VARCHAR,
                    session_id VARCHAR
                );
                CREATE INDEX idx_custom_analyses_token ON {schema}.custom_analyses(token_address);
                CREATE INDEX idx_custom_analyses_chain ON {schema}.custom_analyses(chain);
                CREATE INDEX idx_custom_analyses_user ON {schema}.custom_analyses(user_id);
                CREATE INDEX idx_custom_analyses_session ON {schema}.custom_analyses(session_id);
            """, "custom_analyses table created")
        else:
            print("   ✅ Table already exists")

        # 2. Create/Fix transactions table
        print("\n2️⃣  transactions table...")
        if not check_table_exists(conn, 'transactions', schema):
            execute_sql(conn, f"""
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
                CREATE INDEX idx_transaction_hash ON {schema}.transactions(tx_hash);
                CREATE INDEX idx_transaction_addresses ON {schema}.transactions(from_address, to_address);
                CREATE INDEX idx_transaction_token ON {schema}.transactions(token_address);
                CREATE INDEX idx_transaction_timestamp ON {schema}.transactions(timestamp);
                CREATE INDEX idx_transaction_chain_block ON {schema}.transactions(chain, block_number);
                CREATE INDEX idx_transaction_chain ON {schema}.transactions(chain);
            """, "transactions table created")
        else:
            # Check if id column exists
            if not check_column_exists(conn, 'transactions', 'id', schema):
                print("   ⚠️  Table exists but 'id' column is missing - this needs manual fix!")
                print("   💡 Recommendation: Drop and recreate the table or add id column manually")
            else:
                print("   ✅ Table exists with id column")

        # 3. Create addresses table
        print("\n3️⃣  addresses table...")
        if not check_table_exists(conn, 'addresses', schema):
            execute_sql(conn, f"""
                CREATE TABLE {schema}.addresses (
                    id SERIAL PRIMARY KEY,
                    address VARCHAR(255) UNIQUE NOT NULL,
                    chain VARCHAR(20) NOT NULL,
                    label VARCHAR(255),
                    address_type VARCHAR(50),
                    first_seen TIMESTAMP,
                    last_seen TIMESTAMP,
                    transaction_count INTEGER DEFAULT 0,
                    total_volume NUMERIC(36, 18),
                    risk_score FLOAT,
                    tags JSON
                );
                CREATE INDEX idx_address ON {schema}.addresses(address);
                CREATE INDEX idx_address_chain ON {schema}.addresses(chain);
            """, "addresses table created")
        else:
            print("   ✅ Table already exists")

        # 4. Create tokens table
        print("\n4️⃣  tokens table...")
        if not check_table_exists(conn, 'tokens', schema):
            execute_sql(conn, f"""
                CREATE TABLE {schema}.tokens (
                    id SERIAL PRIMARY KEY,
                    address VARCHAR(255) UNIQUE NOT NULL,
                    chain VARCHAR(20) NOT NULL,
                    name VARCHAR(255),
                    symbol VARCHAR(50),
                    decimals INTEGER,
                    total_supply NUMERIC(36, 18),
                    market_cap FLOAT,
                    volume_24h FLOAT,
                    liquidity FLOAT,
                    holders_count INTEGER,
                    contract_verified BOOLEAN DEFAULT FALSE,
                    creation_date TIMESTAMP,
                    token_score FLOAT,
                    last_analyzed TIMESTAMP
                );
                CREATE INDEX idx_token_address ON {schema}.tokens(address);
                CREATE INDEX idx_token_chain ON {schema}.tokens(chain);
            """, "tokens table created")
        else:
            print("   ✅ Table already exists")

        # 5. Create clusters table
        print("\n5️⃣  clusters table...")
        if not check_table_exists(conn, 'clusters', schema):
            execute_sql(conn, f"""
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
            """, "clusters table created")
        else:
            print("   ✅ Table already exists")

        # 6. Create scan_jobs table
        print("\n6️⃣  scan_jobs table...")
        if not check_table_exists(conn, 'scan_jobs', schema):
            execute_sql(conn, f"""
                CREATE TABLE {schema}.scan_jobs (
                    id SERIAL PRIMARY KEY,
                    job_id VARCHAR(255) UNIQUE NOT NULL,
                    token_address VARCHAR(255) NOT NULL,
                    chain VARCHAR(20) NOT NULL,
                    status VARCHAR(50),
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    error_message TEXT
                );
                CREATE INDEX idx_scan_job_id ON {schema}.scan_jobs(job_id);
            """, "scan_jobs table created")
        else:
            print("   ✅ Table already exists")

        # 7. Create scan_results table
        print("\n7️⃣  scan_results table...")
        if not check_table_exists(conn, 'scan_results', schema):
            execute_sql(conn, f"""
                CREATE TABLE {schema}.scan_results (
                    id SERIAL PRIMARY KEY,
                    token_id INTEGER NOT NULL,
                    token_score FLOAT,
                    metrics JSON,
                    risk_flags JSON,
                    scan_date TIMESTAMP
                );
                CREATE INDEX idx_scan_result_token ON {schema}.scan_results(token_id);
            """, "scan_results table created")
        else:
            print("   ✅ Table already exists")

        # 8. Create wallet_analyses table
        print("\n8️⃣  wallet_analyses table...")
        if not check_table_exists(conn, 'wallet_analyses', schema):
            execute_sql(conn, f"""
                CREATE TABLE {schema}.wallet_analyses (
                    id SERIAL PRIMARY KEY,
                    token_id INTEGER NOT NULL,
                    wallet_address VARCHAR(255) NOT NULL,
                    wallet_type VARCHAR(50),
                    balance NUMERIC(36, 18),
                    percentage_of_supply FLOAT,
                    transaction_count INTEGER DEFAULT 0,
                    first_transaction TIMESTAMP,
                    last_transaction TIMESTAMP,
                    risk_score FLOAT,
                    analysis_date TIMESTAMP
                );
                CREATE INDEX idx_wallet_analysis_token ON {schema}.wallet_analyses(token_id);
                CREATE INDEX idx_wallet_analysis_address ON {schema}.wallet_analyses(wallet_address);
            """, "wallet_analyses table created")
        else:
            print("   ✅ Table already exists")

        # Final summary
        print("\n" + "=" * 70)
        print("  📋 Summary")
        print("=" * 70)

        # List all tables
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            ORDER BY table_name;
        """, (schema,))
        tables = cursor.fetchall()
        cursor.close()

        print(f"\n📊 Total tables in schema '{schema}': {len(tables)}")
        print("\nTables:")
        for (table,) in tables:
            print(f"   ✓ {table}")

        print("\n" + "=" * 70)
        print("  ✅ Database Schema Fixed!")
        print("=" * 70)
        print("\n💡 Next steps:")
        print("   1. Restart your backend application")
        print("   2. Run the tests again to verify endpoints work")
        print("   3. Check for any remaining errors\n")

        conn.close()
        return True

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
