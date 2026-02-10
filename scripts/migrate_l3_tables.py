#!/usr/bin/env python3
"""
Database migration script for Level 3 order book tables

Creates the necessary tables and indexes for L3 order storage.
"""
import asyncio
import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from app.core.backend_crypto_tracker.config.database import get_async_db


async def create_l3_tables():
    """Create L3 tables and indexes"""

    # SQL for creating level3_orders table
    create_orders_table = """
    CREATE TABLE IF NOT EXISTS otc_analysis.level3_orders (
        -- Primary key
        id BIGSERIAL PRIMARY KEY,

        -- Exchange & symbol
        exchange VARCHAR(20) NOT NULL,
        symbol VARCHAR(20) NOT NULL,

        -- Order identification
        order_id VARCHAR(100) NOT NULL,
        sequence BIGINT,

        -- Order details
        side VARCHAR(4) NOT NULL,
        price NUMERIC(20, 8) NOT NULL,
        size NUMERIC(20, 8) NOT NULL,

        -- Lifecycle
        event_type VARCHAR(10) NOT NULL,
        timestamp TIMESTAMP NOT NULL,

        -- Optional metadata (exchange-specific)
        metadata JSONB,

        -- Audit
        created_at TIMESTAMP DEFAULT NOW()
    );
    """

    # Indexes for level3_orders
    create_orders_indexes = [
        "CREATE INDEX IF NOT EXISTS idx_l3_exchange_symbol ON otc_analysis.level3_orders(exchange, symbol);",
        "CREATE INDEX IF NOT EXISTS idx_l3_order_id ON otc_analysis.level3_orders(order_id, exchange);",
        "CREATE INDEX IF NOT EXISTS idx_l3_timestamp ON otc_analysis.level3_orders(timestamp DESC);",
        "CREATE INDEX IF NOT EXISTS idx_l3_symbol_time ON otc_analysis.level3_orders(symbol, timestamp DESC);",
        "CREATE INDEX IF NOT EXISTS idx_l3_sequence ON otc_analysis.level3_orders(exchange, symbol, sequence);",
        "CREATE INDEX IF NOT EXISTS idx_l3_order_lifecycle ON otc_analysis.level3_orders(exchange, order_id, timestamp);",
    ]

    # SQL for creating level3_snapshots table
    create_snapshots_table = """
    CREATE TABLE IF NOT EXISTS otc_analysis.level3_snapshots (
        id BIGSERIAL PRIMARY KEY,
        exchange VARCHAR(20) NOT NULL,
        symbol VARCHAR(20) NOT NULL,
        sequence BIGINT NOT NULL,
        timestamp TIMESTAMP NOT NULL,

        -- Snapshot data (compressed JSON)
        bids JSONB NOT NULL,
        asks JSONB NOT NULL,

        -- Statistics
        total_bid_orders INT,
        total_ask_orders INT,
        total_bid_volume NUMERIC(20, 8),
        total_ask_volume NUMERIC(20, 8),

        created_at TIMESTAMP DEFAULT NOW()
    );
    """

    # Indexes for level3_snapshots
    create_snapshots_indexes = [
        "CREATE INDEX IF NOT EXISTS idx_l3_snap_exchange_symbol ON otc_analysis.level3_snapshots(exchange, symbol);",
        "CREATE INDEX IF NOT EXISTS idx_l3_snap_timestamp ON otc_analysis.level3_snapshots(timestamp DESC);",
        "CREATE INDEX IF NOT EXISTS idx_l3_snap_sequence ON otc_analysis.level3_snapshots(exchange, symbol, sequence DESC);",
    ]

    async for session in get_async_db():
        try:
            print("Creating level3_orders table...")
            await session.execute(text(create_orders_table))

            print("Creating indexes for level3_orders...")
            for idx_sql in create_orders_indexes:
                await session.execute(text(idx_sql))

            print("Creating level3_snapshots table...")
            await session.execute(text(create_snapshots_table))

            print("Creating indexes for level3_snapshots...")
            for idx_sql in create_snapshots_indexes:
                await session.execute(text(idx_sql))

            await session.commit()
            print("\n✅ Successfully created L3 tables and indexes!")

            # Verify tables exist
            result = await session.execute(text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'otc_analysis'
                AND table_name IN ('level3_orders', 'level3_snapshots')
            """))
            tables = [row[0] for row in result.fetchall()]
            print(f"\nVerified tables: {tables}")

        except Exception as e:
            await session.rollback()
            print(f"\n❌ Error creating tables: {e}")
            raise
        finally:
            await session.close()

        break  # Only need first session


async def drop_l3_tables():
    """Drop L3 tables (use with caution!)"""

    drop_tables = [
        "DROP TABLE IF EXISTS otc_analysis.level3_orders CASCADE;",
        "DROP TABLE IF EXISTS otc_analysis.level3_snapshots CASCADE;",
    ]

    async for session in get_async_db():
        try:
            print("Dropping L3 tables...")
            for drop_sql in drop_tables:
                await session.execute(text(drop_sql))

            await session.commit()
            print("\n✅ Successfully dropped L3 tables!")

        except Exception as e:
            await session.rollback()
            print(f"\n❌ Error dropping tables: {e}")
            raise
        finally:
            await session.close()

        break


async def check_l3_tables():
    """Check if L3 tables exist and show statistics"""

    async for session in get_async_db():
        try:
            # Check if tables exist
            result = await session.execute(text("""
                SELECT
                    table_name,
                    pg_size_pretty(pg_total_relation_size(quote_ident(table_schema) || '.' || quote_ident(table_name))) AS size
                FROM information_schema.tables
                WHERE table_schema = 'otc_analysis'
                AND table_name IN ('level3_orders', 'level3_snapshots')
                ORDER BY table_name;
            """))

            tables = result.fetchall()

            if not tables:
                print("❌ L3 tables do not exist. Run migration first.")
                return

            print("\n📊 L3 Tables Status:")
            print("=" * 60)
            for table_name, size in tables:
                print(f"  {table_name}: {size}")

            # Get row counts
            orders_count = await session.execute(text(
                "SELECT COUNT(*) FROM otc_analysis.level3_orders"
            ))
            orders_total = orders_count.scalar()

            snapshots_count = await session.execute(text(
                "SELECT COUNT(*) FROM otc_analysis.level3_snapshots"
            ))
            snapshots_total = snapshots_count.scalar()

            print("\n📈 Row Counts:")
            print(f"  level3_orders: {orders_total:,}")
            print(f"  level3_snapshots: {snapshots_total:,}")

            # Get recent orders
            if orders_total > 0:
                recent = await session.execute(text("""
                    SELECT exchange, symbol, COUNT(*) as count, MAX(timestamp) as latest
                    FROM otc_analysis.level3_orders
                    GROUP BY exchange, symbol
                    ORDER BY latest DESC
                    LIMIT 5
                """))

                print("\n🔄 Recent Activity:")
                for row in recent.fetchall():
                    print(f"  {row[0]} {row[1]}: {row[2]:,} orders (latest: {row[3]})")

        except Exception as e:
            print(f"\n❌ Error checking tables: {e}")
        finally:
            await session.close()

        break


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Manage L3 order book tables")
    parser.add_argument(
        "action",
        choices=["create", "drop", "check"],
        help="Action to perform (create/drop/check)"
    )

    args = parser.parse_args()

    if args.action == "create":
        asyncio.run(create_l3_tables())
    elif args.action == "drop":
        confirm = input("⚠️  Are you sure you want to DROP all L3 tables? (yes/no): ")
        if confirm.lower() == "yes":
            asyncio.run(drop_l3_tables())
        else:
            print("Aborted.")
    elif args.action == "check":
        asyncio.run(check_l3_tables())
