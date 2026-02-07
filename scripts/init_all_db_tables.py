"""
Initialize ALL Database Tables for BlockIntel

This script creates all missing database tables including:
- custom_analyses (for token analysis)
- transactions (with proper id column)
- All other models from the application

Run this script to fix database schema issues.

Usage:
    python scripts/init_all_db_tables.py
"""
import os
import sys
from pathlib import Path
from sqlalchemy import inspect, text

# Add parent directory to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.core.backend_crypto_tracker.config.database import engine, database_config
from app.core.backend_crypto_tracker.processor.database.models import Base as CryptoTrackerBase

# Import OTC models
try:
    from app.core.otc_analysis.models.wallet import Base as OTCBase
    from app.core.otc_analysis.models.wallet import OTCWallet
    from app.core.otc_analysis.models.watchlist import WatchlistItem
    from app.core.otc_analysis.models.alert import Alert
    OTC_AVAILABLE = True
except ImportError as e:
    print(f"⚠️  OTC models not available: {e}")
    OTC_AVAILABLE = False

# Import db_models
try:
    from app.models.db_models import Base as AppBase
    APP_MODELS_AVAILABLE = True
except ImportError as e:
    print(f"⚠️  App models not available: {e}")
    APP_MODELS_AVAILABLE = False


def print_header(text):
    """Print a formatted header"""
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70)


def get_existing_tables():
    """Get list of existing tables in the database"""
    inspector = inspect(engine)
    return inspector.get_table_names(schema=database_config.schema_name)


def check_table_columns(table_name):
    """Check columns in a table"""
    inspector = inspect(engine)
    try:
        columns = inspector.get_columns(table_name, schema=database_config.schema_name)
        return [col['name'] for col in columns]
    except Exception:
        return []


def create_schema():
    """Create database schema if it doesn't exist"""
    print("\n📋 Creating schema...")
    try:
        with engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {database_config.schema_name}"))
            conn.commit()
        print(f"✅ Schema '{database_config.schema_name}' ready")
        return True
    except Exception as e:
        print(f"❌ Error creating schema: {e}")
        return False


def init_crypto_tracker_tables():
    """Initialize crypto tracker tables (transactions, custom_analyses, etc.)"""
    print("\n📊 Initializing Crypto Tracker tables...")

    try:
        # Import all models to ensure they're registered
        from app.core.backend_crypto_tracker.processor.database.models import (
            Address, Cluster, CustomAnalysis, ScanResult, Token,
            Transaction, WalletAnalysisModel, ScanJob
        )

        # Get existing tables before creation
        existing_before = get_existing_tables()

        # Create all tables
        CryptoTrackerBase.metadata.create_all(bind=engine)

        # Get existing tables after creation
        existing_after = get_existing_tables()
        new_tables = set(existing_after) - set(existing_before)

        if new_tables:
            print(f"✅ Created {len(new_tables)} new tables:")
            for table in sorted(new_tables):
                print(f"   ✓ {table}")
        else:
            print("✅ All Crypto Tracker tables already exist")

        # Verify critical tables
        critical_tables = ['custom_analyses', 'transactions']
        for table_name in critical_tables:
            if table_name in existing_after:
                columns = check_table_columns(table_name)
                print(f"✅ Table '{table_name}' exists with columns: {', '.join(columns[:5])}...")

                # Check for id column
                if 'id' in columns:
                    print(f"   ✓ 'id' column present")
                else:
                    print(f"   ⚠️  'id' column MISSING!")
            else:
                print(f"❌ Critical table '{table_name}' not found!")

        return True

    except Exception as e:
        print(f"❌ Error creating Crypto Tracker tables: {e}")
        import traceback
        traceback.print_exc()
        return False


def init_otc_tables():
    """Initialize OTC analysis tables"""
    if not OTC_AVAILABLE:
        print("\n⚠️  Skipping OTC tables (models not available)")
        return True

    print("\n📊 Initializing OTC Analysis tables...")

    try:
        # Get existing tables before creation
        existing_before = get_existing_tables()

        # Create all tables
        OTCBase.metadata.create_all(bind=engine)

        # Get existing tables after creation
        existing_after = get_existing_tables()
        new_tables = set(existing_after) - set(existing_before)

        if new_tables:
            print(f"✅ Created {len(new_tables)} new OTC tables:")
            for table in sorted(new_tables):
                print(f"   ✓ {table}")
        else:
            print("✅ All OTC tables already exist")

        return True

    except Exception as e:
        print(f"❌ Error creating OTC tables: {e}")
        import traceback
        traceback.print_exc()
        return False


def init_app_tables():
    """Initialize app model tables (sentiment_analysis, etc.)"""
    if not APP_MODELS_AVAILABLE:
        print("\n⚠️  Skipping App tables (models not available)")
        return True

    print("\n📊 Initializing App Model tables...")

    try:
        # Get existing tables before creation
        existing_before = get_existing_tables()

        # Create all tables
        AppBase.metadata.create_all(bind=engine)

        # Get existing tables after creation
        existing_after = get_existing_tables()
        new_tables = set(existing_after) - set(existing_before)

        if new_tables:
            print(f"✅ Created {len(new_tables)} new App tables:")
            for table in sorted(new_tables):
                print(f"   ✓ {table}")
        else:
            print("✅ All App tables already exist")

        return True

    except Exception as e:
        print(f"❌ Error creating App tables: {e}")
        import traceback
        traceback.print_exc()
        return False


def verify_database():
    """Verify database connection and list all tables"""
    print("\n🔍 Verifying database...")

    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("✅ Database connection successful")

        # List all tables
        all_tables = get_existing_tables()
        print(f"\n📋 Total tables in schema '{database_config.schema_name}': {len(all_tables)}")

        if all_tables:
            print("\nExisting tables:")
            for table in sorted(all_tables):
                print(f"   • {table}")
        else:
            print("   (no tables found)")

        return True

    except Exception as e:
        print(f"❌ Database verification failed: {e}")
        return False


def main():
    """Main initialization function"""
    print_header("🚀 BlockIntel Database Initialization")

    # Mask password in output
    db_info = f"Host: {database_config.db_host}, DB: {database_config.db_name}"
    print(f"\n📊 Database: {db_info}")
    print(f"🔧 Schema: {database_config.schema_name}")

    # Step 1: Verify connection
    if not verify_database():
        print("\n❌ Cannot connect to database. Exiting.")
        return False

    # Step 2: Create schema
    if not create_schema():
        print("\n❌ Cannot create schema. Exiting.")
        return False

    # Step 3: Initialize all tables
    success = True
    success &= init_crypto_tracker_tables()
    success &= init_otc_tables()
    success &= init_app_tables()

    # Step 4: Final verification
    print_header("📋 Final Verification")
    verify_database()

    # Summary
    print_header("✅ Initialization Complete!")

    if success:
        print("\n✨ All database tables have been created successfully!")
        print("\n💡 Next steps:")
        print("   1. Restart your backend application")
        print("   2. Run tests to verify all endpoints work")
        print("   3. Check logs for any remaining errors")
        return True
    else:
        print("\n⚠️  Some errors occurred during initialization")
        print("   Please review the errors above and fix them")
        return False


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
