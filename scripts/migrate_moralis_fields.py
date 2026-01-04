#!/usr/bin/env python3
"""
Moralis Fields Migration Script
================================
Fügt entity_label und entity_logo Spalten zur otc_wallets Tabelle hinzu.

Nutzung:
    python scripts/migrate_moralis_fields.py
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy import create_engine, text, inspect
from app.core.backend_crypto_tracker.config.database import get_database_url

def main():
    """Führe Migration aus."""
    
    print("=" * 70)
    print("🔧 Moralis Fields Migration")
    print("=" * 70)
    
    try:
        # Database URL
        database_url = get_database_url()
        print(f"Database: {database_url.split('@')[1] if '@' in database_url else 'configured'}")
        print()
        
        # Create engine
        engine = create_engine(database_url)
        
        with engine.connect() as conn:
            # Check existing columns
            inspector = inspect(engine)
            columns = [col['name'] for col in inspector.get_columns('otc_wallets')]
            
            print(f"📊 Existing columns in otc_wallets: {len(columns)}")
            print()
            
            migrations_done = []
            
            # Add entity_label
            if 'entity_label' not in columns:
                print("➕ Adding entity_label column...")
                conn.execute(text("""
                    ALTER TABLE otc_wallets 
                    ADD COLUMN entity_label VARCHAR(255)
                """))
                conn.commit()
                print("   ✅ entity_label added")
                migrations_done.append('entity_label')
            else:
                print("⏭️  entity_label already exists")
            
            # Add entity_logo
            if 'entity_logo' not in columns:
                print("➕ Adding entity_logo column...")
                conn.execute(text("""
                    ALTER TABLE otc_wallets 
                    ADD COLUMN entity_logo VARCHAR(512)
                """))
                conn.commit()
                print("   ✅ entity_logo added")
                migrations_done.append('entity_logo')
            else:
                print("⏭️  entity_logo already exists")
            
            print()
            print("=" * 70)
            
            if migrations_done:
                print(f"✅ Migration completed! Added: {', '.join(migrations_done)}")
            else:
                print("✅ All columns already exist - no migration needed")
            
            print("=" * 70)
            print()
            print("Next steps:")
            print("1. Restart your application")
            print("2. Check logs for 'Moralis enabled'")
            print("3. Test: GET /discovery/debug/transactions")
            print()
            
            return True
            
    except Exception as e:
        print()
        print("=" * 70)
        print(f"❌ ERROR: {e}")
        print("=" * 70)
        print()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
