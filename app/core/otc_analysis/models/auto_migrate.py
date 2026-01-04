"""
Auto-Migration für Moralis Fields
==================================
Führt Migration automatisch beim App-Start aus.
"""

import logging
from sqlalchemy import text, inspect
from app.core.backend_crypto_tracker.config.database import engine

logger = logging.getLogger(__name__)

def add_moralis_fields_if_missing():
    """
    Fügt entity_label und entity_logo Spalten hinzu, falls sie fehlen.
    Wird automatisch beim App-Start ausgeführt.
    """
    try:
        with engine.connect() as conn:
            # Check ob Spalten existieren
            inspector = inspect(engine)
            columns = [col['name'] for col in inspector.get_columns('otc_wallets')]
            
            needs_migration = False
            
            # entity_label hinzufügen
            if 'entity_label' not in columns:
                logger.info("➕ Adding entity_label column...")
                conn.execute(text("""
                    ALTER TABLE otc_wallets 
                    ADD COLUMN entity_label VARCHAR(255)
                """))
                conn.commit()
                logger.info("   ✅ entity_label added")
                needs_migration = True
            
            # entity_logo hinzufügen
            if 'entity_logo' not in columns:
                logger.info("➕ Adding entity_logo column...")
                conn.execute(text("""
                    ALTER TABLE otc_wallets 
                    ADD COLUMN entity_logo VARCHAR(512)
                """))
                conn.commit()
                logger.info("   ✅ entity_logo added")
                needs_migration = True
            
            if needs_migration:
                logger.info("✅ Moralis fields migration completed!")
            else:
                logger.info("⏭️  Moralis fields already exist")
                
            return True
            
    except Exception as e:
        logger.error(f"❌ Migration failed: {e}")
        # Don't crash the app, just log the error
        return False
