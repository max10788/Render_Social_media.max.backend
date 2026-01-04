"""
One-Time Migration Endpoint
============================
Fügt Moralis-Felder zur Datenbank hinzu.
Nach Ausführung: Datei löschen!
"""

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session
import logging

from app.core.backend_crypto_tracker.config.database import get_db

router = APIRouter(tags=["Migration"])
logger = logging.getLogger(__name__)


@router.post("/run-moralis-migration")
async def run_moralis_migration(db: Session = Depends(get_db)):
    """
    🔧 One-Time Migration: Adds entity_label and entity_logo columns.
    
    ⚠️ Run this ONCE, then delete this endpoint!
    
    Steps:
    1. POST to this endpoint
    2. Check response for success
    3. Delete this file (migration.py)
    4. Remove router import from main.py
    """
    
    logger.info("=" * 70)
    logger.info("🔧 MIGRATION: Adding Moralis fields to otc_wallets")
    logger.info("=" * 70)
    
    results = {
        "success": False,
        "entity_label": "not_checked",
        "entity_logo": "not_checked",
        "errors": [],
        "message": ""
    }
    
    try:
        # Check if entity_label exists
        check_label = text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'otc_wallets' 
                AND column_name = 'entity_label'
            )
        """)
        
        has_label = db.execute(check_label).scalar()
        
        # Add entity_label if missing
        if not has_label:
            logger.info("➕ Adding entity_label column...")
            db.execute(text("""
                ALTER TABLE otc_wallets 
                ADD COLUMN entity_label VARCHAR(255)
            """))
            db.commit()
            logger.info("   ✅ entity_label added")
            results["entity_label"] = "added"
        else:
            logger.info("⏭️  entity_label already exists")
            results["entity_label"] = "exists"
        
        # Check if entity_logo exists
        check_logo = text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'otc_wallets' 
                AND column_name = 'entity_logo'
            )
        """)
        
        has_logo = db.execute(check_logo).scalar()
        
        # Add entity_logo if missing
        if not has_logo:
            logger.info("➕ Adding entity_logo column...")
            db.execute(text("""
                ALTER TABLE otc_wallets 
                ADD COLUMN entity_logo VARCHAR(512)
            """))
            db.commit()
            logger.info("   ✅ entity_logo added")
            results["entity_logo"] = "added"
        else:
            logger.info("⏭️  entity_logo already exists")
            results["entity_logo"] = "exists"
        
        # Verify columns
        verify = text("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns 
            WHERE table_name = 'otc_wallets' 
              AND column_name IN ('entity_label', 'entity_logo')
            ORDER BY column_name
        """)
        
        columns = db.execute(verify).fetchall()
        
        results["success"] = True
        results["columns"] = [
            {
                "name": col[0],
                "type": col[1],
                "max_length": col[2]
            }
            for col in columns
        ]
        
        if results["entity_label"] == "added" or results["entity_logo"] == "added":
            results["message"] = "✅ Migration completed! Columns added successfully."
        else:
            results["message"] = "✅ All columns already exist - no migration needed."
        
        logger.info("=" * 70)
        logger.info(results["message"])
        logger.info("=" * 70)
        logger.info("📋 Next steps:")
        logger.info("   1. Delete app/core/otc_analysis/api/migration.py")
        logger.info("   2. Remove router import from main.py")
        logger.info("   3. Redeploy app")
        logger.info("=" * 70)
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Migration failed: {e}", exc_info=True)
        db.rollback()
        
        results["success"] = False
        results["message"] = f"❌ Migration failed: {str(e)}"
        results["errors"].append(str(e))
        
        return results


@router.get("/check-moralis-columns")
async def check_moralis_columns(db: Session = Depends(get_db)):
    """
    🔍 Check if Moralis columns exist.
    
    Use this to verify migration status.
    """
    
    try:
        query = text("""
            SELECT 
                column_name, 
                data_type, 
                character_maximum_length,
                is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'otc_wallets' 
              AND column_name IN ('entity_label', 'entity_logo')
            ORDER BY column_name
        """)
        
        columns = db.execute(query).fetchall()
        
        if not columns:
            return {
                "exists": False,
                "message": "❌ Moralis columns do NOT exist yet",
                "columns": [],
                "action": "Run POST /migration/run-moralis-migration"
            }
        
        return {
            "exists": True,
            "message": "✅ Moralis columns exist",
            "columns": [
                {
                    "name": col[0],
                    "type": col[1],
                    "max_length": col[2],
                    "nullable": col[3]
                }
                for col in columns
            ]
        }
        
    except Exception as e:
        logger.error(f"❌ Check failed: {e}")
        return {
            "exists": False,
            "error": str(e)
        }
