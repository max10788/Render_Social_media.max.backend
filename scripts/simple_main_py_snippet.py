"""
Admin Endpoint zum Initialisieren der OTC Datenbank
Einfach zu deiner main.py hinzufügen!

USAGE:
1. Code in main.py einfügen (nach app = FastAPI())
2. Deploy zu Render
3. Browser öffnen: https://DEINE-APP.onrender.com/admin/init-database
4. Fertig! ✅
"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
import sys
from pathlib import Path

# Import the init script
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from scripts.init_otc_db import init_database

# Create router
admin_router = APIRouter(prefix="/admin", tags=["admin"])


@admin_router.get("/init-database")
async def initialize_database():
    """
    🚀 Initialize OTC Analysis Database
    
    Creates tables and adds sample data if database is empty.
    Safe to call multiple times - won't duplicate data.
    
    Returns:
        JSON with initialization results
    """
    try:
        # Run initialization
        result = init_database(verbose=False)
        
        if result["success"]:
            return JSONResponse(
                status_code=200,
                content={
                    "status": "success",
                    "message": result["message"],
                    "details": {
                        "created": result.get("created", 0),
                        "skipped": result.get("skipped", 0),
                        "total_wallets": result.get("total_wallets", 0),
                        "total_volume": result.get("total_volume", 0)
                    }
                }
            )
        else:
            raise HTTPException(
                status_code=500,
                detail={
                    "status": "error",
                    "message": result["message"],
                    "error": result.get("error", "Unknown error")
                }
            )
            
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": f"Database initialization failed: {str(e)}"
            }
        )


@admin_router.get("/database-status")
async def database_status():
    """
    Check database status
    
    Returns:
        Current wallet count and volume
    """
    try:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from app.core.otc_analysis.models.wallet import OTCWallet
        import os
        
        # Get database URL
        db_url = os.getenv('DATABASE_URL')
        if db_url.startswith('postgres://'):
            db_url = db_url.replace('postgres://', 'postgresql://', 1)
        
        engine = create_engine(db_url)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Query stats
        total_wallets = session.query(OTCWallet).count()
        all_wallets = session.query(OTCWallet).all()
        total_volume = sum(w.total_volume or 0 for w in all_wallets)
        
        session.close()
        
        return {
            "status": "connected",
            "total_wallets": total_wallets,
            "total_volume": total_volume,
            "message": "Database is accessible"
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": f"Cannot connect to database: {str(e)}"
            }
        )


# ============================================================================
# 📋 IN DEINE main.py EINFÜGEN:
# ============================================================================
"""
# Nach: app = FastAPI()

from admin_init_endpoint import admin_router

# Admin routes registrieren
app.include_router(admin_router)

# Das war's! Jetzt kannst du aufrufen:
# GET https://deine-app.onrender.com/admin/init-database
# GET https://deine-app.onrender.com/admin/database-status
"""
