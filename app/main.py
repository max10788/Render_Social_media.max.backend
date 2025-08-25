from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
import os
from pathlib import Path

# Router-Imports
from app.core.backend_crypto_tracker.api.routes.custom_analysis_routes import (
    router as custom_analysis_router,
)
from app.core.backend_crypto_tracker.api.routes import token_routes
from app.core.backend_crypto_tracker.api.routes import transaction_routes
from app.core.backend_crypto_tracker.api.routes import scanner_routes
# Konfiguration und Datenbank
from app.core.backend_crypto_tracker.config.database import database_config
from app.core.backend_crypto_tracker.processor.database.models.manager import DatabaseManager
from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)

# Frontend-Verzeichnisse konfigurieren
BASE_DIR = Path(__file__).resolve().parent  # app/
FRONTEND_DIR = BASE_DIR / "crypto-token-analysis-dashboard"  # app/crypto-token-analysis-dashboard
# For regular Next.js build (not export)
BUILD_DIR = FRONTEND_DIR / ".next" / "standalone"  # Next.js standalone build
STATIC_DIR = FRONTEND_DIR / ".next" / "static"     # Next.js static files
PUBLIC_DIR = FRONTEND_DIR / "public"               # Next.js public files

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager for FastAPI application."""
    logger.info("Starting Low-Cap Token Analyzer")
    
    # Initialisiere die Datenbank
    try:
        db_manager = DatabaseManager()
        await db_manager.initialize()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        logger.info("Continuing without database...")
    
    yield
    logger.info("Shutting down Low-Cap Token Analyzer")

# ------------------------------------------------------------------
# FastAPI-Instanz
# ------------------------------------------------------------------
app = FastAPI(
    title="Low-Cap Token Analyzer",
    description="Enterprise-grade low-cap cryptocurrency token analysis system",
    version="1.0.0",
    lifespan=lifespan,
)

# ------------------------------------------------------------------
# CORS (Anpassen für Production)
# ------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://render-social-media-max-frontend.onrender.com",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=600,
)

# ------------------------------------------------------------------
# API Routes (mount first to prevent conflicts)
# ------------------------------------------------------------------
app.include_router(custom_analysis_router, prefix="/api/v1")
app.include_router(token_routes.router, prefix="/api/v1")
app.include_router(transaction_routes.router, prefix="/api/v1")
app.include_router(scanner_routes.router, prefix="/api/v1")

# ------------------------------------------------------------------
# API-Health-Check
# ------------------------------------------------------------------
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "version": "1.0.0",
        "services": {
            "token_analyzer": "active",
            "blockchain_tracking": "active",
        },
        "database": {
            "host": database_config.db_host,
            "database": database_config.db_name,
            "schema": database_config.schema_name,
        }
    }

# ------------------------------------------------------------------
# Fehlende API-Routen hinzufügen
# ------------------------------------------------------------------
@app.get("/api/assets")
async def get_assets():
    # Implementieren Sie Ihre Logik hier
    return {"assets": []}

@app.get("/api/config")
async def get_config():
    # Implementieren Sie Ihre Logik hier
    return {"config": {}}

@app.get("/api/analytics")
async def get_analytics():
    # Implementieren Sie Ihre Logik hier
    return {"analytics": {}}

@app.get("/api/settings")
async def get_settings():
    # Implementieren Sie Ihre Logik hier
    return {"settings": {}}

# ------------------------------------------------------------------
# Statische Dateien für Next.js
# ------------------------------------------------------------------
if BUILD_DIR.exists():
    app.mount("/_next", StaticFiles(directory=BUILD_DIR / "_next"), name="next-static")
    
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
    
if PUBLIC_DIR.exists():
    app.mount("/public", StaticFiles(directory=PUBLIC_DIR), name="public")

# ------------------------------------------------------------------
# Angepasster 404-Handler
# ------------------------------------------------------------------
@app.exception_handler(404)
async def not_found_handler(request: Request, exc: Exception):
    if request.url.path.startswith("/api/"):
        return JSONResponse(
            status_code=404,
            content={"error": "API endpoint not found"}
        )
    
    # Fallback-HTML für Nicht-API-Routen
    return HTMLResponse(
        content=f"""
        <html>
            <head>
                <title>Low-Cap Token Analyzer</title>
                <style>
                    body {{
                        font-family: system-ui, sans-serif;
                        max-width: 800px;
                        margin: 0 auto;
                        padding: 2rem;
                        line-height: 1.6;
                    }}
                    .status {{ 
                        background: #f0f9ff;
                        border: 1px solid #0ea5e9;
                        border-radius: 0.5rem;
                        padding: 1rem;
                        margin: 1rem 0;
                    }}
                    .commands {{
                        background: #1f2937;
                        color: #f9fafb;
                        padding: 1rem;
                        border-radius: 0.5rem;
                        font-family: monospace;
                        overflow-x: auto;
                    }}
                </style>
            </head>
            <body>
                <h1>🚀 Low-Cap Token Analyzer</h1>
                
                <div class="status">
                    <strong>Status:</strong> Backend API is running<br>
                    <strong>Health Check:</strong> <a href="/health">/health</a><br>
                    <strong>API Documentation:</strong> <a href="/docs">/docs</a>
                </div>
                
                <h2>Frontend Setup Required</h2>
                <p>The Next.js frontend needs to be built and started separately.</p>
                
                <h3>Development Setup:</h3>
                <div class="commands">
# Navigate to frontend directory<br>
cd app/crypto-token-analysis-dashboard<br><br>
# Install dependencies<br>
npm install<br><br>
# Start development server<br>
npm run dev
                </div>
                
                <h3>Production Setup:</h3>
                <div class="commands">
# Build the application<br>
cd app/crypto-token-analysis-dashboard<br>
npm run build<br><br>
# Start production server<br>
npm run start
                </div>
                
                <p>The frontend will be available at <code>http://localhost:3000</code></p>
                <p>This API backend is running on the current port.</p>
                
                <h3>Alternative: Static Export (Limited)</h3>
                <p>If you need static export, you must implement <code>generateStaticParams()</code> for dynamic routes.</p>
            </body>
        </html>
        """,
        status_code=200
    )

# ------------------------------------------------------------------
# Fallback-Route
# ------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def serve_frontend_fallback(request: Request):
    return HTMLResponse(
        content="""
        <html>
            <head>
                <title>Low-Cap Token Analyzer</title>
                <style>
                    body {
                        font-family: system-ui, sans-serif;
                        max-width: 800px;
                        margin: 0 auto;
                        padding: 2rem;
                        line-height: 1.6;
                    }
                    .status { 
                        background: #f0f9ff;
                        border: 1px solid #0ea5e9;
                        border-radius: 0.5rem;
                        padding: 1rem;
                        margin: 1rem 0;
                    }
                </style>
            </head>
            <body>
                <h1>🚀 Low-Cap Token Analyzer</h1>
                <div class="status">
                    <strong>Status:</strong> Backend API is running<br>
                    <strong>Health Check:</strong> <a href="/health">/health</a><br>
                    <strong>API Documentation:</strong> <a href="/docs">/docs</a>
                </div>
            </body>
        </html>
        """,
        status_code=200
    )

# ------------------------------------------------------------------
# Globaler Exception-Handler
# ------------------------------------------------------------------
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "detail": str(exc) if app.debug else "An unexpected error occurred",
        },
    )

# ------------------------------------------------------------------
# Datenbankkonfiguration für Render
# ------------------------------------------------------------------
# Wenn die Anwendung auf Render läuft, verwenden wir die Umgebungsvariablen
if os.environ.get("RENDER"):
    # Datenbankkonfiguration mit Umgebungsvariablen aktualisieren
    database_config.db_host = os.environ.get("DATABASE_HOST", database_config.db_host)
    database_config.db_port = int(os.environ.get("DATABASE_PORT", database_config.db_port))
    database_config.db_name = os.environ.get("DATABASE_NAME", database_config.db_name)
    database_config.db_user = os.environ.get("DATABASE_USER", database_config.db_user)
    database_config.db_password = os.environ.get("DATABASE_PASSWORD", database_config.db_password)
