from fastapi import FastAPI, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
import os
from pathlib import Path
from urllib.parse import urlparse
from typing import Dict, Any, List
from pydantic import BaseModel
import json
import asyncio
import time
import signal
import sys

# Router-Imports
# Router-Imports
from app.core.backend_crypto_tracker.api.routes.custom_analysis_routes import (
    router as custom_analysis_router,
)
from app.core.backend_crypto_tracker.api.routes.contract_routes import router as contracts_router
from app.core.backend_crypto_tracker.api.routes import token_routes
from app.core.backend_crypto_tracker.api.routes import transaction_routes
from app.core.backend_crypto_tracker.api.routes import scanner_routes
from app.core.backend_crypto_tracker.api.routes.frontend_routes import router as frontend_router
from app.core.backend_crypto_tracker.api.routes.wallet_routes import router as wallet_router
from app.core.price_movers.api.routes import router as price_movers_router
from app.core.price_movers.api.analyze_routes import router as analyze_router  # ← DIESE ZEILE HINZUFÜGEN

# Konfiguration und Datenbank
from app.core.backend_crypto_tracker.config.database import database_config
from app.core.backend_crypto_tracker.processor.database.models.manager import DatabaseManager
from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)

# Frontend-Verzeichnisse konfigurieren
BASE_DIR = Path(__file__).resolve().parent  # app/
FRONTEND_DIR = BASE_DIR / "crypto-token-analysis-dashboard"  # app/crypto-token-analysis-dashboard
BUILD_DIR = FRONTEND_DIR / ".next" / "standalone"  # Next.js standalone build
STATIC_DIR = FRONTEND_DIR / ".next" / "static"     # Next.js static files
PUBLIC_DIR = FRONTEND_DIR / "public"               # Next.js public files

# Pydantic-Modelle für die neuen Endpunkte
class AssetInfo(BaseModel):
    id: str
    name: str
    symbol: str

class ExchangeInfo(BaseModel):
    id: str
    name: str
    trading_pairs: int

class BlockchainInfo(BaseModel):
    id: str
    name: str
    block_time: float

class SystemConfig(BaseModel):
    minScore: int
    maxAnalysesPerHour: int
    cacheTTL: int
    supportedChains: List[str]

class AssetPriceRequest(BaseModel):
    assets: List[str]
    base_currency: str = "USD"

class AssetPriceResponse(BaseModel):
    prices: Dict[str, float]
    timestamp: int

class VolatilityRequest(BaseModel):
    asset: str
    timeframe: str = "1d"

class VolatilityResponse(BaseModel):
    volatility: float
    timeframe: str

class CorrelationRequest(BaseModel):
    assets: List[str]
    timeframe: str = "1d"

class CorrelationResponse(BaseModel):
    correlation_matrix: Dict[str, Dict[str, float]]
    timeframe: str

class OptionPricingRequest(BaseModel):
    underlying: str
    strike: float
    maturity: str
    option_type: str  # "call" or "put"

class OptionPricingResponse(BaseModel):
    price: float
    greeks: Dict[str, float]

class ImpliedVolatilityRequest(BaseModel):
    underlying: str
    strike: float
    maturity: str
    option_type: str
    market_price: float

class ImpliedVolatilityResponse(BaseModel):
    implied_volatility: float

class RiskMetricsRequest(BaseModel):
    assets: List[str]
    timeframe: str = "1d"
    confidence_level: float = 0.95

class RiskMetricsResponse(BaseModel):
    var: float
    expected_shortfall: float
    beta: Dict[str, float]

class SimulationProgress(BaseModel):
    simulation_id: str
    status: str  # "pending", "running", "completed", "failed"
    progress: float
    message: str

class SimulationStatusResponse(BaseModel):
    simulations: List[SimulationProgress]

# WebSocket Connection Manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connection established. Total connections: {len(self.active_connections)}")

    async def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logger.info(f"WebSocket connection closed. Total connections: {len(self.active_connections)}")

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception as e:
                logger.error(f"Error broadcasting message: {e}")

manager = ConnectionManager()

# Globale Variablen für die Ressourcenverwaltung
db_manager = None
shutdown_event = asyncio.Event()

def handle_signal(signum, frame):
    """Handler für Shutdown-Signale"""
    logger.info(f"Received signal {signum}, shutting down...")
    shutdown_event.set()

# Registriere Signal-Handler
signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager for FastAPI application."""
    global db_manager
    
    logger.info("Starting Low-Cap Token Analyzer")
    
    # Initialisiere die Datenbank
    try:
        db_manager = DatabaseManager()
        await db_manager.initialize()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        logger.info("Continuing without database...")
        db_manager = None
    
    yield
    
    logger.info("Shutting down Low-Cap Token Analyzer")
    
    # Schließe die Datenbankverbindung
    if db_manager:
        try:
            await db_manager.close()
            logger.info("Database connection closed successfully")
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")
    
    # Warte auf alle ausstehenden Aufgaben
    try:
        # Gib anderen Coroutines Zeit, sich ordnungsgemäß zu beenden
        await asyncio.sleep(1)
        
        # Schließe alle verbleibenden Aufgaben
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        if tasks:
            logger.info(f"Cancelling {len(tasks)} outstanding tasks")
            for task in tasks:
                task.cancel()
            
            # Warte, bis alle Aufgaben abgebrochen sind
            await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")

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
# CORS-Konfiguration (Nur FastAPI-Middleware)
# ------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In der Produktion sollten Sie dies auf spezifische Domains beschränken
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------------------------------------------------
# API Routes (mount first to prevent conflicts)
# ------------------------------------------------------------------
app.include_router(custom_analysis_router)
app.include_router(token_routes.router, prefix="/api/v1")
app.include_router(transaction_routes.router, prefix="/api/v1")
app.include_router(scanner_routes.router, prefix="/api/v1")
app.include_router(contracts_router, prefix="/api/v1")
app.include_router(wallet_router)
app.include_router(frontend_router)
app.include_router(price_movers_router)
app.include_router(analyze_router)  # ← DIESE ZEILE HINZUFÜGEN

# ------------------------------------------------------------------
# WebSocket Endpoint
# ------------------------------------------------------------------
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Sende eine Bestätigung, dass die Verbindung hergestellt wurde
        await websocket.send_text(json.dumps({
            "type": "connection_established",
            "message": "WebSocket connection established",
            "timestamp": time.time()
        }))
        
        while not shutdown_event.is_set():
            try:
                # Setze ein Timeout für receive_text, um regelmäßig auf shutdown_event zu prüfen
                data = await asyncio.wait_for(websocket.receive_text(), timeout=1.0)
                try:
                    message = json.loads(data)
                    logger.info(f"Received WebSocket message: {message}")
                    
                    # Process message and send response
                    response = {
                        "type": "response",
                        "message": f"Received: {message.get('type', 'unknown')}",
                        "timestamp": time.time()
                    }
                    
                    await websocket.send_text(json.dumps(response))
                except json.JSONDecodeError:
                    error_response = {
                        "type": "error",
                        "message": "Invalid JSON format",
                        "timestamp": time.time()
                    }
                    await websocket.send_text(json.dumps(error_response))
                
            except asyncio.TimeoutError:
                # Timeout ist normal, um shutdown_event zu prüfen
                continue
            except WebSocketDisconnect:
                logger.info("WebSocket client disconnected")
                break
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                try:
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "message": str(e),
                        "timestamp": time.time()
                    }))
                except:
                    pass
                break
    finally:
        manager.disconnect(websocket)

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
            "websocket": "active"
        },
        "database": {
            "host": database_config.db_host,
            "database": database_config.db_name,
            "schema": database_config.schema_name,
        }
    }

# NEU: API-Health-Check mit /api/health Pfad
@app.get("/api/health")
async def api_health_check():
    """API health check endpoint"""
    return {
        "status": "healthy",
        "version": "1.0.0",
        "services": {
            "token_analyzer": "active",
            "blockchain_tracking": "active",
            "websocket": "active"
        },
        "database": {
            "host": database_config.db_host,
            "database": database_config.db_name,
            "schema": database_config.schema_name,
        }
    }

# ------------------------------------------------------------------
# System Information Endpoints
# ------------------------------------------------------------------
@app.get("/api/assets", response_model=List[AssetInfo])
async def get_assets():
    """Get available assets for analysis"""
    try:
        return [
            {"id": "bitcoin", "name": "Bitcoin", "symbol": "BTC", "exchanges": ["Binance", "Coinbase", "Kraken"]},
            {"id": "ethereum", "name": "Ethereum", "symbol": "ETH", "exchanges": ["Binance", "Coinbase", "Kraken"]},
            {"id": "solana", "name": "Solana", "symbol": "SOL", "exchanges": ["Binance", "FTX", "Kraken"]},
        ]
    except Exception as e:
        logger.error(f"Error fetching assets: {e}")
        return []
        
@app.get("/api/exchanges", response_model=List[ExchangeInfo])
async def get_exchanges():
    """Get available exchanges"""
    try:
        return [
            {"id": "binance", "name": "Binance", "trading_pairs": 1500},
            {"id": "coinbase", "name": "Coinbase", "trading_pairs": 200},
            {"id": "kraken", "name": "Kraken", "trading_pairs": 300},
        ]
    except Exception as e:
        logger.error(f"Error fetching exchanges: {e}")
        return []
        
@app.get("/api/blockchains", response_model=List[BlockchainInfo])
async def get_blockchains():
    """Get available blockchains"""
    try:
        return [
            {"id": "ethereum", "name": "Ethereum", "block_time": 13.5},
            {"id": "solana", "name": "Solana", "block_time": 0.4},
            {"id": "bitcoin", "name": "Bitcoin", "block_time": 600},
        ]
    except Exception as e:
        logger.error(f"Error fetching blockchains: {e}")
        return []
        
@app.get("/api/config", response_model=SystemConfig)
async def get_config():
    """Get system configuration"""
    try:
        return {
            "minScore": 60,
            "maxAnalysesPerHour": 50,
            "cacheTTL": 3600,
            "supportedChains": ["ethereum", "solana", "sui"]
        }
    except Exception as e:
        logger.error(f"Error fetching config: {e}")
        return {"minScore": 0, "maxAnalysesPerHour": 0, "cacheTTL": 0, "supportedChains": []}
        
@app.get("/api/analytics")
async def get_analytics():
    """Get analytics data"""
    try:
        return {
            "analytics": {
                "totalAnalyses": 1250,
                "successfulAnalyses": 1180,
                "failedAnalyses": 70,
                "averageScore": 72.5
            },
            "status": "success"
        }
    except Exception as e:
        logger.error(f"Error fetching analytics: {e}")
        return {"analytics": {}, "status": "error", "message": str(e)}
        
@app.get("/api/settings")
async def get_settings():
    """Get user settings"""
    try:
        return {
            "settings": {
                "theme": "dark",
                "notifications": True,
                "autoRefresh": True,
                "refreshInterval": 30
            },
            "status": "success"
        }
    except Exception as e:
        logger.error(f"Error fetching settings: {e}")
        return {"settings": {}, "status": "error", "message": str(e)}

# ------------------------------------------------------------------
# Token Endpoints
# ------------------------------------------------------------------
@app.get("/api/tokens/statistics")
async def get_tokens_statistics():
    """Get token statistics"""
    try:
        return {
            "statistics": {
                "totalTokens": 1250,
                "activeTokens": 1180,
                "newTokens24h": 25,
                "topGainers": [
                    {"symbol": "TOKEN1", "change": 25.5},
                    {"symbol": "TOKEN2", "change": 18.3},
                    {"symbol": "TOKEN3", "change": 15.7}
                ]
            },
            "status": "success"
        }
    except Exception as e:
        logger.error(f"Error fetching token statistics: {e}")
        return {"statistics": {}, "status": "error", "message": str(e)}

@app.get("/api/tokens/trending")
async def get_tokens_trending(limit: int = 5):
    """Get trending tokens"""
    try:
        return {
            "tokens": [
                {"symbol": "TOKEN1", "name": "Token One", "price": 0.025, "change24h": 25.5, "volume": 1250000},
                {"symbol": "TOKEN2", "name": "Token Two", "price": 0.045, "change24h": 18.3, "volume": 980000},
                {"symbol": "TOKEN3", "name": "Token Three", "price": 0.075, "change24h": 15.7, "volume": 750000},
                {"symbol": "TOKEN4", "name": "Token Four", "price": 0.015, "change24h": 12.5, "volume": 620000},
                {"symbol": "TOKEN5", "name": "Token Five", "price": 0.035, "change24h": 10.3, "volume": 510000}
            ][:limit],
            "status": "success"
        }
    except Exception as e:
        logger.error(f"Error fetching trending tokens: {e}")
        return {"tokens": [], "status": "error", "message": str(e)}

# ------------------------------------------------------------------
# Asset Prices Endpoints
# ------------------------------------------------------------------
@app.post("/api/asset_prices", response_model=AssetPriceResponse)
async def get_asset_prices(request: AssetPriceRequest):
    """Get asset prices"""
    try:
        # Placeholder-Implementierung
        prices = {asset: 50000.0 if asset == "bitcoin" else 3000.0 for asset in request.assets}
        return {"prices": prices, "timestamp": int(time.time())}
    except Exception as e:
        logger.error(f"Error fetching asset prices: {e}")
        return {"prices": {}, "timestamp": 0}

# ------------------------------------------------------------------
# Volatility Endpoints
# ------------------------------------------------------------------
@app.post("/api/volatility", response_model=VolatilityResponse)
async def get_volatility(request: VolatilityRequest):
    """Get volatility data"""
    try:
        # Placeholder-Implementierung
        return {"volatility": 0.5, "timeframe": request.timeframe}
    except Exception as e:
        logger.error(f"Error calculating volatility: {e}")
        return {"volatility": 0.0, "timeframe": request.timeframe}

# ------------------------------------------------------------------
# Correlation Endpoints
# ------------------------------------------------------------------
@app.post("/api/correlation", response_model=CorrelationResponse)
async def get_correlation(request: CorrelationRequest):
    """Get correlation data"""
    try:
        # Placeholder-Implementierung
        correlation_matrix = {}
        for asset1 in request.assets:
            correlation_matrix[asset1] = {}
            for asset2 in request.assets:
                correlation_matrix[asset1][asset2] = 1.0 if asset1 == asset2 else 0.5
        
        return {"correlation_matrix": correlation_matrix, "timeframe": request.timeframe}
    except Exception as e:
        logger.error(f"Error calculating correlation: {e}")
        return {"correlation_matrix": {}, "timeframe": request.timeframe}

# ------------------------------------------------------------------
# Option Pricing Endpoints
# ------------------------------------------------------------------
@app.post("/api/price_option/start", response_model=Dict[str, str])
async def start_option_pricing(request: OptionPricingRequest):
    """Start option pricing simulation"""
    try:
        # Placeholder-Implementierung
        simulation_id = f"sim_{request.underlying}_{hash(request)}"
        return {"simulation_id": simulation_id}
    except Exception as e:
        logger.error(f"Error starting option pricing: {e}")
        return {"simulation_id": ""}

@app.get("/api/price_option/status/{simulation_id}", response_model=SimulationProgress)
async def get_option_pricing_status(simulation_id: str):
    """Get option pricing simulation status"""
    try:
        # Placeholder-Implementierung
        return {
            "simulation_id": simulation_id,
            "status": "completed",
            "progress": 1.0,
            "message": "Simulation completed successfully"
        }
    except Exception as e:
        logger.error(f"Error fetching option pricing status: {e}")
        return {
            "simulation_id": simulation_id,
            "status": "failed",
            "progress": 0.0,
            "message": str(e)
        }

@app.get("/api/price_option/result/{simulation_id}", response_model=OptionPricingResponse)
async def get_option_pricing_result(simulation_id: str):
    """Get option pricing result"""
    try:
        # Placeholder-Implementierung
        return {
            "price": 100.0,
            "greeks": {
                "delta": 0.5,
                "gamma": 0.1,
                "theta": -0.05,
                "vega": 0.2,
                "rho": 0.01
            }
        }
    except Exception as e:
        logger.error(f"Error fetching option pricing result: {e}")
        return {"price": 0.0, "greeks": {}}

@app.post("/api/price_option", response_model=OptionPricingResponse)
async def price_option(request: OptionPricingRequest):
    """Price option directly"""
    try:
        # Placeholder-Implementierung
        return {
            "price": 100.0,
            "greeks": {
                "delta": 0.5,
                "gamma": 0.1,
                "theta": -0.05,
                "vega": 0.2,
                "rho": 0.01
            }
        }
    except Exception as e:
        logger.error(f"Error pricing option: {e}")
        return {"price": 0.0, "greeks": {}}

# ------------------------------------------------------------------
# Implied Volatility Endpoints
# ------------------------------------------------------------------
@app.post("/api/implied_volatility", response_model=ImpliedVolatilityResponse)
async def calculate_implied_volatility(request: ImpliedVolatilityRequest):
    """Calculate implied volatility"""
    try:
        # Placeholder-Implementierung
        return {"implied_volatility": 0.2}
    except Exception as e:
        logger.error(f"Error calculating implied volatility: {e}")
        return {"implied_volatility": 0.0}

# ------------------------------------------------------------------
# Risk Metrics Endpoints
# ------------------------------------------------------------------
@app.post("/api/risk_metrics", response_model=RiskMetricsResponse)
async def calculate_risk_metrics(request: RiskMetricsRequest):
    """Calculate risk metrics"""
    try:
        # Placeholder-Implementierung
        beta = {asset: 1.0 for asset in request.assets}
        return {
            "var": 1000.0,
            "expected_shortfall": 1500.0,
            "beta": beta
        }
    except Exception as e:
        logger.error(f"Error calculating risk metrics: {e}")
        return {"var": 0.0, "expected_shortfall": 0.0, "beta": {}}

# ------------------------------------------------------------------
# Simulation Status Endpoints
# ------------------------------------------------------------------
@app.get("/api/simulations", response_model=SimulationStatusResponse)
async def get_all_simulations():
    """Get all simulations"""
    try:
        # Placeholder-Implementierung
        return {
            "simulations": [
                {
                    "simulation_id": "sim_1",
                    "status": "completed",
                    "progress": 1.0,
                    "message": "Simulation completed successfully"
                },
                {
                    "simulation_id": "sim_2",
                    "status": "running",
                    "progress": 0.5,
                    "message": "Simulation in progress"
                }
            ]
        }
    except Exception as e:
        logger.error(f"Error fetching simulations: {e}")
        return {"simulations": []}

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
            content={"error": "API endpoint not found", "path": request.url.path}
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
