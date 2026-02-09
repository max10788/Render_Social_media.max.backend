from fastapi import FastAPI, Request, Response, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from sqlalchemy.orm import Session
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

# ✅ NEU: Socket.IO Import
import socketio

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
#price mover routers
from app.core.price_movers.api.routes import router as price_movers_router
from app.core.price_movers.api.analyze_routes import router as analyze_router
from app.core.price_movers.api.wallet_detail_routes import router as wallet_detail_router
from app.core.price_movers.api.hybrid_routes import router as hybrid_router
from app.core.price_movers.api.routes_dex_chart import router as dex_chart_router

from app.core.orderbook_heatmap.api.endpoints import router as orderbook_heatmap_router

from app.core.iceberg_orders.api.endpoints import router as iceberg_orders_router


# ============================================================================
# OTC ANALYSIS API ROUTES - FIXED
# ============================================================================

# ✅ WICHTIG: Importiere die vereinfachten Router
from app.core.otc_analysis.api.admin import router as otc_admin_router
from app.core.otc_analysis.api.desks import router as otc_desks_router
from app.core.otc_analysis.api.wallets import router as otc_wallets_router
from app.core.otc_analysis.api.discovery import router as otc_discovery_router
from app.core.otc_analysis.api.monitoring import router as otc_monitoring_router
from app.core.otc_analysis.api.streams import router as otc_streams_router
from app.core.otc_analysis.api.admin_otc import router as otc_data_admin_router

# ✅ Vereinfachte Router (ohne Sub-Prefixe)
from app.core.otc_analysis.api.statistics import router as otc_statistics_router
from app.core.otc_analysis.api.network import router as otc_network_router
from app.core.otc_analysis.api.flow import router as otc_flow_router
from app.core.otc_analysis.api.websocket import handle_websocket_connection, set_socketio, live_otc_monitor

# Bei den OTC Analysis API Routes Imports (ca. Zeile 45-55)
from app.core.otc_analysis.api.migration import router as otc_migration_router

from scripts.init_otc_db import init_database
from app.core.backend_crypto_tracker.config.database import get_db

from scripts.setup_database import setup_database_on_startup

# Konfiguration und Datenbank
from app.core.backend_crypto_tracker.config.database import database_config
from app.core.backend_crypto_tracker.processor.database.models.manager import DatabaseManager
from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)

# Frontend-Verzeichnisse konfigurieren
BASE_DIR = Path(__file__).resolve().parent  # app/
FRONTEND_DIR = BASE_DIR / "crypto-token-analysis-dashboard"
BUILD_DIR = FRONTEND_DIR / ".next" / "standalone"
STATIC_DIR = FRONTEND_DIR / ".next" / "static"
PUBLIC_DIR = FRONTEND_DIR / "public"

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
    
    # ✅ Transaction Table Migration
    logger.info("🔧 Running database migrations...")
    migration_result = setup_database_on_startup()
    
    if migration_result["success"]:
        logger.info("✅ Database migrations completed")
        if migration_result.get("table_created"):
            logger.info("📦 Created 'transactions' table with indexes")
        else:
            logger.info("📋 Table 'transactions' already exists")
    else:
        logger.error("⚠️ Database migrations had errors - app will continue")
        logger.error(f"Errors: {migration_result.get('errors', [])}")
    
    # ✅ NEW: wallet_links Table Migration
    logger.info("🔧 Running wallet_links migration...")
    from scripts.setup_wallet_links import setup_wallet_links_table
    
    wallet_links_result = setup_wallet_links_table()
    
    if wallet_links_result["success"]:
        logger.info("✅ wallet_links migration completed")
        if wallet_links_result.get("table_created"):
            logger.info("📦 Created 'wallet_links' table with indexes")
        else:
            logger.info("📋 Table 'wallet_links' already exists")
    else:
        logger.error("⚠️ wallet_links migration had errors - app will continue")
        logger.error(f"Error: {wallet_links_result.get('error', 'Unknown error')}")
    
    # ✅ Skip DatabaseManager - OTC nutzt database.py direkt
    db_manager = None
    logger.info("Skipping DatabaseManager (OTC uses database.py directly)")

    # Inject Socket.IO into websocket module and start live monitor
    set_socketio(sio)
    monitor_task = asyncio.create_task(live_otc_monitor(shutdown_event))
    logger.info("Live OTC monitor task started")

    yield

    logger.info("Shutting down Low-Cap Token Analyzer")

    # Stop the live monitor
    shutdown_event.set()
    monitor_task.cancel()
    try:
        await monitor_task
    except asyncio.CancelledError:
        pass

    try:
        await asyncio.sleep(1)
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        if tasks:
            logger.info(f"Cancelling {len(tasks)} outstanding tasks")
            for task in tasks:
                task.cancel()
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
# For Coolify with dynamic sslip.io domains, we need to allow all origins
# since the subdomains are randomly generated and change between deployments
ALLOWED_ORIGINS = ["*"]

# Production origins (uncomment when you have stable domains):
# ALLOWED_ORIGINS = [
#     "https://render-social-media-max-frontend-fk7e.onrender.com",
#     "http://localhost:3000",
#     "http://localhost:3001",
#     "http://localhost:8000",
# ]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,  # Must be False when allow_origins=["*"]
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=3600,
)

# ------------------------------------------------------------------
# ✅ Socket.IO Setup
# ------------------------------------------------------------------
sio = socketio.AsyncServer(
    async_mode='asgi',
    cors_allowed_origins="*",  # Allow all origins for dynamic sslip.io domains
    logger=True,
    engineio_logger=False
)

# Socket.IO Event Handlers
@sio.event
async def connect(sid, environ):
    """Handle client connection"""
    logger.info(f"✅ Socket.IO client connected: {sid}")
    await sio.emit('connection', {
        'status': 'connected',
        'message': 'Connected to OTC live stream',
        'timestamp': time.time()
    }, room=sid)

@sio.event
async def disconnect(sid):
    """Handle client disconnection"""
    logger.info(f"❌ Socket.IO client disconnected: {sid}")

@sio.event
async def subscribe(sid, data):
    """Handle subscription to OTC events"""
    event_types = data.get('events', [])
    logger.info(f"📡 Client {sid} subscribed to: {event_types}")
    
    # Join rooms for each event type
    for event_type in event_types:
        await sio.enter_room(sid, event_type)
    
    await sio.emit('subscription_confirmed', {
        'events': event_types,
        'timestamp': time.time()
    }, room=sid)

@sio.event
async def unsubscribe(sid, data):
    """Handle unsubscription from OTC events"""
    event_types = data.get('events', [])
    logger.info(f"📴 Client {sid} unsubscribed from: {event_types}")
    
    # Leave rooms
    for event_type in event_types:
        await sio.leave_room(sid, event_type)
    
    await sio.emit('unsubscription_confirmed', {
        'events': event_types,
        'timestamp': time.time()
    }, room=sid)

@sio.event
async def ping(sid, data):
    """Handle ping"""
    await sio.emit('pong', {'timestamp': time.time()}, room=sid)

# Helper function to broadcast OTC events
async def broadcast_otc_event(event_type: str, data: dict):
    """Broadcast OTC event to all subscribed clients"""
    logger.info(f"📢 Broadcasting {event_type} event")
    await sio.emit(event_type, data, room=event_type)

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
app.include_router(analyze_router)
app.include_router(wallet_detail_router)
app.include_router(hybrid_router)
app.include_router(dex_chart_router, prefix="/api/v1")
app.include_router(orderbook_heatmap_router)
app.include_router(iceberg_orders_router, prefix="/api")

# ------------------------------------------------------------------
# OTC Analysis API Routes (Modular Structure)
# ------------------------------------------------------------------
# ✅ WICHTIG: Alte endpoints.py Router entfernen wenn vorhanden!
# app.include_router(otc_analysis_router)  # ← DEPRECATED - nicht mehr nutzen

# ✅ NEU: Modulare OTC Router (9 Module)
app.include_router(otc_desks_router, prefix="/api/otc")          # Desk endpoints
app.include_router(otc_wallets_router, prefix="/api/otc")        # Wallet endpoints
app.include_router(otc_statistics_router, prefix="/api/otc")     # Statistics endpoints
app.include_router(otc_network_router, prefix="/api/otc")        # Network endpoints
app.include_router(otc_flow_router, prefix="/api/otc")           # Flow endpoints
app.include_router(otc_monitoring_router, prefix="/api/otc")     # Monitoring endpoints
app.include_router(otc_admin_router, prefix="/api/otc")          # Admin endpoints
app.include_router(otc_streams_router, prefix="/api/otc")        # Streams endpoints (MORALIS)

# ✅ NEU: Discovery Router
app.include_router(otc_discovery_router, prefix="/api/otc")

# Bei den OTC Analysis API Routes (ca. Zeile 350-365)
# ✅ TEMPORÄR: Migration Endpoint (nach Ausführung löschen!)
app.include_router(otc_migration_router, prefix="/api/migration", tags=["Migration"])

app.include_router(otc_data_admin_router, prefix="/api/otc")

# ------------------------------------------------------------------
# OTC Wallet Tag Descriptions (matches frontend path /api/otc/wallets/tags/descriptions)
# ------------------------------------------------------------------
@app.get("/api/otc/wallets/tags/descriptions")
async def get_wallet_tag_descriptions():
    """Return descriptions for all wallet classification tags."""
    from app.core.otc_analysis.discovery.wallet_tagger import WalletTagger
    tagger = WalletTagger()
    return {"success": True, "data": tagger.get_tag_descriptions()}

# ✅ Optional: Validators & WebSocket (wenn du sie nutzt)
# app.include_router(otc_validators_router, prefix="/api/otc")
# app.include_router(otc_websocket_router, prefix="/api/otc")

# ------------------------------------------------------------------
# WebSocket Endpoint (behält den nativen /ws Endpoint)
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
# OTC Analysis WebSocket Endpoint
# ------------------------------------------------------------------
@app.websocket("/ws/otc/live")
async def otc_websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for real-time OTC analysis updates.
    
    Handles:
    - Live transaction monitoring  
    - Cluster activity alerts
    - Desk interaction notifications
    
    Frontend connects to: wss://backend.com/ws/otc/live
    """
    logger.info("🔌 OTC WebSocket connection attempt")
    await handle_websocket_connection(websocket)

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
            "websocket": "active",
            "socketio": "active"  # ✅ NEU
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
            "websocket": "active",
            "socketio": "active"  # ✅ NEU
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
# Admin Endpoints - OTC Database Initialization
# ------------------------------------------------------------------
@app.get("/admin/init-database")
async def initialize_otc_database():
    """
    🚀 Initialize OTC Analysis Database
    
    Creates tables and adds sample data if database is empty.
    Safe to call multiple times - won't duplicate data.
    
    Returns:
        JSON with initialization results
    """
    try:
        logger.info("🔧 Admin: Database initialization requested")
        
        # Run initialization
        result = init_database(verbose=False)
        
        if result["success"]:
            logger.info(f"✅ Admin: Database initialized - {result['message']}")
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
            logger.error(f"❌ Admin: Database initialization failed - {result.get('error', 'Unknown error')}")
            return JSONResponse(
                status_code=500,
                content={
                    "status": "error",
                    "message": result["message"],
                    "error": result.get("error", "Unknown error")
                }
            )
            
    except Exception as e:
        logger.error(f"❌ Admin: Exception during initialization: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Database initialization failed: {str(e)}"
            }
        )


@app.get("/admin/database-status")
async def check_database_status():
    """
    📊 Check OTC Database Status
    
    Returns current wallet count and total volume.
    """
    try:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from app.core.otc_analysis.models.wallet import OTCWallet
        import os
        
        logger.info("🔍 Admin: Database status check requested")
        
        # Get database URL
        db_url = os.getenv('DATABASE_URL')
        if not db_url:
            raise ValueError("DATABASE_URL not set")
            
        # Fix postgres:// to postgresql://
        if db_url.startswith('postgres://'):
            db_url = db_url.replace('postgres://', 'postgresql://', 1)
        
        # Create engine and session
        engine = create_engine(db_url)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        try:
            # Query stats
            total_wallets = session.query(OTCWallet).count()
            all_wallets = session.query(OTCWallet).all()
            total_volume = sum(w.total_volume or 0 for w in all_wallets)
            
            # Get some sample data
            sample_wallets = session.query(OTCWallet).limit(3).all()
            samples = [
                {
                    "address": w.address[:10] + "...",
                    "label": w.label,
                    "volume": w.total_volume
                }
                for w in sample_wallets
            ]
            
            logger.info(f"✅ Admin: Database status - {total_wallets} wallets, ${total_volume:,.0f} volume")
            
            return {
                "status": "connected",
                "total_wallets": total_wallets,
                "total_volume": total_volume,
                "sample_wallets": samples,
                "message": "Database is accessible and operational"
            }
            
        finally:
            session.close()
        
    except Exception as e:
        logger.error(f"❌ Admin: Database status check failed: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Cannot connect to database: {str(e)}"
            }
        )

# ============================================================================
# Transaction Sync Endpoints
# ============================================================================

@app.post("/admin/sync-transactions")
async def sync_wallet_transactions(
    wallet_address: str,
    max_transactions: int = 100,
    force_refresh: bool = False,
    db: Session = Depends(get_db)
):
    """
    🔄 Sync Transactions for Single Wallet
    
    Fetches transactions from blockchain and saves to database.
    Smart caching: checks DB first, only fetches new data if needed.
    
    Args:
        wallet_address: Ethereum address
        max_transactions: Max TXs to fetch (default: 100)
        force_refresh: Ignore cache (default: false)
    
    Returns:
        Sync statistics
    """
    try:
        from app.core.otc_analysis.api.dependencies import sync_wallet_transactions_to_db
        
        logger.info(f"🔄 Admin: Transaction sync requested for {wallet_address[:10]}...")
        
        # Validate address
        from app.core.otc_analysis.api.validators import validate_ethereum_address
        validated_address = validate_ethereum_address(wallet_address)
        
        # Sync transactions
        stats = await sync_wallet_transactions_to_db(
            db=db,
            wallet_address=validated_address,
            max_transactions=max_transactions,
            force_refresh=force_refresh
        )
        
        logger.info(
            f"✅ Admin: Sync complete - "
            f"Fetched: {stats['fetched_count']}, "
            f"Saved: {stats['saved_count']}, "
            f"Source: {stats['source']}"
        )
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": f"Synced transactions for {validated_address[:10]}...",
                "statistics": stats,
                "next_steps": [
                    "Test heatmap: GET /api/otc/heatmap",
                    "Check transactions: SELECT COUNT(*) FROM transactions",
                    f"View wallet TXs: SELECT * FROM transactions WHERE from_address='{validated_address}' OR to_address='{validated_address}'"
                ]
            }
        )
        
    except ValueError as e:
        logger.error(f"❌ Admin: Invalid address: {e}")
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": f"Invalid address: {str(e)}"}
        )
    except Exception as e:
        logger.error(f"❌ Admin: Transaction sync failed: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": f"Sync failed: {str(e)}"}
        )


@app.post("/admin/sync-all-transactions")
async def sync_all_wallet_transactions(
    max_wallets: int = 10,
    max_transactions_per_wallet: int = 100,
    db: Session = Depends(get_db)
):
    """
    🔄 Sync Transactions for ALL Wallets
    
    Fetches and saves transactions for all active OTC wallets.
    This populates the transactions table for heatmap visualization.
    
    ⚠️ WARNING: Can take several minutes for many wallets!
    
    Args:
        max_wallets: Max wallets to process (default: 10)
        max_transactions_per_wallet: Max TXs per wallet (default: 100)
    
    Returns:
        Overall sync statistics
    """
    try:
        from app.core.otc_analysis.api.dependencies import sync_all_wallets_transactions
        
        logger.info(
            f"🔄 Admin: Bulk transaction sync requested - "
            f"max_wallets={max_wallets}, max_tx={max_transactions_per_wallet}"
        )
        
        # Sync all wallets
        stats = await sync_all_wallets_transactions(
            db=db,
            max_wallets=max_wallets,
            max_transactions_per_wallet=max_transactions_per_wallet
        )
        
        logger.info(
            f"✅ Admin: Bulk sync complete - "
            f"Processed: {stats['wallets_processed']}, "
            f"Saved: {stats['total_saved']}, "
            f"Duration: {stats.get('duration_seconds', 0):.1f}s"
        )
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": f"Synced {stats['wallets_processed']} wallets",
                "statistics": {
                    "wallets_processed": stats["wallets_processed"],
                    "transactions_fetched": stats["total_fetched"],
                    "transactions_saved": stats["total_saved"],
                    "transactions_skipped": stats["total_skipped"],
                    "errors": stats["errors"],
                    "duration_seconds": stats.get("duration_seconds", 0)
                },
                "next_steps": [
                    "Test heatmap: GET /api/otc/heatmap",
                    "Check total: SELECT COUNT(*) FROM transactions",
                    "View recent: SELECT * FROM transactions ORDER BY timestamp DESC LIMIT 10"
                ]
            }
        )
        
    except Exception as e:
        logger.error(f"❌ Admin: Bulk sync failed: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": f"Bulk sync failed: {str(e)}"}
        )


@app.get("/admin/transactions-by-wallet/{wallet_address}")
async def get_wallet_transactions(
    wallet_address: str,
    limit: int = 50,
    db: Session = Depends(get_db)
):
    """
    📊 Get Transactions for Specific Wallet
    
    Shows transactions from database for a given wallet.
    Useful to verify sync worked correctly.
    
    Args:
        wallet_address: Ethereum address
        limit: Max transactions to return (default: 50)
    
    Returns:
        List of transactions
    """
    try:
        from app.core.otc_analysis.models.transaction import Transaction
        from sqlalchemy import or_
        
        logger.info(f"📊 Admin: Fetching transactions for {wallet_address[:10]}...")
        
        # Validate address
        from app.core.otc_analysis.api.validators import validate_ethereum_address
        validated_address = validate_ethereum_address(wallet_address)
        
        # Query transactions
        transactions = db.query(Transaction).filter(
            or_(
                Transaction.from_address == validated_address.lower(),
                Transaction.to_address == validated_address.lower()
            )
        ).order_by(Transaction.timestamp.desc()).limit(limit).all()
        
        if not transactions:
            return JSONResponse(
                status_code=404,
                content={
                    "status": "not_found",
                    "message": f"No transactions found for {validated_address[:10]}...",
                    "hint": f"Run: POST /admin/sync-transactions?wallet_address={validated_address}"
                }
            )
        
        # Format response
        tx_list = [
            {
                "tx_hash": tx.tx_hash[:20] + "...",
                "timestamp": tx.timestamp.isoformat(),
                "from": tx.from_address[:10] + "...",
                "to": tx.to_address[:10] + "...",
                "usd_value": float(tx.usd_value) if tx.usd_value else 0,
                "is_suspected_otc": tx.is_suspected_otc,
                "otc_score": float(tx.otc_score) if tx.otc_score else 0
            }
            for tx in transactions
        ]
        
        logger.info(f"✅ Admin: Found {len(transactions)} transactions")
        
        return {
            "status": "success",
            "wallet_address": validated_address,
            "transaction_count": len(transactions),
            "transactions": tx_list
        }
        
    except ValueError as e:
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": f"Invalid address: {str(e)}"}
        )
    except Exception as e:
        logger.error(f"❌ Admin: Error fetching transactions: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(e)}
        )

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

# ------------------------------------------------------------------
# ✅ Wrap FastAPI app with Socket.IO
# ------------------------------------------------------------------
from starlette.middleware.cors import CORSMiddleware as StarletteCorsMW

# Erstelle Socket.IO App MIT CORS
socket_app = socketio.ASGIApp(
    socketio_server=sio,
    other_asgi_app=app,
    socketio_path='/socket.io'
)

# ✅ WICHTIG: CORS nochmal auf socket_app Ebene anwenden
from starlette.applications import Starlette
from starlette.middleware import Middleware

# Wrap socket_app mit CORS Middleware
final_app = StarletteCorsMW(
    socket_app,
    allow_origins=["*"],  # Allow all origins for dynamic sslip.io domains
    allow_credentials=False,  # Must be False when allow_origins=["*"]
    allow_methods=["*"],
    allow_headers=["*"],
)
