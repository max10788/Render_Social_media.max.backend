# Low-Cap Token Analyzer - Backend Overview

## 1. Project Summary

The **Low-Cap Token Analyzer** is an enterprise-grade cryptocurrency analysis platform that combines blockchain monitoring, OTC (Over-the-Counter) transaction detection, social media sentiment analysis, and advanced financial modeling. The system provides real-time insights into token trading patterns, wallet profiling, and trading desk identification across multiple blockchain networks.

**Primary Use Cases:**
- Monitor and identify OTC trading desks and market makers
- Track transaction flows across blockchain networks
- Analyze price movements and correlate with trading volumes
- Detect suspicious trading patterns and OTC activities
- Real-time sentiment analysis from social media
- Smart wallet tracking and network analysis
- DEX charting and price mover analysis

---

## 2. Tech Stack

### Core Framework & Server
- **FastAPI** 0.109.0 - Modern async Python web framework
- **Uvicorn** 0.27.0 - ASGI server with 4 workers for concurrency
- **Gunicorn** 21.2.0 - Production-grade application server
- **Starlette** - ASGI application framework (via FastAPI)

### Real-time Communication
- **Socket.IO** 5.11.0+ - Real-time bidirectional event-based communication
- **WebSockets** 10.4 - Native WebSocket support for live updates
- **python-socketio** - Async Socket.IO support

### Database
- **PostgreSQL** - Primary relational database via DATABASE_URL
- **SQLAlchemy** 2.0.20 - ORM for database operations
- **asyncpg** 0.28.0 - Async PostgreSQL driver
- **psycopg2-binary** 2.9.6 - PostgreSQL adapter
- **Redis** 4.6.0 - Caching and session management
- **Alembic** 1.10.0 - Database migrations
- **SQLAlchemy-Utils** 0.38.2 - SQLAlchemy utilities

### Blockchain & Web3
- **web3.py** 6.17.1 - Ethereum interaction library
- **Solana** 0.30.0 - Solana blockchain support
- **Solders** 0.15.1 - Solana serialization library
- **base58** 2.1.1 - Base58 encoding/decoding
- **eth-abi** 4.2.1 - Ethereum ABI utilities
- **eth-utils** 2.3.1 - Ethereum utility functions
- **python-bitcoinrpc** 1.0 - Bitcoin RPC client

### Data Processing & Analysis
- **pandas** 2.1.4 - Data manipulation and analysis
- **numpy** 1.26.3 - Numerical computing
- **scipy** 1.11.4 - Scientific computing (for option pricing)
- **scikit-learn** 1.0.0-1.4.0 - Machine learning toolkit
- **NetworkX** 3.0+ - Network graph analysis for wallet clustering

### NLP & Sentiment Analysis
- **VADER Sentiment** 3.3.2 - Social media sentiment analysis
- **TextBlob** 0.15.3 - NLP library for text processing
- **NLTK** 3.8.1 - Natural Language Toolkit
- **LangDetect** 1.0.9 - Language detection

### Twitter/Social Integration
- **Tweepy** 4.14.0 - Twitter API client
- **Beautiful Soup** 4.12.3 - HTML parsing

### API & Data Providers
- **CCXT** 4.4.30 - Cryptocurrency exchange API abstraction
- **httpx** 0.23.3 - Modern HTTP client
- **aiohttp** 3.9.1 - Async HTTP client
- **requests** 2.31.0 - HTTP library

### Task Scheduling
- **Celery** 5.3.4 - Distributed task queue
- **Flower** 1.2.0 - Celery monitoring tool
- **Schedule** 1.2.0 - Simple job scheduler

### Security & Cryptography
- **cryptography** 41.0.1 - Cryptographic library
- **python-jose** 3.3.0 - JWT/JWS support
- **python-dotenv** 1.0.0 - Environment variable management

### Monitoring & Logging
- **Loguru** 0.7.2 - Modern Python logging
- **Prometheus-client** 0.16.0 - Prometheus metrics
- **Structlog** 23.1.0 - Structured logging
- **python-json-logger** 2.0.7 - JSON logging

### Development & Testing
- **pytest** 7.4.4 - Testing framework
- **pytest-asyncio** 0.23.3 - Async test support
- **pytest-cov** 4.1.0 - Code coverage
- **black** 23.12.1 - Code formatter
- **flake8** 7.0.0 - Linting
- **mypy** 1.8.0 - Static type checking
- **pydantic** 1.10.13 - Data validation (v1 for compatibility)

### Containerization
- **Docker** - Containerized deployment
- **Python 3.11** - Base image (slim variant)

---

## 3. Project Structure

```
Render_Social_media.max.backend/
├── app/
│   ├── main.py                              # Main FastAPI application entry point
│   ├── __init__.py
│   ├── sentiment.py                         # Sentiment analysis module
│   ├── models/
│   │   ├── __init__.py
│   │   ├── schemas.py                       # Pydantic request/response models
│   │   └── db_models.py                     # SQLAlchemy ORM models
│   │
│   └── core/                                # Core feature modules
│       ├── backend_crypto_tracker/          # Token analysis & tracking
│       │   ├── api/routes/
│       │   │   ├── token_routes.py          # Token endpoints
│       │   │   ├── transaction_routes.py    # Transaction endpoints
│       │   │   ├── scanner_routes.py        # Token scanner
│       │   │   ├── wallet_routes.py         # Wallet analysis
│       │   │   ├── contract_routes.py       # Contract analysis
│       │   │   ├── custom_analysis_routes.py
│       │   │   └── frontend_routes.py
│       │   ├── config/
│       │   │   └── database.py              # Database configuration
│       │   └── processor/database/models/
│       │
│       ├── otc_analysis/                    # OTC desk detection & analysis
│       │   ├── api/
│       │   │   ├── desks.py                 # OTC desk endpoints
│       │   │   ├── wallets.py               # Wallet profile endpoints
│       │   │   ├── statistics.py            # Statistics & analytics
│       │   │   ├── discovery.py             # OTC discovery
│       │   │   ├── monitoring.py            # Watchlist & alerts
│       │   │   ├── flow.py                  # Transaction flow analysis
│       │   │   ├── network.py               # Network analysis
│       │   │   ├── streams.py               # Moralis streams webhook
│       │   │   ├── admin.py                 # Admin operations
│       │   │   ├── admin_otc.py             # OTC address management
│       │   │   ├── migration.py             # Migration utilities
│       │   │   ├── validators.py            # Input validation
│       │   │   ├── dependencies.py          # FastAPI dependencies
│       │   │   ├── transactions.py          # Transaction analysis
│       │   │   └── websocket.py             # WebSocket handlers
│       │   │
│       │   ├── models/
│       │   │   ├── wallet.py                # OTCWallet model
│       │   │   ├── transaction.py           # Transaction model
│       │   │   ├── wallet_link.py           # Wallet clustering
│       │   │   ├── cluster.py               # Cluster model
│       │   │   ├── otc_activity.py          # OTC activity tracking
│       │   │   ├── watchlist.py             # Watchlist model
│       │   │   ├── alert.py                 # Alert model
│       │   │   └── auto_migrate.py          # Migration utilities
│       │   │
│       │   ├── blockchain/
│       │   │   └── moralis_streams.py       # Moralis webhook integration
│       │   │
│       │   ├── analysis/
│       │   │   ├── network_graph.py         # Network analysis
│       │   │   └── otc_detector.py          # OTC detection logic
│       │   │
│       │   ├── utils/
│       │   │   ├── chart_generators.py      # Chart data generation
│       │   │   └── ...
│       │   │
│       │   ├── data_sources/                # Data enrichment
│       │   ├── core/                        # Core analysis logic
│       │   ├── detection/                   # Detection algorithms
│       │   ├── workers/                     # Background tasks
│       │   └── discovery/                   # Discovery modules
│       │
│       ├── price_movers/                    # Price movement analysis
│       │   ├── api/
│       │   │   ├── routes.py                # Main price mover endpoints
│       │   │   ├── analyze_routes.py        # Analysis endpoints
│       │   │   ├── wallet_detail_routes.py  # Wallet details
│       │   │   ├── hybrid_routes.py         # Hybrid analysis
│       │   │   └── routes_dex_chart.py      # DEX chart data
│       │   └── collectors/                  # Exchange data collection
│       │
│       ├── orderbook_heatmap/               # Orderbook visualization
│       │   ├── api/endpoints.py             # Heatmap endpoints
│       │   ├── aggregator/                  # Orderbook aggregation
│       │   ├── models/                      # Heatmap data models
│       │   ├── websocket/                   # Real-time updates
│       │   └── exchanges/                   # Exchange integrations
│       │
│       ├── iceberg_orders/                  # Iceberg order detection
│       │   └── api/endpoints.py             # Iceberg endpoints
│       │
│       ├── option_pricing/                  # Option pricing models
│       │   └── api/routes/option_routes.py
│       │
│       └── smart_wallet_tracker/            # Smart wallet tracking
│           ├── api/                         # API endpoints
│           ├── modes/                       # Tracking modes
│           ├── visualization/               # Visualization
│           └── utils/                       # Utilities
│
├── scripts/
│   ├── init_otc_db.py                       # OTC database initialization
│   ├── setup_database.py                    # Database setup
│   ├── setup_wallet_links.py                # Wallet links table setup
│   └── migrate_db.sh                        # Database migration script
│
├── tests/
│   ├── test_api.py                          # API tests
│   └── test_twitter_api.py                  # Twitter API tests
│
├── data/                                    # Data storage directory
│
├── requirements.txt                         # Python dependencies
├── Dockerfile                               # Container configuration
├── .env                                     # Environment variables
├── .dockerignore                            # Docker build exclusions
└── README.md                                # Project documentation
```

---

## 4. Main Endpoints/Routes

### Health & System
```
GET /health                          # Health check endpoint
GET /api/health                      # API health check
GET /admin/system/health             # System health with detailed status
```

### Configuration & Info
```
GET /api/assets                      # List available assets
GET /api/exchanges                   # List available exchanges
GET /api/blockchains                 # List supported blockchains
GET /api/config                      # System configuration
GET /api/analytics                   # Analytics data
GET /api/settings                    # User settings
```

### Token Analysis
```
GET /api/v1/tokens                   # List tokens (token_routes.py)
GET /api/v1/tokens/{token_id}        # Get token details
GET /api/v1/tokens/address/{address} # Get token by address
GET /api/v1/tokens/{address}/wallets # Get wallets holding token
POST /api/v1/tokens/analyze          # Analyze token
GET /api/v1/tokens/analysis/history  # Analysis history
GET /api/v1/tokens/statistics/chains # Statistics by blockchain
GET /api/tokens/statistics           # Token statistics
GET /api/tokens/trending             # Trending tokens
```

### Transaction Analysis
```
GET /api/v1/transactions/{tx_hash}   # Get transaction details
GET /api/v1/transactions/{tx_hash}/detail
POST /api/v1/transactions/analyze    # Analyze transaction
GET /api/v1/transactions/address/{address}      # Address transactions
GET /api/v1/transactions/token/{token_address}  # Token transactions
GET /api/v1/transactions/graph/{address}        # Transaction graph
GET /api/v1/transactions/recent      # Recent transactions
GET /api/v1/transactions/statistics  # Transaction statistics
GET /api/v1/transactions/search      # Search transactions
```

### Scanner
```
GET /api/v1/scanner/status           # Scanner status
```

### Contract Analysis
```
GET /api/v1/contracts/{address}/info       # Contract info
GET /api/v1/contracts/{address}/interactions
GET /api/v1/contracts/{address}/security   # Security analysis
GET /api/v1/contracts/{address}/time-series
```

### Wallet Analysis
```
POST /api/analyze                    # Analyze wallet
POST /api/analyze/top-matches         # Find matching wallets
POST /api/analyze/batch               # Batch wallet analysis
GET /api/wallet/health                # Wallet service health
```

### OTC Analysis - Desks
```
GET /api/otc/desks                    # List all OTC desks (registry + database)
GET /api/otc/desks/database           # Database-validated desks only
POST /api/otc/desks/discover          # Auto-discover desks
GET /api/otc/desks/{desk_name}        # Get desk details
POST /api/otc/desks/analyze/transaction  # Analyze transaction for OTC
```

### OTC Analysis - Wallets
```
GET /api/otc/wallet/{address}         # Get wallet profile
GET /api/otc/wallet/{address}/chart/activity   # Activity chart
GET /api/otc/wallet/{address}/chart/transfers  # Transfer size chart
GET /api/otc/wallet/{address}/period-volume    # Period volume (7d, 30d, etc)
GET /api/otc/wallet/{address}/network-metrics  # Network position metrics
GET /api/otc/wallet/{address}/behavior        # Behavioral analysis
```

### OTC Analysis - Statistics
```
GET /api/otc/statistics               # OTC statistics with 24h change
GET /api/otc/distributions            # Volume & activity distributions
GET /api/otc/stats                    # Aggregated OTC stats
```

### OTC Analysis - Network
```
GET /api/otc/network/graph            # Network graph visualization
GET /api/otc/network/clusters         # Wallet clusters
GET /api/otc/network/node/{address}   # Node details
```

### OTC Analysis - Monitoring
```
GET /api/otc/watchlist                # Get watchlist items
POST /api/otc/watchlist               # Add to watchlist
DELETE /api/otc/watchlist/{item_id}   # Remove from watchlist
GET /api/otc/alerts                   # Get alerts
```

### Price Movers
```
GET /price-movers                     # Get price movers for trading pair
POST /price-movers/analyze            # Analyze price movers
GET /price-movers/{exchange}/{symbol} # Get movers for pair
```

### Price Movers - DEX Charts
```
GET /api/v1/dex/chart/candles         # Candlestick data
GET /api/v1/dex/chart/candle/{timestamp}/movers
POST /api/v1/dex/chart/batch-analyze  # Batch analysis
```

### Orderbook Heatmap
```
GET /api/v1/orderbook-heatmap/{exchange}/{symbol}  # Heatmap data
GET /api/v1/orderbook-heatmap/{exchange}/{symbol}/levels
POST /api/v1/orderbook-heatmap/subscribe            # WebSocket subscription
```

### Iceberg Orders
```
GET /api/iceberg-orders/{exchange}/{symbol}  # Detect iceberg orders
POST /api/iceberg-orders/analyze              # Analyze orders
```

### Real-time Communication
```
WebSocket /ws                                # Native WebSocket (general)
WebSocket /ws/otc/live                      # OTC WebSocket (live updates)
Socket.IO /socket.io                        # Socket.IO endpoint
```

---

## 5. Key Models/Schemas

### OTC Wallet Model
```python
OTCWallet:
  - address (PK): Ethereum address
  - entity_type: 'otc_desk', 'market_maker', 'cex', 'prop_trading', 'whale'
  - entity_name: Display name
  - entity_label: Moralis label
  - entity_logo: Logo URL
  - first_seen: Discovery timestamp
  - last_active: Last activity timestamp
  - total_transactions: TX count
  - total_volume: Total USD volume
  - transaction_frequency: TXs per day
  - unique_counterparties: Number of unique peers
  - has_defi_interactions: Bool
  - has_dex_swaps: Bool
  - otc_probability: Confidence score (0-1)
  - is_known_otc_desk: Bool
  - cluster_id: Network cluster
  - labels: Array of labels
  - tags: Array of tags
  - risk_score: Risk assessment (0-1)
```

### Transaction Model
```python
Transaction:
  - tx_hash (PK): Transaction hash
  - block_number: Block number
  - timestamp: Execution time
  - from_address: Sender address
  - to_address: Recipient address
  - token_address: Token (if transfer)
  - value: Amount in smallest unit
  - value_decimal: Human-readable amount
  - usd_value: USD value at time
  - gas_used: Gas consumed
  - gas_price: Gas price in wei
  - is_contract_interaction: Bool
  - method_id: 4-byte function selector
  - otc_score: OTC probability (0-1)
  - is_suspected_otc: Bool
  - chain_id: Network ID (1=Ethereum, 56=BSC)
```

### Sentiment Analysis Model
```python
SentimentAnalysis:
  - id (PK): Analysis ID
  - query: Search query
  - sentiment_score: Score (-1 to 1)
  - post_count: Posts analyzed
  - created_at: Timestamp
```

---

## 6. Configuration

### Key Environment Variables

**Database**
```
DATABASE_URL=postgresql://user:pass@host:5432/db
OTC_SCHEMA=otc_analysis
DB_POOL_SIZE=10
```

**Blockchain RPC**
```
SOLANA_RPC_URL=https://...
ETHEREUM_RPC_URL=https://...
HELIUS_RPC_URL=https://...
HELIUS_API_KEY=...
```

**API Keys - Data Providers**
```
MORALIS_API_KEY=...
COINGECKO_API_KEY=...
COINMARKETCAP_API_KEY=...
ETHERSCAN_API_KEY=...
BSCSCAN_API_KEY=...
```

**Exchange APIs**
```
BINANCE_API_KEY=...
BINANCE_SECRET_KEY=...
```

**Social Media**
```
TWITTER_BEARER_TOKEN=...
TWITTER_API_KEY=...
```

**System**
```
LOG_LEVEL=INFO
DEBUG=True
RENDER=true
REDIS_URL=redis://localhost:6379/0
```

---

## 7. How to Run

### Local Development

```bash
# Create virtual environment
python3.11 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your credentials

# Initialize database
python scripts/setup_database.py
python scripts/init_otc_db.py

# Run development server
uvicorn app.main:socket_app --reload --host 0.0.0.0 --port 8000
```

### Production (Docker)

```bash
# Build
docker build -t lowcap-analyzer .

# Run
docker run -d \
  -e DATABASE_URL=postgresql://... \
  -p 8000:8000 \
  lowcap-analyzer
```

### Access Points
- **API Docs**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health

---

## Summary

This backend provides a comprehensive cryptocurrency analysis platform with:
- **Real-time monitoring** via WebSocket/Socket.IO
- **OTC desk detection** using advanced network analysis
- **Multi-chain support** (Ethereum, Solana, Binance, Polygon)
- **Sentiment analysis** from social media
- **Price correlation** and trading analysis
- **Scalable architecture** with async processing
- **Production-ready** with Docker, 4-worker Uvicorn deployment
