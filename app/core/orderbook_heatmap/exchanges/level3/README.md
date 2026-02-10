# Level 3 Order Book Integration

Level 3 (L3) order book data provides the most granular market view - individual orders with unique IDs, prices, sizes, and lifecycle events (add/modify/cancel) rather than aggregated price levels (L2).

## Features

- **Full order tracking**: Track individual orders through their entire lifecycle
- **Real-time streaming**: WebSocket feeds for live updates
- **Database persistence**: PostgreSQL storage with optimized indexes
- **Periodic snapshots**: Automatic full orderbook snapshots for recovery
- **Multiple exchanges**: Coinbase Pro and Bitfinex support (MVP)

## Supported Exchanges

### ✅ Bitfinex (Raw Books) - EMPFOHLEN
- **Channel**: `book` with precision `R0`
- **Authentication**: ✅ **NICHT erforderlich** (public)
- **Format**: `[ORDER_ID, PRICE, AMOUNT]`
- **Notes**: Amount sign indicates side (positive = bid, negative = ask)
- **Vorteil**: Sofort einsatzbereit, keine API-Keys nötig!

### Coinbase Exchange/Pro (Legacy API)
- **Channel**: `full` channel
- **Authentication**: ❌ Required (API key, secret, **passphrase**)
- **Event types**: `open`, `done`, `change`, `match`
- **Format**: JSON with order IDs, prices, sizes
- **Nachteil**: Benötigt 3 Credentials, Exchange API nicht für alle verfügbar

### ⚠️ Hinweis: Coinbase Advanced Trade API
Die neue **Coinbase Advanced Trade API** (mit nur 2 Credentials) bietet **KEIN Level 3**!
- Nur Level 2 verfügbar
- Für Level 3 benötigst du die alte Exchange/Pro API (3 Credentials)

## Architecture

```
app/core/orderbook_heatmap/
├── exchanges/level3/
│   ├── base_l3.py           # Base class for L3 exchanges
│   ├── coinbase_l3.py       # Coinbase implementation
│   └── bitfinex_l3.py       # Bitfinex implementation
├── models/level3.py         # L3 data models
├── storage/
│   ├── l3_repository.py     # Database operations
│   └── snapshot_manager.py  # Snapshot management
└── api/
    └── level3_endpoints.py  # REST and WebSocket APIs
```

## Quick Start

### 1. Configure Credentials (Optional)

**Für Bitfinex:** Keine Credentials erforderlich! ✅

**Für Coinbase (optional):** Add to `.env`:

```bash
# OPTIONAL - Nur für Coinbase Exchange/Pro API
# COINBASE_API_KEY=your_key
# COINBASE_API_SECRET=your_secret
# COINBASE_API_PASSPHRASE=your_passphrase

# L3 settings
L3_PERSIST_ENABLED=true
L3_SNAPSHOT_INTERVAL=60
L3_MAX_ORDERS_MEMORY=100000
```

### 2. Create Database Tables

```bash
python scripts/migrate_l3_tables.py create
```

Verify tables:

```bash
python scripts/migrate_l3_tables.py check
```

### 3. Start L3 Stream

**REST API (Bitfinex - empfohlen):**

```bash
curl -X POST http://localhost:8000/api/v1/orderbook-heatmap/level3/start \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "BTC/USDT",
    "exchanges": ["bitfinex"],
    "persist": true,
    "snapshot_interval_seconds": 60
  }'
```

**REST API (mit Coinbase):**

```bash
curl -X POST http://localhost:8000/api/v1/orderbook-heatmap/level3/start \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "BTC-USD",
    "exchanges": ["coinbase", "bitfinex"],
    "persist": true,
    "snapshot_interval_seconds": 60
  }'
```

**Response:**

```json
{
  "symbol": "BTC-USD",
  "exchanges": ["coinbase", "bitfinex"],
  "is_active": true,
  "orders_received": 0,
  "orders_persisted": 0,
  "snapshots_taken": 0,
  "started_at": "2026-02-10T12:00:00Z"
}
```

### 4. Query Historical Data

**Get orders:**

```bash
curl "http://localhost:8000/api/v1/orderbook-heatmap/level3/orders/coinbase/BTC-USD?start_time=2026-02-10T00:00:00&end_time=2026-02-10T23:59:59&limit=100"
```

**Get latest snapshot:**

```bash
curl "http://localhost:8000/api/v1/orderbook-heatmap/level3/snapshot/coinbase/BTC-USD"
```

### 5. WebSocket Streaming

**JavaScript:**

```javascript
const ws = new WebSocket('ws://localhost:8000/api/v1/orderbook-heatmap/level3/ws/BTC-USD');

ws.onmessage = (event) => {
  const message = JSON.parse(event.data);

  switch(message.type) {
    case 'l3_snapshot':
      console.log('Snapshot received:', message.statistics);
      break;

    case 'l3_order':
      console.log('Order event:', message.data);
      break;

    case 'l3_statistics':
      console.log('Statistics:', message.data);
      break;
  }
};
```

**Python:**

```python
import websockets
import json
import asyncio

async def stream_l3():
    uri = "ws://localhost:8000/api/v1/orderbook-heatmap/level3/ws/BTC-USD"

    async with websockets.connect(uri) as ws:
        async for message in ws:
            data = json.loads(message)
            print(f"Received {data['type']}: {data}")

asyncio.run(stream_l3())
```

## API Endpoints

### REST API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/orderbook-heatmap/level3/start` | POST | Start L3 data collection |
| `/api/v1/orderbook-heatmap/level3/stop/{exchange}/{symbol}` | POST | Stop L3 stream |
| `/api/v1/orderbook-heatmap/level3/status/{exchange}/{symbol}` | GET | Get stream status |
| `/api/v1/orderbook-heatmap/level3/orders/{exchange}/{symbol}` | GET | Query historical orders |
| `/api/v1/orderbook-heatmap/level3/snapshot/{exchange}/{symbol}` | GET | Get latest snapshot |
| `/api/v1/orderbook-heatmap/level3/statistics/{exchange}/{symbol}` | GET | Get statistics |

### WebSocket

| Endpoint | Description |
|----------|-------------|
| `/api/v1/orderbook-heatmap/level3/ws/{symbol}` | Real-time L3 updates |

## Message Types

### L3 Order Event

```json
{
  "type": "l3_order",
  "exchange": "coinbase",
  "symbol": "BTC-USD",
  "data": {
    "order_id": "d50ec984-77a8-460a-b958-66f114b0de9b",
    "sequence": 12345,
    "side": "bid",
    "price": 50000.00,
    "size": 0.5,
    "event_type": "open",
    "timestamp": "2026-02-10T12:00:00Z",
    "metadata": {}
  }
}
```

### L3 Snapshot

```json
{
  "type": "l3_snapshot",
  "exchange": "coinbase",
  "symbol": "BTC-USD",
  "sequence": 1000,
  "timestamp": "2026-02-10T12:00:00Z",
  "statistics": {
    "total_orders": 5234,
    "bid_count": 2617,
    "ask_count": 2617,
    "total_bid_volume": 125.5,
    "total_ask_volume": 128.3,
    "best_bid": 49950.0,
    "best_ask": 50050.0,
    "spread": 100.0,
    "mid_price": 50000.0
  },
  "bids": [...],
  "asks": [...]
}
```

## Database Schema

### `level3_orders` Table

Stores individual order events:

```sql
CREATE TABLE otc_analysis.level3_orders (
    id BIGSERIAL PRIMARY KEY,
    exchange VARCHAR(20) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    order_id VARCHAR(100) NOT NULL,
    sequence BIGINT,
    side VARCHAR(4) NOT NULL,
    price NUMERIC(20, 8) NOT NULL,
    size NUMERIC(20, 8) NOT NULL,
    event_type VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);
```

**Indexes:**
- `(exchange, symbol)` - Query by exchange/symbol
- `(order_id, exchange)` - Track individual orders
- `(timestamp DESC)` - Time-based queries
- `(exchange, symbol, sequence)` - Sequence validation

### `level3_snapshots` Table

Stores periodic full snapshots:

```sql
CREATE TABLE otc_analysis.level3_snapshots (
    id BIGSERIAL PRIMARY KEY,
    exchange VARCHAR(20) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    sequence BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    bids JSONB NOT NULL,
    asks JSONB NOT NULL,
    total_bid_orders INT,
    total_ask_orders INT,
    total_bid_volume NUMERIC(20, 8),
    total_ask_volume NUMERIC(20, 8),
    created_at TIMESTAMP DEFAULT NOW()
);
```

## Performance

### Throughput

- **Coinbase BTC-USD**: ~10,000-50,000 orders/minute
- **Database inserts**: 1,000+ orders/second (batch mode)
- **WebSocket latency**: <50ms

### Optimization

1. **Batch inserts**: Buffer 1,000 orders, bulk insert every 1 second
2. **Indexes**: Optimized for time-range and order tracking queries
3. **Snapshots**: Compressed JSONB storage
4. **Memory limits**: Auto-flush at 100K orders

## Use Cases

### 1. Market Manipulation Detection

Track individual orders to detect:
- **Spoofing**: Large orders placed and quickly canceled
- **Layering**: Multiple orders at different price levels
- **Icebergs**: Hidden large orders split into smaller chunks

### 2. Liquidity Analysis

- **Order book depth**: Real-time liquidity at each price level
- **Order flow**: Track order arrival and cancellation rates
- **Market impact**: Measure how orders affect price

### 3. Trading Strategy Development

- **Order replay**: Reconstruct historical order book states
- **Backtesting**: Test strategies against L3 data
- **Execution analysis**: Optimize order placement timing

### 4. Whale Tracking

- **Large orders**: Filter orders above size threshold
- **Pattern recognition**: Identify recurring order patterns
- **Correlation**: Match L3 orders with OTC wallet activity

## Troubleshooting

### No data received

1. Check credentials:
   ```bash
   echo $COINBASE_API_KEY
   ```

2. Verify stream is active:
   ```bash
   curl http://localhost:8000/api/v1/orderbook-heatmap/level3/status/coinbase/BTC-USD
   ```

3. Check logs:
   ```bash
   tail -f logs/app.log | grep "L3"
   ```

### Database errors

1. Verify tables exist:
   ```bash
   python scripts/migrate_l3_tables.py check
   ```

2. Check database connection:
   ```bash
   psql $DATABASE_URL -c "SELECT COUNT(*) FROM otc_analysis.level3_orders;"
   ```

### High memory usage

1. Reduce buffer size in `.env`:
   ```bash
   L3_MAX_ORDERS_MEMORY=50000
   ```

2. Increase flush frequency (reduce interval in code)

## Future Enhancements

1. **More exchanges**: Kraken, Bitso, Poloniex, KuCoin
2. **Order flow analysis**: Detect spoofing, layering, icebergs
3. **Market microstructure**: Order arrival rates, cancellation rates
4. **Machine learning**: Predict price movements from L3 patterns
5. **OTC correlation**: Match L3 orders with wallet activity

## Contributing

To add a new exchange:

1. Create new file: `app/core/orderbook_heatmap/exchanges/level3/{exchange}_l3.py`
2. Extend `L3Exchange` base class
3. Implement required methods:
   - `get_l3_snapshot()`
   - `subscribe_l3_updates()`
   - `parse_l3_event()`
   - `normalize_symbol()`
4. Add to `__init__.py`
5. Update stream manager in `level3_endpoints.py`

## License

Part of the Block Intel backend system.
