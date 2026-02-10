# Bitfinex Level 3 - Quick Start Guide

Bitfinex bietet Level 3 (Raw Order Books) **ohne Authentication** - du kannst sofort loslegen!

## ✅ Voraussetzungen

1. Backend läuft
2. PostgreSQL Datenbank ist erreichbar
3. Keine API-Keys erforderlich! 🎉

## 🚀 Installation in 3 Schritten

### Schritt 1: Datenbank-Tabellen erstellen

```bash
cd /home/josua/Block_Intel/backend/Render_Social_media.max.backend
python3 scripts/migrate_l3_tables.py create
```

**Erwartete Ausgabe:**
```
Creating level3_orders table...
Creating indexes for level3_orders...
Creating level3_snapshots table...
Creating indexes for level3_snapshots...

✅ Successfully created L3 tables and indexes!

Verified tables: ['level3_orders', 'level3_snapshots']
```

**Prüfen:**
```bash
python3 scripts/migrate_l3_tables.py check
```

### Schritt 2: Backend starten

```bash
# Falls noch nicht gestartet
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Schritt 3: Level 3 Stream starten

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

**Erfolgreiche Antwort:**
```json
{
  "symbol": "BTC/USDT",
  "exchanges": ["bitfinex"],
  "is_active": true,
  "orders_received": 0,
  "orders_persisted": 0,
  "snapshots_taken": 0,
  "started_at": "2026-02-10T..."
}
```

## 📊 Daten abfragen

### Stream Status prüfen

```bash
curl http://localhost:8000/api/v1/orderbook-heatmap/level3/status/bitfinex/BTC/USDT
```

**Antwort:**
```json
{
  "symbol": "BTC/USDT",
  "exchanges": ["bitfinex"],
  "is_active": true,
  "orders_received": 15234,
  "orders_persisted": 15230,
  "snapshots_taken": 10,
  "started_at": "2026-02-10T12:00:00Z",
  "last_update": "2026-02-10T12:15:00Z",
  "errors": []
}
```

### Aktuellen Snapshot abrufen

```bash
curl http://localhost:8000/api/v1/orderbook-heatmap/level3/snapshot/bitfinex/BTC/USDT
```

### Historische Orders abfragen

```bash
curl "http://localhost:8000/api/v1/orderbook-heatmap/level3/orders/bitfinex/BTC/USDT?limit=100"
```

### Statistiken abrufen

```bash
curl http://localhost:8000/api/v1/orderbook-heatmap/level3/statistics/bitfinex/BTC/USDT
```

## 🔴 Live WebSocket Stream

### JavaScript/Browser

```javascript
const ws = new WebSocket('ws://localhost:8000/api/v1/orderbook-heatmap/level3/ws/BTC/USDT');

ws.onopen = () => {
    console.log('✅ Connected to Bitfinex L3 stream');
};

ws.onmessage = (event) => {
    const message = JSON.parse(event.data);

    switch(message.type) {
        case 'l3_snapshot':
            console.log('📸 Initial Snapshot:', {
                total_orders: message.statistics.total_orders,
                best_bid: message.statistics.best_bid,
                best_ask: message.statistics.best_ask,
                spread: message.statistics.spread
            });
            break;

        case 'l3_order':
            console.log('📦 Order Event:', {
                event: message.data.event_type,
                side: message.data.side,
                price: message.data.price,
                size: message.data.size,
                order_id: message.data.order_id
            });
            break;

        case 'l3_statistics':
            console.log('📊 Statistics Update:', message.data);
            break;
    }
};

ws.onerror = (error) => {
    console.error('❌ WebSocket error:', error);
};

ws.onclose = () => {
    console.log('🔌 Disconnected');
};
```

### Python

```python
import asyncio
import websockets
import json

async def stream_bitfinex_l3():
    uri = "ws://localhost:8000/api/v1/orderbook-heatmap/level3/ws/BTC/USDT"

    async with websockets.connect(uri) as ws:
        print("✅ Connected to Bitfinex L3 stream")

        async for message in ws:
            data = json.loads(message)

            if data['type'] == 'l3_snapshot':
                print(f"📸 Snapshot: {data['statistics']['total_orders']} orders")

            elif data['type'] == 'l3_order':
                order = data['data']
                print(f"📦 {order['event_type']}: {order['side']} {order['size']} @ {order['price']}")

asyncio.run(stream_bitfinex_l3())
```

## 🗄️ Datenbank direkt abfragen

### Gesamtzahl Orders

```sql
SELECT COUNT(*) as total_orders
FROM otc_analysis.level3_orders
WHERE exchange = 'bitfinex';
```

### Letzte 10 Orders

```sql
SELECT
    order_id,
    side,
    price,
    size,
    event_type,
    timestamp
FROM otc_analysis.level3_orders
WHERE exchange = 'bitfinex'
  AND symbol = 'BTC/USDT'
ORDER BY timestamp DESC
LIMIT 10;
```

### Orders pro Minute

```sql
SELECT
    DATE_TRUNC('minute', timestamp) as minute,
    COUNT(*) as order_count,
    COUNT(DISTINCT order_id) as unique_orders
FROM otc_analysis.level3_orders
WHERE exchange = 'bitfinex'
  AND symbol = 'BTC/USDT'
  AND timestamp > NOW() - INTERVAL '1 hour'
GROUP BY minute
ORDER BY minute DESC;
```

### Größte Orders (letzte Stunde)

```sql
SELECT
    order_id,
    side,
    price,
    size,
    price * size as value_usd,
    event_type,
    timestamp
FROM otc_analysis.level3_orders
WHERE exchange = 'bitfinex'
  AND symbol = 'BTC/USDT'
  AND timestamp > NOW() - INTERVAL '1 hour'
  AND event_type = 'open'
ORDER BY size DESC
LIMIT 20;
```

### Aktuelle Snapshots

```sql
SELECT
    symbol,
    sequence,
    timestamp,
    total_bid_orders,
    total_ask_orders,
    total_bid_volume,
    total_ask_volume
FROM otc_analysis.level3_snapshots
WHERE exchange = 'bitfinex'
ORDER BY timestamp DESC
LIMIT 5;
```

## 🛑 Stream stoppen

```bash
curl -X POST http://localhost:8000/api/v1/orderbook-heatmap/level3/stop/bitfinex/BTC/USDT
```

## 📈 Unterstützte Symbole

Bitfinex nutzt das Format `tBTCUSDT` (mit 't' Präfix). Die API konvertiert automatisch:

| Deine Eingabe | Bitfinex Format |
|---------------|-----------------|
| `BTC/USDT` | `tBTCUSDT` |
| `ETH/USDT` | `tETHUSDT` |
| `SOL/USDT` | `tSOLUSDT` |
| `BTC/USD` | `tBTCUSD` |

### Mehrere Symbole gleichzeitig

Du kannst mehrere Symbole parallel streamen:

```bash
# BTC/USDT
curl -X POST http://localhost:8000/api/v1/orderbook-heatmap/level3/start \
  -d '{"symbol":"BTC/USDT","exchanges":["bitfinex"],"persist":true}'

# ETH/USDT
curl -X POST http://localhost:8000/api/v1/orderbook-heatmap/level3/start \
  -d '{"symbol":"ETH/USDT","exchanges":["bitfinex"],"persist":true}'
```

## 🎯 Was sind Level 3 Daten?

Level 3 zeigt **jede einzelne Order** im Orderbook mit:

- **Order ID**: Eindeutige Identifikation
- **Price**: Preis der Order
- **Size**: Menge/Größe
- **Side**: Bid (Kauforder) oder Ask (Verkaufsorder)
- **Event Type**:
  - `open`: Neue Order platziert
  - `change`: Order geändert
  - `done`: Order ausgeführt oder storniert

**Vorteil gegenüber Level 2:**
- Level 2: Aggregierte Preislevel (z.B. "10 BTC @ $50,000")
- Level 3: Einzelne Orders (z.B. "Order #123: 2.5 BTC @ $50,000")

**Use Cases:**
- 🕵️ Market Maker Aktivität tracken
- 🐋 Große Orders (Whales) erkennen
- 📊 Order Flow Analysis
- 🎯 Spoofing/Layering Detection
- ⏱️ Execution Timing optimieren

## 🔧 Troubleshooting

### "WebSocket error" / Keine Verbindung

**Prüfe Backend-Logs:**
```bash
tail -f logs/app.log | grep -i "bitfinex\|l3"
```

**Häufige Ursachen:**
- Backend nicht gestartet
- Firewall blockiert WebSocket (Port 8000)
- Symbol-Format falsch

### "No orders received"

**Prüfe Status:**
```bash
curl http://localhost:8000/api/v1/orderbook-heatmap/level3/status/bitfinex/BTC/USDT
```

**Wenn `orders_received: 0`:**
1. Warte 10-20 Sekunden (Snapshot dauert einen Moment)
2. Prüfe Symbol-Format: `BTC/USDT` (nicht `BTCUSDT`)
3. Prüfe Backend-Logs auf Fehler

### "Database error"

**Tabellen nicht erstellt?**
```bash
python3 scripts/migrate_l3_tables.py check
```

**Wenn Fehler:**
```bash
python3 scripts/migrate_l3_tables.py create
```

### Hoher Speicherverbrauch

**In `.env` anpassen:**
```bash
L3_MAX_ORDERS_MEMORY=50000  # Standard: 100000
L3_BUFFER_SIZE=500          # Standard: 1000
```

## 📝 API Dokumentation

**Vollständige API-Docs:**
http://localhost:8000/docs

**Swagger UI:**
http://localhost:8000/redoc

## 🎉 Fertig!

Du kannst jetzt:
- ✅ Bitfinex Level 3 Daten in Echtzeit streamen
- ✅ Historische Order-Daten abfragen
- ✅ WebSocket-Feeds konsumieren
- ✅ Keine Authentication erforderlich!

**Nächste Schritte:**
1. Frontend-Dashboard bauen
2. Alerts für große Orders einrichten
3. Order Flow Analytics implementieren
4. Machine Learning auf L3-Daten trainieren

Bei Fragen oder Problemen: Logs prüfen oder mich fragen! 🚀
