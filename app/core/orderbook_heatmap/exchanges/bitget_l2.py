"""
Bitget CEX Layer 2 Data Integration
Holt Orderbook-Snapshots für L2-Token von Bitget's öffentlicher REST API.

Unterstützte L2-Netzwerke und ihre nativen Token:
- Polygon   → MATIC (POL)
- Arbitrum  → ARB
- Optimism  → OP
- Immutable → IMX
- Loopring  → LRC
- Metis     → METIS
- StarkNet  → STRK
- zkSync    → ZK
"""
import asyncio
import logging
from typing import Optional, Dict, List, Any

import aiohttp

logger = logging.getLogger(__name__)

# L2-Token zu Bitget-Symbol-Mapping
# Format: { "network": { "token": "BITGET_SYMBOL" } }
L2_TOKEN_MAP: Dict[str, Dict[str, str]] = {
    "polygon": {
        "MATIC": "MATICUSDT",
        "POL":   "POLUSDT",
    },
    "arbitrum": {
        "ARB": "ARBUSDT",
    },
    "optimism": {
        "OP": "OPUSDT",
    },
    "immutable": {
        "IMX": "IMXUSDT",
    },
    "loopring": {
        "LRC": "LRCUSDT",
    },
    "metis": {
        "METIS": "METISUSDT",
    },
    "starknet": {
        "STRK": "STRKUSDT",
    },
    "zksync": {
        "ZK": "ZKUSDT",
    },
}

# Alle unterstützten Symbole flach für schnellen Lookup
ALL_L2_SYMBOLS: List[str] = [
    sym
    for tokens in L2_TOKEN_MAP.values()
    for sym in tokens.values()
]

BITGET_REST = "https://api.bitget.com"


class BitgetL2DataFetcher:
    """
    Holt Orderbook-Snapshots für L2-Token von Bitget.
    Nutzt ausschließlich die öffentliche REST API (kein API-Key nötig).
    """

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    # ------------------------------------------------------------------
    # Einzelner Snapshot
    # ------------------------------------------------------------------

    async def get_orderbook(
        self,
        symbol: str,
        limit: int = 50,
    ) -> Optional[Dict[str, Any]]:
        """
        Holt Orderbook für ein einzelnes Symbol von Bitget V2.

        Args:
            symbol: Bitget Symbol (z.B. "ARBUSDT")
            limit:  Anzahl der Levels (max 150)

        Returns:
            Dict mit bids/asks/timestamp oder None bei Fehler
        """
        session = await self._get_session()
        url = f"{BITGET_REST}/api/v2/spot/market/orderbook"
        params = {"symbol": symbol, "limit": str(limit), "type": "step0"}

        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status != 200:
                    logger.warning(f"Bitget L2: HTTP {resp.status} for {symbol}")
                    return None
                result = await resp.json()

                if result.get("code") != "00000":
                    logger.warning(f"Bitget L2: API error for {symbol}: {result.get('msg')}")
                    return None

                data = result.get("data", {})
                return {
                    "symbol": symbol,
                    "bids": data.get("bids", []),   # [[price, qty], ...]
                    "asks": data.get("asks", []),
                    "ts": data.get("ts"),
                }

        except asyncio.TimeoutError:
            logger.warning(f"Bitget L2: Timeout for {symbol}")
            return None
        except Exception as e:
            logger.error(f"Bitget L2: Error fetching {symbol}: {e}")
            return None

    # ------------------------------------------------------------------
    # Ticker (Mid-Price + 24h Volume)
    # ------------------------------------------------------------------

    async def get_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Holt Ticker-Daten (letzter Preis, 24h-Vol, Bid/Ask) für ein Symbol.
        """
        session = await self._get_session()
        url = f"{BITGET_REST}/api/v2/spot/market/tickers"
        params = {"symbol": symbol}

        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status != 200:
                    return None
                result = await resp.json()
                if result.get("code") != "00000":
                    return None

                items = result.get("data", [])
                if not items:
                    return None
                t = items[0]
                return {
                    "symbol": symbol,
                    "last_price": float(t.get("lastPr", 0) or 0),
                    "bid":        float(t.get("bidPr", 0) or 0),
                    "ask":        float(t.get("askPr", 0) or 0),
                    "volume_24h": float(t.get("baseVolume", 0) or 0),
                    "quote_vol":  float(t.get("quoteVolume", 0) or 0),
                    "change_24h": float(t.get("change24h", 0) or 0),
                }
        except Exception as e:
            logger.error(f"Bitget L2: Ticker error for {symbol}: {e}")
            return None

    # ------------------------------------------------------------------
    # Alle L2-Token eines Netzwerks
    # ------------------------------------------------------------------

    async def get_l2_orderbooks_for_network(
        self,
        network: str,
        limit: int = 50,
    ) -> Dict[str, Any]:
        """
        Holt Orderbooks für alle L2-Token eines Netzwerks gleichzeitig.

        Args:
            network: Netzwerkname (z.B. "arbitrum", "polygon")
            limit:   Orderbook-Tiefe

        Returns:
            Dict { token_symbol: orderbook_data }
        """
        network = network.lower()
        tokens = L2_TOKEN_MAP.get(network)
        if not tokens:
            return {}

        tasks = {
            token: asyncio.create_task(self.get_orderbook(bitget_sym, limit))
            for token, bitget_sym in tokens.items()
        }
        await asyncio.gather(*tasks.values(), return_exceptions=True)

        result: Dict[str, Any] = {}
        for token, task in tasks.items():
            try:
                data = task.result()
                if data:
                    result[token] = data
            except Exception as e:
                logger.warning(f"Bitget L2: Failed for {token} on {network}: {e}")

        return result

    # ------------------------------------------------------------------
    # Alle L2-Token (alle Netzwerke) + Heatmap-Format
    # ------------------------------------------------------------------

    async def get_all_l2_heatmap_data(
        self,
        limit: int = 50,
    ) -> Dict[str, Any]:
        """
        Holt Orderbooks + Ticker für ALLE L2-Token und gibt
        ein heatmap-fähiges Format zurück.

        Returns:
            {
              "networks": {
                "arbitrum": {
                  "ARB": {
                    "symbol": "ARBUSDT",
                    "bids": [[price, qty], ...],
                    "asks": [[price, qty], ...],
                    "ticker": { last_price, volume_24h, ... },
                    "bid_depth_usd":  float,   # Summe aller Bid-USD-Werte
                    "ask_depth_usd":  float,
                    "mid_price":      float,
                  }
                },
                ...
              },
              "heatmap_matrix": {
                "networks": [...],
                "metrics":  ["bid_depth_usd", "ask_depth_usd", "volume_24h"],
                "matrix":   [[...], ...]   # [network][metric]
              }
            }
        """
        # Alle Symbole parallel abrufen
        ob_tasks: Dict[str, asyncio.Task] = {}
        tk_tasks: Dict[str, asyncio.Task] = {}

        for network, tokens in L2_TOKEN_MAP.items():
            for token, sym in tokens.items():
                key = f"{network}:{token}"
                ob_tasks[key] = asyncio.create_task(self.get_orderbook(sym, limit))
                tk_tasks[key] = asyncio.create_task(self.get_ticker(sym))

        await asyncio.gather(
            *ob_tasks.values(),
            *tk_tasks.values(),
            return_exceptions=True,
        )

        networks_data: Dict[str, Dict[str, Any]] = {}

        for network, tokens in L2_TOKEN_MAP.items():
            networks_data[network] = {}
            for token, sym in tokens.items():
                key = f"{network}:{token}"

                try:
                    ob = ob_tasks[key].result()
                except Exception:
                    ob = None
                try:
                    tk = tk_tasks[key].result()
                except Exception:
                    tk = None

                if not ob and not tk:
                    continue

                mid_price = 0.0
                bid_depth_usd = 0.0
                ask_depth_usd = 0.0

                if ob:
                    bids = ob.get("bids", [])
                    asks = ob.get("asks", [])
                    if bids and asks:
                        best_bid = float(bids[0][0]) if bids else 0.0
                        best_ask = float(asks[0][0]) if asks else 0.0
                        mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else 0.0

                    bid_depth_usd = sum(
                        float(p) * float(q) for p, q in bids if p and q
                    )
                    ask_depth_usd = sum(
                        float(p) * float(q) for p, q in asks if p and q
                    )

                networks_data[network][token] = {
                    "symbol":        sym,
                    "bids":          ob.get("bids", []) if ob else [],
                    "asks":          ob.get("asks", []) if ob else [],
                    "ticker":        tk or {},
                    "bid_depth_usd": round(bid_depth_usd, 2),
                    "ask_depth_usd": round(ask_depth_usd, 2),
                    "mid_price":     round(mid_price, 6),
                }

        # Heatmap-Matrix: pro Netzwerk aggregierte Werte
        network_list = list(networks_data.keys())
        metrics = ["bid_depth_usd", "ask_depth_usd", "volume_24h"]
        matrix: List[List[float]] = []

        for net in network_list:
            row_bid = sum(t.get("bid_depth_usd", 0) for t in networks_data[net].values())
            row_ask = sum(t.get("ask_depth_usd", 0) for t in networks_data[net].values())
            row_vol = sum(
                t.get("ticker", {}).get("volume_24h", 0) * t.get("mid_price", 0)
                for t in networks_data[net].values()
            )
            matrix.append([round(row_bid, 2), round(row_ask, 2), round(row_vol, 2)])

        return {
            "source":   "bitget_cex",
            "networks": networks_data,
            "heatmap_matrix": {
                "networks": network_list,
                "metrics":  metrics,
                "matrix":   matrix,
            },
        }

    # ------------------------------------------------------------------
    # Hilfsmethode: verfügbare Netzwerke
    # ------------------------------------------------------------------

    @staticmethod
    def supported_networks() -> List[str]:
        return list(L2_TOKEN_MAP.keys())

    @staticmethod
    def supported_tokens(network: str) -> Dict[str, str]:
        """Gibt { token: bitget_symbol } für ein Netzwerk zurück."""
        return L2_TOKEN_MAP.get(network.lower(), {})
