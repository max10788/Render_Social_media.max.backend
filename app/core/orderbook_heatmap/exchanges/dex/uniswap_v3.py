"""
Uniswap v3 DEX Integration
"""
import asyncio
import logging
from typing import Optional, Dict, Any, List
import aiohttp
from decimal import Decimal
import math
import os  # ← NEU: Für API Key aus Environment

from app.core.orderbook_heatmap.exchanges.dex.base import DEXExchange, BaseDEX
from app.core.orderbook_heatmap.models.orderbook import (
    Orderbook, OrderbookLevel, OrderbookSide, Exchange, DEXLiquidityTick
)


logger = logging.getLogger(__name__)


class UniswapV3Exchange(BaseDEX):
    """
    Uniswap v3 Integration
    
    Nutzt The Graph API oder direkten RPC-Call zum Pool Contract
    """
    
    # ============================================================================
    # NEU: Subgraph IDs für The Graph Decentralized Network (Dezember 2024)
    # Alte URL funktioniert nicht mehr seit Juni 2023!
    # ============================================================================
    SUBGRAPH_IDS = {
        "ethereum": "5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV",
        "polygon": "3hCPRGf4z88VC5rsBKU5AA9FBBq5nF3jbKJG7VZCbhjm",
        "arbitrum": "FbCGRftH4a3yZugY7TnbYgPJVEv2LvMT6oF1fxPe9aJM",
        "optimism": "Cghf4LfVqPiFw6fp6Y5X5Ubc8UpmUhSfJL82zwiBFLaj",
        "base": "43Hwfi3dJSoGpyas9VwNoDAv55yjgGrPpNSmbQZArzMG"
    }
    
    # Uniswap v3 Pool ABI (vereinfacht)
    POOL_ABI = [
        "function slot0() view returns (uint160 sqrtPriceX96, int24 tick, uint16 observationIndex, uint16 observationCardinality, uint16 observationCardinalityNext, uint8 feeProtocol, bool unlocked)",
        "function liquidity() view returns (uint128)",
        "function token0() view returns (address)",
        "function token1() view returns (address)",
    ]
    
    def __init__(self):
        super().__init__(Exchange.UNISWAP_V3)
        self.session: Optional[aiohttp.ClientSession] = None
        self._pool_cache: Dict[str, Dict] = {}
    
    # ============================================================================
    # NEU: Methode zum Holen der Subgraph URL mit API Key Support
    # ============================================================================
    def _get_subgraph_url(self, network: str = "ethereum") -> str:
        """
        Holt Subgraph URL mit API Key Support
        
        Args:
            network: Netzwerk (ethereum, polygon, arbitrum, optimism, base)
            
        Returns:
            Subgraph URL oder leerer String wenn kein API Key
        """
        api_key = os.getenv("THE_GRAPH_API_KEY", "")
        
        if not api_key:
            logger.error("=" * 80)
            logger.error("❌ THE_GRAPH_API_KEY NOT SET!")
            logger.error("=" * 80)
            logger.error("The Graph Hosted Service was shut down in June 2023.")
            logger.error("You MUST set THE_GRAPH_API_KEY environment variable.")
            logger.error("")
            logger.error("HOW TO FIX:")
            logger.error("1. Go to: https://thegraph.com/studio/")
            logger.error("2. Create account / Sign in")
            logger.error("3. Click 'API Keys' → 'Create API Key'")
            logger.error("4. Copy the key")
            logger.error("5. On Render Dashboard:")
            logger.error("   - Go to Environment tab")
            logger.error("   - Add Environment Variable:")
            logger.error("     Key: THE_GRAPH_API_KEY")
            logger.error("     Value: [your API key]")
            logger.error("   - Save Changes (auto-deploy takes 2-3 min)")
            logger.error("=" * 80)
            return ""
        
        subgraph_id = self.SUBGRAPH_IDS.get(network, self.SUBGRAPH_IDS["ethereum"])
        url = f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}"
        logger.info(f"✅ Using The Graph URL for {network}: {url[:80]}...")
        return url
        
    async def get_pool_info(self, pool_address: str) -> Dict[str, Any]:
        """Holt Pool-Informationen via The Graph"""
        try:
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            # GraphQL Query
            query = """
            query($poolAddress: String!) {
                pool(id: $poolAddress) {
                    id
                    token0 {
                        id
                        symbol
                        decimals
                    }
                    token1 {
                        id
                        symbol
                        decimals
                    }
                    sqrtPrice
                    liquidity
                    tick
                    feeTier
                }
            }
            """
            
            variables = {"poolAddress": pool_address.lower()}
            
            # ============================================================================
            # GEÄNDERT: Nutze _get_subgraph_url() statt self.SUBGRAPH_URL
            # ============================================================================
            subgraph_url = self._get_subgraph_url()
            if not subgraph_url:
                logger.error("❌ Cannot get pool info: Subgraph URL not available (missing API key)")
                return {}
            
            async with self.session.post(
                subgraph_url,  # ← GEÄNDERT!
                json={"query": query, "variables": variables}
            ) as resp:
                if resp.status != 200:
                    logger.error(f"The Graph API error: {resp.status}")
                    return {}
                
                result = await resp.json()
                pool_data = result.get("data", {}).get("pool", {})
                
                if pool_data:
                    self._pool_cache[pool_address] = pool_data
                
                return pool_data
                
        except Exception as e:
            logger.error(f"Failed to get Uniswap pool info: {e}")
            return {}
    
    async def get_liquidity_ticks(self, pool_address: str) -> List[DEXLiquidityTick]:
        """
        Holt Liquiditäts-Ticks vom Pool via The Graph
        """
        try:
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            # GraphQL Query für Ticks
            query = """
            query($poolAddress: String!, $skip: Int!) {
                ticks(
                    first: 1000
                    skip: $skip
                    where: { poolAddress: $poolAddress }
                    orderBy: tickIdx
                ) {
                    tickIdx
                    liquidityGross
                    liquidityNet
                }
            }
            """
            
            all_ticks = []
            skip = 0
            
            # ============================================================================
            # GEÄNDERT: Hole URL einmal vor der Loop
            # ============================================================================
            subgraph_url = self._get_subgraph_url()
            if not subgraph_url:
                logger.error("❌ Cannot get liquidity ticks: Subgraph URL not available (missing API key)")
                return []
            
            # Hole Ticks in Batches (max 1000 per Query)
            while True:
                variables = {
                    "poolAddress": pool_address.lower(),
                    "skip": skip
                }
                
                async with self.session.post(
                    subgraph_url,  # ← GEÄNDERT!
                    json={"query": query, "variables": variables}
                ) as resp:
                    if resp.status != 200:
                        break
                    
                    result = await resp.json()
                    ticks = result.get("data", {}).get("ticks", [])
                    
                    if not ticks:
                        break
                    
                    all_ticks.extend(ticks)
                    
                    if len(ticks) < 1000:
                        break
                    
                    skip += 1000
            
            # Konvertiere zu DEXLiquidityTick
            return self._parse_ticks(all_ticks)
            
        except Exception as e:
            logger.error(f"Failed to get Uniswap ticks: {e}")
            return []
    
    def _parse_ticks(self, ticks_data: List[Dict]) -> List[DEXLiquidityTick]:
        """Parst Tick-Daten zu DEXLiquidityTick"""
        result = []
        
        for tick in ticks_data:
            try:
                tick_idx = int(tick["tickIdx"])
                liquidity = float(tick["liquidityGross"])
                
                if liquidity == 0:
                    continue
                
                # Berechne Preis-Bounds aus Tick Index
                # Price = 1.0001^tick
                price_lower = self._tick_to_price(tick_idx)
                price_upper = self._tick_to_price(tick_idx + 1)
                
                result.append(DEXLiquidityTick(
                    tick_index=tick_idx,
                    liquidity=liquidity,
                    price_lower=price_lower,
                    price_upper=price_upper
                ))
                
            except Exception as e:
                logger.warning(f"Failed to parse tick: {e}")
                continue
        
        return result
    
    def _tick_to_price(self, tick: int) -> float:
        """
        Konvertiert Tick Index zu Preis
        Uniswap v3 Formula: price = 1.0001^tick
        """
        return math.pow(1.0001, tick)
    
    async def get_pool_liquidity(self, pool_address: str) -> Dict[str, Any]:
        """Holt Pool-Liquidität"""
        pool_info = await self.get_pool_info(pool_address)
        ticks = await self.get_liquidity_ticks(pool_address)
        
        return {
            "pool_info": pool_info,
            "ticks": ticks,
            "total_ticks": len(ticks),
            "total_liquidity": sum(t.liquidity for t in ticks)
        }
    
    async def get_orderbook_snapshot(self, symbol: str, limit: int = 100) -> Optional[Orderbook]:
        """
        Holt Orderbuch-Snapshot (konvertiert DEX Liquidity zu Orderbook)
        
        Args:
            symbol: Pool Address (z.B. 0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640)
            limit: Anzahl Ticks (vereinfacht)
        """
        try:
            pool_address = symbol
            
            # Hole Pool Info
            pool_info = await self.get_pool_info(pool_address)
            if not pool_info:
                logger.error(f"Pool not found: {pool_address}")
                return None
            
            # Hole Liquidity Ticks
            ticks = await self.get_liquidity_ticks(pool_address)
            if not ticks:
                logger.warning(f"No ticks found for pool: {pool_address}")
                return None
            
            # Berechne aktuellen Preis aus sqrtPriceX96
            sqrt_price_x96 = int(pool_info.get("sqrtPrice", 0))
            current_price = self._sqrt_price_to_price(sqrt_price_x96)
            
            # Erstelle Symbol aus Token-Symbolen
            token0 = pool_info.get("token0", {}).get("symbol", "TOKEN0")
            token1 = pool_info.get("token1", {}).get("symbol", "TOKEN1")
            trading_symbol = f"{token0}/{token1}"
            
            # Konvertiere Ticks zu Orderbook
            orderbook = self.ticks_to_orderbook(ticks, current_price, trading_symbol)
            
            # Limitiere auf top N levels
            orderbook.bids.levels = orderbook.bids.levels[:limit]
            orderbook.asks.levels = orderbook.asks.levels[:limit]
            
            logger.info(
                f"Uniswap v3 orderbook: {len(orderbook.bids.levels)} bids, "
                f"{len(orderbook.asks.levels)} asks, price={current_price:.4f}"
            )
            
            return orderbook
            
        except Exception as e:
            logger.error(f"Failed to get Uniswap snapshot: {e}")
            return None
    
    def _sqrt_price_to_price(self, sqrt_price_x96: int) -> float:
        """
        Konvertiert sqrtPriceX96 zu Preis
        price = (sqrtPriceX96 / 2^96)^2
        """
        if sqrt_price_x96 == 0:
            return 0.0
        
        Q96 = 2 ** 96
        sqrt_price = sqrt_price_x96 / Q96
        price = sqrt_price ** 2
        return price
    
    async def _handle_orderbook_update(self, data: Dict[str, Any]):
        """Nicht genutzt - DEX nutzt Polling statt WebSocket"""
        pass
