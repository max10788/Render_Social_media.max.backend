"""
PancakeSwap v3 DEX Integration
"""
import asyncio
import logging
from typing import Optional, Dict, Any, List
import aiohttp
from decimal import Decimal
import math
import os

from app.core.orderbook_heatmap.exchanges.dex.base import BaseDEX
from app.core.orderbook_heatmap.models.orderbook import (
    Orderbook, OrderbookLevel, OrderbookSide, Exchange, DEXLiquidityTick
)


logger = logging.getLogger(__name__)


class PancakeSwapExchange(BaseDEX):
    """
    PancakeSwap v3 Integration (BSC)
    
    Ähnlich wie Uniswap v3 - nutzt concentrated liquidity
    """
    
    # PancakeSwap v3 Subgraph IDs
    SUBGRAPH_IDS = {
        "bsc": "F85MNzP6cq45Mys3CbWDUepTgX7jsCEjqxFbQSZGqKP9",  # BSC Mainnet
        "ethereum": "8NdDWMxzAc7F4kxWLSyZcXVj4VpSFxXJe5fVxJ9pN2M3",  # ETH
        "arbitrum": "2J8EzVgZB2KxFbVPj9pHLmFX9Kc4qQVxJfN8pWKJ3N5P"  # Arbitrum
    }
    
    def __init__(self):
        super().__init__(Exchange.PANCAKESWAP)
        self.session: Optional[aiohttp.ClientSession] = None
        self._pool_cache: Dict[str, Dict] = {}
    
    def _get_subgraph_url(self, network: str = "bsc") -> str:
        """
        Holt Subgraph URL mit API Key Support
        
        Args:
            network: Netzwerk (bsc, ethereum, arbitrum)
            
        Returns:
            Subgraph URL oder leerer String wenn kein API Key
        """
        api_key = os.getenv("THE_GRAPH_API_KEY", "")
        
        if not api_key:
            logger.error("=" * 80)
            logger.error("❌ THE_GRAPH_API_KEY NOT SET!")
            logger.error("=" * 80)
            logger.error("Please set THE_GRAPH_API_KEY environment variable.")
            logger.error("=" * 80)
            return ""
        
        subgraph_id = self.SUBGRAPH_IDS.get(network, self.SUBGRAPH_IDS["bsc"])
        url = f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}"
        logger.info(f"✅ Using The Graph URL for PancakeSwap v3 {network}")
        return url
    
    async def get_pool_info(self, pool_address: str) -> Dict[str, Any]:
        """Holt Pool-Informationen via The Graph"""
        try:
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            # GraphQL Query (same structure as Uniswap v3)
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
            
            subgraph_url = self._get_subgraph_url()
            if not subgraph_url:
                logger.error("❌ Cannot get pool info: Subgraph URL not available")
                return {}
            
            async with self.session.post(
                subgraph_url,
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
            logger.error(f"Failed to get PancakeSwap pool info: {e}")
            return {}
    
    async def get_liquidity_ticks(self, pool_address: str) -> List[DEXLiquidityTick]:
        """
        Holt Liquiditäts-Ticks vom Pool
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
            
            subgraph_url = self._get_subgraph_url()
            if not subgraph_url:
                logger.error("❌ Cannot get liquidity ticks: Subgraph URL not available")
                return []
            
            # Hole Ticks in Batches
            while True:
                variables = {
                    "poolAddress": pool_address.lower(),
                    "skip": skip
                }
                
                async with self.session.post(
                    subgraph_url,
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
            logger.error(f"Failed to get PancakeSwap ticks: {e}")
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
        PancakeSwap v3 Formula (same as Uniswap): price = 1.0001^tick
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
            symbol: Pool Address
            limit: Anzahl Ticks
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
                logger.warning(f"No ticks for pool: {pool_address}")
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
            
            # Limitiere
            orderbook.bids.levels = orderbook.bids.levels[:limit]
            orderbook.asks.levels = orderbook.asks.levels[:limit]
            
            logger.info(
                f"PancakeSwap v3 orderbook: {len(orderbook.bids.levels)} bids, "
                f"{len(orderbook.asks.levels)} asks, price={current_price:.4f}"
            )
            
            return orderbook
            
        except Exception as e:
            logger.error(f"Failed to get PancakeSwap snapshot: {e}")
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
        """Nicht genutzt - DEX nutzt Polling"""
        pass
