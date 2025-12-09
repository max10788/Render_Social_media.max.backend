"""
Curve v2 DEX Integration
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


class CurveV2Exchange(BaseDEX):
    """
    Curve v2 Integration
    
    Nutzt The Graph API für Pool-Daten
    Curve v2 nutzt "Cryptoswap" Invariant für volatile Assets
    """
    
    # Curve v2 Subgraph IDs für verschiedene Netzwerke
    SUBGRAPH_IDS = {
        "ethereum": "FNZXWQJz9FtuPwGxZnALg2Ej5xGMEgYBPFQqJkFCX8mH",  # Curve v2 Ethereum
        "polygon": "3cYpGP8N1JkWPUVCLqGNZf9sF3UYZVQ4NKGZfBmAj8Kq",
        "arbitrum": "2VqFNfaMCfJLWNXFg6HJMqUTZZQDDmLhkB9pEKN8B6K9",
        "optimism": "5XwP9vDZqQfBCJmNkLX3Hg7VYxmEPWj8mZKB2pQR6C4N"
    }
    
    def __init__(self):
        super().__init__(Exchange.CURVE_V2)
        self.session: Optional[aiohttp.ClientSession] = None
        self._pool_cache: Dict[str, Dict] = {}
    
    def _get_subgraph_url(self, network: str = "ethereum") -> str:
        """
        Holt Subgraph URL mit API Key Support
        
        Args:
            network: Netzwerk (ethereum, polygon, arbitrum, optimism)
            
        Returns:
            Subgraph URL oder leerer String wenn kein API Key
        """
        api_key = os.getenv("THE_GRAPH_API_KEY", "")
        
        if not api_key:
            logger.error("=" * 80)
            logger.error("❌ THE_GRAPH_API_KEY NOT SET!")
            logger.error("=" * 80)
            logger.error("Please set THE_GRAPH_API_KEY environment variable.")
            logger.error("See instructions in Uniswap v3 integration.")
            logger.error("=" * 80)
            return ""
        
        subgraph_id = self.SUBGRAPH_IDS.get(network, self.SUBGRAPH_IDS["ethereum"])
        url = f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}"
        logger.info(f"✅ Using The Graph URL for Curve v2 {network}")
        return url
    
    async def get_pool_info(self, pool_address: str) -> Dict[str, Any]:
        """Holt Pool-Informationen via The Graph"""
        try:
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            # GraphQL Query für Curve Pool
            query = """
            query($poolAddress: String!) {
                pool(id: $poolAddress) {
                    id
                    coins
                    balances
                    A
                    gamma
                    mid_fee
                    out_fee
                    fee_gamma
                    virtualPrice
                    xcpProfit
                    xcpProfitA
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
            logger.error(f"Failed to get Curve pool info: {e}")
            return {}
    
    async def get_liquidity_ticks(self, pool_address: str) -> List[DEXLiquidityTick]:
        """
        Holt Liquiditäts-Ticks vom Pool
        
        Curve v2 hat keine diskreten Ticks wie Uniswap v3,
        stattdessen kontinuierliche Bonding Curve.
        Wir approximieren durch Sampling der Curve.
        """
        try:
            pool_info = await self.get_pool_info(pool_address)
            
            if not pool_info:
                logger.warning(f"No pool info for {pool_address}")
                return []
            
            # Extrahiere Balances
            balances = pool_info.get("balances", [])
            if len(balances) < 2:
                logger.error("Curve pool needs at least 2 tokens")
                return []
            
            balance_0 = float(balances[0]) if balances[0] else 0
            balance_1 = float(balances[1]) if balances[1] else 0
            
            if balance_0 == 0 or balance_1 == 0:
                logger.error("Pool has zero balance")
                return []
            
            # Berechne aktuellen Preis
            current_price = balance_1 / balance_0
            
            # Sample die Curve um aktuellen Preis
            # Erstelle Ticks von -50% bis +50% um current_price
            ticks = []
            num_samples = 100
            
            for i in range(num_samples):
                # Price range: 0.5x bis 2x vom current_price
                price_multiplier = 0.5 + (1.5 * i / num_samples)
                sample_price = current_price * price_multiplier
                
                # Berechne Liquidität bei diesem Preis
                # Für Curve v2: Liquidität ist etwa konstant in einem Bereich
                # Vereinfachung: Nutze geometrisches Mittel der Balances
                liquidity = math.sqrt(balance_0 * balance_1)
                
                # Erstelle Tick
                price_lower = sample_price * 0.99
                price_upper = sample_price * 1.01
                
                tick = DEXLiquidityTick(
                    tick_index=i,
                    liquidity=liquidity,
                    price_lower=price_lower,
                    price_upper=price_upper
                )
                ticks.append(tick)
            
            logger.info(f"Generated {len(ticks)} liquidity samples for Curve pool")
            return ticks
            
        except Exception as e:
            logger.error(f"Failed to get Curve liquidity: {e}")
            return []
    
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
            
            # Berechne aktuellen Preis
            balances = pool_info.get("balances", [])
            if len(balances) < 2:
                return None
            
            balance_0 = float(balances[0]) if balances[0] else 0
            balance_1 = float(balances[1]) if balances[1] else 0
            
            if balance_0 == 0:
                return None
            
            current_price = balance_1 / balance_0
            
            # Erstelle Symbol aus Coins
            coins = pool_info.get("coins", [])
            if len(coins) >= 2:
                trading_symbol = f"Token0/Token1"  # Simplified
            else:
                trading_symbol = "CURVE_POOL"
            
            # Konvertiere Ticks zu Orderbook
            orderbook = self.ticks_to_orderbook(ticks, current_price, trading_symbol)
            
            # Limitiere
            orderbook.bids.levels = orderbook.bids.levels[:limit]
            orderbook.asks.levels = orderbook.asks.levels[:limit]
            
            logger.info(
                f"Curve v2 orderbook: {len(orderbook.bids.levels)} bids, "
                f"{len(orderbook.asks.levels)} asks"
            )
            
            return orderbook
            
        except Exception as e:
            logger.error(f"Failed to get Curve snapshot: {e}")
            return None
    
    async def _handle_orderbook_update(self, data: Dict[str, Any]):
        """Nicht genutzt - DEX nutzt Polling"""
        pass
