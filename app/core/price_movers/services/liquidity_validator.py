"""
Liquidity Event Validator

Validates and normalizes liquidity events (ADD/REMOVE)
to prevent them from distorting price calculations.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

logger = logging.getLogger(__name__)


class LiquidityValidator:
    """
    Validates liquidity events and separates them from regular trades
    
    Why needed:
    - Liquidity events have HUGE volumes
    - But they don't represent actual trading
    - They distort price calculations if treated as swaps
    """
    
    # Reasonable limits for SOL/USDT trading
    MAX_REASONABLE_TRADE_VALUE = 10_000_000  # $10M
    MAX_REASONABLE_PRICE = 10_000.0          # $10k per SOL
    MIN_REASONABLE_PRICE = 1.0               # $1 per SOL
    
    # Liquidity-specific thresholds
    MIN_LIQUIDITY_EVENT_VALUE = 50_000  # $50k minimum to be considered liquidity
    
    @classmethod
    def validate_trade(
        cls,
        trade: Dict[str, Any],
        symbol: str = "SOL/USDT"
    ) -> Dict[str, Any]:
        """
        Validate a trade and classify it
        
        Returns:
            {
                'is_valid': bool,
                'is_liquidity_event': bool,
                'classification': str,  # 'swap', 'add_liquidity', 'remove_liquidity', 'invalid'
                'warning': Optional[str],
                'corrected_trade': Optional[Dict]
            }
        """
        try:
            price = trade.get('price', 0)
            amount = trade.get('amount', 0)
            value_usd = amount * price
            tx_type = trade.get('transaction_type', 'SWAP')
            
            # ✅ CHECK 1: Price range
            if not (cls.MIN_REASONABLE_PRICE <= price <= cls.MAX_REASONABLE_PRICE):
                return {
                    'is_valid': False,
                    'is_liquidity_event': False,
                    'classification': 'invalid',
                    'warning': f"Abnormal price: ${price:.2f}",
                    'corrected_trade': None
                }
            
            # ✅ CHECK 2: Trade value
            if value_usd > cls.MAX_REASONABLE_TRADE_VALUE:
                # Could be liquidity event
                if tx_type in ['ADD_LIQUIDITY', 'REMOVE_LIQUIDITY']:
                    return {
                        'is_valid': True,
                        'is_liquidity_event': True,
                        'classification': tx_type.lower(),
                        'warning': f"Large liquidity event: ${value_usd:,.0f}",
                        'corrected_trade': trade
                    }
                else:
                    # Too large for regular swap
                    return {
                        'is_valid': False,
                        'is_liquidity_event': False,
                        'classification': 'invalid',
                        'warning': f"Trade too large: ${value_usd:,.0f}",
                        'corrected_trade': None
                    }
            
            # ✅ CHECK 3: Detect mis-classified liquidity events
            if value_usd > cls.MIN_LIQUIDITY_EVENT_VALUE and tx_type == 'SWAP':
                # Large "swap" might actually be liquidity
                liquidity_delta = trade.get('liquidity_delta', 0)
                
                if liquidity_delta > 0:
                    logger.warning(
                        f"⚠️ SWAP mis-classified as liquidity event\n"
                        f"   Value: ${value_usd:,.0f}\n"
                        f"   Liquidity Delta: {liquidity_delta:.4f}\n"
                        f"   → Reclassifying"
                    )
                    
                    corrected = trade.copy()
                    corrected['transaction_type'] = 'ADD_LIQUIDITY'
                    
                    return {
                        'is_valid': True,
                        'is_liquidity_event': True,
                        'classification': 'add_liquidity',
                        'warning': f"Reclassified from SWAP",
                        'corrected_trade': corrected
                    }
            
            # ✅ Valid regular trade
            return {
                'is_valid': True,
                'is_liquidity_event': tx_type in ['ADD_LIQUIDITY', 'REMOVE_LIQUIDITY'],
                'classification': tx_type.lower(),
                'warning': None,
                'corrected_trade': trade
            }
            
        except Exception as e:
            logger.error(f"Validation error: {e}")
            return {
                'is_valid': False,
                'is_liquidity_event': False,
                'classification': 'error',
                'warning': str(e),
                'corrected_trade': None
            }
    
    @classmethod
    def filter_trades(
        cls,
        trades: List[Dict[str, Any]],
        include_liquidity: bool = True
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Filter and separate trades
        
        Returns:
            {
                'valid_swaps': [...],
                'liquidity_events': [...],
                'invalid_trades': [...]
            }
        """
        valid_swaps = []
        liquidity_events = []
        invalid_trades = []
        
        for trade in trades:
            validation = cls.validate_trade(trade)
            
            if not validation['is_valid']:
                invalid_trades.append({
                    'trade': trade,
                    'reason': validation['warning']
                })
                continue
            
            corrected = validation['corrected_trade'] or trade
            
            if validation['is_liquidity_event']:
                liquidity_events.append(corrected)
            else:
                valid_swaps.append(corrected)
        
        logger.info(
            f"📊 Trade Filtering:\n"
            f"   Valid Swaps: {len(valid_swaps)}\n"
            f"   Liquidity Events: {len(liquidity_events)}\n"
            f"   Invalid: {len(invalid_trades)}"
        )
        
        return {
            'valid_swaps': valid_swaps,
            'liquidity_events': liquidity_events,
            'invalid_trades': invalid_trades
        }
