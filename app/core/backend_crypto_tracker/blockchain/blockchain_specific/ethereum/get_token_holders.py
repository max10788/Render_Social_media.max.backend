from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_token_holders(
    provider, 
    token_address: str, 
    chain: str, 
    limit: int = 100
) -> List[Dict[str, Any]]:
    """Holt Token-Holder über den EtherscanProvider"""
    try:
        if provider.etherscan_provider:
            logger.info(f"Getting token holders for {token_address} on {chain} via EtherscanProvider")
            return await provider.etherscan_provider.get_token_holders(token_address, chain, limit)
        else:
            logger.warning("No EtherscanProvider available for token holders")
            return []
    except Exception as e:
        logger.error(f"Error getting token holders: {e}")
        return []
