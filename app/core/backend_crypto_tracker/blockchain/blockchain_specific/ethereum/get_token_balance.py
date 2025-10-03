from typing import Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_token_balance(provider, token_address: str, wallet_address: str) -> float:
    """Holt den Token-Bestand einer Wallet"""
    try:
        if not provider.w3:
            logger.error("Web3 connection not initialized")
            return 0
        
        # ERC20-ABI für balanceOf
        erc20_abi = [
            {
                "constant": True,
                "inputs": [{"name": "_owner", "type": "address"}],
                "name": "balanceOf",
                "outputs": [{"name": "balance", "type": "uint256"}],
                "type": "function"
            },
            {
                "constant": True,
                "inputs": [],
                "name": "decimals",
                "outputs": [{"name": "", "type": "uint8"}],
                "type": "function"
            }
        ]
        
        # Erstelle Contract-Instanz
        contract = provider.w3.eth.contract(address=token_address, abi=erc20_abi)
        
        # Hole Decimals und Balance
        decimals = await contract.functions.decimals().call()
        balance_raw = await contract.functions.balanceOf(wallet_address).call()
        
        # Konvertiere in lesbare Zahl
        return balance_raw / (10 ** decimals)
    except Exception as e:
        logger.error(f"Fehler beim Abrufen des Token-Bestands: {e}")
        return 0
