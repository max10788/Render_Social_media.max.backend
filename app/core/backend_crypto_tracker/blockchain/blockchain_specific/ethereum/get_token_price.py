from datetime import datetime
from typing import Optional, Dict, Any
import aiohttp
import os
from web3 import Web3
from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.blockchain.data_models.token_price_data import TokenPriceData

logger = get_logger(__name__)

# ERC20 ABI für balanceOf und decimals
ERC20_ABI = [
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
    },
    {
        "constant": True,
        "inputs": [],
        "name": "name",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "totalSupply",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function"
    }
]


async def get_onchain_metadata(token_address: str, rpc_url: str) -> Optional[Dict[str, Any]]:
    """Holt Token-Metadaten direkt von der Blockchain"""
    try:
        w3 = Web3(Web3.HTTPProvider(rpc_url))
        
        if not w3.is_connected():
            logger.warning(f"Could not connect to Ethereum RPC")
            return None
        
        logger.info(f"Connected to Ethereum RPC: {rpc_url}")
        
        # Erstelle Contract-Instanz
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(token_address),
            abi=ERC20_ABI
        )
        
        # Hole Token-Metadaten
        name = contract.functions.name().call()
        symbol = contract.functions.symbol().call()
        decimals = contract.functions.decimals().call()
        
        try:
            total_supply = contract.functions.totalSupply().call()
        except:
            total_supply = 0
        
        logger.info(f"Found token on-chain: {name} ({symbol}), decimals: {decimals}")
        
        return {
            'name': name,
            'symbol': symbol,
            'decimals': decimals,
            'total_supply': total_supply
        }
        
    except Exception as e:
        logger.warning(f"Could not fetch token metadata from RPC: {e}")
        return None


async def execute_get_token_price(token_address: str, chain: str) -> Optional[TokenPriceData]:
    """
    Ethereum-spezifische Token-Preisabfrage
    1. Holt On-Chain-Metadaten (Name, Symbol, Decimals)
    2. Versucht Preis von CoinGecko zu holen
    3. Gibt Token-Daten zurück - auch OHNE Preis wenn on-chain gefunden
    """
    try:
        logger.info(f"Fetching price for {token_address} on {chain}")
        
        # Hole RPC URL aus Environment
        rpc_url = os.getenv('ETHEREUM_RPC_URL')
        
        onchain_metadata = None
        
        # Schritt 1: Hole On-Chain-Metadaten
        if rpc_url:
            onchain_metadata = await get_onchain_metadata(token_address, rpc_url)
        
        # Schritt 2: Versuche Preis von CoinGecko zu holen
        platform_map = {
            'ethereum': 'ethereum',
            'bsc': 'binance-smart-chain',
            'polygon': 'polygon-pos',
            'avalanche': 'avalanche'
        }
        
        platform = platform_map.get(chain.lower(), 'ethereum')
        
        url = f"https://api.coingecko.com/api/v3/simple/token_price/{platform}"
        params = {
            'contract_addresses': token_address.lower(),
            'vs_currencies': 'usd',
            'include_market_cap': 'true',
            'include_24hr_vol': 'true',
            'include_24hr_change': 'true'
        }
        
        coingecko_data = None
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        data = await response.json()
                        token_data = data.get(token_address.lower(), {})
                        
                        if token_data and 'usd' in token_data:
                            logger.info(f"Successfully fetched price for {token_address} from CoinGecko")
                            coingecko_data = token_data
                        else:
                            logger.warning(f"No price data found for {token_address} on CoinGecko")
                    elif response.status == 429:
                        logger.warning(f"Rate limit exceeded for CoinGecko API")
                    else:
                        logger.warning(f"CoinGecko API returned status {response.status}")
        except Exception as e:
            logger.warning(f"Error fetching from CoinGecko: {e}")
        
        # Schritt 3: Kombiniere Daten
        # WICHTIG: Gib Token-Daten zurück, auch wenn CoinGecko keinen Preis hat!
        if onchain_metadata:
            # Token existiert on-chain! ✅
            return TokenPriceData(
                token_address=token_address,
                chain=chain,
                name=onchain_metadata.get('name', 'Unknown'),
                symbol=onchain_metadata.get('symbol', 'UNKNOWN'),
                decimals=onchain_metadata.get('decimals'),
                price=float(coingecko_data.get('usd', 0)) if coingecko_data else 0.0,
                market_cap=float(coingecko_data.get('usd_market_cap', 0)) if coingecko_data else 0.0,
                volume_24h=float(coingecko_data.get('usd_24h_vol', 0)) if coingecko_data else 0.0,
                price_change_24h=float(coingecko_data.get('usd_24h_change', 0)) if coingecko_data else 0.0,
                source="Ethereum RPC + CoinGecko" if coingecko_data else "Ethereum RPC (no price)",
                last_updated=datetime.now()
            )
        elif coingecko_data:
            # Nur CoinGecko-Daten verfügbar
            return TokenPriceData(
                token_address=token_address,
                chain=chain,
                price=float(coingecko_data.get('usd', 0)),
                market_cap=float(coingecko_data.get('usd_market_cap', 0)),
                volume_24h=float(coingecko_data.get('usd_24h_vol', 0)),
                price_change_24h=float(coingecko_data.get('usd_24h_change', 0)),
                source="CoinGecko",
                last_updated=datetime.now()
            )
        else:
            # Weder on-chain noch CoinGecko
            logger.warning(f"Token {token_address} not found on-chain or in CoinGecko")
            return None
                    
    except Exception as e:
        logger.error(f"Error fetching Ethereum token price: {e}")
        return None
