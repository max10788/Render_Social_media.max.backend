import asyncio
from typing import Any, Dict, Optional, List
from solders.pubkey import Pubkey
from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_confirmed_signatures_for_address2(
    provider,
    address: str,
    limit: int = 25
) -> Optional[List[Dict[str, Any]]]:
    """
    Holt die letzten bestätigten Signaturen für eine Solana-Adresse.

    Uses the same pattern as SolanaClient.get_next_transactions:
    - solders.pubkey.Pubkey for address conversion
    - run_in_executor for sync Client calls
    - response.value for typed response parsing
    """
    try:
        if not provider:
            raise Exception("Provider ist None")

        if not address:
            raise Exception("Address ist leer")

        logger.info(f"Rufe Solana Signaturen für {address} ab (limit={limit})")

        # Convert string to Pubkey (same as solana_client.py:401)
        try:
            pubkey = Pubkey.from_string(address)
        except Exception as e:
            logger.error(f"Invalid PublicKey: {address}, Error: {str(e)}")
            return None

        # Run sync Client call in executor to avoid blocking event loop
        # (same pattern as solana_client.py:302-310, 404-407)
        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(
            None,
            lambda: provider.get_signatures_for_address(pubkey, limit=limit)
        )

        # Parse typed response (same as solana_client.py:408)
        signatures = getattr(response, "value", [])

        if not signatures:
            logger.info(f"Keine Signaturen für Adresse {address} gefunden")
            return []

        # Convert typed signature objects to standard dict format
        formatted_sigs = []
        for sig_obj in signatures:
            formatted_sigs.append({
                'signature': str(sig_obj.signature),
                'slot': sig_obj.slot,
                'err': sig_obj.err,
                'blockTime': getattr(sig_obj, 'block_time', None),
                'confirmationStatus': getattr(sig_obj, 'confirmation_status', None)
            })

        logger.info(f"Gefunden {len(formatted_sigs)} Signaturen für {address}")
        return formatted_sigs

    except Exception as e:
        logger.error(f"Error fetching Solana confirmed signatures: {str(e)}", exc_info=True)
        return None
