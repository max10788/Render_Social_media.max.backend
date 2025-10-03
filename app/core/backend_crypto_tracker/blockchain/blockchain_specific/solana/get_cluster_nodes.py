from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_cluster_nodes(provider) -> Optional[List[Dict[str, Any]]]:
    """Holt Informationen über Cluster-Nodes"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'getClusterNodes',
            'params': []
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            nodes = []
            for node in data['result']:
                nodes.append({
                    'pubkey': node.get('pubkey'),
                    'gossip': node.get('gossip'),
                    'tpu': node.get('tpu'),
                    'version': node.get('version'),
                    'last_updated': datetime.now()
                })
            
            return nodes
    except Exception as e:
        logger.error(f"Error fetching Solana cluster nodes: {e}")
    
    return None
