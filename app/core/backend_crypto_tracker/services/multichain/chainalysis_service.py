# services/multichain/chainalysis_service.py
import httpx
import asyncio
import logging
from typing import Optional, Dict, Any, List
from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.utils.exceptions import APIException, RateLimitExceededException

logger = get_logger(__name__)

class ChainalysisIntegration:
    """Enhanced integration with Chainalysis API for professional blockchain analysis"""
    def __init__(self, api_key: str = None):
        self.api_key = api_key
        if not self.api_key:
            logger.warning("Chainalysis API key not provided.")
        self.base_url = "https://api.chainalysis.com/api/kyt/v1"
        self.rate_limit_delay = 1.0  # 1 second between requests
        self.session_timeout = 30.0
        self.client = None
        
    async def __aenter__(self):
        """Implement context manager for proper resource management"""
        self.client = httpx.AsyncClient(timeout=self.session_timeout)
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Ensure proper cleanup of resources"""
        if self.client:
            await self.client.aclose()
    
    async def get_address_risk(self, address: str, asset: str) -> Optional[Dict]:
        """Get comprehensive risk score and entity information from Chainalysis"""
        if not self.api_key:
            logger.warning("Chainalysis API key not provided.")
            return None
            
        try:
            headers = {
                "X-API-Key": self.api_key,
                "Content-Type": "application/json"
            }
            
            # Use the context-managed client instead of creating a new one
            response = await self.client.get(
                f"{self.base_url}/addresses/{address}",
                params={"asset": asset},
                headers=headers
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Parse and enrich the response
                enriched_data = {
                    'address': address,
                    'asset': asset,
                    'risk_score': data.get('riskScore', 0),
                    'entity_type': data.get('entityType', ''),
                    'entity_name': data.get('entityName', ''),
                    'category': data.get('category', ''),
                    'total_received': data.get('totalReceived', 0),
                    'total_sent': data.get('totalSent', 0),
                    'first_seen': data.get('firstSeen'),
                    'last_seen': data.get('lastSeen'),
                    'exposure_details': data.get('exposureDetails', {}),
                    'cluster_info': data.get('clusterInfo', {})
                }
                
                await asyncio.sleep(self.rate_limit_delay)  # Rate limiting
                return enriched_data
                
            elif response.status_code == 429:  # Rate limit
                logger.warning("Chainalysis rate limit hit, waiting...")
                await asyncio.sleep(5.0)
                return None
            else:
                logger.warning(f"Chainalysis API error: {response.status_code} - {response.text}")
                return None
                
        except asyncio.TimeoutError:
            logger.error("Chainalysis API timeout")
            return None
        except Exception as e:
            logger.error(f"Chainalysis API call failed: {e}")
            return None
    
    async def screen_address(self, address: str) -> Optional[Dict]:
        """Enhanced sanctions screening with detailed violation information"""
        if not self.api_key:
            logger.warning("Chainalysis API key not provided.")
            return None
            
        try:
            headers = {
                "X-API-Key": self.api_key,
                "Content-Type": "application/json"
            }
            
            # Use the context-managed client
            response = await self.client.get(
                f"{self.base_url}/addresses/{address}/sanctions",
                headers=headers
            )
            
            if response.status_code == 200:
                data = response.json()
                
                sanctions_info = {
                    'is_sanctioned': data.get('isSanctioned', False),
                    'sanction_lists': data.get('sanctionLists', []),
                    'violation_type': data.get('violationType', ''),
                    'sanction_date': data.get('sanctionDate'),
                    'jurisdiction': data.get('jurisdiction', ''),
                    'confidence_level': data.get('confidenceLevel', 'medium')
                }
                
                await asyncio.sleep(self.rate_limit_delay)
                return sanctions_info
                
            return None
                
        except Exception as e:
            logger.error(f"Sanctions screening failed: {e}")
            return None
    
    async def get_cluster_analysis(self, address: str, asset: str) -> Optional[Dict]:
        """Get cluster analysis for address relationships"""
        if not self.api_key:
            logger.warning("Chainalysis API key not provided.")
            return None
            
        try:
            headers = {"X-API-Key": self.api_key}
            
            # Use the context-managed client
            response = await self.client.get(
                f"{self.base_url}/addresses/{address}/cluster",
                params={"asset": asset},
                headers=headers
            )
            
            if response.status_code == 200:
                await asyncio.sleep(self.rate_limit_delay)
                return response.json()
            return None
                
        except Exception as e:
            logger.error(f"Cluster analysis failed: {e}")
            return None
