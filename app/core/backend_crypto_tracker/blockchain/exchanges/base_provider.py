"""
Base provider class for all API providers.
"""

import asyncio
import os
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import aiohttp

from app.core.backend_crypto_tracker.utils.exceptions import APIException, RateLimitExceededException
from app.core.backend_crypto_tracker.utils.logger import get_logger
from ..rate_limiters.rate_limiter import RateLimiter

logger = get_logger(__name__)


class BaseAPIProvider(ABC):
    """Basisklasse für alle API-Anbieter"""
    
    def __init__(self, name: str, base_url: str, api_key: Optional[str] = None, api_key_env: Optional[str] = None):
        self.name = name
        self.base_url = base_url
        # Wenn kein API-Schlüssel übergeben wurde, versuchen, ihn aus der Umgebungsvariable zu lesen
        if api_key is None and api_key_env is not None:
            self.api_key = os.getenv(api_key_env)
        else:
            self.api_key = api_key
        self.session = None
        self.rate_limiter = RateLimiter()
        self.last_request_time = 0
        self.min_request_interval = 1.0  # Sekunden zwischen Anfragen
        self.is_available = True
        self.retry_count = 0
        self.max_retries = 3
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            # Schließe zuerst den Connector
            if hasattr(self.session, 'connector') and self.session.connector:
                await self.session.connector.close()
            # Dann schließe die Session
            await self.session.close()
    
    @abstractmethod
    async def get_token_price(self, token_address: str, chain: str):
        """Abstrakte Methode zum Abrufen von Token-Preisen"""
        pass
    
    @abstractmethod
    def get_rate_limits(self) -> Dict[str, int]:
        """Gibt die Rate-Limits zurück (requests_per_minute, requests_per_hour, etc.)"""
        pass
    
    async def _make_request(self, url: str, params: Dict[str, Any], headers: Dict[str, str] = None) -> Dict[str, Any]:
        """Interne Methode für HTTP-Anfragen mit Rate-Limiting und Retry-Logik"""
        # Rate-Limiting prüfen
        rate_limits = self.get_rate_limits()
        if not await self.rate_limiter.acquire(self.name, rate_limits.get("requests_per_minute", 10), 60):
            raise RateLimitExceededException(self.name, rate_limits.get("requests_per_minute", 10), "minute")
        
        # Mindestabstand zwischen Anfragen
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            await asyncio.sleep(self.min_request_interval - time_since_last)
        self.last_request_time = time.time()
        
        try:
            if not self.session:
                self.session = aiohttp.ClientSession()
                
            async with self.session.get(url, params=params, headers=headers) as response:
                if response.status == 429:
                    error_text = await response.text()
                    logger.warning(f"Rate limit exceeded for {self.name}: {error_text}")
                    
                    # Retry-Logik für Rate-Limit-Fehler
                    self.retry_count += 1
                    if self.retry_count <= self.max_retries:
                        # Exponentielles Backoff
                        retry_after = int(response.headers.get('Retry-After', 5 * (2 ** (self.retry_count - 1))))
                        logger.warning(f"Rate limit exceeded. Retry {self.retry_count}/{self.max_retries} after {retry_after} seconds")
                        await asyncio.sleep(retry_after)
                        return await self._make_request(url, params, headers)
                    else:
                        self.retry_count = 0  # Reset retry count
                        raise RateLimitExceededException(self.name, rate_limits.get("requests_per_minute", 10), "minute")
                
                # Reset retry count bei erfolgreicher Anfrage
                self.retry_count = 0
                
                if response.status >= 400:
                    error_text = await response.text()
                    logger.error(f"HTTP error {response.status} for {self.name}: {error_text}")
                    raise APIException(f"HTTP error {response.status}: {error_text}")
                
                try:
                    return await response.json()
                except Exception as e:
                    logger.error(f"Error parsing JSON response from {self.name}: {e}")
                    raise APIException(f"Error parsing response: {str(e)}")
                    
        except aiohttp.ClientError as e:
            logger.error(f"Network error for {self.name}: {e}")
            raise APIException(f"Network error: {str(e)}")
        except asyncio.TimeoutError:
            logger.error(f"Timeout error for {self.name}")
            raise APIException(f"Timeout error for {self.name}")
    
    async def _make_post_request(self, url: str, json_data: Dict[str, Any], headers: Dict[str, str] = None) -> Dict[str, Any]:
        """Interne Methode für POST-Anfragen mit Rate-Limiting und Retry-Logik"""
        # Rate-Limiting prüfen
        rate_limits = self.get_rate_limits()
        if not await self.rate_limiter.acquire(self.name, rate_limits.get("requests_per_minute", 10), 60):
            raise RateLimitExceededException(self.name, rate_limits.get("requests_per_minute", 10), "minute")
        
        # Mindestabstand zwischen Anfragen
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            await asyncio.sleep(self.min_request_interval - time_since_last)
        self.last_request_time = time.time()
        
        try:
            if not self.session:
                self.session = aiohttp.ClientSession()
                
            async with self.session.post(url, json=json_data, headers=headers) as response:
                if response.status == 429:
                    error_text = await response.text()
                    logger.warning(f"Rate limit exceeded for {self.name}: {error_text}")
                    
                    # Retry-Logik für Rate-Limit-Fehler
                    self.retry_count += 1
                    if self.retry_count <= self.max_retries:
                        # Exponentielles Backoff
                        retry_after = int(response.headers.get('Retry-After', 5 * (2 ** (self.retry_count - 1))))
                        logger.warning(f"Rate limit exceeded. Retry {self.retry_count}/{self.max_retries} after {retry_after} seconds")
                        await asyncio.sleep(retry_after)
                        return await self._make_post_request(url, json_data, headers)
                    else:
                        self.retry_count = 0  # Reset retry count
                        raise RateLimitExceededException(self.name, rate_limits.get("requests_per_minute", 10), "minute")
                
                # Reset retry count bei erfolgreicher Anfrage
                self.retry_count = 0
                
                if response.status >= 400:
                    error_text = await response.text()
                    logger.error(f"HTTP error {response.status} for {self.name}: {error_text}")
                    raise APIException(f"HTTP error {response.status}: {error_text}")
                
                try:
                    return await response.json()
                except Exception as e:
                    logger.error(f"Error parsing JSON response from {self.name}: {e}")
                    raise APIException(f"Error parsing response: {str(e)}")
                    
        except aiohttp.ClientError as e:
            logger.error(f"Network error for {self.name}: {e}")
            raise APIException(f"Network error: {str(e)}")
        except asyncio.TimeoutError:
            logger.error(f"Timeout error for {self.name}")
            raise APIException(f"Timeout error for {self.name}")
    
    def check_availability(self) -> bool:
        """Prüft, ob der Anbieter verfügbar ist"""
        return self.is_available
    
    async def close(self):
        """Schließt alle offenen Ressourcen wie Client-Sessions."""
        if hasattr(self, 'session') and self.session:
            # Schließe zuerst den Connector
            if hasattr(self.session, 'connector') and self.session.connector:
                await self.session.connector.close()
            # Dann schließe die Session
            await self.session.close()
            logger.info(f"{self.name} provider client session closed successfully")
