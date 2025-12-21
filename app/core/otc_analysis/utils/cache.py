import redis
import json
import pickle
from typing import Optional, Any, List
from datetime import timedelta, datetime
import hashlib
import os
import logging

logger = logging.getLogger(__name__)

class CacheManager:
    """
    Redis cache manager for storing frequently accessed data.
    
    Cache strategy from doc:
    - Wallet profiles: TTL 1 hour
    - Known OTC desk list: TTL 24 hours
    - Graph neighborhoods: TTL 30 minutes
    
    Gracefully degrades if Redis is unavailable.
    """
    
    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0):
        # Check if Redis should be disabled
        redis_url = os.getenv('REDIS_URL', f'redis://{host}:{port}/{db}')
        
        if redis_url == 'disabled' or redis_url == 'none':
            logger.warning("⚠️  Redis cache disabled via environment variable")
            self.redis_client = None
            self.enabled = False
            return
        
        try:
            # Try to connect to Redis
            if redis_url.startswith('redis://'):
                self.redis_client = redis.from_url(redis_url, decode_responses=False)
            else:
                self.redis_client = redis.Redis(
                    host=host,
                    port=port,
                    db=db,
                    decode_responses=False,
                    socket_connect_timeout=2,
                    socket_timeout=2
                )
            
            # Test connection
            self.redis_client.ping()
            self.enabled = True
            logger.info("✅ Redis cache connected successfully")
            
        except Exception as e:
            logger.warning(f"⚠️  Redis unavailable, cache disabled: {e}")
            self.redis_client = None
            self.enabled = False
        
        # Default TTL values (in seconds)
        self.default_ttls = {
            'wallet_profile': 3600,      # 1 hour
            'otc_desk_list': 86400,      # 24 hours
            'graph_neighborhood': 1800,   # 30 minutes
            'price_data': 300,            # 5 minutes
            'transaction': 7200,          # 2 hours
            'cluster': 3600,              # 1 hour
        }
    
    def _generate_key(self, prefix: str, identifier: str) -> str:
        """Generate cache key with prefix."""
        return f"{prefix}:{identifier}"
    
    def _serialize_value(self, value: Any) -> bytes:
        """Serialize value for storage, handling datetime objects."""
        try:
            if isinstance(value, (dict, list)):
                # Convert datetime objects to ISO strings
                serialized = json.dumps(value, default=self._json_serializer)
                return serialized.encode('utf-8')
            else:
                return pickle.dumps(value)
        except Exception as e:
            logger.debug(f"Serialization failed: {e}")
            # Fallback to pickle
            return pickle.dumps(value)
    
    def _json_serializer(self, obj):
        """Custom JSON serializer for datetime objects."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, timedelta):
            return obj.total_seconds()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
    
    def _deserialize_value(self, value: bytes) -> Any:
        """Deserialize value from storage."""
        try:
            # Try JSON first
            return json.loads(value.decode('utf-8'))
        except:
            # Fall back to pickle
            try:
                return pickle.loads(value)
            except:
                return None
    
    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        prefix: str = ''
    ) -> bool:
        """
        Store value in cache.
        
        Args:
            key: Cache key
            value: Value to cache (will be serialized)
            ttl: Time to live in seconds
            prefix: Key prefix for organization
        """
        if not self.enabled:
            return False
        
        try:
            full_key = self._generate_key(prefix, key) if prefix else key
            serialized = self._serialize_value(value)
            
            if ttl:
                self.redis_client.setex(full_key, ttl, serialized)
            else:
                self.redis_client.set(full_key, serialized)
            
            return True
        except Exception as e:
            logger.debug(f"Cache set failed: {e}")
            return False
    
    def get(self, key: str, prefix: str = '') -> Optional[Any]:
        """
        Retrieve value from cache.
        
        Returns: Cached value or None if not found/expired
        """
        if not self.enabled:
            return None
        
        try:
            full_key = self._generate_key(prefix, key) if prefix else key
            value = self.redis_client.get(full_key)
            
            if value is None:
                return None
            
            return self._deserialize_value(value)
        except Exception as e:
            logger.debug(f"Cache get failed: {e}")
            return None
    
    def delete(self, key: str, prefix: str = '') -> bool:
        """Delete key from cache."""
        if not self.enabled:
            return False
        
        try:
            full_key = self._generate_key(prefix, key) if prefix else key
            self.redis_client.delete(full_key)
            return True
        except Exception as e:
            logger.debug(f"Cache delete failed: {e}")
            return False
    
    def exists(self, key: str, prefix: str = '') -> bool:
        """Check if key exists in cache."""
        if not self.enabled:
            return False
        
        try:
            full_key = self._generate_key(prefix, key) if prefix else key
            return bool(self.redis_client.exists(full_key))
        except Exception as e:
            logger.debug(f"Cache exists check failed: {e}")
            return False
    
    # Specialized cache methods
    
    def cache_wallet_profile(self, address: str, profile_data: dict) -> bool:
        """Cache wallet profile with default TTL."""
        return self.set(
            address,
            profile_data,
            ttl=self.default_ttls['wallet_profile'],
            prefix='wallet_profile'
        )
    
    def get_wallet_profile(self, address: str) -> Optional[dict]:
        """Retrieve cached wallet profile."""
        return self.get(address, prefix='wallet_profile')
    
    def cache_otc_desk_list(self, desk_list: List[dict]) -> bool:
        """Cache known OTC desk list."""
        return self.set(
            'known_desks',
            desk_list,
            ttl=self.default_ttls['otc_desk_list'],
            prefix='otc'
        )
    
    def get_otc_desk_list(self) -> Optional[List[dict]]:
        """Retrieve cached OTC desk list."""
        return self.get('known_desks', prefix='otc')
    
    def cache_graph_neighborhood(
        self,
        address: str,
        neighbors: set,
        hops: int = 2
    ) -> bool:
        """Cache graph neighborhood for an address."""
        key = f"{address}:hops_{hops}"
        return self.set(
            key,
            list(neighbors),  # Convert set to list for JSON
            ttl=self.default_ttls['graph_neighborhood'],
            prefix='graph'
        )
    
    def get_graph_neighborhood(self, address: str, hops: int = 2) -> Optional[set]:
        """Retrieve cached graph neighborhood."""
        key = f"{address}:hops_{hops}"
        neighbors = self.get(key, prefix='graph')
        return set(neighbors) if neighbors else None
    
    def cache_price(self, token_address: str, price_usd: float) -> bool:
        """Cache token price."""
        return self.set(
            token_address or 'ETH',
            price_usd,
            ttl=self.default_ttls['price_data'],
            prefix='price'
        )
    
    def get_price(self, token_address: str) -> Optional[float]:
        """Retrieve cached token price."""
        return self.get(token_address or 'ETH', prefix='price')
    
    def cache_transaction(self, tx_hash: str, tx_data: dict) -> bool:
        """Cache transaction data."""
        return self.set(
            tx_hash,
            tx_data,
            ttl=self.default_ttls['transaction'],
            prefix='tx'
        )
    
    def get_transaction(self, tx_hash: str) -> Optional[dict]:
        """Retrieve cached transaction."""
        return self.get(tx_hash, prefix='tx')
    
    def cache_cluster(self, cluster_id: str, cluster_data: dict) -> bool:
        """Cache cluster data."""
        return self.set(
            cluster_id,
            cluster_data,
            ttl=self.default_ttls['cluster'],
            prefix='cluster'
        )
    
    def get_cluster(self, cluster_id: str) -> Optional[dict]:
        """Retrieve cached cluster."""
        return self.get(cluster_id, prefix='cluster')
    
    def invalidate_pattern(self, pattern: str) -> int:
        """
        Delete all keys matching a pattern.
        
        Args:
            pattern: Redis pattern (e.g., 'wallet_profile:0x*')
        
        Returns: Number of keys deleted
        """
        if not self.enabled:
            return 0
        
        try:
            keys = self.redis_client.keys(pattern)
            if keys:
                return self.redis_client.delete(*keys)
            return 0
        except Exception as e:
            logger.debug(f"Pattern invalidation failed: {e}")
            return 0
    
    def clear_all(self) -> bool:
        """Clear entire cache. Use with caution!"""
        if not self.enabled:
            return False
        
        try:
            self.redis_client.flushdb()
            return True
        except Exception as e:
            logger.debug(f"Cache clear failed: {e}")
            return False
    
    def get_stats(self) -> dict:
        """Get cache statistics."""
        if not self.enabled:
            return {
                'enabled': False,
                'status': 'disabled'
            }
        
        try:
            info = self.redis_client.info('stats')
            return {
                'enabled': True,
                'status': 'connected',
                'total_keys': self.redis_client.dbsize(),
                'hits': info.get('keyspace_hits', 0),
                'misses': info.get('keyspace_misses', 0),
                'hit_rate': info.get('keyspace_hits', 0) / max(info.get('keyspace_hits', 0) + info.get('keyspace_misses', 0), 1)
            }
        except Exception as e:
            logger.debug(f"Stats fetch failed: {e}")
            return {
                'enabled': False,
                'status': 'error',
                'error': str(e)
            }
