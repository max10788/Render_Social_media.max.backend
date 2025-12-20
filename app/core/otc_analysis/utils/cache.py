import redis
import json
import pickle
from typing import Optional, Any, List
from datetime import timedelta
import hashlib

class CacheManager:
    """
    Redis cache manager for storing frequently accessed data.
    
    Cache strategy from doc:
    - Wallet profiles: TTL 1 hour
    - Known OTC desk list: TTL 24 hours
    - Graph neighborhoods: TTL 30 minutes
    """
    
    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0):
        self.redis_client = redis.Redis(
            host=host,
            port=port,
            db=db,
            decode_responses=False  # We'll handle encoding/decoding
        )
        
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
        try:
            full_key = self._generate_key(prefix, key) if prefix else key
            
            # Serialize based on type
            if isinstance(value, (dict, list)):
                serialized = json.dumps(value)
            else:
                serialized = pickle.dumps(value)
            
            if ttl:
                self.redis_client.setex(full_key, ttl, serialized)
            else:
                self.redis_client.set(full_key, serialized)
            
            return True
        except Exception as e:
            print(f"Cache set error: {e}")
            return False
    
    def get(self, key: str, prefix: str = '') -> Optional[Any]:
        """
        Retrieve value from cache.
        
        Returns: Cached value or None if not found/expired
        """
        try:
            full_key = self._generate_key(prefix, key) if prefix else key
            value = self.redis_client.get(full_key)
            
            if value is None:
                return None
            
            # Try JSON first, fall back to pickle
            try:
                return json.loads(value)
            except:
                return pickle.loads(value)
        except Exception as e:
            print(f"Cache get error: {e}")
            return None
    
    def delete(self, key: str, prefix: str = '') -> bool:
        """Delete key from cache."""
        try:
            full_key = self._generate_key(prefix, key) if prefix else key
            self.redis_client.delete(full_key)
            return True
        except Exception as e:
            print(f"Cache delete error: {e}")
            return False
    
    def exists(self, key: str, prefix: str = '') -> bool:
        """Check if key exists in cache."""
        try:
            full_key = self._generate_key(prefix, key) if prefix else key
            return bool(self.redis_client.exists(full_key))
        except Exception as e:
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
            token_address,
            price_usd,
            ttl=self.default_ttls['price_data'],
            prefix='price'
        )
    
    def get_price(self, token_address: str) -> Optional[float]:
        """Retrieve cached token price."""
        return self.get(token_address, prefix='price')
    
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
        try:
            keys = self.redis_client.keys(pattern)
            if keys:
                return self.redis_client.delete(*keys)
            return 0
        except Exception as e:
            print(f"Pattern invalidation error: {e}")
            return 0
    
    def clear_all(self) -> bool:
        """Clear entire cache. Use with caution!"""
        try:
            self.redis_client.flushdb()
            return True
        except Exception as e:
            print(f"Cache clear error: {e}")
            return False
    
    def get_stats(self) -> dict:
        """Get cache statistics."""
        try:
            info = self.redis_client.info('stats')
            return {
                'total_keys': self.redis_client.dbsize(),
                'hits': info.get('keyspace_hits', 0),
                'misses': info.get('keyspace_misses', 0),
                'hit_rate': info.get('keyspace_hits', 0) / max(info.get('keyspace_hits', 0) + info.get('keyspace_misses', 0), 1)
            }
        except Exception as e:
            print(f"Stats error: {e}")
            return {}
