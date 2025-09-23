# utils/cache.py
import asyncio
import time
import json
from typing import Any, Optional, Dict, Union, List
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from app.core.backend_crypto_tracker.utils.logger import get_logger
logger = get_logger(__name__)

@dataclass
class CacheEntry:
    """Ein Eintrag im Cache"""
    data: Any
    timestamp: float
    ttl: int
    access_count: int = 0
    last_access: float = field(default_factory=time.time)

class AnalysisCache:
    def __init__(self, max_size: int = 1000, default_ttl: int = 3600):
        """
        Initialisiert den Cache
        
        Args:
            max_size: Maximale Anzahl von Einträgen im Cache
            default_ttl: Standard-TTL in Sekunden
        """
        self.cache: Dict[str, CacheEntry] = {}
        self.max_size = max_size
        self.default_ttl = default_ttl
        self.lock = asyncio.Lock()
        self.hits = 0
        self.misses = 0
        self.evictions = 0
    
    def _get_cache_key(self, *args, **kwargs) -> str:
        """Erzeugt einen eindeutigen Cache-Schlüssel aus den Argumenten"""
        # Kombiniere alle Argumente zu einem Schlüssel
        key_parts = []
        
        # Positionale Argumente
        for arg in args:
            if isinstance(arg, (str, int, float, bool)):
                key_parts.append(str(arg))
            elif isinstance(arg, dict):
                key_parts.append(json.dumps(arg, sort_keys=True))
            else:
                key_parts.append(str(hash(arg)))
        
        # Schlüsselwort-Argumente
        if kwargs:
            key_parts.append(json.dumps(kwargs, sort_keys=True))
        
        return ":".join(key_parts)
    
    async def get(self, *args, **kwargs) -> Optional[Any]:
        """Holt einen Eintrag aus dem Cache"""
        cache_key = self._get_cache_key(*args, **kwargs)
        
        async with self.lock:
            if cache_key not in self.cache:
                self.misses += 1
                return None
            
            entry = self.cache[cache_key]
            
            # Prüfe, ob der Eintrag abgelaufen ist
            if time.time() - entry.timestamp > entry.ttl:
                del self.cache[cache_key]
                self.evictions += 1
                self.misses += 1
                return None
            
            # Aktualisiere die Zugriffsstatistik
            entry.access_count += 1
            entry.last_access = time.time()
            self.hits += 1
            
            logger.debug(f"Cache hit for key: {cache_key}")
            return entry.data
    
    async def set(self, data: Any, ttl: Optional[int] = None, *args, **kwargs) -> None:
        """Speichert einen Eintrag im Cache"""
        cache_key = self._get_cache_key(*args, **kwargs)
        ttl = ttl or self.default_ttl
        
        async with self.lock:
            # Wenn der Cache voll ist, entferne den ältesten Eintrag
            if len(self.cache) >= self.max_size:
                self._evict_oldest()
            
            self.cache[cache_key] = CacheEntry(
                data=data,
                timestamp=time.time(),
                ttl=ttl
            )
            
            logger.debug(f"Cache set for key: {cache_key}")
    
    async def delete(self, *args, **kwargs) -> bool:
        """Löscht einen Eintrag aus dem Cache"""
        cache_key = self._get_cache_key(*args, **kwargs)
        
        async with self.lock:
            if cache_key in self.cache:
                del self.cache[cache_key]
                logger.debug(f"Cache delete for key: {cache_key}")
                return True
            return False
    
    async def clear(self) -> None:
        """Leert den gesamten Cache"""
        async with self.lock:
            self.cache.clear()
            logger.info("Cache cleared")
    
    async def clear_expired(self) -> int:
        """Entfernt alle abgelaufenen Einträge aus dem Cache"""
        async with self.lock:
            current_time = time.time()
            expired_keys = []
            
            for key, entry in self.cache.items():
                if current_time - entry.timestamp > entry.ttl:
                    expired_keys.append(key)
            
            for key in expired_keys:
                del self.cache[key]
            
            count = len(expired_keys)
            if count > 0:
                self.evictions += count
                logger.info(f"Cleared {count} expired cache entries")
            
            return count
    
    def _evict_oldest(self) -> None:
        """Entfernt den ältesten Eintrag aus dem Cache"""
        if not self.cache:
            return
        
        oldest_key = min(self.cache.keys(), key=lambda k: self.cache[k].timestamp)
        del self.cache[oldest_key]
        self.evictions += 1
    
    def _evict_lru(self) -> None:
        """Entfernt den am wenigsten genutzten Eintrag aus dem Cache"""
        if not self.cache:
            return
        
        lru_key = min(self.cache.keys(), key=lambda k: self.cache[k].last_access)
        del self.cache[lru_key]
        self.evictions += 1
    
    def get_stats(self) -> Dict[str, Any]:
        """Gibt Cache-Statistiken zurück"""
        total_requests = self.hits + self.misses
        hit_rate = (self.hits / total_requests * 100) if total_requests > 0 else 0
        
        return {
            'size': len(self.cache),
            'max_size': self.max_size,
            'hits': self.hits,
            'misses': self.misses,
            'hit_rate': f"{hit_rate:.2f}%",
            'evictions': self.evictions,
            'default_ttl': self.default_ttl
        }
    
    async def get_keys(self) -> List[str]:
        """Gibt alle Cache-Schlüssel zurück"""
        async with self.lock:
            return list(self.cache.keys())

# Globale Cache-Instanz
cache = AnalysisCache()

# Hilfsfunktionen für die einfache Verwendung
async def get_cached_analysis(token_address: str, chain: str) -> Optional[Dict]:
    """Holt eine gecachte Analyse für ein Token"""
    return await cache.get(token_address, chain)

async def set_cached_analysis(analysis: Dict, token_address: str, chain: str, ttl: int = 3600) -> None:
    """Speichert eine Analyse im Cache"""
    await cache.set(analysis, ttl, token_address, chain)

async def invalidate_cache(token_address: str, chain: str) -> bool:
    """Macht einen Cache-Eintrag ungültig"""
    return await cache.delete(token_address, chain)
