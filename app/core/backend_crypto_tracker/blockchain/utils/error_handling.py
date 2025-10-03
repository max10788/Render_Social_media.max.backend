# blockchain/utils/error_handling.py
import time
import functools
from typing import Any, Callable, TypeVar, Optional
import logging

logger = logging.getLogger(__name__)

T = TypeVar('T')

class APIError(Exception):
    """Base API error"""
    pass

class RateLimitError(APIError):
    """Rate limit exceeded"""
    pass

class DataNotFoundError(APIError):
    """Requested data not found"""
    pass

def handle_api_error(error: Exception) -> None:
    """Handle API errors appropriately"""
    if isinstance(error, RateLimitError):
        logger.warning(f"Rate limit hit: {error}")
        raise
    elif isinstance(error, DataNotFoundError):
        logger.info(f"Data not found: {error}")
        raise
    else:
        logger.error(f"API error: {error}")
        raise APIError(f"API request failed: {error}")

def retry_on_failure(
    max_retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0
) -> Callable:
    """Decorator to retry function on failure"""
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            last_exception = None
            current_delay = delay
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except RateLimitError:
                    raise  # Don't retry rate limits
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        time.sleep(current_delay)
                        current_delay *= backoff
                    logger.warning(
                        f"Attempt {attempt + 1}/{max_retries} failed: {e}"
                    )
            
            raise last_exception
        return wrapper
    return decorator
