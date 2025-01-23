from functools import wraps
from datetime import timedelta
from data.cache import get_cache_key, save_to_cache, load_from_cache
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def cache_api_call(prefix: str, duration: timedelta = timedelta(hours=24)):
    """Decorator to cache API calls"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Generate cache key
            cache_key = get_cache_key(prefix, **kwargs)
            
            # Try to get from cache
            cached_data = load_from_cache(cache_key, duration)
            if cached_data is not None:
                logger.info(f"Cache HIT for {prefix}: {kwargs.get('ticker', '')}")
                return cached_data
            
            # Call the function if not in cache
            logger.info(f"Cache MISS for {prefix}: {kwargs.get('ticker', '')}")
            result = func(*args, **kwargs)
            
            # Save to cache if we got a result
            if result is not None:
                save_to_cache(cache_key, result)
                logger.info(f"Saved to cache: {prefix}: {kwargs.get('ticker', '')}")
            
            return result
        return wrapper
    return decorator 