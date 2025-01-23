import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import pickle
from typing import Any, Optional

CACHE_DIR = Path("cache")
CACHE_DURATION = timedelta(hours=24)  # Cache data for 24 hours by default

def get_cache_key(prefix: str, **kwargs) -> str:
    """Generate a cache key from the function arguments"""
    # Sort kwargs to ensure consistent keys
    sorted_items = sorted(kwargs.items())
    # Join all arguments into a single string
    args_str = "_".join(f"{k}={v}" for k, v in sorted_items)
    return f"{prefix}_{args_str}"

def get_cache_path(key: str) -> Path:
    """Get the full path for a cache file"""
    # Create cache directory if it doesn't exist
    CACHE_DIR.mkdir(exist_ok=True)
    return CACHE_DIR / f"{key}.pickle"

def save_to_cache(key: str, data: Any) -> None:
    """Save data to cache with timestamp"""
    cache_data = {
        "timestamp": datetime.now(),
        "data": data
    }
    with open(get_cache_path(key), "wb") as f:
        pickle.dump(cache_data, f)

def load_from_cache(key: str, max_age: Optional[timedelta] = None) -> Optional[Any]:
    """Load data from cache if it exists and is not expired"""
    cache_path = get_cache_path(key)
    if not cache_path.exists():
        return None

    try:
        with open(cache_path, "rb") as f:
            cache_data = pickle.load(f)
        
        # Check if cache is expired
        age = datetime.now() - cache_data["timestamp"]
        if max_age and age > max_age:
            return None
            
        return cache_data["data"]
    except (EOFError, pickle.UnpicklingError, KeyError):
        return None

def clear_cache() -> None:
    """Clear all cached data"""
    if CACHE_DIR.exists():
        for cache_file in CACHE_DIR.glob("*.pickle"):
            cache_file.unlink()

class Cache:
    """In-memory cache for API responses."""

    def __init__(self):
        self._prices_cache: dict[str, list[dict[str, any]]] = {}
        self._financial_metrics_cache: dict[str, list[dict[str, any]]] = {}
        self._line_items_cache: dict[str, list[dict[str, any]]] = {}
        self._insider_trades_cache: dict[str, list[dict[str, any]]] = {}
        self._company_news_cache: dict[str, list[dict[str, any]]] = {}

    def _merge_data(self, existing: list[dict] | None, new_data: list[dict], key_field: str) -> list[dict]:
        """Merge existing and new data, avoiding duplicates based on a key field."""
        if not existing:
            return new_data
        
        # Create a set of existing keys for O(1) lookup
        existing_keys = {item[key_field] for item in existing}
        
        # Only add items that don't exist yet
        merged = existing.copy()
        merged.extend([item for item in new_data if item[key_field] not in existing_keys])
        return merged

    def get_prices(self, ticker: str) -> list[dict[str, any]] | None:
        """Get cached price data if available."""
        return self._prices_cache.get(ticker)

    def set_prices(self, ticker: str, data: list[dict[str, any]]):
        """Append new price data to cache."""
        self._prices_cache[ticker] = self._merge_data(
            self._prices_cache.get(ticker),
            data,
            key_field="time"
        )

    def get_financial_metrics(self, ticker: str) -> list[dict[str, any]]:
        """Get cached financial metrics if available."""
        return self._financial_metrics_cache.get(ticker)

    def set_financial_metrics(self, ticker: str, data: list[dict[str, any]]):
        """Append new financial metrics to cache."""
        self._financial_metrics_cache[ticker] = self._merge_data(
            self._financial_metrics_cache.get(ticker),
            data,
            key_field="report_period"
        )

    def get_line_items(self, ticker: str) -> list[dict[str, any]] | None:
        """Get cached line items if available."""
        return self._line_items_cache.get(ticker)

    def set_line_items(self, ticker: str, data: list[dict[str, any]]):
        """Append new line items to cache."""
        self._line_items_cache[ticker] = self._merge_data(
            self._line_items_cache.get(ticker),
            data,
            key_field="report_period"
        )

    def get_insider_trades(self, ticker: str) -> list[dict[str, any]] | None:
        """Get cached insider trades if available."""
        return self._insider_trades_cache.get(ticker)

    def set_insider_trades(self, ticker: str, data: list[dict[str, any]]):
        """Append new insider trades to cache."""
        self._insider_trades_cache[ticker] = self._merge_data(
            self._insider_trades_cache.get(ticker),
            data,
            key_field="filing_date"  # Could also use transaction_date if preferred
        )

    def get_company_news(self, ticker: str) -> list[dict[str, any]] | None:
        """Get cached company news if available."""
        return self._company_news_cache.get(ticker)

    def set_company_news(self, ticker: str, data: list[dict[str, any]]):
        """Append new company news to cache."""
        self._company_news_cache[ticker] = self._merge_data(
            self._company_news_cache.get(ticker),
            data,
            key_field="date"
        )


# Global cache instance
_cache = Cache()


def get_cache() -> Cache:
    """Get the global cache instance."""
    return _cache
