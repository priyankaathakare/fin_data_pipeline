from django.core.cache import cache

ANALYTICS_CACHE_VERSION_KEY = "analytics_version"
ANALYTICS_CACHE_TTL = 300  # 5 minutes


def get_analytics_cache_version() -> int:
    return cache.get(ANALYTICS_CACHE_VERSION_KEY, 1)


def invalidate_analytics_cache() -> None:
    """Bump the analytics cache version in O(1) so stale queries are bypassed immediately."""
    try:
        cache.incr(ANALYTICS_CACHE_VERSION_KEY)
    except ValueError:
        cache.set(ANALYTICS_CACHE_VERSION_KEY, 2, timeout=None)