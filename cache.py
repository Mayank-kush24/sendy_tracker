"""
Application-level cache with two backends:

  1. Redis   – when REDIS_URL is set (shared across workers, survives restarts).
  2. In-process dict – fallback when REDIS_URL is empty or Redis is down.

Environment
-----------
REDIS_URL                  redis://[:password@]host:port[/db]  or rediss:// for TLS
CACHE_KEY_PREFIX         key prefix (default: st) — all keys are ``{prefix}:<sha256>``
REDIS_SOCKET_CONNECT_TIMEOUT   seconds (default: 3)
REDIS_SOCKET_TIMEOUT           seconds (default: 3)
REDIS_MAX_CONNECTIONS          pool size (default: 32)
REDIS_HEALTH_CHECK_INTERVAL    seconds between pool health checks (default: 30)
CACHE_TTL_SECONDS          default TTL when @cached() omits ttl (default: 60)
CACHE_MAXSIZE              max in-process entries (default: 512)

Usage
-----
    from cache import cached, cache_info, cache_clear, init_cache

    @cached(ttl=300)
    def expensive_query(...):
        ...

    # optional: call at app startup to connect early and log backend
    init_cache()
"""

from __future__ import annotations

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

import functools
import hashlib
import inspect
import json
import logging
import os
import pickle
import time

logger = logging.getLogger(__name__)

_DEFAULT_TTL = int(os.getenv("CACHE_TTL_SECONDS", "60"))
_CACHE_MAXSIZE = int(os.getenv("CACHE_MAXSIZE", "512"))

# ---------------------------------------------------------------------------
# In-process backend
# ---------------------------------------------------------------------------
_store: dict[str, tuple[object, float]] = {}


def _key_prefix() -> str:
    return os.getenv("CACHE_KEY_PREFIX", os.getenv("REDIS_KEY_PREFIX", "st"))


def _local_get(key: str):
    entry = _store.get(key)
    if entry is None:
        return None
    val, exp = entry
    if time.monotonic() > exp:
        _store.pop(key, None)
        return None
    return val


def _local_set(key: str, value, ttl: int) -> None:
    if len(_store) >= _CACHE_MAXSIZE:
        _evict_expired()
    _store[key] = (value, time.monotonic() + ttl)


def _local_clear() -> None:
    _store.clear()


def _evict_expired() -> int:
    now = time.monotonic()
    dead = [k for k, (_, exp) in _store.items() if exp <= now]
    for k in dead:
        _store.pop(k, None)
    return len(dead)


def _local_info() -> dict:
    now = time.monotonic()
    live = sum(1 for _, (_, exp) in _store.items() if exp > now)
    return {
        "backend":     "in-process",
        "total_keys":  len(_store),
        "live_keys":   live,
        "maxsize":     _CACHE_MAXSIZE,
        "default_ttl": _DEFAULT_TTL,
    }


# ---------------------------------------------------------------------------
# Redis backend
# ---------------------------------------------------------------------------
_redis_client = None


def _redis_reset() -> None:
    """Drop the pooled client (e.g. after connection errors)."""
    global _redis_client
    if _redis_client is not None:
        try:
            _redis_client.close()
        except Exception:
            pass
    _redis_client = None


def _redis_connect():
    """
    Create a new Redis client. Returns None if REDIS_URL unset or connect fails.
    """
    url = os.getenv("REDIS_URL", "").strip()
    if not url:
        return None

    try:
        import redis  # type: ignore[import]

        connect_timeout = float(os.getenv("REDIS_SOCKET_CONNECT_TIMEOUT", "3"))
        socket_timeout = float(os.getenv("REDIS_SOCKET_TIMEOUT", "3"))
        max_conn = int(os.getenv("REDIS_MAX_CONNECTIONS", "32"))
        health = int(os.getenv("REDIS_HEALTH_CHECK_INTERVAL", "30"))

        client = redis.Redis.from_url(
            url,
            decode_responses=False,
            socket_connect_timeout=connect_timeout,
            socket_timeout=socket_timeout,
            max_connections=max_conn,
            health_check_interval=health,
        )
        client.ping()
        logger.info(
            "Cache: Redis connected (prefix=%s, max_connections=%s)",
            _key_prefix(),
            max_conn,
        )
        return client
    except Exception as exc:
        logger.warning("Cache: Redis unavailable (%s) – using in-process store", exc)
        return None


def _redis():
    """Singleton Redis client, or None."""
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    if not os.getenv("REDIS_URL", "").strip():
        return None
    _redis_client = _redis_connect()
    return _redis_client


def init_cache() -> dict:
    """
    Eagerly initialise the cache backend (call from Flask ``create_app``).

    Returns a small status dict for logging / ``/api/cache``.
    """
    _redis_reset()
    r = _redis()
    if r is not None:
        return {"backend": "redis", "prefix": _key_prefix(), "ok": True}
    if os.getenv("REDIS_URL", "").strip():
        return {"backend": "in-process", "prefix": _key_prefix(), "ok": True, "note": "redis_fallback"}
    return {"backend": "in-process", "prefix": _key_prefix(), "ok": True}


class _CacheSentinel:
    pass


_REDIS_MISS = _CacheSentinel()      # key absent in Redis
_REDIS_USE_LOCAL = _CacheSentinel()  # Redis down → read/write local store


def _redis_get(key: str):
    """
    Return cached value, ``_REDIS_MISS`` if key absent, ``_REDIS_USE_LOCAL`` if
    Redis is unavailable (caller should use in-process cache).
    """
    r = _redis()
    if r is None:
        return _REDIS_USE_LOCAL
    try:
        raw = r.get(key)
        if raw is None:
            return _REDIS_MISS
        return pickle.loads(raw)
    except Exception as exc:
        logger.warning("Cache: Redis GET failed (%s) – reconnecting", exc)
        _redis_reset()
        r2 = _redis()
        if r2 is None:
            return _REDIS_USE_LOCAL
        try:
            raw = r2.get(key)
            if raw is None:
                return _REDIS_MISS
            return pickle.loads(raw)
        except Exception:
            return _REDIS_USE_LOCAL


def _redis_set(key: str, value, ttl: int) -> bool:
    """Return True if stored in Redis; False → caller uses in-process."""
    r = _redis()
    if r is None:
        return False
    blob = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
    try:
        r.setex(key, ttl, blob)
        return True
    except Exception as exc:
        logger.warning("Cache: Redis SET failed (%s) – reconnecting", exc)
        _redis_reset()
        r2 = _redis()
        if r2 is None:
            return False
        try:
            r2.setex(key, ttl, blob)
            return True
        except Exception:
            return False


def _redis_scan_delete(pattern: str) -> int:
    """Delete keys matching *pattern* using SCAN (non-blocking vs KEYS)."""
    r = _redis()
    if r is None:
        return 0
    deleted = 0
    cursor = 0
    try:
        while True:
            cursor, keys = r.scan(cursor=cursor, match=pattern, count=500)
            if keys:
                deleted += r.delete(*keys)
            if cursor == 0:
                break
    except Exception as exc:
        logger.warning("Cache: Redis SCAN/DELETE failed (%s)", exc)
        _redis_reset()
    return deleted


# ---------------------------------------------------------------------------
# Public primitives
# ---------------------------------------------------------------------------

def cache_get(key: str):
    """
    Return the cached value, or ``None`` on miss.

    If ``REDIS_URL`` is set and Redis is reachable, only Redis is read (miss
    does not fall through to in-process — avoids stale per-worker data).

    If Redis is configured but down, falls back to in-process.
    """
    if not os.getenv("REDIS_URL", "").strip():
        return _local_get(key)
    got = _redis_get(key)
    if got is _REDIS_USE_LOCAL:
        return _local_get(key)
    if got is _REDIS_MISS:
        return None
    return got


def cache_set(key: str, value, ttl: int) -> None:
    """Store *value* under *key* with a *ttl*-second lifetime."""
    if os.getenv("REDIS_URL", "").strip() and _redis_set(key, value, ttl):
        return
    _local_set(key, value, ttl)


def cache_clear() -> dict:
    """
    Flush all cache entries created by this application (matching key prefix).
    """
    prefix = _key_prefix()
    pattern = f"{prefix}:*"
    flushed = _redis_scan_delete(pattern)
    _local_clear()
    return {"flushed": flushed, "pattern": pattern}


def cache_info() -> dict:
    """Return a snapshot of the current cache state."""
    r = _redis()
    prefix = _key_prefix()
    if r is not None:
        try:
            mem = r.info("memory")
            # Approximate our keys only (full DBSIZE can include other apps)
            n = 0
            cur = 0
            while True:
                cur, keys = r.scan(cursor=cur, match=f"{prefix}:*", count=200)
                n += len(keys)
                if cur == 0:
                    break
            return {
                "backend":           "redis",
                "key_prefix":        prefix,
                "keys_matching":     n,
                "redis_dbsize":      r.dbsize(),
                "used_memory_human": mem.get("used_memory_human"),
                "maxmemory_human":   mem.get("maxmemory_human"),
            }
        except Exception as exc:
            logger.warning("Cache: cache_info Redis error (%s)", exc)
    return _local_info()


def make_key(namespace: str, bound_args: dict) -> str:
    """Build ``{prefix}:`` + SHA-256(namespace + args)."""
    payload = json.dumps(
        {"_ns": namespace, **bound_args},
        sort_keys=True,
        default=str,
    )
    return f"{_key_prefix()}:" + hashlib.sha256(payload.encode()).hexdigest()


# ---------------------------------------------------------------------------
# @cached decorator
# ---------------------------------------------------------------------------

def cached(ttl: int | None = None):
    """
    Decorator that caches the function return value in Redis (if configured)
    or in-process.
    """
    effective_ttl = ttl if ttl is not None else _DEFAULT_TTL

    def decorator(fn):
        sig = inspect.signature(fn)

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
            key = make_key(fn.__qualname__, dict(bound.arguments))

            hit = cache_get(key)
            if hit is not None:
                logger.debug("Cache HIT  %-40s key=%.12s…", fn.__qualname__, key[len(_key_prefix()) + 1 :])
                return hit

            logger.debug("Cache MISS %-40s key=%.12s…", fn.__qualname__, key[len(_key_prefix()) + 1 :])
            result = fn(*args, **kwargs)
            cache_set(key, result, effective_ttl)
            return result

        wrapper.cache_clear = cache_clear
        return wrapper

    return decorator
