import time
from abc import ABC, abstractmethod


class CacheBackend(ABC):
    @abstractmethod
    async def get(self, key: str) -> str | None: ...

    @abstractmethod
    async def set(self, key: str, value: str, ttl: int) -> None: ...

    @abstractmethod
    async def delete(self, key: str) -> None: ...


class InMemoryCache(CacheBackend):
    def __init__(self) -> None:
        self._store: dict[str, tuple[str, float]] = {}

    async def get(self, key: str) -> str | None:
        if key not in self._store:
            return None
        value, expires_at = self._store[key]
        if time.time() > expires_at:
            del self._store[key]
            return None
        return value

    async def set(self, key: str, value: str, ttl: int) -> None:
        self._store[key] = (value, time.time() + ttl)

    async def delete(self, key: str) -> None:
        self._store.pop(key, None)


class RedisCache(CacheBackend):
    def __init__(self, redis_url: str) -> None:
        import redis.asyncio as aioredis

        self._redis = aioredis.from_url(redis_url)

    async def get(self, key: str) -> str | None:
        value = await self._redis.get(key)
        if value is None:
            return None
        return value if isinstance(value, str) else value.decode()

    async def set(self, key: str, value: str, ttl: int) -> None:
        await self._redis.setex(key, ttl, value)

    async def delete(self, key: str) -> None:
        await self._redis.delete(key)


def build_cache(redis_url: str | None) -> CacheBackend:
    if redis_url:
        return RedisCache(redis_url)
    return InMemoryCache()
