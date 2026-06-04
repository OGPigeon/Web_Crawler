import hashlib
import math
from redis.asyncio import Redis


class BloomFilter:
    """
    Distributed bloom filter backed by a Redis bit array.

    All workers sharing the same Redis instance share one filter, enabling
    cross-worker URL deduplication without any coordination overhead beyond
    the Redis round-trip.
    """

    def __init__(
        self,
        redis: Redis,
        key: str,
        capacity: int,
        error_rate: float = 0.01,
        ttl_seconds: int = 86400 * 7,
    ) -> None:
        self.redis = redis
        self.key = key
        self.ttl_seconds = ttl_seconds
        self.num_bits, self.num_hashes = self._optimal_params(capacity, error_rate)

    async def add(self, item: str) -> None:
        async with self.redis.pipeline(transaction=False) as pipe:
            for pos in self._positions(item):
                pipe.setbit(self.key, pos, 1)
            pipe.expire(self.key, self.ttl_seconds)
            await pipe.execute()

    async def contains(self, item: str) -> bool:
        async with self.redis.pipeline(transaction=False) as pipe:
            for pos in self._positions(item):
                pipe.getbit(self.key, pos)
            results = await pipe.execute()
        return all(results)

    async def add_if_not_exists(self, item: str) -> bool:
        """
        Atomically check and set all k bits in one pipeline round-trip.

        Returns True  if the item was NOT previously seen (it was added now).
        Returns False if all k bits were already set (probably already seen).
        """
        positions = self._positions(item)
        async with self.redis.pipeline(transaction=False) as pipe:
            for pos in positions:
                pipe.getbit(self.key, pos)
            for pos in positions:
                pipe.setbit(self.key, pos, 1)
            pipe.expire(self.key, self.ttl_seconds)
            results = await pipe.execute()
        old_bits = results[: len(positions)]
        return not all(old_bits)  # True = at least one bit was 0 → item is new

    async def clear(self) -> None:
        await self.redis.delete(self.key)

    async def info(self) -> dict:
        bit_count = await self.redis.bitcount(self.key)
        fill_ratio = bit_count / self.num_bits if self.num_bits else 0
        return {
            "key": self.key,
            "num_bits": self.num_bits,
            "num_hashes": self.num_hashes,
            "bits_set": bit_count,
            "fill_ratio": round(fill_ratio, 4),
        }

    def _positions(self, item: str) -> list[int]:
        """Return k independent bit positions for the given item."""
        encoded = item.encode("utf-8")
        positions = []
        for i in range(self.num_hashes):
            # Derive independent hashes by appending a seed integer.
            digest = hashlib.sha256(encoded + i.to_bytes(4, "big")).digest()
            pos = int.from_bytes(digest[:8], "big") % self.num_bits
            positions.append(pos)
        return positions

    @staticmethod
    def _optimal_params(capacity: int, error_rate: float) -> tuple[int, int]:
        """Return (num_bits, num_hashes) for the given capacity and error rate."""
        num_bits = math.ceil(
            -capacity * math.log(error_rate) / (math.log(2) ** 2)
        )
        num_hashes = max(1, round((num_bits / capacity) * math.log(2)))
        return num_bits, num_hashes
