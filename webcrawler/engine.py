from __future__ import annotations

import asyncio
import json

import redis.asyncio as aioredis

from .url_filter import BloomFilter
from .settings import CrawlerConfig
from .worker import CrawlWorker
from .politeness import RobotsCache


class CrawlerPool:
    """
    Orchestrator that wires together Redis, the bloom filter, robots cache,
    and N concurrent CrawlWorker coroutines.

    Usage:
        pool = CrawlerPool(config, seed_urls=["https://example.com"])
        asyncio.run(pool.run())
    """

    def __init__(self, config: CrawlerConfig, seed_urls: list[str]) -> None:
        self.config = config
        self.seed_urls = seed_urls

    async def run(self) -> None:
        cfg = self.config
        redis_url = self._build_redis_url(cfg)

        redis = aioredis.from_url(
            redis_url,
            encoding="utf-8",
            decode_responses=False,  # raw bytes — workers decode job JSON manually
        )

        try:
            bloom = BloomFilter(
                redis=redis,
                key=cfg.bloom_redis_key,
                capacity=cfg.bloom_capacity,
                error_rate=cfg.bloom_error_rate,
                ttl_seconds=cfg.bloom_ttl_seconds,
            )

            if not cfg.bloom_persist:
                print("[Pool] bloom_persist=False — clearing bloom filter and queue.")
                await bloom.clear()
                await redis.delete(cfg.redis_queue_key)
                await redis.set(cfg.redis_active_workers_key, 0)

            # Seed initial URLs.
            for url in self.seed_urls:
                job = json.dumps({"url": url, "depth": 0})
                await redis.rpush(cfg.redis_queue_key, job)
            print(f"[Pool] Seeded {len(self.seed_urls)} URL(s). Starting {cfg.num_workers} workers.")

            # Shared mutable state passed to all workers.
            semaphore = asyncio.Semaphore(cfg.max_concurrent_requests)
            domain_locks: dict[str, asyncio.Lock] = {}
            domain_last_fetch: dict[str, float] = {}
            pages_crawled: list[int] = [0]
            shutdown_event = asyncio.Event()
            robots = RobotsCache(user_agent=cfg.user_agent)

            # Initialize the active-workers counter so idle detection works.
            await redis.set(cfg.redis_active_workers_key, cfg.num_workers)

            workers = [
                CrawlWorker(
                    worker_id=i,
                    config=cfg,
                    redis=redis,
                    bloom=bloom,
                    robots=robots,
                    semaphore=semaphore,
                    domain_locks=domain_locks,
                    domain_last_fetch=domain_last_fetch,
                    pages_crawled=pages_crawled,
                    shutdown_event=shutdown_event,
                )
                for i in range(cfg.num_workers)
            ]

            tasks = [asyncio.create_task(w.run()) for w in workers]
            watcher = asyncio.create_task(
                self._shutdown_watcher(shutdown_event, tasks)
            )

            results = await asyncio.gather(*tasks, return_exceptions=True)
            watcher.cancel()

            for i, result in enumerate(results):
                if isinstance(result, Exception) and not isinstance(result, asyncio.CancelledError):
                    print(f"[Pool] Worker {i} exited with error: {result!r}")

            info = await bloom.info()
            print(
                f"[Pool] Crawl complete. Pages crawled: {pages_crawled[0]}. "
                f"Bloom filter: {info['bits_set']} bits set "
                f"({info['fill_ratio']*100:.1f}% fill, "
                f"~{info['bits_set'] // max(info['num_hashes'], 1)} URLs seen)."
            )

        finally:
            await redis.aclose()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    async def _shutdown_watcher(
        shutdown_event: asyncio.Event, tasks: list[asyncio.Task]
    ) -> None:
        """Cancel all worker tasks once the shutdown event is set."""
        await shutdown_event.wait()
        for task in tasks:
            task.cancel()

    @staticmethod
    def _build_redis_url(cfg: CrawlerConfig) -> str:
        if cfg.redis_url:
            return cfg.redis_url
        if cfg.redis_password:
            auth = f":{cfg.redis_password}@"
        else:
            auth = ""
        return f"redis://{auth}{cfg.redis_host}:{cfg.redis_port}/{cfg.redis_db}"
