from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable


@dataclass
class CrawlerConfig:
    """
    Complete configuration for a CrawlerPool run.

    Dependency injection
    --------------------
    relevancy_fn(url, html_bytes, metadata) -> bool
        Return True to pass the page to storage_fn. The URL is added to the
        bloom filter regardless of this return value. Links are extracted from
        ALL successfully fetched pages (relevant or not) so the crawl graph
        is fully explored even if most pages are filtered out.
        If None, every fetched page is treated as relevant.

    storage_fn(url, html_bytes, metadata) -> None
        Called synchronously (wrapped in run_in_executor by the worker) only
        when relevancy_fn returns True. If None, pages are crawled but not
        stored. metadata keys: status_code, content_type, depth, headers.

    Domain closure
    --------------
    allowed_domains: list of hostnames (e.g. ["example.com"]).
        Subdomain matching is applied: "example.com" permits "www.example.com".
        Set to None for an open crawl (all domains allowed).
        URLs outside allowed_domains are not fetched or enqueued; they are
        already in the bloom filter from the moment they were enqueued, so
        they won't be re-enqueued by other pages.

    Bloom filter persistence
    ------------------------
    bloom_persist=True  (default): reuse existing Redis bloom key across runs.
    bloom_persist=False: delete the bloom key and queue on startup (fresh run).
    """

    # --- Redis connection ---
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: str | None = None
    redis_url: str | None = None  # overrides host/port/db/password when set

    # --- Queue / bloom keys ---
    redis_queue_key: str = "crawler:queue"
    redis_active_workers_key: str = "crawler:active_workers"
    bloom_redis_key: str = "crawler:bloom"

    # --- Bloom filter tuning ---
    bloom_capacity: int = 1_000_000
    bloom_error_rate: float = 0.01
    bloom_ttl_seconds: int = 86400 * 7  # 7 days
    bloom_persist: bool = True

    # --- Workers ---
    num_workers: int = 4
    max_concurrent_requests: int = 16  # asyncio.Semaphore ceiling

    # --- Termination ---
    max_pages: int | None = None  # stop after N pages crawled; None = unlimited
    max_depth: int | None = None  # don't enqueue links beyond this depth; None = unlimited

    # --- HTTP ---
    user_agent: str = "GeneralCrawler/1.0 (+https://github.com/example/crawler)"
    request_timeout: float = 10.0
    max_redirects: int = 5
    verify_ssl: bool = True

    # --- Rate limiting ---
    default_crawl_delay: float = 3.0  # seconds; used when robots.txt has no Crawl-delay

    # --- Robots ---
    respect_robots: bool = True

    # --- Domain closure ---
    allowed_domains: list[str] | None = None

    # --- Dependency injection ---
    relevancy_fn: Callable[[str, bytes, dict], bool] | None = None
    storage_fn: Callable[[str, bytes, dict], None] | None = None
