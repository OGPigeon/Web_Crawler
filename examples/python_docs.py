"""
Example: crawl Python documentation and store pages that mention asyncio.

Demonstrates:
  - Domain closure (restricted to docs.python.org)
  - Custom relevancy_fn (keyword filter)
  - JSONL storage via make_jsonl_storage
  - bloom_persist=True so the crawl can be resumed after interruption
  - max_depth and max_pages as safety limits
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from webcrawler import CrawlerConfig, CrawlerPool, make_jsonl_storage


def docs_relevancy(url: str, html_bytes: bytes, metadata: dict) -> bool:
    """Only store pages whose HTML body mentions 'asyncio'."""
    return b"asyncio" in html_bytes.lower()


config = CrawlerConfig(
    allowed_domains=["docs.python.org"],
    redis_url="redis://localhost:6379/0",

    num_workers=4,
    max_concurrent_requests=8,

    max_pages=200,
    max_depth=4,

    # Rate limiting: 1 request per second per domain
    default_crawl_delay=1.0,

    # Bloom filter: resume from previous run if any
    bloom_persist=True,
    bloom_capacity=100_000,

    relevancy_fn=docs_relevancy,
    storage_fn=make_jsonl_storage("python_asyncio_docs.jsonl"),
)

SEED_URLS = [
    "https://docs.python.org/3/library/asyncio.html",
    "https://docs.python.org/3/library/asyncio-task.html",
    "https://docs.python.org/3/library/concurrent.futures.html",
]

if __name__ == "__main__":
    pool = CrawlerPool(config=config, seed_urls=SEED_URLS)
    try:
        asyncio.run(pool.run())
    except KeyboardInterrupt:
        print("\n[Main] Interrupted. Bloom filter state is saved in Redis — "
              "re-run to resume from where you left off.")
