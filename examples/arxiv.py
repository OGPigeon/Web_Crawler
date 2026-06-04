"""
Example: crawl arXiv — follows listing pages, stores all abstract pages.

Output: arxiv_crawl.jsonl  (one JSON record per abstract page)
Each record: url, status_code, content_type, depth, html_length, content_hash, crawled_at

To also store the raw HTML, swap make_jsonl_storage for make_jsonl_storage_with_content.

Usage:
    pip install -r requirements.txt
    # Redis must be running
    python examples/arxiv.py

Resume after interruption:
    Flip bloom_persist back to True so already-visited URLs are skipped.
"""

import asyncio
import sys
import os
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from webcrawler import CrawlerConfig, CrawlerPool, make_jsonl_storage

OUTPUT = Path("arxiv_crawl.jsonl")


def arxiv_relevancy(url: str, _html_bytes: bytes, _metadata: dict) -> bool:
    """Store every abstract page; skip listing/search/PDF pages."""
    return "/abs/" in url


config = CrawlerConfig(
    allowed_domains=["arxiv.org"],
    redis_url="redis://localhost:6379/0",

    num_workers=2,
    max_concurrent_requests=4,
    default_crawl_delay=3.0,
    max_pages=100,
    max_depth=3,

    bloom_persist=False,   # flip to True after the first successful run to resume
    bloom_capacity=500_000,

    relevancy_fn=arxiv_relevancy,
    storage_fn=make_jsonl_storage(str(OUTPUT)),
)

SEED_URLS = [
    "https://arxiv.org/list/cs.CV/recent",
    "https://arxiv.org/list/cs.LG/recent",
]

if __name__ == "__main__":
    print(f"[Main] Saving to: {OUTPUT.resolve()}")
    pool = CrawlerPool(config=config, seed_urls=SEED_URLS)
    try:
        asyncio.run(pool.run())
    except KeyboardInterrupt:
        print("\n[Main] Interrupted.")
    print(f"[Main] Records saved: {sum(1 for _ in OUTPUT.open()) if OUTPUT.exists() else 0}")
