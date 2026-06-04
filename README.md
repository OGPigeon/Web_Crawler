# WebCrawler

An async, distributed web crawler built on **aiohttp** and **Redis**. Supports domain-scoped crawls, robots.txt compliance, per-domain rate limiting, and resumable runs via a Redis-backed bloom filter.

## Features

- **Async workers** — N concurrent `asyncio` coroutines share one Redis queue
- **Redis bloom filter** — cross-worker URL deduplication with zero coordination overhead
- **Resumable crawls** — re-run after interruption; already-visited URLs are skipped automatically
- **robots.txt** — fetched once per domain, cached in memory, full Disallow/Allow/Crawl-delay support
- **Per-domain rate limiting** — serialized via `asyncio.Lock`; respects `Crawl-delay` from robots.txt
- **Domain closure** — restrict crawl to a whitelist of hostnames (with subdomain matching)
- **Dependency injection** — plug in your own `relevancy_fn` and `storage_fn`
- **PDF link extraction** — follows clickable hyperlinks in PDFs via pymupdf (optional)

## Installation

**Requirements:** Python 3.10+, Redis running locally (or any accessible Redis URL)

```bash
pip install -r requirements.txt

# Optional — PDF link extraction
pip install pymupdf
```

## Quick Start

```bash
# Make sure Redis is running, then:
python examples/python_docs.py   # crawl docs.python.org, keep asyncio pages
python examples/arxiv.py         # crawl arXiv listing pages, store abstracts
```

## Configuration Reference

All options live in `CrawlerConfig` ([webcrawler/settings.py](webcrawler/settings.py)).

| Parameter | Default | Description |
|---|---|---|
| `redis_url` | `None` | Full Redis URL (overrides host/port/db) |
| `redis_host` | `"localhost"` | Redis host |
| `redis_port` | `6379` | Redis port |
| `redis_db` | `0` | Redis DB index |
| `redis_password` | `None` | Redis password |
| `num_workers` | `4` | Number of concurrent crawl coroutines |
| `max_concurrent_requests` | `16` | Global asyncio semaphore cap |
| `max_pages` | `None` | Stop after N pages (unlimited if None) |
| `max_depth` | `None` | Max link depth from seed URLs |
| `default_crawl_delay` | `3.0` | Seconds between requests to the same domain |
| `respect_robots` | `True` | Honour robots.txt Disallow rules |
| `allowed_domains` | `None` | Hostname whitelist; `None` = open crawl |
| `bloom_persist` | `True` | Reuse bloom filter across runs for resumability |
| `bloom_capacity` | `1_000_000` | Expected unique URL count |
| `bloom_error_rate` | `0.01` | Target false-positive rate |
| `bloom_ttl_seconds` | `604800` | Redis key TTL (7 days) |
| `user_agent` | `"GeneralCrawler/1.0 ..."` | HTTP User-Agent header |
| `request_timeout` | `10.0` | Per-request timeout in seconds |
| `verify_ssl` | `True` | Verify TLS certificates |
| `relevancy_fn` | `None` | `fn(url, html_bytes, metadata) -> bool` |
| `storage_fn` | `None` | `fn(url, html_bytes, metadata) -> None` |

## Storage Options

Three built-in options in [webcrawler/writers.py](webcrawler/writers.py):

```python
from webcrawler import make_jsonl_storage, make_jsonl_storage_with_content, InMemoryStorage

# Metadata only (URL, status, depth, content hash, timestamp)
storage_fn = make_jsonl_storage("output.jsonl")

# Metadata + raw HTML
storage_fn = make_jsonl_storage_with_content("output_full.jsonl")

# In-memory (for testing / small crawls)
store = InMemoryStorage()
storage_fn = store.save
# After crawl: store.pages is a list of dicts
```

## Resuming a Crawl

`bloom_persist=True` (the default) stores the bloom filter and queue in Redis. Stop with `Ctrl+C` at any time and re-run the same script — already-visited URLs are skipped.

To start fresh, set `bloom_persist=False` for one run (clears the Redis bloom key and queue on startup).

## Project Structure

```
webcrawler/           # Core package
  __init__.py         # Public API re-exports
  settings.py         # CrawlerConfig dataclass — all tuneable parameters
  engine.py           # CrawlerPool — orchestrator (Redis setup, worker lifecycle)
  worker.py           # CrawlWorker — per-coroutine fetch/parse/enqueue logic
  url_filter.py       # BloomFilter — Redis bit-array deduplication
  politeness.py       # RobotsCache — robots.txt fetching and enforcement
  writers.py          # Storage helpers: JSONL, JSONL-with-HTML, InMemoryStorage

examples/
  python_docs.py      # Crawl docs.python.org, keep pages mentioning asyncio
  arxiv.py            # Crawl arXiv listing pages, store abstract pages
```

## License

MIT — see [LICENSE](LICENSE).
