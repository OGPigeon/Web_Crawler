from __future__ import annotations

import asyncio
import inspect
import json
import time
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup

from .url_filter import BloomFilter
from .settings import CrawlerConfig
from .politeness import RobotsCache

# max_redirects in ClientSession was added in aiohttp 3.10; detect once at import time.
_SESSION_SUPPORTS_MAX_REDIRECTS = (
    "max_redirects" in inspect.signature(aiohttp.ClientSession).parameters
)


class CrawlWorker:
    """
    Single async crawler coroutine. Multiple instances share:
      - one Redis client (and thus one bloom filter + queue)
      - one RobotsCache
      - one asyncio.Semaphore (cap on concurrent in-flight HTTP requests)
      - per-domain locks and last-fetch timestamps (for rate limiting)
      - a mutable pages_crawled counter and a shutdown_event

    Workers BLPOP from the shared Redis queue. Idle detection: when BLPOP
    times out (queue empty), the worker decrements the active-workers counter
    in Redis. If the counter reaches 0 AND the queue is still empty, it sets
    the shutdown_event. Otherwise it re-increments and keeps waiting.
    """

    def __init__(
        self,
        worker_id: int,
        config: CrawlerConfig,
        redis,
        bloom: BloomFilter,
        robots: RobotsCache,
        semaphore: asyncio.Semaphore,
        domain_locks: dict[str, asyncio.Lock],
        domain_last_fetch: dict[str, float],
        pages_crawled: list[int],
        shutdown_event: asyncio.Event,
    ) -> None:
        self.id = worker_id
        self.config = config
        self.redis = redis
        self.bloom = bloom
        self.robots = robots
        self.semaphore = semaphore
        self.domain_locks = domain_locks
        self.domain_last_fetch = domain_last_fetch
        self.pages_crawled = pages_crawled
        self.shutdown_event = shutdown_event

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        try:
            await self._run()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[Worker {self.id}] Fatal error: {exc!r}")
            raise

    async def _run(self) -> None:
        # ssl=None → verify certs (default); ssl=False → skip verification.
        ssl_param = None if self.config.verify_ssl else False
        connector = aiohttp.TCPConnector(ssl=ssl_param)
        session_kwargs: dict = {
            "connector": connector,
            "headers": {"User-Agent": self.config.user_agent},
            "timeout": aiohttp.ClientTimeout(total=self.config.request_timeout),
        }
        if _SESSION_SUPPORTS_MAX_REDIRECTS:
            session_kwargs["max_redirects"] = self.config.max_redirects

        print(f"[Worker {self.id}] Starting.")
        async with aiohttp.ClientSession(**session_kwargs) as session:
            while not self.shutdown_event.is_set():
                raw = await self.redis.blpop(self.config.redis_queue_key, timeout=2)

                if raw is None:
                    # Queue appears empty; check if crawl is truly finished.
                    active = await self.redis.decr(
                        self.config.redis_active_workers_key
                    )
                    # Brief grace period so any in-flight worker can finish
                    # pushing its last batch of child URLs before we declare done.
                    await asyncio.sleep(0.5)
                    queue_len = await self.redis.llen(self.config.redis_queue_key)
                    print(f"[Worker {self.id}] Queue empty. active_workers={active}, queue_len={queue_len}.")
                    if active <= 0 and queue_len == 0:
                        print(f"[Worker {self.id}] All workers idle and queue empty — signalling shutdown.")
                        self.shutdown_event.set()
                        return
                    # Another worker is still active or added items; re-register.
                    await self.redis.incr(self.config.redis_active_workers_key)
                    continue

                try:
                    job = json.loads(raw[1])
                    url: str = job["url"]
                    depth: int = job.get("depth", 0)
                except (KeyError, json.JSONDecodeError, TypeError):
                    continue

                await self._process_job(session, url, depth)

        print(f"[Worker {self.id}] Exiting (shutdown_event set).")

    # ------------------------------------------------------------------
    # Per-URL processing
    # ------------------------------------------------------------------

    async def _process_job(
        self, session: aiohttp.ClientSession, url: str, depth: int
    ) -> None:
        cfg = self.config

        # Enforce max_depth: still bloom-add to prevent re-enqueueing.
        if cfg.max_depth is not None and depth > cfg.max_depth:
            await self.bloom.add(url)
            return

        # Atomic dedup: returns True only if URL was not previously seen.
        if not await self.bloom.add_if_not_exists(url):
            return

        # Domain closure: URL is already in bloom; just don't fetch it.
        if not self._is_allowed_domain(url):
            print(f"[Worker {self.id}] Domain skip: {url}")
            return

        # Robots.txt compliance.
        if cfg.respect_robots and not await self.robots.can_fetch(session, url):
            print(f"[Worker {self.id}] Robots skip: {url}")
            return

        # Per-domain rate limiting.
        robots_delay = await self.robots.get_crawl_delay(session, url)
        if robots_delay is not None:
            crawl_delay = robots_delay
            delay_source = f"robots.txt ({robots_delay}s)"
        else:
            crawl_delay = cfg.default_crawl_delay
            delay_source = f"config default ({cfg.default_crawl_delay}s)"
        domain = urlparse(url).netloc
        await self._apply_rate_limit(domain, crawl_delay, delay_source)

        print(f"[Worker {self.id}] Fetching (depth={depth}): {url}")

        # Fetch.
        result = await self._fetch(session, url)
        if result is None:
            print(f"[Worker {self.id}] Fetch failed: {url}")
            return

        status_code, content_type, body, resp_headers = result
        print(f"[Worker {self.id}] OK {status_code} {content_type} ({len(body)} bytes): {url}")

        metadata: dict = {
            "status_code": status_code,
            "content_type": content_type,
            "depth": depth,
            "headers": dict(resp_headers),
        }

        # Increment pages counter; check max_pages limit.
        self.pages_crawled[0] += 1
        if cfg.max_pages is not None and self.pages_crawled[0] >= cfg.max_pages:
            self.shutdown_event.set()

        is_relevant = (
            cfg.relevancy_fn(url, body, metadata)
            if cfg.relevancy_fn is not None
            else True
        )

        if is_relevant and cfg.storage_fn is not None:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, cfg.storage_fn, url, body, metadata)
            print(f"[Worker {self.id}] Saved: {url}")
        elif not is_relevant:
            print(f"[Worker {self.id}] Filtered out: {url}")

        # Extract and enqueue links from ALL fetched pages regardless of
        # relevancy so the crawl graph is fully explored.
        if content_type.startswith("text/html"):
            links = self._extract_links(body, url)
        elif content_type == "application/pdf":
            links = self._extract_links_pdf(body, url)
        else:
            links = []

        if links:
            allowed = [l for l in links if self._is_allowed_domain(l)]
            print(f"[Worker {self.id}] Enqueuing {len(allowed)}/{len(links)} links from {url}")
            for link in allowed:
                await self._enqueue(link, depth + 1)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _is_allowed_domain(self, url: str) -> bool:
        """
        Return True if the URL's hostname is within the allowed domains.
        Subdomain matching: "example.com" permits "www.example.com".
        Returns True when allowed_domains is None (open crawl).
        """
        if self.config.allowed_domains is None:
            return True
        try:
            netloc = urlparse(url).netloc.lower().split(":")[0]  # strip port
        except Exception:
            return False
        for domain in self.config.allowed_domains:
            d = domain.lower()
            if netloc == d or netloc.endswith("." + d):
                return True
        return False

    async def _apply_rate_limit(
        self, domain: str, delay: float, delay_source: str = ""
    ) -> None:
        """
        Serialize same-domain requests across all workers using a per-domain
        asyncio.Lock. Acquires lock → sleeps if needed → updates timestamp →
        releases lock BEFORE the actual HTTP request so other domains proceed.
        """
        if domain not in self.domain_locks:
            self.domain_locks[domain] = asyncio.Lock()

        async with self.domain_locks[domain]:
            last = self.domain_last_fetch.get(domain)
            if last is not None:
                wait = delay - (time.monotonic() - last)
                if wait > 0:
                    src = f" [{delay_source}]" if delay_source else ""
                    print(f"[Worker {self.id}] Rate limit: sleeping {wait:.1f}s for {domain}{src}")
                    await asyncio.sleep(wait)
            self.domain_last_fetch[domain] = time.monotonic()

    async def _fetch(
        self, session: aiohttp.ClientSession, url: str
    ) -> tuple[int, str, bytes, dict] | None:
        """
        Fetch url; returns (status_code, content_type, body, headers) or None.
        Acquires the global semaphore for the duration of the network call.
        """
        try:
            async with self.semaphore:
                async with session.get(url, allow_redirects=True) as resp:
                    content_type = resp.headers.get("Content-Type", "").split(";")[0].strip()
                    # Download text and PDF bodies; skip everything else (images,
                    # archives, etc.) to avoid wasting bandwidth.
                    if not content_type.startswith("text/") and content_type != "application/pdf":
                        return resp.status, content_type, b"", dict(resp.headers)
                    body = await resp.read()
                    return resp.status, content_type, body, dict(resp.headers)
        except Exception as exc:
            print(f"[Worker {self.id}] Fetch exception for {url}: {exc!r}")
            return None

    def _extract_links(self, html_bytes: bytes, base_url: str) -> list[str]:
        """
        Parse HTML and return normalised absolute URLs found in <a href> tags.
        Strips fragments, limits to http/https, deduplicates.
        """
        try:
            soup = BeautifulSoup(html_bytes, "lxml")
        except Exception:
            return []

        seen: set[str] = set()
        links: list[str] = []
        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            absolute = urljoin(base_url, href)
            parsed = urlparse(absolute)
            if parsed.scheme not in ("http", "https"):
                continue
            normalised = parsed._replace(fragment="").geturl()
            if normalised not in seen:
                seen.add(normalised)
                links.append(normalised)
        return links

    def _extract_links_pdf(self, pdf_bytes: bytes, base_url: str) -> list[str]:
        """
        Extract embedded hyperlinks from a PDF using pypdf.
        Returns normalised absolute http/https URLs, deduplicated.
        Only hyperlink annotations are used — no regex over raw text — so
        links must be explicitly clickable in the PDF to be discovered.
        """
        if not pdf_bytes:
            return []
        try:
            import io
            import pypdf
        except ImportError:
            print(
                f"[Worker {self.id}] pypdf not installed — PDF links skipped. "
                "Run: pip install pypdf"
            )
            return []
        try:
            reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
        except Exception as exc:
            print(f"[Worker {self.id}] PDF parse failed for {base_url}: {exc!r}")
            return []

        seen: set[str] = set()
        links: list[str] = []
        for page in reader.pages:
            for annot in page.get("/Annots", []):
                obj = annot.get_object()
                if obj.get("/Subtype") != "/Link":
                    continue
                action = obj.get("/A")
                if not action or action.get("/S") != "/URI":
                    continue
                uri = action.get("/URI", "")
                if not uri:
                    continue
                absolute = urljoin(base_url, uri)
                parsed = urlparse(absolute)
                if parsed.scheme not in ("http", "https"):
                    continue
                normalised = parsed._replace(fragment="").geturl()
                if normalised not in seen:
                    seen.add(normalised)
                    links.append(normalised)

        return links

    async def _enqueue(self, url: str, depth: int) -> None:
        job = json.dumps({"url": url, "depth": depth})
        await self.redis.rpush(self.config.redis_queue_key, job)
