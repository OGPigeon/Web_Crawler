from __future__ import annotations

import asyncio
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import aiohttp


class RobotsCache:
    """
    Per-domain robots.txt cache.

    Robots files are fetched lazily on first access for each domain and then
    cached in memory for the lifetime of this object. A per-domain asyncio
    lock prevents multiple workers from fetching the same robots.txt file
    simultaneously (stampede prevention).

    Enforcement delegates entirely to Python's stdlib RobotFileParser so the
    matching logic (user-agent substring matching, Allow vs Disallow precedence,
    wildcard paths) is always correct.
    """

    def __init__(self, user_agent: str = "*") -> None:
        self._user_agent = user_agent
        # Short name used for robots.txt agent matching: "GeneralCrawler/1.0" → "generalcrawler"
        self._short_agent = user_agent.split("/")[0].lower()
        self._cache: dict[str, RobotFileParser | None] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def can_fetch(self, session: aiohttp.ClientSession, url: str) -> bool:
        """Return True if this domain's robots.txt permits fetching the URL."""
        parser = await self._get(session, url)
        if parser is None:
            return True  # failed to load robots.txt → assume allowed
        return parser.can_fetch(self._user_agent, url)

    async def get_crawl_delay(
        self, session: aiohttp.ClientSession, url: str
    ) -> float | None:
        """
        Return the Crawl-delay from this domain's robots.txt, or None.
        Callers should fall back to their configured default when None is returned.
        """
        parser = await self._get(session, url)
        if parser is None:
            return None
        delay = parser.crawl_delay(self._user_agent)
        if delay is not None:
            return float(delay)
        # Request-rate is an older directive: N requests per M seconds → M/N s/request
        rate = parser.request_rate(self._user_agent)
        if rate is not None and rate.requests:
            return rate.seconds / rate.requests
        return None

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _domain_key(self, url: str) -> str:
        """Return 'https://example.com' — scheme + netloc, no path."""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"

    def _robots_url(self, domain_key: str) -> str:
        return f"{domain_key}/robots.txt"

    def _agent_applies(self, robots_agent: str) -> bool:
        """
        Mirror Python stdlib's RobotFileParser.applies_to() logic:
        - "*" matches everything.
        - Otherwise check if the robots.txt agent name is a substring of our
          short agent name (case-insensitive).
        """
        if robots_agent == "*":
            return True
        return robots_agent.lower() in self._short_agent

    async def _get(
        self, session: aiohttp.ClientSession, url: str
    ) -> RobotFileParser | None:
        domain_key = self._domain_key(url)

        if domain_key in self._cache:
            return self._cache[domain_key]

        # Lazily create the per-domain lock.
        if domain_key not in self._locks:
            self._locks[domain_key] = asyncio.Lock()

        async with self._locks[domain_key]:
            # Re-check after acquiring the lock — another coroutine may have
            # populated the cache while we were waiting.
            if domain_key in self._cache:
                return self._cache[domain_key]
            await self._load(session, domain_key)

        return self._cache.get(domain_key)

    async def _load(
        self, session: aiohttp.ClientSession, domain_key: str
    ) -> None:
        """Fetch, parse, and log robots.txt; store None on failure."""
        robots_url = self._robots_url(domain_key)
        try:
            async with session.get(
                robots_url, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    text = await resp.text(errors="replace")
                    parser = RobotFileParser(robots_url)
                    parser.parse(text.splitlines())
                    self._cache[domain_key] = parser
                    self._log_robots(domain_key, parser, text)
                else:
                    print(
                        f"[Robots] {domain_key}: HTTP {resp.status} "
                        f"— treating as no restrictions."
                    )
                    self._cache[domain_key] = None
        except Exception as exc:
            print(
                f"[Robots] {domain_key}: fetch failed ({exc!r}) "
                f"— treating as no restrictions."
            )
            self._cache[domain_key] = None

    def _log_robots(
        self, domain_key: str, parser: RobotFileParser, raw_text: str
    ) -> None:
        """Print a summary of the robots.txt rules that apply to our user agent."""
        delay = parser.crawl_delay(self._user_agent)
        rate = parser.request_rate(self._user_agent)

        disallowed: list[str] = []
        allowed: list[str] = []

        section_agents: list[str] = []
        section_disallow: list[str] = []
        section_allow: list[str] = []

        def _commit() -> None:
            if any(self._agent_applies(a) for a in section_agents):
                disallowed.extend(section_disallow)
                allowed.extend(section_allow)
            section_agents.clear()
            section_disallow.clear()
            section_allow.clear()

        for raw_line in raw_text.splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                _commit()
                continue
            lower = line.lower()
            if lower.startswith("user-agent:"):
                if section_disallow or section_allow:
                    _commit()
                agent = line.split(":", 1)[1].strip()
                section_agents.append(agent)
            elif lower.startswith("disallow:"):
                path = line.split(":", 1)[1].strip()
                if path:
                    section_disallow.append(path)
            elif lower.startswith("allow:"):
                path = line.split(":", 1)[1].strip()
                if path:
                    section_allow.append(path)

        _commit()

        parts: list[str] = [f"[Robots] {domain_key}"]
        if delay is not None:
            parts.append(f"Crawl-delay={delay}s")
        elif rate is not None:
            parts.append(f"Request-rate={rate.requests}/{rate.seconds}s")
        else:
            parts.append("no Crawl-delay")

        if disallowed:
            shown = disallowed[:10]
            suffix = f" +{len(disallowed) - 10} more" if len(disallowed) > 10 else ""
            parts.append(f"Disallow=[{', '.join(shown)}{suffix}]")
        else:
            parts.append("no Disallow rules for our agent")

        if allowed:
            shown = allowed[:5]
            suffix = f" +{len(allowed) - 5} more" if len(allowed) > 5 else ""
            parts.append(f"Allow=[{', '.join(shown)}{suffix}]")

        print("  ".join(parts))
