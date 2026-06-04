from __future__ import annotations

import hashlib
import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable


def make_jsonl_storage(output_path: str = "crawled_pages.jsonl") -> Callable:
    """
    Return a thread-safe storage_fn that appends one JSON line per page.

    Each record contains URL, status, content type, depth, a sha256 content
    hash for dedup verification, and a crawl timestamp. HTML bytes are NOT
    stored inline (use make_jsonl_storage_with_content for that).

    The returned callable matches the storage_fn signature:
        fn(url: str, html_bytes: bytes, metadata: dict) -> None
    """
    path = Path(output_path)
    lock = threading.Lock()

    def _store(url: str, html_bytes: bytes, metadata: dict) -> None:
        record = {
            "url": url,
            "status_code": metadata.get("status_code"),
            "content_type": metadata.get("content_type"),
            "depth": metadata.get("depth"),
            "html_length": len(html_bytes),
            "content_hash": hashlib.sha256(html_bytes).hexdigest(),
            "crawled_at": datetime.now(timezone.utc).isoformat(),
        }
        line = json.dumps(record, ensure_ascii=False)
        with lock:
            with path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")

    return _store


def make_jsonl_storage_with_content(
    output_path: str = "crawled_pages_full.jsonl",
) -> Callable:
    """
    Like make_jsonl_storage but also stores HTML as a UTF-8 string.

    Content is decoded with error replacement so the output is always valid
    JSON. Suitable for small crawls only — file size grows proportional to
    total HTML bytes crawled.
    """
    path = Path(output_path)
    lock = threading.Lock()

    def _store(url: str, html_bytes: bytes, metadata: dict) -> None:
        record = {
            "url": url,
            "status_code": metadata.get("status_code"),
            "content_type": metadata.get("content_type"),
            "depth": metadata.get("depth"),
            "html_length": len(html_bytes),
            "content_hash": hashlib.sha256(html_bytes).hexdigest(),
            "html": html_bytes.decode("utf-8", errors="replace"),
            "crawled_at": datetime.now(timezone.utc).isoformat(),
        }
        line = json.dumps(record, ensure_ascii=False)
        with lock:
            with path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")

    return _store


class InMemoryStorage:
    """
    Stores pages in memory. Useful for testing or small crawls.

    Usage:
        store = InMemoryStorage()
        config = CrawlerConfig(storage_fn=store.save, ...)
        # After crawl: store.pages contains all stored pages.
    """

    def __init__(self) -> None:
        self.pages: list[dict] = []
        self._lock = threading.Lock()

    def save(self, url: str, html_bytes: bytes, metadata: dict) -> None:
        record = {"url": url, "html_bytes": html_bytes, **metadata}
        with self._lock:
            self.pages.append(record)
