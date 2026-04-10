from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests


DATA_DIR = Path(__file__).resolve().parent / "data"
USER_AGENT = "beginner-trending-pipeline/1.0 (learning project)"
HEADERS = {"User-Agent": USER_AGENT}


@dataclass(frozen=True)
class SourceResult:
    source: str
    fetched_at_utc: str
    items: list[dict[str, Any]]


def _now_utc_iso() -> str:
    # Keep as a simple ISO-ish string without external deps.
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def fetch_hackernews_top(limit: int, timeout_s: int) -> SourceResult:
    top_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    item_url = "https://hacker-news.firebaseio.com/v0/item/{id}.json"

    print("[info] Fetching Hacker News top stories...")
    try:
        r = requests.get(top_url, headers=HEADERS, timeout=timeout_s)
        r.raise_for_status()
        ids: list[int] = (r.json() or [])[:limit]
    except Exception as e:
        print(f"[warn] Hacker News request failed: {e}")
        return SourceResult(source="hackernews_topstories", fetched_at_utc=_now_utc_iso(), items=[])

    items: list[dict[str, Any]] = []
    for story_id in ids:
        try:
            rr = requests.get(item_url.format(id=story_id), headers=HEADERS, timeout=timeout_s)
            rr.raise_for_status()
            obj = rr.json() or {}
        except Exception as e:
            print(f"[warn] Failed to fetch story {story_id}: {e}")
            continue
        if obj.get("type") != "story":
            continue
        # Simple validation: skip items with missing titles.
        if not obj.get("title"):
            continue
        items.append(
            {
                "id": obj.get("id"),
                "title": obj.get("title"),
                "by": obj.get("by"),
                "score": obj.get("score"),
                "descendants": obj.get("descendants"),
                "url": obj.get("url"),
                "time": obj.get("time"),
            }
        )

    return SourceResult(source="hackernews_topstories", fetched_at_utc=_now_utc_iso(), items=items)


def fetch_reddit_r_python_hot(limit: int, timeout_s: int) -> SourceResult:
    # Public JSON endpoint; include a User-Agent.
    url = "https://www.reddit.com/r/Python/hot.json"
    params = {"limit": limit, "raw_json": 1}

    print("[info] Fetching Reddit /r/Python hot posts...")
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=timeout_s)
        r.raise_for_status()
        payload = r.json() or {}
    except Exception as e:
        print(f"[warn] Reddit request failed: {e}")
        return SourceResult(source="reddit_r_python_hot", fetched_at_utc=_now_utc_iso(), items=[])

    children = (((payload.get("data") or {}).get("children")) or [])[:limit]
    items: list[dict[str, Any]] = []
    for child in children:
        d = (child or {}).get("data") or {}
        # Simple validation: skip items with missing titles.
        if not d.get("title"):
            continue
        items.append(
            {
                "id": d.get("id"),
                "title": d.get("title"),
                "author": d.get("author"),
                "score": d.get("score"),
                "num_comments": d.get("num_comments"),
                "url": d.get("url"),
                "created_utc": d.get("created_utc"),
                "subreddit": d.get("subreddit"),
                "permalink": d.get("permalink"),
            }
        )

    return SourceResult(source="reddit_r_python_hot", fetched_at_utc=_now_utc_iso(), items=items)


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch live trending datasets and store raw JSON.")
    parser.add_argument("--hn-limit", type=int, default=30, help="How many HN top stories to fetch.")
    parser.add_argument("--reddit-limit", type=int, default=30, help="How many Reddit /r/Python hot posts to fetch.")
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout seconds.")
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print("[info] Fetching data...")
    results: list[SourceResult] = []
    results.append(fetch_hackernews_top(limit=args.hn_limit, timeout_s=args.timeout))

    # Reddit can rate-limit sometimes; if it fails, we just get an empty dataset.
    results.append(fetch_reddit_r_python_hot(limit=args.reddit_limit, timeout_s=args.timeout))

    raw_path = DATA_DIR / "raw_trending.json"
    raw_obj = {"generated_at_utc": _now_utc_iso(), "datasets": [r.__dict__ for r in results]}
    raw_path.write_text(json.dumps(raw_obj, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {raw_path} ({sum(len(r.items) for r in results)} total items).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
