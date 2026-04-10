from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


DATA_DIR = Path(__file__).resolve().parent / "data"


_WS_RE = re.compile(r"\s+")


def _clean_text(s: str | None) -> str:
    if not s:
        return ""
    s = s.strip()
    s = _WS_RE.sub(" ", s)
    return s


def _utc_from_epoch_seconds(x) -> datetime | None:
    try:
        if x is None:
            return None
        return datetime.fromtimestamp(float(x), tz=timezone.utc)
    except Exception:
        return None


def _load_raw(raw_path: Path) -> dict:
    return json.loads(raw_path.read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Clean raw trending JSON into a single analysis-ready CSV.")
    parser.add_argument("--in", dest="in_path", default=str(DATA_DIR / "raw_trending.json"))
    parser.add_argument("--out", dest="out_path", default=str(DATA_DIR / "clean_trending.csv"))
    args = parser.parse_args()

    in_path = Path(args.in_path)
    out_path = Path(args.out_path)
    if not in_path.exists():
        raise FileNotFoundError(f"Missing input file: {in_path}. Run 01_fetch_trending.py first.")

    print("[info] Cleaning data...")
    raw = _load_raw(in_path)
    datasets = raw.get("datasets") or []

    rows: list[dict] = []
    for ds in datasets:
        source = ds.get("source") or "unknown"
        fetched_at_utc = ds.get("fetched_at_utc")
        for item in ds.get("items") or []:
            if source == "hackernews_topstories":
                published = _utc_from_epoch_seconds(item.get("time"))
                rows.append(
                    {
                        "source": source,
                        "fetched_at_utc": fetched_at_utc,
                        "item_id": str(item.get("id") or ""),
                        "title": _clean_text(item.get("title")),
                        "author": _clean_text(item.get("by")),
                        "score": pd.to_numeric(item.get("score"), errors="coerce"),
                        "comments": pd.to_numeric(item.get("descendants"), errors="coerce"),
                        "url": _clean_text(item.get("url")),
                        "published_at_utc": published.isoformat().replace("+00:00", "Z") if published else None,
                        "domain": None,
                        "permalink": None,
                    }
                )
            elif source == "reddit_r_python_hot":
                published = _utc_from_epoch_seconds(item.get("created_utc"))
                permalink = item.get("permalink")
                rows.append(
                    {
                        "source": source,
                        "fetched_at_utc": fetched_at_utc,
                        "item_id": str(item.get("id") or ""),
                        "title": _clean_text(item.get("title")),
                        "author": _clean_text(item.get("author")),
                        "score": pd.to_numeric(item.get("score"), errors="coerce"),
                        "comments": pd.to_numeric(item.get("num_comments"), errors="coerce"),
                        "url": _clean_text(item.get("url")),
                        "published_at_utc": published.isoformat().replace("+00:00", "Z") if published else None,
                        "domain": _clean_text(item.get("subreddit")),
                        "permalink": f"https://www.reddit.com{permalink}" if permalink else None,
                    }
                )

    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(
            columns=[
                "source",
                "fetched_at_utc",
                "item_id",
                "title",
                "author",
                "score",
                "comments",
                "url",
                "published_at_utc",
                "domain",
                "permalink",
            ]
        )

    # Small check for the main columns we expect to work with.
    required_cols = ["title", "score", "source", "item_id"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f"[warn] Missing columns: {missing}")
        # Add them as empty so the rest of the script can still run.
        for c in missing:
            df[c] = pd.NA

    # Simple deduplication: keep the first seen item per source+id.
    df = df.drop_duplicates(subset=["source", "item_id"], keep="first")

    # Basic cleanup / normalisation
    df["title"] = df["title"].fillna("").astype(str).map(_clean_text)
    df["author"] = df["author"].fillna("").astype(str).map(_clean_text)
    df["source"] = df["source"].fillna("unknown").astype(str)
    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    df["comments"] = pd.to_numeric(df["comments"], errors="coerce")

    # Add convenience columns for analysis.
    df["title_len"] = df["title"].astype(str).map(len)
    df["published_dt"] = pd.to_datetime(df["published_at_utc"], errors="coerce", utc=True)
    df["published_hour_utc"] = df["published_dt"].dt.hour

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Wrote {out_path} ({len(df)} rows).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
