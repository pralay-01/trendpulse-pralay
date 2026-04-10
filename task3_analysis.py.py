from __future__ import annotations

import argparse
import json
import math
import re
from collections import Counter
from pathlib import Path

import pandas as pd


DATA_DIR = Path(__file__).resolve().parent / "data"


_WORD_RE = re.compile(r"[a-z0-9][a-z0-9\-\']{1,}")

_STOPWORDS = {
    "the",
    "a",
    "an",
    "and",
    "or",
    "to",
    "of",
    "in",
    "for",
    "on",
    "with",
    "is",
    "are",
    "was",
    "were",
    "be",
    "as",
    "at",
    "by",
    "from",
    "it",
    "this",
    "that",
    "these",
    "those",
    "you",
    "your",
    "we",
    "our",
    "i",
    "my",
    "not",
    "but",
    "can",
    "how",
    "why",
    "what",
    "when",
    "who",
    "vs",
}


def _tokenize_title(title: str) -> list[str]:
    title = (title or "").lower()
    toks = _WORD_RE.findall(title)
    return [t for t in toks if t not in _STOPWORDS and len(t) >= 3]


def _safe_float(x) -> float:
    try:
        if x is None:
            return float("nan")
        return float(x)
    except Exception:
        return float("nan")


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze the cleaned trending CSV and write summary JSON/CSV.")
    parser.add_argument("--in", dest="in_path", default=str(DATA_DIR / "clean_trending.csv"))
    parser.add_argument("--out-json", dest="out_json", default=str(DATA_DIR / "analysis_summary.json"))
    parser.add_argument("--out-topwords", dest="out_topwords", default=str(DATA_DIR / "top_words.csv"))
    args = parser.parse_args()

    in_path = Path(args.in_path)
    if not in_path.exists():
        raise FileNotFoundError(f"Missing input file: {in_path}. Run 02_clean_trending.py first.")

    print("[info] Running analysis...")
    df = pd.read_csv(in_path)
    if df.empty:
        summary = {"rows": 0, "by_source": {}, "top_words": []}
        Path(args.out_json).write_text(json.dumps(summary, indent=2), encoding="utf-8")
        pd.DataFrame(columns=["word", "count"]).to_csv(args.out_topwords, index=False, encoding="utf-8")
        print("No data to analyze.")
        return 0

    # Small check for columns we expect to analyze.
    required_cols = ["title", "score", "comments", "source"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f"[warn] Missing columns: {missing}")
        # Add them as empty so the rest of the script can still run.
        for c in missing:
            df[c] = pd.NA

    print("[info] Calculating statistics...")

    # Basic per-source stats
    by_source = {}
    for src, g in df.groupby("source", dropna=False):
        scores = pd.to_numeric(g["score"], errors="coerce")
        comments = pd.to_numeric(g["comments"], errors="coerce")
        by_source[str(src)] = {
            "rows": int(len(g)),
            "score_mean": float(scores.mean(skipna=True)) if len(g) else float("nan"),
            "score_median": float(scores.median(skipna=True)) if len(g) else float("nan"),
            "comments_mean": float(comments.mean(skipna=True)) if len(g) else float("nan"),
            "max_score": float(scores.max(skipna=True)) if len(g) else float("nan"),
        }

    # Simple insights
    df_scores = df.copy()
    df_scores["score"] = pd.to_numeric(df_scores["score"], errors="coerce")

    top_5_posts = []
    top_df = df_scores.dropna(subset=["score"]).sort_values("score", ascending=False).head(5)
    for _, row in top_df.iterrows():
        top_5_posts.append(
            {
                "source": str(row.get("source") or ""),
                "title": str(row.get("title") or ""),
                "score": _safe_float(row.get("score")),
                "comments": _safe_float(row.get("comments")),
            }
        )

    avg_score_per_source = {}
    for src, g in df_scores.groupby("source", dropna=False):
        avg_score_per_source[str(src)] = _safe_float(g["score"].mean(skipna=True))

    # Best posting hour (UTC) based on average score.
    best_hour = None
    best_hour_avg_score = float("nan")
    if "published_hour_utc" in df_scores.columns:
        hour_scores = df_scores.dropna(subset=["published_hour_utc", "score"]).copy()
        hour_scores["published_hour_utc"] = pd.to_numeric(hour_scores["published_hour_utc"], errors="coerce")
        hour_scores = hour_scores.dropna(subset=["published_hour_utc"])
        if not hour_scores.empty:
            hour_means = hour_scores.groupby("published_hour_utc")["score"].mean()
            if not hour_means.empty:
                best_hour = int(hour_means.idxmax())
                best_hour_avg_score = _safe_float(hour_means.loc[best_hour])

    # Top words across titles
    c = Counter()
    for t in df["title"].fillna("").astype(str):
        c.update(_tokenize_title(t))
    top_words = [{"word": w, "count": int(n)} for (w, n) in c.most_common(50)]

    # Write artifacts
    out_json = Path(args.out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)

    # Convert NaNs to None-friendly JSON by mapping non-finite floats.
    def _json_sanitize(obj):
        if isinstance(obj, dict):
            return {k: _json_sanitize(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_json_sanitize(v) for v in obj]
        if isinstance(obj, float):
            return None if not math.isfinite(obj) else obj
        return obj

    summary = _json_sanitize(
        {
            "rows": int(len(df)),
            "sources": sorted({str(s) for s in df["source"].dropna().unique()}),
            "by_source": by_source,
            "avg_score_per_source": avg_score_per_source,
            "top_5_posts_by_score": top_5_posts,
            "best_posting_hour_utc": best_hour,
            "best_posting_hour_avg_score": best_hour_avg_score,
            "top_words": top_words,
        }
    )
    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    out_tw = Path(args.out_topwords)
    pd.DataFrame(top_words).to_csv(out_tw, index=False, encoding="utf-8")

    print(f"Wrote {out_json} and {out_tw}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
