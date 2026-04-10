from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


DATA_DIR = Path(__file__).resolve().parent / "data"
OUT_DIR = Path(__file__).resolve().parent / "outputs"


def main() -> int:
    parser = argparse.ArgumentParser(description="Visualize cleaned trending data into PNG charts.")
    parser.add_argument("--in", dest="in_path", default=str(DATA_DIR / "clean_trending.csv"))
    parser.add_argument("--out-dir", dest="out_dir", default=str(OUT_DIR))
    parser.add_argument("--top-n", dest="top_n", type=int, default=15, help="How many top items to show.")
    args = parser.parse_args()

    in_path = Path(args.in_path)
    out_dir = Path(args.out_dir)
    if not in_path.exists():
        raise FileNotFoundError(f"Missing input file: {in_path}. Run 02_clean_trending.py first.")

    df = pd.read_csv(in_path)
    if df.empty:
        print("No data to visualize.")
        return 0

    # Small check for the main columns we expect for these charts.
    required_cols = ["score", "source", "published_at_utc", "title"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f"[warn] Missing columns: {missing}")
        # Add them as empty so the rest of the script can still run.
        for c in missing:
            df[c] = pd.NA

    out_dir.mkdir(parents=True, exist_ok=True)

    sns.set_theme(style="whitegrid")

    print("[info] Generating charts...")

    # 1) Score distribution by source
    plt.figure(figsize=(10, 5))
    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    score_df = df.dropna(subset=["score"])
    if not score_df.empty:
        sns.histplot(
            data=score_df,
            x="score",
            hue="source",
            bins=20,
            element="step",
            stat="count",
            common_norm=False,
        )
        plt.title("Score distribution (by source)")
        plt.xlabel("Score")
        plt.ylabel("Count")
        plt.tight_layout()
        p = out_dir / "score_distribution.png"
        print(f"[info] Saving chart: {p}")
        plt.savefig(p, dpi=160)
    plt.close()

    # 2) Top items by score (bar)
    plt.figure(figsize=(12, 6))
    top_df = df.dropna(subset=["score"]).sort_values("score", ascending=False).head(args.top_n)
    if not top_df.empty:
        # Shorten long titles to keep the chart readable.
        top_df = top_df.assign(
            title_short=top_df["title"].astype(str).map(lambda s: (s[:70] + "…") if len(s) > 71 else s)
        )
        sns.barplot(data=top_df, y="title_short", x="score", hue="source", dodge=False)
        plt.title(f"Top {len(top_df)} items by score")
        plt.xlabel("Score")
        plt.ylabel("")
        plt.legend(title="Source", loc="lower right", frameon=True)
        plt.tight_layout()
        p = out_dir / "top_items_by_score.png"
        print(f"[info] Saving chart: {p}")
        plt.savefig(p, dpi=160)
    plt.close()

    # 3) Published hour (UTC) activity
    plt.figure(figsize=(10, 5))
    df["published_dt"] = pd.to_datetime(df["published_at_utc"], errors="coerce", utc=True)
    df["hour_utc"] = df["published_dt"].dt.hour
    hour_df = df.dropna(subset=["hour_utc"])
    if not hour_df.empty:
        hour_df["hour_utc"] = hour_df["hour_utc"].astype(int)
        sns.countplot(data=hour_df, x="hour_utc", hue="source")
        plt.title("Posts by hour (UTC)")
        plt.xlabel("Hour (UTC)")
        plt.ylabel("Count")
        plt.tight_layout()
        p = out_dir / "posts_by_hour_utc.png"
        print(f"[info] Saving chart: {p}")
        plt.savefig(p, dpi=160)
    plt.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
