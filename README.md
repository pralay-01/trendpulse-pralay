# Live Trending Data Pipeline (4 Python scripts)

Fetches **live trending data**, cleans it, analyzes it, and generates visualizations.

## What it pulls (live)

- **Hacker News**: Top stories via the official Firebase API.
- **Reddit**: `/r/Python` hot posts via the public JSON endpoint (may rate-limit; pipeline continues even if Reddit is empty).

## Project structure

- `01_fetch_trending.py`: fetches raw JSON into `data/raw_trending.json`
- `02_clean_trending.py`: flattens + cleans into `data/clean_trending.csv`
- `03_analyze_trending.py`: writes `data/analysis_summary.json` and `data/top_words.csv`
- `04_visualize_trending.py`: saves charts into `outputs/`

## Setup

```bash
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

## Run the full pipeline

```bash
python 01_fetch_trending.py
python 02_clean_trending.py
python 03_analyze_trending.py
python 04_visualize_trending.py
```

## Outputs

- `data/raw_trending.json`: raw fetched data (two datasets)
- `data/clean_trending.csv`: unified clean table
- `data/analysis_summary.json`: per-source stats + top words
- `outputs/*.png`: plots (score distribution, top items, activity by hour)

## Useful options

```bash
python 01_fetch_trending.py --hn-limit 50 --reddit-limit 50 --timeout 30
python 02_clean_trending.py --in data/raw_trending.json --out data/clean_trending.csv
python 04_visualize_trending.py --in data/clean_trending.csv --out-dir outputs
```
