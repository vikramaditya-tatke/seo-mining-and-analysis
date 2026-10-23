# SEO Mining and Analysis

Offline command-line ETL pipeline for extracting, transforming, and analyzing SEO
metrics from saved SimilarWeb HTML files.

Repository: [vikramaditya-tatke/seo-mining-and-analysis](https://github.com/vikramaditya-tatke/seo-mining-and-analysis).
The local checkout directory is `seo-mining-and-analysis`. This project runs as a
Python CLI; Prefect orchestration and a web UI are not implemented.

## Run and verify

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
git clone git@github.com:vikramaditya-tatke/seo-mining-and-analysis.git
cd seo-mining-and-analysis
uv sync --frozen
uv run --frozen main.py --output-root .verification
uv run --frozen pytest -q
uv run --frozen ruff check .
uv run --frozen ruff format --check .
```

The pipeline reads the five saved HTML scrapes in `data/raw/scraped_html` and runs
offline. It produces `.verification/data/transformed/polars_dump.csv`,
`.verification/db/scraped_data.duckdb`, and six PNG charts under
`.verification/data/visualizations`. Omit `--output-root` to update the original
`data/` and `db/` outputs. Use `--input-dir PATH` to select another input directory.
Processing failures raise exceptions and return a nonzero exit status. An empty
input directory retains the existing behavior: log a warning and exit successfully
without writing outputs. The extraction function retains its `None`-on-failure
contract; the pipeline rejects that result before publishing incomplete data.

The end-to-end tests run the actual CLI with isolated output directories, validate
all five domains against the saved source values, check numeric and JSON database
types, verify monthly calculations and relative rankings, check all six PNGs, and
repeat the run to check that it replaces data without duplication. They also cover
malformed inputs, missing or invalid SQL, missing traffic history, and zero baselines.

The supplied data covers October-December 2022. Byte Trading has rank history but
no visit history, so its visit growth remains null. Relative rankings sort missing
growth last and sum the visits and rank-improvement positions; lower combined
scores are better. Positive `rank_change` means improvement, while positive
`rank_change_percent` means the numerical rank increased (worsened).
Monthly charts include both metric levels and growth percentages, with calendar
month labels so the growth analysis is visible in the exported PNGs.

This repository processes saved scrapes; it contains no live scraper or Prefect
flow. Verification covers extraction through chart generation.

## Verification results

Verified on 23 October 2026 using the supplied five HTML files:

| Check | Result |
| --- | --- |
| Automated tests | 15 passed, including CLI execution and rerun checks |
| Extracted and loaded domains | 5 |
| Monthly visit observations with data | 12 across 4 domains |
| Monthly rank observations | 15 across 5 domains |
| Relative ranking results | All 5 domains, including missing traffic history |
| Saved visualizations | 6 PNGs, visually inspected |
| Code checks | Ruff lint, format, and Git whitespace checks passed |

Verification reproduced and corrected duration integer overflow, cleaned metrics
being dropped during database loading, malformed open-ended age labels, domains
being omitted from rankings when all traffic history is absent, and failed
processing being reported as successful. Regression tests preserve the existing
extraction and empty-input contracts and prevent failed extraction from publishing
null-domain records. Google's `00:10:35` is now stored as **635 seconds** in both
the intermediate CSV and DuckDB.

## Data Flow

<img width="1738" height="1099" alt="Data Scraping Pipeline excalidraw(1)" src="https://github.com/user-attachments/assets/3e761b71-aa88-455c-8c40-462359b43219" />

## Task

5 scrapes of 5 websites have been made, which correspond to the websites here:
1. https://www.similarweb.com/website/byte-trading.com/#overview
2. https://www.similarweb.com/website/crunchbase.com/#overview
3. https://www.similarweb.com/website/pitchbook.com/#overview
4. https://www.similarweb.com/website/stripe.com/#overview
5. https://www.similarweb.com/website/google.com/#overview

## Step 1: Extract the data and store them in a single CSV file

Extract the following data points:
1. Global Rank
2. Total Visits
3. Bounce Rate
4. Pages per Visit
5. Avg Visit Duration
6. The change in rank over October, November and December
7. The total number of visits in October, November and December
8. Last Month Change in traffic
9. Top Countries
10. Age Distribution

## Step 2: Transform the extracted data, and load it into a DuckDB file

- Clean and normalise the data for example
  - The string "87.0B" is better represented as 87,000,000,000
  - The duration "00:10:35" is better represented as seconds 635
- Import the CSV into DuckDB
- Attempt to cast the data to data types, please don't cast all the data to Text

## Step 3: Analyze the data and plot the results on appropriate graphs and save the graphs as image files.

1. Month-on-month growth on the web visits
2. Month-on-month growth on the rank changes
3. Rank the results based on growth in visits and rank using a relative scale
