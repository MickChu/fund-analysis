# CLAUDE.md

This file provides context for AI assistants working with this repository.

## Project Overview

基金持仓分析系统 — Personal fund portfolio tracking toolkit. Fetches fund NAV (Net Asset Value) from public APIs, calculates P&L from Excel trade records, runs backtests, and generates weekly reports.

**Language:** Python 3.8+, all comments and UI output in Chinese.

## Repository Structure

```
V1/程序/          # Source code (all scripts here)
V1/数据/          # Data files (gitignored, not uploaded)
  ├── 基金交易记录.xlsx   # Trade records (sheets: 买入记录, 持仓份额)
  ├── 买入记录.xlsx       # Purchase history
  └── nav_cache/          # JSON cache per fund code
V1/输出/          # Generated reports (gitignored)
```

## Running Scripts

All scripts should be run from the repo root, paths are relative to `V1/`:

```bash
python V1/程序/nav_fetcher.py --codes 000051,001717
python V1/程序/portfolio_tracker.py
python V1/程序/backtest_engine.py --days 365
python V1/程序/weekly_advisor.py
python V1/程序/analyze_19_funds.py
```

## Key Data Sources

- **Fund NAV API:** `https://fundgz.1702.com/js/xxxxx.js` (天天基金 JSONP)
- **Historical NAV:** `https://api.fund.eastmoney.com/f10/lsjz` (东方财富)
- **Trade records:** Excel files in `V1/数据/`, GBK/UTF-8 mixed encoding

## Coding Conventions

- Chinese file names are intentional (data files, output). Do not rename.
- `logger.py` provides `setup_logging()` — use it instead of `print()` in all scripts.
- Nav cache files: `V1/数据/nav_cache/{6-digit fund code}.json`.
- Excel reading: always handle both GBK and UTF-8 encodings gracefully.
- When adding new scripts, follow the existing pattern: argparse CLI → load config → main logic → output to `V1/输出/`.

## Dependencies

- Standard library only (`urllib`, `json`, `csv`, `datetime`, `os`, `re`, etc.)
- `openpyxl` for Excel read/write
- No sklearn, no xgboost, no heavy ML libs in V1.
