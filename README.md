# 基金持仓分析系统

个人基金管理工具集 — 净值获取、持仓跟踪、定投回测、周报生成。

## 脚本说明

| 脚本 | 功能 |
|------|------|
| `nav_fetcher.py` | 净值抓取（天天基金 API） |
| `portfolio_tracker.py` | 持仓跟踪与盈亏计算 |
| `backtest_engine.py` | 定投回测引擎 |
| `weekly_advisor.py` | 周报生成 |
| `analyze_19_funds.py` | 批量基金分析 |
| `import_alipay_csv.py` | 支付宝 CSV 导入 |
| `logger.py` | 日志模块 |

## 快速开始

```bash
# 获取净值
python nav_fetcher.py --codes 000051,001717

# 持仓跟踪
python portfolio_tracker.py

# 定投回测
python backtest_engine.py --days 365

# 生成周报
python weekly_advisor.py
```

## 数据文件

数据文件位于 `数据/` 目录（不上传）：
- `基金交易记录.xlsx` — 交易记录
- `nav_cache/*.json` — 净值缓存

## 依赖

- Python 3.8+
- openpyxl

## License

MIT
