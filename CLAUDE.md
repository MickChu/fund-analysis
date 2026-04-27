# CLAUDE.md

AI 助手工作指南。

## 项目概述

个人基金持仓跟踪工具。从天天基金 API 获取净值，基于 Excel 交易记录计算盈亏，支持定投回测和周报生成。

**语言**：Python 3.8+，中文注释和输出。

## 运行方式

脚本在仓库根目录运行：

```bash
python nav_fetcher.py --codes 000051,001717
python portfolio_tracker.py
python backtest_engine.py --days 365
python weekly_advisor.py
```

## 数据源

- 净值 API：`https://fundgz.1702.com/js/xxxxx.js`（天天基金 JSONP）
- 历史净值：`https://api.fund.eastmoney.com/f10/lsjz`（东方财富）
- 交易记录：`数据/基金交易记录.xlsx`（GBK/UTF-8 混合编码）

## 编码规范

- 中文文件名是有意的（数据文件、输出文件），不要重命名
- 使用 `logger.py` 的 `setup_logging()`，不用 `print()`
- 净值缓存：`数据/nav_cache/{基金代码}.json`
- 读取 Excel 时兼容 GBK 和 UTF-8 编码
- 新脚本遵循：argparse CLI → 加载配置 → 主逻辑 → 输出到 `输出/`

## 依赖

- 标准库
- openpyxl（Excel 读写）

无重型 ML 库。
