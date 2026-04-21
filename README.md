# 基金持仓分析系统

自动化基金净值获取、持仓盈亏计算、定投回测、周报生成的工具集。

## 📁 目录结构

```
fund-analysis/
├── V1/                    # 核心程序
│   ├── 程序/              # Python 脚本
│   │   ├── nav_fetcher.py          # 净值抓取（天天基金API）
│   │   ├── portfolio_tracker.py    # 持仓跟踪与盈亏计算
│   │   ├── backtest_engine.py      # 定投回测引擎
│   │   ├── weekly_advisor.py       # 周报生成
│   │   ├── analyze_19_funds.py     # 批量基金分析
│   │   ├── import_alipay_csv.py    # 支付宝CSV导入
│   │   └── logger.py               # 日志模块
│   └── 数据/              # 净值数据与交易记录（不上传）
│
└── AI持仓基金指令.md       # AI 操作指令（当前使用）
```

## 🚀 快速开始

```bash
cd V1/程序

# 获取持仓基金净值
python nav_fetcher.py

# 持仓盈亏跟踪
python portfolio_tracker.py

# 定投回测
python backtest_engine.py

# 生成周报
python weekly_advisor.py
```

## 🛠 脚本说明

| 脚本 | 功能 |
|------|------|
| `nav_fetcher.py` | 获取基金净值历史，可单只或批量 |
| `portfolio_tracker.py` | 持仓跟踪，读取Excel计算盈亏 |
| `backtest_engine.py` | 定投回测（`--days 180/365/730`） |
| `weekly_advisor.py` | 周报生成，含回测和操作建议 |
| `analyze_19_funds.py` | 批量分析多只基金 |
| `import_alipay_csv.py` | 支付宝CSV导入 |
| `logger.py` | 日志模块（按日分割，自动清理） |

## ⚠️ 注意事项

- 净值数据来自天天基金网（东方财富），仅供个人学习研究
- `V1/数据/` 目录包含个人交易记录，已通过 .gitignore 排除上传
- 所有 Excel 文件保持 GBK/UTF-8 混合编码，程序内部统一处理

## License

MIT
