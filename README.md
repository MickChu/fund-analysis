# 基金持仓分析系统

自动化基金净值获取、持仓盈亏计算、定投回测、周报生成的工具集。

## 目录结构

```
基金分析/
├── V1/                    # 第一版程序（规则引擎）
│   ├── 程序/              # Python 程序
│   │   ├── nav_fetcher.py          # 净值抓取（天天基金API）
│   │   ├── portfolio_tracker.py    # 持仓跟踪与盈亏计算
│   │   ├── backtest_engine.py      # 定投回测引擎
│   │   ├── weekly_advisor.py       # 周报生成
│   │   ├── analyze_19_funds.py     # 批量基金分析
│   │   ├── import_alipay_csv.py    # 支付宝CSV导入
│   │   └── logger.py               # 日志模块
│   ├── 数据/              # 净值数据与交易记录
│   │   ├── nav_cache/              # 净值缓存
│   │   ├── 买入记录.xlsx           # 交易记录
│   │   └── 基金交易记录.xlsx
│   └── 输出/              # 分析报告
│
├── 自适应模型/            # 第二版（ML预测系统，WIP）
│   ├── crawler.py        # 数据抓取（三层策略）
│   ├── database.py       # SQLite 持久化
│   ├── calculator.py     # 盈亏计算
│   ├── trainer.py        # 模型训练（sklearn/XGBoost）
│   ├── predictor.py      # 预测生成
│   ├── validator.py      # 预测验证
│   └── scheduler.py      # 命令行调度器
│
└── AI持仓基金指令v4.txt  # AI 操作指令（当前使用）
```

## 快速开始

### V1（规则引擎）

```bash
# 进入程序目录
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

### 自适应模型（需 sklearn / xgboost）

```bash
cd 自适应模型
pip install -r requirements.txt  # 如有

# 更新净值
python scheduler.py update-nav

# 盈亏计算
python scheduler.py calculate-pnl

# 训练模型
python scheduler.py train-model

# 生成预测
python scheduler.py predict
```

## 数据文件说明

- `V1/数据/买入记录.xlsx` — 买入记录（Sheet: 基金代码 记录买卖）
- `V1/数据/基金交易记录.xlsx` — 综合交易记录（含持仓份额）
- `V1/数据/nav_cache/` — 历史净值缓存（JSON）
- `V1/输出/` — 周报、回测报告等输出文件

## 注意事项

- 净值数据来自天天基金网（东方财富），仅供个人学习研究
- `自适应模型/` 目录正在开发中，部分功能（如大盘指数预测）尚未完成
- 所有 Excel 文件保持 GBK/UTF-8 混合编码，程序内部统一处理

## License

MIT License