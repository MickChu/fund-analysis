# -*- coding: utf-8 -*-
"""
weekly_advisor.py -- weekly report generator for fund analysis project

Combines:
  1. Market data (from cached fin-craw JSON)
  2. Portfolio status (from portfolio_tracker logic)
  3. Backtest results (from backtest_engine logic)

Usage:
  python weekly_advisor.py --skip-market   # skip market data
  python weekly_advisor.py --skip-backtest  # skip backtest
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')

import os
import json
import time
import random
import argparse
from datetime import datetime
from pathlib import Path
from logger import get_logger, cleanup_old_logs

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_DIR = PROJECT_DIR / "数据"
OUTPUT_DIR = PROJECT_DIR / "输出"
NAV_CACHE = DATA_DIR / "nav_cache"
FIN_CRAW_CACHE = DATA_DIR / "fund_data_20260418.json"  # existing market cache

# 初始化日志
log = get_logger('weekly_advisor')

FUND_LIST = {
    "000051": "华夏沪深300ETF联接A",
    "001717": "工银前沿医疗股票A",
    "006479": "广发纳指100ETF联接C",
    "007760": "景顺沪港深低波指数C",
    "008702": "华夏黄金ETF联接C",
    "010339": "国投瑞银新能源混合C",
    "012323": "华宝医疗ETF联接C",
    "014162": "万家人工智能混合C",
    "020640": "广发半导体ETF联接C",
    "022460": "易方达A500ETF联接C",
    "023918": "华夏自由现金流ETF联接C",
    "024617": "大成自由现金流ETF联接C",
    "162216": "宏利中证500增强",
}

# Subscription fee rates (1-fold discount, i.e. 10% of standard rate)
# C-class funds: 0% subscription fee (but daily sales service fee deducted from NAV)
# A-class funds: discounted rate on most platforms (e.g. TianTianFund, Alipay)
FUND_FEE_RATE = {
    "000051": 0.0012,   # A-class, standard 1.2%, discount 0.12%
    "001717": 0.0015,   # A-class, standard 1.5%, discount 0.15%
    "006479": 0.0,      # C-class, no subscription fee
    "007760": 0.0,      # C-class
    "008702": 0.0,      # C-class
    "010339": 0.0,      # C-class
    "012323": 0.0,      # C-class
    "014162": 0.0,      # C-class
    "020640": 0.0,      # C-class
    "022460": 0.0,      # C-class
    "023918": 0.0,      # C-class
    "024617": 0.0,      # C-class
    "162216": 0.0,      # LOF, typically 0 on most platforms
}


def load_market_data():
    """Load cached market data from fin-craw JSON output"""
    if not FIN_CRAW_CACHE.exists():
        return None
    try:
        with open(FIN_CRAW_CACHE, "r", encoding="utf-8") as f:
            d = json.load(f)
        return d.get("data", {})
    except Exception as e:
        print(f"Warning: failed to load market cache: {e}")
        return None


def load_nav_for_date(code, date_str):
    cache_file = NAV_CACHE / f"{code}.json"
    if not cache_file.exists():
        return None
    with open(cache_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    for item in data.get("nav_data", []):
        if item["date"] == date_str:
            return item["nav"]
    return None


def load_latest_nav(code):
    cache_file = NAV_CACHE / f"{code}.json"
    if not cache_file.exists():
        return None, None
    with open(cache_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    nav_data = data.get("nav_data", [])
    if not nav_data:
        return None, None
    latest = nav_data[-1]
    return latest["nav"], latest["date"]


def load_buy_records():
    import openpyxl
    excel_path = DATA_DIR / "买入记录.xlsx"
    if not excel_path.exists():
        return []

    wb = openpyxl.load_workbook(excel_path, data_only=True)
    ws = wb["买入记录"]
    records = []

    for row in ws.iter_rows(min_row=2, values_only=True):
        if not row or not row[0] or not row[1]:
            continue
        date_val = row[0]
        code = str(row[1]).strip()
        amount = row[3]
        fee_pct = row[4]  # 申购费率(%)，用户输入如 0.12 表示 0.12%
        nav = row[5]
        remark = str(row[6]) if len(row) > 6 and row[6] else ""

        if code not in FUND_LIST:
            continue

        if isinstance(date_val, datetime):
            date_str = date_val.strftime("%Y-%m-%d")
        else:
            date_str = str(date_val)[:10]

        try:
            amount = float(amount) if amount else 0
        except:
            amount = 0
        # 申购费率(%)，留空用默认值
        try:
            if fee_pct is not None and fee_pct != "":
                fee_rate = float(fee_pct) / 100.0
            else:
                fee_rate = FUND_FEE_RATE.get(code, 0.0)
        except:
            fee_rate = FUND_FEE_RATE.get(code, 0.0)
        try:
            nav = float(nav) if nav else None
        except:
            nav = None

        if amount > 0:
            records.append({
                "date": date_str, "code": code,
                "amount": amount, "fee_rate": fee_rate, "nav": nav, "remark": remark,
            })

    records.sort(key=lambda x: x["date"])
    return records


def calculate_portfolio(records):
    from collections import defaultdict

    fund_records = defaultdict(list)
    for r in records:
        fund_records[r["code"]].append(r)

    portfolio = {}
    total_cost = 0
    total_value = 0

    for code, trades in fund_records.items():
        total_fund_cost = 0
        total_shares = 0
        for trade in trades:
            nav = trade["nav"]
            if nav is None:
                nav = load_nav_for_date(code, trade["date"])
            if nav is None:
                nav = 1.0
            # Use per-trade fee rate (from Excel), fallback to default
            fee_rate = trade.get("fee_rate", FUND_FEE_RATE.get(code, 0.0))
            net_amount = trade["amount"] / (1 + fee_rate) if fee_rate > 0 else trade["amount"]
            shares = net_amount / nav
            total_fund_cost += trade["amount"]
            total_shares += shares

        latest_nav, latest_date = load_latest_nav(code)
        if latest_nav is None:
            continue

        market_value = total_shares * latest_nav
        profit = market_value - total_fund_cost
        profit_rate = (profit / total_fund_cost * 100) if total_fund_cost > 0 else 0

        portfolio[code] = {
            "name": FUND_LIST.get(code, ""),
            "trades": len(trades),
            "cost": round(total_fund_cost, 2),
            "shares": round(total_shares, 4),
            "nav": latest_nav,
            "nav_date": latest_date,
            "value": round(market_value, 2),
            "profit": round(profit, 2),
            "profit_rate": round(profit_rate, 2),
        }
        total_cost += total_fund_cost
        total_value += market_value

    total_profit = total_value - total_cost
    total_profit_rate = (total_profit / total_cost * 100) if total_cost > 0 else 0

    return {
        "funds": portfolio,
        "total_cost": round(total_cost, 2),
        "total_value": round(total_value, 2),
        "total_profit": round(total_profit, 2),
        "total_profit_rate": round(total_profit_rate, 2),
        "fund_count": len(portfolio),
        "has_positions": len(records) > 0,
    }


def run_backtest(codes, days=365, amount=1000, samples=20):
    """Backtest on cached NAV data, returns optimal weekday per fund"""
    import random as rnd

    WEEKDAYS_CN = ["周一", "周二", "周三", "周四", "周五"]

    def get_wd(ds):
        return datetime.strptime(ds, "%Y-%m-%d").weekday()

    results = {}
    for code in codes:
        cache_file = NAV_CACHE / f"{code}.json"
        if not cache_file.exists():
            continue
        with open(cache_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        nav_data = data.get("nav_data", [])
        if len(nav_data) < days + samples:
            continue

        fund_result = {}
        best_wd = None
        best_mean = -9999

        for wd_idx, wd_name in enumerate(WEEKDAYS_CN):
            max_start = len(nav_data) - days
            starts = rnd.sample(range(max_start), min(samples, max_start))
            rates = []
            for si in starts:
                period = nav_data[si:si + days]
                invest_navs = [it["nav"] for it in period if get_wd(it["date"]) == wd_idx]
                if len(invest_navs) < 2:
                    continue
                total_cost = len(invest_navs) * amount
                shares = sum(amount / n for n in invest_navs)
                final_val = shares * period[-1]["nav"]
                rates.append((final_val - total_cost) / total_cost * 100)

            if rates:
                mean = sum(rates) / len(rates)
                fund_result[wd_name] = {"mean": round(mean, 2), "count": len(rates)}
                if mean > best_mean:
                    best_mean = mean
                    best_wd = wd_name

        if fund_result:
            results[code] = {"best": best_wd, "days": fund_result}

    return results


def build_report(market, portfolio, backtest):
    now = datetime.now()
    today_str = now.strftime("%Y-%m-%d")
    weekdays = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    today_wd = weekdays[now.weekday()]

    lines = []
    lines.append(f"# 基金周报  {today_str}（{today_wd}）")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Section 1: Market
    lines.append("## 一、市场概况")
    lines.append("")
    if market:
        idx_map = {
            "sh000001": "上证指数",
            "sz399001": "深证成指",
            "sz399006": "创业板指",
            "sh000688": "科创50",
            "sh000300": "沪深300",
        }
        idx_data = market.get("index_spot", {})
        if isinstance(idx_data, dict) and "indices" in idx_data:
            lines.append("| 指数 | 收盘 | 涨跌幅 |")
            lines.append("|------|------|--------|")
            for item in idx_data["indices"]:
                name = idx_map.get(item["code"], item.get("name", item["code"]))
                price = item.get("price", "—")
                chg = item.get("change_pct", "—")
                try:
                    chg_str = f"{float(chg):+.2f}%"
                except:
                    chg_str = str(chg)
                lines.append(f"| {name} | {price} | {chg_str} |")
            lines.append("")

        # Sectors
        board = market.get("industry_board", {})
        top10 = board.get("top10", [])[:5]
        if top10:
            names = [f"{i['name']}({i['change_pct']:+.2f}%)" for i in top10]
            lines.append(f"**强势板块**：{'  /  '.join(names)}")
        bottom10 = board.get("bottom10", [])
        if bottom10:
            names = [f"{i['name']}({i['change_pct']:+.2f}%)" for i in bottom10[:5]]
            lines.append(f"**弱势板块**：{'  /  '.join(names)}")
        lines.append("")

    lines.append(f"*数据日期：{today_str}（来自缓存）*")
    lines.append("")

    # Section 2: Portfolio
    lines.append("## 二、持仓状态")
    lines.append("")
    if portfolio["has_positions"]:
        pv = portfolio["total_value"]
        lines.append(f"**汇总**：{portfolio['fund_count']} 只基金，"
                     f"总成本 ¥{portfolio['total_cost']:,.2f}，"
                     f"总市值 ¥{pv:,.2f}，"
                     f"总收益 ¥{portfolio['total_profit']:,.2f}"
                     f"（{portfolio['total_profit_rate']:+.2f}%）")
        lines.append("")
        lines.append("| 基金 | 成本 | 市值 | 收益 | 收益率 |")
        lines.append("|------|------|------|------|--------|")
        for code, f in sorted(portfolio["funds"].items()):
            lines.append(
                f"| {f['name']} | ¥{f['cost']:,.0f} | ¥{f['value']:,.0f} | "
                f"¥{f['profit']:,.0f} | {f['profit_rate']:+.2f}% |"
            )
        lines.append("")
        lines.append(f"*净值日期：{list(portfolio['funds'].values())[0]['nav_date']}*")
        # Note about fee handling
        a_class = [c for c in portfolio["funds"] if FUND_FEE_RATE.get(c, 0) > 0]
        if a_class:
            fee_info = ", ".join(f"{FUND_LIST[c]}({FUND_FEE_RATE[c]*100:.2f}%)" for c in a_class)
            lines.append(f"*A类基金已自动扣除申购费（1折优惠）：{fee_info}*")
        lines.append(f"*C类基金申购费为0（销售服务费已包含在净值中）*")
    else:
        lines.append("*暂无持仓记录，请先在「买入记录.xlsx」中填入数据*")
    lines.append("")

    # Section 3: Backtest
    lines.append("## 三、最优定投日（365天历史回测）")
    lines.append("")
    if backtest:
        lines.append("| 基金 | 最优日 | 平均收益 | 评价 |")
        lines.append("|------|--------|---------|------|")
        for code, res in sorted(backtest.items()):
            name = FUND_LIST.get(code, code)
            best = res["best"]
            d = res["days"].get(best, {})
            mean = d.get("mean", 0)
            if mean > 15:
                tag = "强支撑"
            elif mean > 5:
                tag = "正收益"
            elif mean > -5:
                tag = "震荡"
            else:
                tag = "偏弱"
            lines.append(f"| {name} | {best} | {mean:+.2f}% | {tag} |")
        lines.append("")
    else:
        lines.append("*暂无回测数据*")
    lines.append("")

    # Section 4: Suggestions
    lines.append("## 四、下周操作建议")
    lines.append("")
    suggestions = []
    if portfolio["has_positions"]:
        for code, f in portfolio["funds"].items():
            bt = (backtest.get(code) or {}).get("best", "") if backtest else ""
            pr = f["profit_rate"]

            if pr > 20:
                suggestions.append({
                    "name": f["name"],
                    "action": "可考虑部分止盈",
                    "reason": f"收益率 {pr:+.1f}%，注意锁定收益",
                    "level": "建议",
                })
            elif pr > 10:
                suggestions.append({
                    "name": f["name"],
                    "action": "持有观察",
                    "reason": f"收益率 {pr:+.1f}%，趋势良好",
                    "level": "参考",
                })
            elif pr < -10:
                reason = f"浮亏 {pr:+.1f}%，逢低积累筹码"
                if bt:
                    reason += f"，最优定投日为 {bt}"
                suggestions.append({
                    "name": f["name"],
                    "action": "可小幅加仓（定投优先）",
                    "reason": reason,
                    "level": "重要",
                })
            else:
                suggestions.append({
                    "name": f["name"],
                    "action": f"坚持定投" + (f"，最优日在 {bt}" if bt else ""),
                    "reason": "保持纪律性定投",
                    "level": "参考",
                })
    else:
        suggestions.append({
            "name": "整体",
            "action": "分批建仓",
            "reason": "市场中期趋势向上，可考虑分批布局",
            "level": "重要",
        })

    level_order = {"重要": 0, "建议": 1, "参考": 2}
    suggestions.sort(key=lambda x: level_order.get(x["level"], 3))
    for s in suggestions:
        tag = {"重要": "[重要]", "建议": "[建议]", "参考": "[参考]"}[s["level"]]
        lines.append(f"- **{tag} {s['name']}**：{s['action']}，{s['reason']}")
    lines.append("")

    # Section 5: Risk
    lines.append("## 五、风险提示")
    lines.append("")
    lines.append("- 回测结果基于历史数据，不代表未来收益")
    lines.append("- 定投策略需长期坚持，短期内波动属正常现象")
    lines.append("- 市场有风险，投资需谨慎，本报告仅供参考")
    lines.append("")
    lines.append("---")
    lines.append(f"*报告生成：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="基金周报生成器")
    parser.add_argument("--skip-market", action="store_true")
    parser.add_argument("--skip-backtest", action="store_true")
    parser.add_argument("--days", type=int, default=365)
    parser.add_argument("--cleanup", type=int, default=30, help="清理N天前的日志，默认30天")
    args = parser.parse_args()

    # 清理日志
    if args.cleanup >= 0:
        deleted = cleanup_old_logs(args.cleanup)
        if deleted:
            log.info(f"已清理 {len(deleted)} 个日志文件")

    log.info(f"基金周报生成启动 | skip_market={args.skip_market}, skip_backtest={args.skip_backtest}, days={args.days}")
    print(f"=== 基金周报 {datetime.now().strftime('%Y-%m-%d %H:%M')} ===\n")

    # 1. Market data
    if args.skip_market:
        log.info("[1/3] 跳过市场数据")
        print("[1/3] 跳过市场数据")
        market = None
    else:
        print("[1/3] 读取市场数据...")
        market = load_market_data()
        if market:
            log.info("市场数据加载成功")
            print("  市场数据加载成功")
        else:
            log.warning("未找到市场数据缓存")
            print("  警告：未找到市场数据缓存")

    # 2. Portfolio
    print("[2/3] 计算持仓状态...")
    records = load_buy_records()
    portfolio = calculate_portfolio(records)
    if portfolio["has_positions"]:
        log.info(f"持仓 {portfolio['fund_count']} 只，成本 ¥{portfolio['total_cost']:,.2f}，市值 ¥{portfolio['total_value']:,.2f}，收益 ¥{portfolio['total_profit']:,.2f}（{portfolio['total_profit_rate']:+.2f}%）")
        print(f"  持仓 {portfolio['fund_count']} 只，"
              f"成本 ¥{portfolio['total_cost']:,.2f}，"
              f"市值 ¥{portfolio['total_value']:,.2f}，"
              f"收益 ¥{portfolio['total_profit']:,.2f}（{portfolio['total_profit_rate']:+.2f}%）")
    else:
        log.warning("暂无持仓数据")
        print("  暂无持仓数据")

    # 3. Backtest
    if args.skip_backtest:
        print("[3/3] 跳过回测")
        backtest = {}
    else:
        print(f"[3/3] 运行回测（{args.days}天）...")
        backtest = run_backtest(list(FUND_LIST.keys()), days=args.days)
        ok = sum(1 for v in backtest.values() if v.get("best"))
        print(f"  完成 {ok}/{len(FUND_LIST)} 只")

    # 4. Generate report
    print("\n生成报告...")
    report = build_report(market, portfolio, backtest)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d")
    out_path = OUTPUT_DIR / f"weekly_report_{ts}.md"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"报告已保存: {out_path}")

    # Console output
    print("\n" + "=" * 50)
    print(report)


if __name__ == "__main__":
    main()
