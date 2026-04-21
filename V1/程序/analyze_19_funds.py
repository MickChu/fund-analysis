# -*- coding: utf-8 -*-
"""analyze_19_funds.py - 19只基金批量分析"""
import sys, os, json, random
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(__file__))

from logger import get_logger, cleanup_old_logs

# 项目路径
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(PROJECT_DIR, "数据")
NAV_CACHE_DIR = os.path.join(DATA_DIR, "nav_cache")

# 初始化日志
log = get_logger('analyze_19_funds')

# 清理旧日志
deleted = cleanup_old_logs(30)
if deleted:
    log.info(f"已清理 {len(deleted)} 个日志文件")

funds = {
    "003956": "\u5357\u65b9\u4ea7\u4e1a\u667a\u9009\u80a1\u7968A",
    "014330": "\u56fd\u8054\u4f18\u52bf\u4ea7\u4e1a\u6df7\u5408C",
    "020106": "\u5174\u4e1a\u6210\u957f\u52a8\u529b\u6df7\u5408C",
    "018963": "\u56fd\u6cf0\u6d77\u901a\u91cf\u5316\u9009\u80a1\u6df7\u5408\u53d1\u8d77D",
    "002036": "\u5b89\u4fe1\u4f18\u52bf\u589e\u957f\u6df7\u5408C",
    "001167": "\u91d1\u9e70\u79d1\u6280\u521b\u65b0\u80a1\u7968A",
    "015309": "\u56fd\u6295\u745e\u94f6\u5883\u7115\u7075\u6d3b\u914d\u7f6e\u6df7\u5408E",
    "019894": "\u5929\u5b8f\u901a\u5229\u6df7\u5408C",
    "017750": "\u56fd\u6295\u745e\u94f6\u666f\u6c14\u9a7c\u52a8\u6df7\u5408C",
    "005763": "\u4e2d\u6b27\u7535\u5b50\u4fe1\u606f\u4ea7\u4e1a\u6c89\u6e2f\u6df1\u80a1\u7968C",
    "014842": "\u4e1c\u65b9\u963f\u5c14\u6cd5\u533b\u7597\u5065\u5eb7\u6df7\u5408\u53d1\u8d77C",
    "008481": "\u6c38\u8d62\u80a1\u606f\u4f18\u9009C",
    "014340": "\u957f\u6c5f\u667a\u80fd\u5236\u9020\u6df7\u5408\u53d1\u8d77\u5f0fC",
    "020594": "\u534e\u590d\u8f6f\u4ef6\u9f99\u5934\u6df7\u5408\u53d1\u8d77\u5f0fC",
    "014061": "\u6d66\u94f6\u5b89\u76db\u65b0\u4ea7\u4e1a\u6df7\u5408C",
    "018104": "\u6613\u65b9\u8fbe\u4e2d\u8bc1\u6e2f\u80a1\u6d88\u8d39\u4e3b\u9898ETF\u53d1\u8d77\u5f0f\u8054\u63a5C",
    "012349": "\u5929\u5b8f\u6052\u751f\u79d1\u6280ETF\u8054\u63a5C",
    "017787": "\u4e07\u5bb6\u5b8f\u89c2\u62e9\u65f6\u591a\u7b56\u7565\u6df7\u5408C",
    "000850": "\u6c47\u4e30\u664b\u4fe1\u53cc\u6838\u7b56\u7565\u6df7\u5408C",
}

def load_nav(code):
    path = os.path.join(NAV_CACHE_DIR, "{}.json".format(code))
    if not os.path.exists(path):
        return []
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
    return data.get('nav_data', [])

def weekday(date_str):
    from datetime import datetime
    return datetime.strptime(date_str, "%Y-%m-%d").weekday()

def backtest(nav_data, days=365, amount=1000, samples=20):
    if len(nav_data) < days + samples:
        return None
    rates = []
    max_start = len(nav_data) - days
    if max_start < samples:
        return None
    random.seed(42)
    starts = random.sample(range(max_start), min(samples, max_start))
    for si in starts:
        period = nav_data[si:si+days]
        invest_navs = [item["nav"] for item in period if weekday(item["date"]) < 5]
        if len(invest_navs) < 2:
            continue
        total_cost = len(invest_navs) * amount
        total_shares = sum(amount / nav for nav in invest_navs)
        final_value = total_shares * period[-1]["nav"]
        rate = (final_value - total_cost) / total_cost * 100
        rates.append(rate)
    if not rates:
        return None
    rates.sort()
    return {
        "mean": sum(rates)/len(rates),
        "min": min(rates),
        "max": max(rates),
        "median": rates[len(rates)//2],
        "p25": rates[len(rates)//4],
        "p75": rates[3*len(rates)//4],
        "count": len(rates),
        "positive_pct": sum(1 for r in rates if r > 0) / len(rates) * 100,
    }

def backtest_180(nav_data, amount=1000, samples=20):
    return backtest(nav_data, days=180, amount=amount, samples=samples)

def recent_trend(nav_data):
    if len(nav_data) < 10:
        return {}
    latest = nav_data[-1]["nav"]
    result = {"nav": latest, "date": nav_data[-1]["date"]}
    def find_ago(tdays):
        idx = len(nav_data) - 1
        cnt = 0
        while idx >= 0 and cnt < tdays:
            idx -= 1; cnt += 1
        if idx >= 0:
            return nav_data[idx]["nav"]
        return None
    for label, tdays in [("1m", 22), ("3m", 66), ("6m", 132), ("1y", 244)]:
        old = find_ago(tdays)
        if old:
            result[label] = (latest - old) / old * 100
    return result

# Print header
print("=" * 120)
print("\u65b0\u589e19\u53ea\u57fa\u91d1\u5206\u6790\u62a5\u544a  \u6570\u636e\u622a\u201c2026-04-17\u201d")
print("=" * 120)

print("\n--- \u62a5\u8868\u603b\u89c8 ---")
print("{:<6} {:<30} {:>6} {:>6} {:>6} {:>6} {:>8} {:>8} {:>6}".format(
    "\u4ee3\u7801", "\u540d\u79f0", "1\u6708", "3\u6708", "6\u6708", "1\u5e74",
    "365\u5929\u5747", "365\u5929\u6700\u4f4e", "胜\u7387"))
print("-" * 100)
for code, name in funds.items():
    nav = load_nav(code)
    t = recent_trend(nav)
    r365 = backtest(nav, 365) if len(nav) >= 385 else None
    r180 = backtest(nav, 180) if len(nav) >= 200 else None
    def fmt(v): return "{:+.1f}%".format(v) if v is not None else "N/A"
    r365_mean = r365["mean"] if r365 else None
    r365_min = r365["min"] if r365 else None
    pos_pct = r365["positive_pct"] if r365 else None
    print("{:<6} {:<30} {:>6} {:>6} {:>6} {:>6} {:>8} {:>8} {:>6}".format(
        code, name,
        fmt(t.get("1m")), fmt(t.get("3m")), fmt(t.get("6m")), fmt(t.get("1y")),
        fmt(r365_mean), fmt(r365_min),
        "{:.0f}%".format(pos_pct) if pos_pct is not None else "N/A"
    ))

# Judgment
print("\n" + "=" * 120)
print("\u8bc4\u4ef7\u7ed3\u8bba")
print("=" * 120)
print("{:<6} {:<30} {:<12} {:<30}".format("\u4ee3\u7801", "\u540d\u79f0", "\u5224\u65ad", "\u7406\u7531"))
print("-" * 110)
for code, name in funds.items():
    nav = load_nav(code)
    t = recent_trend(nav)
    r365 = backtest(nav, 365) if len(nav) >= 385 else None
    r180 = backtest(nav, 180) if len(nav) >= 200 else None

    m1 = t.get("1m") or 0
    m3 = t.get("3m") or 0
    m6 = t.get("6m") or 0
    m1y = t.get("1y") or 0
    avg365 = r365["mean"] if r365 else 0
    min365 = r365["min"] if r365 else 0
    pos365 = r365["positive_pct"] if r365 else 0

    # Categorize
    reasons = []

    if r365 is None and r180 is None:
        verdict = "\u6570\u636e\u4e0d\u8db3"
        reasons.append("\u7eaa\u5f55\u6570<{}".format(len(nav)))
    elif r365 is None:
        verdict = "\u6570\u636e\u4e0d\u8db3(365\u5929)"
    else:
        # Momentum assessment
        if m1 > 15:
            verdict = "\u26a0\ufe0f\u77ed\u671f\u8fc7\u70ed"
            reasons.append("1\u6708\u6da8{}%".format(round(m1,1)))
        elif m1 < -10:
            verdict = "\u2705\u53ef\u5efa\u4ed3"
            reasons.append("1\u6708\u8dcc{}%\uff0c\u4f4e\u5438\u673a\u4f1a".format(round(m1,1)))
        elif avg365 > 30 and pos365 >= 80:
            verdict = "\u2705\u957f\u671f\u5f3a\u52bf"
            reasons.append("365\u5929\u5e73\u5747{}/{}%".format(round(avg365,1), round(min365,1)))
        elif avg365 > 15 and pos365 >= 60:
            verdict = "\u2705\u53ef\u5b89\u6162\u5438"
            reasons.append("365\u5929\u5e73\u5747{}/{}%".format(round(avg365,1), round(min365,1)))
        elif avg365 < 0 and pos365 < 50:
            verdict = "\u274c\u957f\u671f\u8d70\u52bf"
            reasons.append("365\u5929\u5e73\u5747{}/{}%".format(round(avg365,1), round(min365,1)))
        elif m3 > 25 or m6 > 40:
            verdict = "\u26a0\ufe0f\u5df2\u5927\u5e45\u62ac\u5347"
            reasons.append("3\u6708\u6da8{}%/6\u6708\u6da8{}%".format(round(m3,1), round(m6,1)))
        elif m3 < -15:
            verdict = "\u2705\u56de\u8c03\u540e\u5efa\u4ed3"
            reasons.append("3\u6708\u8dcc{}%".format(round(m3,1)))
        else:
            verdict = "\u2139\ufe0f\u6307\u6807\u6b63\u5e38"
            reasons.append("1\u6708{}/3\u6708{}/365\u5929\u5e73\u5747{}%".format(round(m1,1), round(m3,1), round(avg365,1)))

        # Additional context
        if m1y > 50:
            reasons.append("\u8fd11\u5e74+{}%".format(round(m1y,1)))
        if min365 < -15:
            reasons.append("\u6e10\u5927\u6ca1\u7387-{}%".format(round(min365,1)))

    print("{:<6} {:<30} {:<12} {:<30}".format(code, name, verdict, " | ".join(reasons)))

print("\n>>> DONE", flush=True)
