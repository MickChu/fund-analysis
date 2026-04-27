# -*- coding: utf-8 -*-
"""
nav_fetcher.py — 基金历史净值获取与缓存工具

数据源：
1. 天天基金API (fund.eastmoney.com) — 主数据源，全量历史净值
2. xalpha (xa.get_daily) — 降级方案（网络不稳定时可能失败）

缓存策略：
- 首次拉取：获取基金成立以来全量净值
- 后续更新：只追加新日期的净值
- 缓存路径：数据/nav_cache/{code}.json

用法：
  python nav_fetcher.py                    # 更新全部13只基金
  python nav_fetcher.py --codes 000051     # 只更新指定基金
  python nav_fetcher.py --force            # 强制全量重新拉取
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')

import os
import json
import time
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from logger import get_logger, cleanup_old_logs

# 项目根目录 = 此脚本的上上级目录
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
NAV_CACHE_DIR = PROJECT_DIR / "数据" / "nav_cache"

# 初始化日志
log = get_logger('nav_fetcher')

# 13只基金清单
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


def fetch_via_xalpha(code, name):
    """通过 xalpha 获取历史净值，返回 list of dict"""
    try:
        import xalpha as xa
        fund = xa.fund(code)
        df = fund.get_daily()
        # xalpha 返回 DataFrame，列: date, nav, acc_nav, ...
        records = []
        for _, row in df.iterrows():
            record = {
                "date": str(row["date"])[:10] if "date" in row else None,
                "nav": float(row["nav"]) if "nav" in row else None,
                "acc_nav": float(row["acc_nav"]) if "acc_nav" in row else None,
            }
            if record["date"] and record["nav"]:
                records.append(record)
        return records
    except Exception as e:
        log.warning(f"[xalpha] {code} 获取失败: {e}")
        return None


def fetch_via_eastmoney(code, name):
    """通过天天基金API获取历史净值（主数据源），返回 list of dict
    
    天天基金API分页返回，每页最多5000条。带重试机制防限流。
    对于增量更新场景，如果缓存已有数据，只拉取最近一页即可。
    """
    import urllib.request

    records = []
    page = 1
    page_size = 200  # API实际固定返回20条/页，但传此值以防后续调整
    max_retries = 3

    try:
        while True:
            for retry in range(max_retries):
                try:
                    url = (
                        f"https://api.fund.eastmoney.com/f10/lsjz"
                        f"?fundCode={code}&pageIndex={page}&pageSize={page_size}"
                    )
                    req = urllib.request.Request(url, headers={"Referer": "https://fund.eastmoney.com/"})
                    with urllib.request.urlopen(req, timeout=20) as resp:
                        text = resp.read().decode("utf-8")

                    data = json.loads(text)
                    break  # 成功则跳出重试循环
                except Exception as e:
                    if retry < max_retries - 1:
                        wait = (retry + 1) * 3  # 递增等待：3s, 6s
                        log.warning(f"[重试{retry+1}/{max_retries}] 等待{wait}s...")
                        time.sleep(wait)
                    else:
                        raise e

            # 解析数据
            if data is None:
                break
            items = data.get("Data", {}).get("LSJZList") if isinstance(data, dict) else None
            if not items:
                # 没有数据，可能是最后一页
                break

            for item in items:
                record = {
                    "date": item.get("FSRQ"),
                    "nav": _safe_float(item.get("DWJZ")),
                    "acc_nav": _safe_float(item.get("LJJZ")),
                }
                if record["date"] and record["nav"]:
                    records.append(record)

            total_count = data.get("TotalCount", 0) if isinstance(data, dict) else 0
            # API实际每页返回20条（忽略pageSize参数）
            actual_page_size = len(items) if items else 20
            if page * actual_page_size >= total_count or actual_page_size == 0:
                break
            page += 1
            time.sleep(0.3)  # 页间延迟

        return records
    except Exception as e:
        log.critical(f"[天天基金API] {code} 获取失败: {e}")
        return None


def _safe_float(val):
    """安全转换浮点数"""
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def load_cache(code):
    """从缓存加载净值数据"""
    cache_file = NAV_CACHE_DIR / f"{code}.json"
    if cache_file.exists():
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def save_cache(code, name, records):
    """保存净值数据到缓存"""
    NAV_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = NAV_CACHE_DIR / f"{code}.json"

    data = {
        "code": code,
        "name": name,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(records),
        "nav_data": records,
    }
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def update_fund(code, name, force=False):
    """更新单只基金净值缓存，返回新增条数"""
    print(f"  {code} {name} ...", end=" ")

    # 读取缓存
    cached = load_cache(code) if not force else None
    cached_dates = set()
    if cached and "nav_data" in cached:
        cached_dates = {r["date"] for r in cached["nav_data"]}

    # 获取数据：优先天天基金API，降级xalpha
    records = fetch_via_eastmoney(code, name)
    if records is None:
        records = fetch_via_xalpha(code, name)
    if records is None:
        print("全部数据源失败")
        return 0

    # 计算新增
    new_records = [r for r in records if r["date"] not in cached_dates]

    if cached and not force and new_records:
        # 增量合并
        all_records = cached["nav_data"] + new_records
        # 按日期排序
        all_records.sort(key=lambda x: x["date"])
    elif cached and not force:
        all_records = cached["nav_data"]
        print(f"已是最新({len(all_records)}条)")
        return 0
    else:
        all_records = records
        all_records.sort(key=lambda x: x["date"])

    save_cache(code, name, all_records)
    print(f"完成, 新增{len(new_records)}条, 共{len(all_records)}条")
    return len(new_records)


def get_nav_range(code, start_date, end_date=None):
    """从缓存中获取指定日期范围的净值数据，返回 list of dict"""
    cached = load_cache(code)
    if not cached or "nav_data" not in cached:
        return []

    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    return [
        r for r in cached["nav_data"]
        if start_date <= r["date"] <= end_date
    ]


def main():
    parser = argparse.ArgumentParser(description="基金历史净值获取与缓存工具")
    parser.add_argument("--codes", nargs="+", help="指定基金代码，默认全部13只")
    parser.add_argument("--force", action="store_true", help="强制全量重新拉取")
    parser.add_argument("--cleanup", type=int, default=30, help="清理N天前的日志，默认30天")
    args = parser.parse_args()

    # 清理日志
    if args.cleanup >= 0:
        deleted = cleanup_old_logs(args.cleanup)
        if deleted:
            log.info(f"已清理 {len(deleted)} 个日志文件")

    codes = args.codes if args.codes else list(FUND_LIST.keys())
    force = args.force

    log.info(f"基金净值获取工具启动 | {'强制全量' if force else '增量更新'} | 共{len(codes)}只基金")
    print("=" * 50)
    print("基金历史净值获取工具")
    print(f"{'强制全量拉取' if force else '增量更新'} | 共{len(codes)}只基金")
    print("=" * 50)

    total_new = 0
    for i, code in enumerate(codes, 1):
        name = FUND_LIST.get(code, "未知")
        new = update_fund(code, name, force=force)
        total_new += new
        if i < len(codes):
            time.sleep(2)  # 基金间间隔2秒，防限流

    log.info(f"完成！新增净值记录共{total_new}条")
    print(f"\n完成！新增净值记录共{total_new}条")
    print(f"缓存目录: {NAV_CACHE_DIR}")


if __name__ == "__main__":
    main()
