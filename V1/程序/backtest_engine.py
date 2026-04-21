# -*- coding: utf-8 -*-
"""
backtest_engine.py — 定投回测引擎

功能：
1. 对指定基金进行定投回测（周一至周五分别模拟）
2. 多起点采样，避免起点偏差
3. 生成收益率分布小提琴图
4. 输出最优定投日建议

用法：
  python backtest_engine.py --code 000051 --days 365          # 单基金回测
  python backtest_engine.py --all --days 730 --plot           # 全部基金+绘图
  python backtest_engine.py --code 000051 --days 365 --amount 500  # 自定义定投金额
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')

import os
import json
import argparse
import random
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict
from logger import get_logger, cleanup_old_logs

# 项目路径
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_DIR = PROJECT_DIR / "数据"
NAV_CACHE_DIR = DATA_DIR / "nav_cache"
OUTPUT_DIR = PROJECT_DIR / "输出"

# 初始化日志
log = get_logger('backtest_engine')

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

WEEKDAYS = ["周一", "周二", "周三", "周四", "周五"]


def load_nav_data(code):
    """加载基金历史净值数据"""
    cache_file = NAV_CACHE_DIR / f"{code}.json"
    if not cache_file.exists():
        return None

    with open(cache_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    return data.get("nav_data", [])


def get_weekday(date_str):
    """获取日期是周几 (0=周一, 6=周日)"""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return dt.weekday()


def backtest_weekday(nav_data, weekday, invest_amount, days, sample_count=20):
    """
    对指定定投日进行回测
    
    参数:
        nav_data: 净值数据列表
        weekday: 定投日 (0=周一, 4=周五)
        invest_amount: 每次定投金额
        days: 回测周期（天数）
        sample_count: 采样起点数量（避免起点偏差）
    
    返回:
        list of return_rates: 各起点的收益率列表
    """
    if len(nav_data) < days + sample_count:
        return []

    return_rates = []

    # 多起点采样
    max_start_idx = len(nav_data) - days
    sample_starts = random.sample(range(max_start_idx), min(sample_count, max_start_idx))

    for start_idx in sample_starts:
        end_idx = start_idx + days
        period_data = nav_data[start_idx:end_idx]

        # 筛选定投日的净值
        invest_navs = []
        for item in period_data:
            if get_weekday(item["date"]) == weekday:
                invest_navs.append(item["nav"])

        if len(invest_navs) < 2:
            continue

        # 计算定投结果
        total_cost = len(invest_navs) * invest_amount
        total_shares = sum(invest_amount / nav for nav in invest_navs)
        final_value = total_shares * period_data[-1]["nav"]
        return_rate = (final_value - total_cost) / total_cost * 100

        return_rates.append(return_rate)

    return return_rates


def backtest_fund(code, days=365, invest_amount=1000, sample_count=20):
    """
    对单只基金进行完整回测
    
    返回:
        dict: 各定投日的回测结果
    """
    nav_data = load_nav_data(code)
    if not nav_data or len(nav_data) < days + sample_count:
        return None

    results = {}
    for weekday in range(5):  # 周一到周五
        rates = backtest_weekday(nav_data, weekday, invest_amount, days, sample_count)
        if rates:
            results[WEEKDAYS[weekday]] = {
                "rates": rates,
                "mean": sum(rates) / len(rates),
                "min": min(rates),
                "max": max(rates),
                "count": len(rates),
            }

    return results


def find_best_weekday(results):
    """找出平均收益率最高的定投日"""
    if not results:
        return None
    return max(results.items(), key=lambda x: x[1]["mean"])[0]


def generate_violin_plot(all_results, days, output_path):
    """生成小提琴图"""
    import matplotlib.pyplot as plt
    import numpy as np

    fig, axes = plt.subplots(4, 4, figsize=(16, 12))
    axes = axes.flatten()

    for idx, (code, name) in enumerate(FUND_LIST.items()):
        ax = axes[idx]
        results = all_results.get(code)

        if not results:
            ax.text(0.5, 0.5, f"{code}\n数据不足", ha="center", va="center", transform=ax.transAxes)
            ax.set_title(name[:10], fontsize=9)
            ax.axis("off")
            continue

        # 准备数据
        data = [results[wd]["rates"] for wd in WEEKDAYS if wd in results]
        labels = [wd for wd in WEEKDAYS if wd in results]

        # 小提琴图
        parts = ax.violinplot(data, positions=range(len(labels)), showmeans=True, showmedians=True)

        # 颜色
        for pc in parts['bodies']:
            pc.set_facecolor('#4472C4')
            pc.set_alpha(0.7)

        ax.set_xticks(range(len(labels)))
        ax.set_xticklabels(labels, fontsize=8)
        ax.set_title(f"{name[:12]}\n({code})", fontsize=9)
        ax.axhline(y=0, color='r', linestyle='--', alpha=0.5)
        ax.grid(axis='y', alpha=0.3)

        # 标注最优日
        best = find_best_weekday(results)
        if best and best in labels:
            best_idx = labels.index(best)
            ax.axvline(x=best_idx, color='g', linestyle=':', alpha=0.7)

    # 隐藏多余的子图
    for idx in range(len(FUND_LIST), len(axes)):
        axes[idx].axis("off")

    plt.suptitle(f"定投收益率分布 ({days}天回测)", fontsize=14, y=1.02)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"小提琴图已保存: {output_path}")


def output_json(all_results, days, output_path):
    """输出JSON结果"""
    summary = {}
    for code, results in all_results.items():
        if not results:
            continue
        best = find_best_weekday(results)
        summary[code] = {
            "name": FUND_LIST.get(code, ""),
            "best_weekday": best,
            "weekdays": {
                wd: {
                    "mean": round(data["mean"], 2),
                    "min": round(data["min"], 2),
                    "max": round(data["max"], 2),
                }
                for wd, data in results.items()
            },
        }

    data = {
        "backtest_days": days,
        "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "results": summary,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"JSON结果已保存: {output_path}")


def output_markdown(all_results, days, output_path):
    """输出Markdown报告"""
    lines = []
    lines.append("# 定投回测报告")
    lines.append(f"\n回测周期: {days}天")
    lines.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("\n---\n")

    lines.append("## 最优定投日汇总")
    lines.append("\n| 基金代码 | 基金名称 | 最优定投日 | 平均收益率 |")
    lines.append("|---------|---------|-----------|-----------|")

    for code in sorted(all_results.keys()):
        results = all_results[code]
        name = FUND_LIST.get(code, "")
        if not results:
            lines.append(f"| {code} | {name} | 数据不足 | - |")
            continue

        best = find_best_weekday(results)
        best_mean = results[best]["mean"] if best else 0
        lines.append(f"| {code} | {name} | {best} | {best_mean:+.2f}% |")

    lines.append("\n---\n")
    lines.append("## 详细数据")

    for code in sorted(all_results.keys()):
        results = all_results[code]
        name = FUND_LIST.get(code, "")
        lines.append(f"\n### {code} {name}")

        if not results:
            lines.append("\n数据不足，无法回测")
            continue

        lines.append("\n| 定投日 | 平均收益率 | 最小值 | 最大值 | 样本数 |")
        lines.append("|--------|-----------|--------|--------|--------|")

        for wd in WEEKDAYS:
            if wd in results:
                r = results[wd]
                marker = " ★" if wd == find_best_weekday(results) else ""
                lines.append(f"| {wd}{marker} | {r['mean']:+.2f}% | {r['min']:+.2f}% | {r['max']:+.2f}% | {r['count']} |")

    lines.append("\n---\n")
    lines.append("*注：★ 标记为平均收益率最高的定投日")
    lines.append("*回测方法：多起点采样，避免单一起点偏差")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"Markdown报告已保存: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="定投回测引擎")
    parser.add_argument("--code", help="指定基金代码")
    parser.add_argument("--all", action="store_true", help="回测全部13只基金")
    parser.add_argument("--days", type=int, default=365, help="回测周期天数 (180/365/730)")
    parser.add_argument("--amount", type=int, default=1000, help="每次定投金额")
    parser.add_argument("--samples", type=int, default=20, help="采样起点数量")
    parser.add_argument("--plot", action="store_true", help="生成小提琴图")
    parser.add_argument("--output", choices=["json", "md"], default="md", help="输出格式")
    parser.add_argument("--cleanup", type=int, default=30, help="清理N天前的日志，默认30天")
    args = parser.parse_args()

    # 清理日志
    if args.cleanup >= 0:
        deleted = cleanup_old_logs(args.cleanup)
        if deleted:
            log.info(f"已清理 {len(deleted)} 个日志文件")

    if not args.code and not args.all:
        log.error("未指定 --code 或 --all")
        print("错误: 请指定 --code 或 --all")
        return

    codes = [args.code] if args.code else list(FUND_LIST.keys())

    print(f"回测参数: 周期={args.days}天, 定投额=¥{args.amount}, 采样={args.samples}次")
    print(f"回测基金: {len(codes)} 只\n")

    all_results = {}
    for code in codes:
        name = FUND_LIST.get(code, "")
        print(f"回测 {code} {name} ...", end=" ")

        results = backtest_fund(code, args.days, args.amount, args.samples)
        all_results[code] = results

        if results:
            best = find_best_weekday(results)
            best_mean = results[best]["mean"] if best else 0
            print(f"最优={best}, 收益={best_mean:+.2f}%")
        else:
            print("数据不足")

    # 输出
    timestamp = datetime.now().strftime("%Y%m%d")

    if args.output == "json":
        output_path = OUTPUT_DIR / f"backtest_{args.days}d_{timestamp}.json"
        output_json(all_results, args.days, output_path)
    else:
        output_path = OUTPUT_DIR / f"backtest_report_{args.days}d_{timestamp}.md"
        output_markdown(all_results, args.days, output_path)

    # 生成图表
    if args.plot:
        plot_path = OUTPUT_DIR / f"backtest_violin_{args.days}d_{timestamp}.png"
        generate_violin_plot(all_results, args.days, plot_path)

    print(f"\n回测完成!")


if __name__ == "__main__":
    main()
