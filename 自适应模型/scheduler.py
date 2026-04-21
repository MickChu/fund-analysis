#!/usr/bin/env python3
"""
基金追踪系统主调度脚本
提供命令行接口运行各个模块
"""

import argparse
import logging
import sys
import yaml
import time
from datetime import datetime, timedelta
import os

def setup_logging():
    """配置日志系统：输出到控制台和文件"""
    try:
        # 加载配置
        with open('config.yaml', 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # 日志级别
        log_level = config.get('logging', {}).get('level', 'INFO')
        level_map = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR
        }
        log_level_num = level_map.get(log_level.upper(), logging.INFO)
        
        # 日志文件路径
        log_path = config.get('output', {}).get('log_path', 'logs/fund_tracker.log')
        
        # 确保日志目录存在
        log_dir = os.path.dirname(log_path)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        
        # 配置根logger
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level_num)
        
        # 避免重复添加处理器
        if not root_logger.handlers:
            # 控制台处理器
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(log_level_num)
            console_format = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(console_format)
            root_logger.addHandler(console_handler)
            
            # 文件处理器
            file_handler = logging.FileHandler(log_path, encoding='utf-8', mode='a')
            file_handler.setLevel(log_level_num)
            file_format = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            file_handler.setFormatter(file_format)
            root_logger.addHandler(file_handler)
        
        return True
    except Exception as e:
        # 如果配置失败，使用基础配置
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        logging.error(f"日志配置失败: {e}")
        return False

# 初始化日志
setup_logging()
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='基金追踪系统调度器')
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # check-excel: 检查持仓表格
    parser_check = subparsers.add_parser('check-excel', help='检查持仓Excel文件结构')
    
    # update-nav: 更新净值数据
    parser_update = subparsers.add_parser('update-nav', help='更新基金净值数据')
    parser_update.add_argument('--date', help='指定日期（YYYY-MM-DD），默认今天')
    parser_update.add_argument('--use-browser', action='store_true', help='使用浏览器抓取（如果API失败）')
    parser_update.add_argument('--fund', help='只更新指定基金代码')
    
    # calculate-pnl: 计算盈亏
    parser_pnl = subparsers.add_parser('calculate-pnl', help='计算持仓盈亏')
    parser_pnl.add_argument('--date', help='指定日期（YYYY-MM-DD），默认今天')
    parser_pnl.add_argument('--output', choices=['excel', 'json', 'both'], default='excel', 
                           help='输出格式')
    
    # train-model: 训练预测模型
    parser_train = subparsers.add_parser('train-model', help='训练预测模型')
    parser_train.add_argument('--fund', help='只训练指定基金代码')
    parser_train.add_argument('--algorithm', help='指定算法（覆盖配置）')
    
    # predict: 生成预测
    parser_predict = subparsers.add_parser('predict', help='生成基金涨跌预测')
    parser_predict.add_argument('--date', help='预测目标日期（YYYY-MM-DD），默认明天')
    parser_predict.add_argument('--fund', help='只预测指定基金代码')
    
    # validate: 验证预测准确性
    parser_validate = subparsers.add_parser('validate', help='验证预测准确性')
    parser_validate.add_argument('--date', help='验证日期（YYYY-MM-DD），默认昨天')
    parser_validate.add_argument('--summary', action='store_true', help='生成近期摘要')
    parser_validate.add_argument('--days', type=int, default=7, help='摘要回溯天数')
    
    # run-daily: 运行每日完整流程
    parser_daily = subparsers.add_parser('run-daily', help='运行每日完整流程')
    parser_daily.add_argument('--date', help='指定日期，默认今天')
    
    # fetch-history: 批量抓取历史净值数据
    parser_history = subparsers.add_parser('fetch-history', help='批量抓取历史净值数据')
    parser_history.add_argument('--days', type=int, default=365, help='抓取历史天数（默认365天）')
    parser_history.add_argument('--fund', help='只抓取指定基金代码')
    parser_history.add_argument('--all', action='store_true', help='抓取所有持仓基金的历史数据')
    parser_history.add_argument('--use-browser', action='store_true', help='使用浏览器抓取（如果API失败）')
    
    # config: 显示当前配置
    parser_config = subparsers.add_parser('config', help='显示当前配置')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    try:
        # 加载配置
        with open('config.yaml', 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        if args.command == 'check-excel':
            from calculator import PnLCalculator
            calculator = PnLCalculator()
            funds = calculator.load_holdings()
            
            if funds:
                print(f"找到 {len(funds)} 只基金:")
                for fund in funds:
                    print(f"  代码: {fund['fund_code']}, 名称: {fund['fund_name']}, "
                          f"份额: {fund['shares']}, 成本: {fund['cost_price']}")
                return 0
            else:
                print("未找到基金数据或读取失败")
                return 1
        
        elif args.command == 'update-nav':
            from crawler import EastMoneyCrawler
            from database import get_db
            
            crawler = EastMoneyCrawler()
            db = get_db()
            
            # 获取持仓基金
            from calculator import PnLCalculator
            calculator = PnLCalculator()
            funds = calculator.load_holdings()
            
            if not funds:
                logger.error("无持仓数据")
                return 1
            
            # 如果指定了基金，只更新该基金
            if args.fund:
                # 对输入的基金代码进行格式化（与 calculator.py 中保持一致）
                input_fund_code = str(args.fund).strip()
                # 1. 如果长度大于6，取后6位（如 "OF000051" → "000051"）
                if len(input_fund_code) > 6:
                    input_fund_code = input_fund_code[-6:]
                # 2. 如果长度小于6，左侧补0到6位（如 "51" → "000051"）
                elif len(input_fund_code) < 6:
                    input_fund_code = input_fund_code.zfill(6)
                # 3. 长度等于6，保持不变
                
                funds = [f for f in funds if f['fund_code'] == input_fund_code]
                if not funds:
                    logger.error(f"未找到基金 {args.fund}（格式化后：{input_fund_code}）")
                    return 1
            
            logger.info(f"开始更新 {len(funds)} 只基金的净值数据")
            
            for fund in funds:
                fund_code = fund['fund_code']
                logger.info(f"更新基金 {fund_code}...")
                
                # 获取最新净值
                nav_record = crawler.fetch_latest_nav(fund_code)
                
                if nav_record:
                    db.add_nav_history(
                        fund_code=fund_code,
                        nav_date=nav_record['date'],
                        nav_value=nav_record['nav'],
                        change_rate=nav_record.get('change_rate')
                    )
                    logger.info(f"  净值 {nav_record['nav']} ({nav_record['date']})")
                else:
                    logger.warning(f"  获取净值失败")
            
            logger.info("净值更新完成")
            return 0
        
        elif args.command == 'calculate-pnl':
            from calculator import PnLCalculator
            
            calculator = PnLCalculator()
            result = calculator.calculate_daily_pnl(args.date)
            
            if result:
                print(f"日期: {result['date']}")
                print(f"基金数量: {result['total_funds']}")
                print(f"总成本: ¥{result['total_cost']:,.2f}")
                print(f"总市值: ¥{result['total_value']:,.2f}")
                print(f"总盈亏: ¥{result['total_pnl']:,.2f}")
                print(f"总收益率: {result['overall_pnl_rate']:.2f}%")
                
                # 生成报告
                if args.output in ['excel', 'both']:
                    excel_path = calculator.generate_report(result, 'excel')
                    if excel_path:
                        print(f"\nExcel报告: {excel_path}")
                
                if args.output in ['json', 'both']:
                    json_path = calculator.generate_report(result, 'json')
                    if json_path:
                        print(f"JSON报告: {json_path}")
                
                return 0
            else:
                logger.error("盈亏计算失败")
                return 1
        
        elif args.command == 'train-model':
            from trainer import FundPredictorTrainer
            
            trainer = FundPredictorTrainer()
            
            if args.fund:
                # 格式化基金代码（与 calculator.py 中保持一致）
                fund_code = str(args.fund).strip()
                # 1. 如果长度大于6，取后6位（如 "OF000051" → "000051"）
                if len(fund_code) > 6:
                    fund_code = fund_code[-6:]
                # 2. 如果长度小于6，左侧补0到6位（如 "51" → "000051"）
                elif len(fund_code) < 6:
                    fund_code = fund_code.zfill(6)
                # 3. 长度等于6，保持不变
                
                result = trainer.train_for_fund(fund_code)
                if result['success']:
                    print(f"基金 {fund_code} 训练结果:")
                    print(f"- 样本数: {result['sample_count']}")
                    print(f"- 特征数: {result['feature_count']}")
                    print(f"- 最佳算法: {result['best_algorithm']}")
                    print(f"- 最佳准确率: {result['best_accuracy']:.2%}")
                    print(f"- 模型版本: {result['model_version']}")
                    return 0
                else:
                    print(f"训练失败: {result.get('error', '未知错误')}")
                    return 1
            else:
                trainer.train_all_funds()
                return 0
        
        elif args.command == 'predict':
            from predictor import FundPredictor
            
            predictor = FundPredictor()
            
            if args.fund:
                # 格式化基金代码（与 calculator.py 中保持一致）
                fund_code = str(args.fund).strip()
                # 1. 如果长度大于6，取后6位（如 "OF000051" → "000051"）
                if len(fund_code) > 6:
                    fund_code = fund_code[-6:]
                # 2. 如果长度小于6，左侧补0到6位（如 "51" → "000051"）
                elif len(fund_code) < 6:
                    fund_code = fund_code.zfill(6)
                # 3. 长度等于6，保持不变
                
                result = predictor.predict_for_fund(fund_code, args.date)
                if result:
                    print(f"基金 {fund_code} 预测结果:")
                    print(f"  预测日期: {result['predict_date']}")
                    print(f"  预测方向: {result['predicted_direction']}")
                    print(f"  预测涨跌: {result['predicted_change_pct']:.2f}%")
                    print(f"  置信度: {result['confidence']:.2%}")
                    print(f"  模型版本: {result['model_version']}")
                    return 0
                else:
                    logger.error(f"基金 {fund_code} 预测失败")
                    return 1
            else:
                result = predictor.predict_all_funds(args.date)
                if result['success']:
                    summary = result['summary']
                    print(f"预测完成: {summary['successful_predictions']}/{summary['total_funds']}")
                    print(f"看涨基金: {summary['up_funds']} ({summary['up_ratio']:.1%})")
                    print(f"看跌基金: {summary['down_funds']}")
                    print(f"平均置信度: {summary['avg_confidence']:.2%}")
                    print(f"平均预测涨跌: {summary['avg_predicted_change']:.2%}")
                    print(f"\n详细结果: {result.get('output_path', '')}")
                    return 0
                else:
                    logger.error(f"预测失败: {result.get('error', '未知错误')}")
                    return 1
        
        elif args.command == 'validate':
            from validator import PredictionValidator
            
            validator = PredictionValidator()
            
            if args.summary:
                summary = validator.generate_validation_summary(args.days)
                print(f"验证摘要 ({args.days}天):")
                print(f"整体准确率: {summary['overall_accuracy']:.2%}")
                
                for daily in summary['daily_accuracy']:
                    print(f"  {daily['date']}: {daily['accuracy']:.2%} ({daily['correct']}/{daily['total']})")
                return 0
            else:
                result = validator.validate_predictions(args.date)
                if result['success']:
                    report = result['report']
                    print(f"验证日期: {report['validation_date']}")
                    print(f"方向准确率: {report['direction_accuracy']:.2%} "
                          f"({report['direction_correct_count']}/{report['direction_total_count']})")
                    print(f"平均涨跌幅误差: {report['avg_change_error']:.4%}")
                    print(f"详细报告: {result['report_path']}")
                    return 0
                else:
                    logger.error(f"验证失败: {result.get('error', '未知错误')}")
                    return 1
        
        elif args.command == 'run-daily':
            logger.info("开始执行每日完整流程")
            
            # 1. 更新净值
            logger.info("步骤1/4: 更新净值数据...")
            from crawler import EastMoneyCrawler
            from database import get_db
            from calculator import PnLCalculator
            
            crawler = EastMoneyCrawler()
            db = get_db()
            calculator = PnLCalculator()
            
            funds = calculator.load_holdings()
            if not funds:
                logger.error("无持仓数据")
                return 1
            
            for fund in funds:
                nav_record = crawler.fetch_latest_nav(fund['fund_code'])
                if nav_record:
                    db.add_nav_history(
                        fund_code=fund['fund_code'],
                        nav_date=nav_record['date'],
                        nav_value=nav_record['nav'],
                        change_rate=nav_record.get('change_rate')
                    )
            
            logger.info("净值更新完成")
            
            # 2. 计算盈亏
            logger.info("步骤2/4: 计算盈亏...")
            date_str = args.date or datetime.now().strftime('%Y-%m-%d')
            pnl_result = calculator.calculate_daily_pnl(date_str)
            
            if pnl_result:
                report_path = calculator.generate_report(pnl_result, 'excel')
                logger.info(f"盈亏报告: {report_path}")
            else:
                logger.error("盈亏计算失败")
            
            # 3. 训练模型（如果在训练窗口）
            logger.info("步骤3/4: 训练模型...")
            from trainer import FundPredictorTrainer
            trainer = FundPredictorTrainer()
            trainer.train_all_funds()
            
            # 4. 生成预测
            logger.info("步骤4/4: 生成预测...")
            from predictor import FundPredictor
            predictor = FundPredictor()
            predict_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
            predict_result = predictor.predict_all_funds(predict_date)
            
            if predict_result['success']:
                logger.info(f"预测完成: {predict_result['output_path']}")
            else:
                logger.error("预测失败")
            
            logger.info("每日流程执行完成")
            return 0
        
        elif args.command == 'fetch-history':
            from crawler import EastMoneyCrawler
            from database import get_db
            from calculator import PnLCalculator
            from datetime import datetime, timedelta
            
            crawler = EastMoneyCrawler()
            db = get_db()
            calculator = PnLCalculator()
            
            # 获取持仓基金
            all_funds = calculator.load_holdings()
            if not all_funds:
                logger.error("无持仓数据")
                return 1
            
            # 确定要抓取的基金列表
            if args.fund:
                # 格式化基金代码
                input_fund_code = str(args.fund).strip()
                if len(input_fund_code) > 6:
                    input_fund_code = input_fund_code[-6:]
                elif len(input_fund_code) < 6:
                    input_fund_code = input_fund_code.zfill(6)
                
                funds = [f for f in all_funds if f['fund_code'] == input_fund_code]
                if not funds:
                    logger.error(f"未找到基金 {args.fund}（格式化后：{input_fund_code}）")
                    return 1
            elif args.all:
                funds = all_funds
            else:
                logger.error("请指定 --fund <代码> 或 --all")
                return 1
            
            logger.info(f"开始抓取 {len(funds)} 只基金的历史净值数据，回溯 {args.days} 天")
            
            total_added = 0
            for fund in funds:
                fund_code = fund['fund_code']
                fund_name = fund['fund_name']
                
                # 检查已有数据量
                existing_count = db.get_nav_count(fund_code)
                logger.info(f"基金 {fund_code} {fund_name}：已有 {existing_count} 条记录")
                
                # 计算日期范围
                end_date = datetime.now().strftime('%Y-%m-%d')
                start_date = (datetime.now() - timedelta(days=args.days)).strftime('%Y-%m-%d')
                
                logger.info(f"抓取 {fund_code} 历史净值: {start_date} 至 {end_date}")
                
                try:
                    # 使用分页抓取所有历史数据
                    records = crawler.fetch_nav_history_all_pages(
                        fund_code=fund_code,
                        start_date=start_date,
                        end_date=end_date
                    )
                    
                    added_count = 0
                    for record in records:
                        # 存入数据库（自动去重）
                        success = db.add_nav_history(
                            fund_code=fund_code,
                            nav_date=record['date'],
                            nav_value=record['nav'],
                            change_rate=record.get('change_rate')
                        )
                        if success:
                            added_count += 1
                    
                    total_added += added_count
                    logger.info(f"基金 {fund_code} 新增 {added_count} 条净值记录，总计 {existing_count + added_count} 条")
                    
                except Exception as e:
                    logger.error(f"抓取基金 {fund_code} 失败: {e}")
                    continue
                
                # 延迟避免频率限制（基金之间）
                time.sleep(1)
            
            logger.info(f"历史净值抓取完成，共新增 {total_added} 条记录")
            return 0
        
        elif args.command == 'config':
            print("当前配置:")
            print(yaml.dump(config, default_flow_style=False, allow_unicode=True))
            return 0
        
    except FileNotFoundError as e:
        logger.error(f"配置文件不存在: {e}")
        return 1
    except ImportError as e:
        logger.error(f"导入模块失败: {e}")
        logger.error("请确保已安装依赖: pip install -r requirements.txt")
        return 1
    except Exception as e:
        logger.error(f"执行命令失败: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())