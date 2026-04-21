"""
预测验证模块
验证前一天预测的准确性，计算胜率、误差等指标
"""
import logging
import yaml
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os
from typing import List, Dict, Optional, Tuple
from database import get_db

logger = logging.getLogger(__name__)

class PredictionValidator:
    def __init__(self, config_path: str = "config.yaml"):
        """初始化验证器"""
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.db = get_db()
        self.validation_dir = "validation"
        os.makedirs(self.validation_dir, exist_ok=True)
        
        logger.info("预测验证器初始化完成")
    
    def validate_predictions(self, validation_date: str = None) -> Dict:
        """
        验证指定日期的预测准确性
        Args:
            validation_date: 验证日期（即预测的目标日期），默认昨天
        Returns:
            验证结果字典
        """
        if validation_date is None:
            validation_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        logger.info(f"开始验证预测准确性，目标日期: {validation_date}")
        
        # 获取该日期的所有预测
        predictions = []
        all_funds = self.db.get_funds()
        
        for fund in all_funds:
            fund_code = fund['fund_code']
            fund_predictions = self.db.get_predictions(fund_code, limit=10)
            
            for pred in fund_predictions:
                if pred['predict_date'] == validation_date:
                    predictions.append({
                        'fund_code': fund_code,
                        'predict_date': pred['predict_date'],
                        'predicted_nav': pred['predict_nav'],
                        'predicted_change': pred['predict_change'],
                        'confidence': pred['confidence'],
                        'model_version': pred['model_version']
                    })
                    break
        
        if not predictions:
            logger.warning(f"日期 {validation_date} 没有找到预测记录")
            return {'success': False, 'error': '无预测记录'}
        
        logger.info(f"找到 {len(predictions)} 个预测记录需要验证")
        
        validation_results = []
        correct_predictions = 0
        direction_correct = 0
        total_direction = 0
        
        for pred in predictions:
            fund_code = pred['fund_code']
            
            # 获取当日的实际净值
            nav_history = self.db.get_nav_history(fund_code, limit=10)
            actual_nav = None
            prev_nav = None
            
            for nav in nav_history:
                if nav['nav_date'] == validation_date:
                    actual_nav = nav
                    break
            
            # 获取前一交易日的净值（用于计算实际涨跌）
            if nav_history:
                prev_date = None
                for nav in nav_history:
                    if nav['nav_date'] < validation_date:
                        prev_nav = nav
                        prev_date = nav['nav_date']
                        break
            
            if not actual_nav:
                logger.warning(f"基金 {fund_code} 在 {validation_date} 无实际净值数据")
                continue
            
            if not prev_nav:
                logger.warning(f"基金 {fund_code} 在 {validation_date} 前无净值数据")
                continue
            
            # 计算实际涨跌
            actual_change = (actual_nav['nav_value'] - prev_nav['nav_value']) / prev_nav['nav_value']
            actual_direction = 1 if actual_change > 0 else 0
            
            # 预测涨跌方向
            predicted_direction = 1 if pred['predicted_change'] > 0 else 0
            
            # 方向是否正确
            direction_match = actual_direction == predicted_direction
            
            # 计算净值误差
            nav_error = abs(pred['predicted_nav'] - actual_nav['nav_value']) if pred['predicted_nav'] else None
            
            # 计算涨跌幅误差
            change_error = abs(pred['predicted_change'] - actual_change)
            
            result = {
                'fund_code': fund_code,
                'validation_date': validation_date,
                'actual_nav': actual_nav['nav_value'],
                'actual_change': actual_change,
                'actual_change_pct': actual_change * 100,
                'actual_direction': '上涨' if actual_direction == 1 else '下跌',
                'predicted_nav': pred['predicted_nav'],
                'predicted_change': pred['predicted_change'],
                'predicted_change_pct': pred['predicted_change'] * 100,
                'predicted_direction': '上涨' if predicted_direction == 1 else '下跌',
                'direction_correct': direction_match,
                'nav_error': nav_error,
                'change_error': change_error,
                'confidence': pred['confidence'],
                'model_version': pred['model_version'],
                'prev_nav': prev_nav['nav_value'],
                'prev_nav_date': prev_nav['nav_date']
            }
            
            validation_results.append(result)
            
            if direction_match:
                direction_correct += 1
            total_direction += 1
        
        if not validation_results:
            logger.error("无有效的验证结果")
            return {'success': False, 'error': '无有效验证数据'}
        
        # 计算总体指标
        total_predictions = len(validation_results)
        
        # 方向准确率
        direction_accuracy = direction_correct / total_direction if total_direction > 0 else 0
        
        # 净值误差统计
        nav_errors = [r['nav_error'] for r in validation_results if r['nav_error'] is not None]
        avg_nav_error = np.mean(nav_errors) if nav_errors else None
        median_nav_error = np.median(nav_errors) if nav_errors else None
        
        # 涨跌幅误差统计
        change_errors = [r['change_error'] for r in validation_results]
        avg_change_error = np.mean(change_errors)
        median_change_error = np.median(change_errors)
        
        # 置信度与准确率的关系
        confidences = [r['confidence'] for r in validation_results]
        correct_confidences = [r['confidence'] for r in validation_results if r['direction_correct']]
        wrong_confidences = [r['confidence'] for r in validation_results if not r['direction_correct']]
        
        avg_confidence = np.mean(confidences) if confidences else 0
        avg_correct_confidence = np.mean(correct_confidences) if correct_confidences else 0
        avg_wrong_confidence = np.mean(wrong_confidences) if wrong_confidences else 0
        
        # 按模型版本分组统计
        model_stats = {}
        for result in validation_results:
            model_version = result['model_version']
            if model_version not in model_stats:
                model_stats[model_version] = {
                    'total': 0,
                    'correct': 0,
                    'nav_errors': [],
                    'change_errors': []
                }
            
            stats = model_stats[model_version]
            stats['total'] += 1
            if result['direction_correct']:
                stats['correct'] += 1
            
            if result['nav_error'] is not None:
                stats['nav_errors'].append(result['nav_error'])
            stats['change_errors'].append(result['change_error'])
        
        # 计算每个模型的指标
        for model_version, stats in model_stats.items():
            stats['accuracy'] = stats['correct'] / stats['total'] if stats['total'] > 0 else 0
            stats['avg_change_error'] = np.mean(stats['change_errors']) if stats['change_errors'] else 0
            if stats['nav_errors']:
                stats['avg_nav_error'] = np.mean(stats['nav_errors'])
        
        # 生成验证报告
        validation_report = {
            'validation_date': validation_date,
            'validation_performed_at': datetime.now().isoformat(),
            'total_predictions_validated': total_predictions,
            'direction_accuracy': direction_accuracy,
            'direction_correct_count': direction_correct,
            'direction_total_count': total_direction,
            'avg_confidence': avg_confidence,
            'avg_correct_confidence': avg_correct_confidence,
            'avg_wrong_confidence': avg_wrong_confidence,
            'avg_change_error': avg_change_error,
            'median_change_error': median_change_error,
            'avg_nav_error': avg_nav_error,
            'median_nav_error': median_nav_error,
            'model_statistics': model_stats,
            'detailed_results': validation_results
        }
        
        # 保存验证报告
        report_path = os.path.join(
            self.validation_dir,
            f"validation_report_{validation_date}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(validation_report, f, indent=2, ensure_ascii=False)
        
        # 更新模型表现到数据库
        for model_version, stats in model_stats.items():
            if stats['total'] > 0:
                self.db.update_model_performance(
                    model_version=model_version,
                    eval_date=validation_date,
                    accuracy=stats['accuracy'],
                    mean_error=stats['avg_change_error'],
                    total_predictions=stats['total'],
                    correct_predictions=stats['correct']
                )
        
        logger.info(f"预测验证完成: 方向准确率 {direction_accuracy:.2%} "
                   f"({direction_correct}/{total_direction}), "
                   f"平均涨跌幅误差 {avg_change_error:.4%}")
        logger.info(f"验证报告已保存: {report_path}")
        
        return {
            'success': True,
            'report': validation_report,
            'report_path': report_path
        }
    
    def generate_validation_summary(self, days: int = 7) -> Dict:
        """
        生成最近几天的验证摘要
        Args:
            days: 回溯天数
        Returns:
            摘要报告
        """
        summary = {
            'period_start': (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d'),
            'period_end': datetime.now().strftime('%Y-%m-%d'),
            'daily_accuracy': [],
            'overall_accuracy': 0,
            'model_performance': {}
        }
        
        total_correct = 0
        total_predictions = 0
        
        # 收集每天的验证报告
        validation_files = glob.glob(os.path.join(self.validation_dir, "validation_report_*.json"))
        recent_files = []
        
        for file_path in validation_files:
            file_date = os.path.basename(file_path).split('_')[2]  # 提取日期
            if len(file_date) == 8:  # YYYYMMDD
                file_date_obj = datetime.strptime(file_date, '%Y%m%d')
                if (datetime.now() - file_date_obj).days <= days:
                    recent_files.append(file_path)
        
        for file_path in recent_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    report = json.load(f)
                
                date_str = report['validation_date']
                accuracy = report['direction_accuracy']
                correct = report['direction_correct_count']
                total = report['direction_total_count']
                
                summary['daily_accuracy'].append({
                    'date': date_str,
                    'accuracy': accuracy,
                    'correct': correct,
                    'total': total
                })
                
                total_correct += correct
                total_predictions += total
                
            except Exception as e:
                logger.error(f"读取验证报告失败 {file_path}: {e}")
        
        # 计算整体准确率
        if total_predictions > 0:
            summary['overall_accuracy'] = total_correct / total_predictions
        
        # 保存摘要报告
        summary_path = os.path.join(
            self.validation_dir,
            f"validation_summary_{datetime.now().strftime('%Y%m%d')}.json"
        )
        
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        logger.info(f"验证摘要生成完成，共 {len(summary['daily_accuracy'])} 天数据，"
                   f"整体准确率 {summary['overall_accuracy']:.2%}")
        
        return summary

def main():
    """测试函数"""
    import sys
    logging.basicConfig(level=logging.INFO)
    
    validator = PredictionValidator()
    
    if len(sys.argv) > 1:
        # 验证指定日期
        validation_date = sys.argv[1]
        result = validator.validate_predictions(validation_date)
    else:
        # 验证昨天
        result = validator.validate_predictions()
    
    if result['success']:
        report = result['report']
        
        print("=" * 80)
        print(f"预测验证报告 - {report['validation_date']}")
        print("=" * 80)
        print(f"验证预测数: {report['total_predictions_validated']}")
        print(f"方向准确率: {report['direction_accuracy']:.2%} "
              f"({report['direction_correct_count']}/{report['direction_total_count']})")
        print(f"平均涨跌幅误差: {report['avg_change_error']:.4%}")
        
        if report['avg_nav_error']:
            print(f"平均净值误差: {report['avg_nav_error']:.4f}")
        
        print(f"平均置信度: {report['avg_confidence']:.2%}")
        print(f"正确预测平均置信度: {report['avg_correct_confidence']:.2%}")
        print(f"错误预测平均置信度: {report['avg_wrong_confidence']:.2%}")
        
        # 显示模型表现
        print("\n模型表现:")
        for model_version, stats in report['model_statistics'].items():
            print(f"  {model_version}: 准确率 {stats.get('accuracy', 0):.2%} "
                  f"({stats.get('correct', 0)}/{stats.get('total', 0)}), "
                  f"平均误差 {stats.get('avg_change_error', 0):.4%}")
        
        print(f"\n详细报告: {result['report_path']}")
        
        # 生成近期摘要
        if len(sys.argv) == 1:  # 只有在验证昨天时才生成摘要
            print("\n" + "=" * 80)
            print("近期验证摘要 (最近7天)")
            print("=" * 80)
            
            summary = validator.generate_validation_summary(days=7)
            
            for daily in summary['daily_accuracy']:
                print(f"{daily['date']}: {daily['accuracy']:.2%} "
                      f"({daily['correct']}/{daily['total']})")
            
            print(f"\n整体准确率: {summary['overall_accuracy']:.2%}")
    else:
        print(f"验证失败: {result.get('error', '未知错误')}")

if __name__ == "__main__":
    main()