"""
预测生成模块
使用训练好的模型预测基金未来涨跌
"""
import logging
import yaml
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pickle
import os
import json
from typing import List, Dict, Optional, Tuple, Any
from database import get_db
import glob

logger = logging.getLogger(__name__)

class FundPredictor:
    def __init__(self, config_path: str = "config.yaml"):
        """初始化预测器"""
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.db = get_db()
        self.models_dir = "models"
        self.predictions_dir = self.config['output']['prediction_dir']
        os.makedirs(self.predictions_dir, exist_ok=True)
        
        logger.info("基金预测器初始化完成")
    
    def load_latest_model(self, fund_code: str) -> Optional[Dict]:
        """
        加载指定基金的最新模型
        Returns:
            包含模型和元数据的字典，或 None
        """
        # 查找该基金的最新模型文件
        pattern = os.path.join(self.models_dir, f"{fund_code}_*.pkl")
        model_files = glob.glob(pattern)
        
        if not model_files:
            logger.warning(f"基金 {fund_code} 无训练好的模型")
            return None
        
        # 按时间排序，取最新的
        model_files.sort(key=os.path.getmtime, reverse=True)
        latest_model_path = model_files[0]
        
        # 查找对应的元数据文件
        base_name = os.path.splitext(os.path.basename(latest_model_path))[0]
        metadata_path = os.path.join(self.models_dir, f"{base_name}_metadata.json")
        
        try:
            # 加载模型
            with open(latest_model_path, 'rb') as f:
                model = pickle.load(f)
            
            # 加载元数据
            if os.path.exists(metadata_path):
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
            else:
                metadata = {
                    'fund_code': fund_code,
                    'model_path': latest_model_path,
                    'loaded_at': datetime.now().isoformat()
                }
            
            return {
                'model': model,
                'metadata': metadata,
                'model_path': latest_model_path
            }
            
        except Exception as e:
            logger.error(f"加载模型失败 {latest_model_path}: {e}")
            return None
    
    def prepare_prediction_features(self, fund_code: str, model_features: List[str]) -> Optional[pd.DataFrame]:
        """
        准备预测所需的特征数据
        Args:
            fund_code: 基金代码
            model_features: 模型训练时使用的特征列表
        Returns:
            特征 DataFrame（单行），或 None
        """
        # 获取历史净值数据
        nav_records = self.db.get_nav_history(fund_code, limit=100)
        if len(nav_records) < 30:
            logger.warning(f"基金 {fund_code} 历史数据不足 ({len(nav_records)} 条)")
            return None
        
        # 转换为 DataFrame
        df = pd.DataFrame(nav_records)
        df['nav_date'] = pd.to_datetime(df['nav_date'])
        df = df.sort_values('nav_date')
        
        # 使用最近的 lookback_days 数据
        lookback_days = min(60, len(df))
        recent_df = df.tail(lookback_days).copy()
        
        # 特征工程（需与训练时一致）
        nav_series = recent_df['nav_value'].values
        
        # 构建特征字典
        features = {}
        
        # 历史净值特征
        if 'historical_nav' in self.config['model']['features']:
            # 滞后特征
            for lag in [1, 2, 3, 5, 10]:
                if lag < len(nav_series):
                    features[f'nav_lag_{lag}'] = nav_series[-lag] if lag <= len(nav_series) else 0
            
            # 移动平均
            for window in [5, 10, 20]:
                if window <= len(nav_series):
                    features[f'nav_ma_{window}'] = np.mean(nav_series[-window:])
            
            # 变化率
            if len(nav_series) >= 2:
                features['nav_change_1'] = nav_series[-1] / nav_series[-2] - 1
            if len(nav_series) >= 6:
                features['nav_change_5'] = nav_series[-1] / nav_series[-6] - 1
            if len(nav_series) >= 11:
                features['nav_change_10'] = nav_series[-1] / nav_series[-11] - 1
        
        # 大盘指数特征（这里需要实际数据）
        if 'market_index' in self.config['model']['features']:
            # TODO: 集成真实的大盘指数数据
            features['market_change'] = 0.0  # 占位
        
        # 确保特征顺序与训练时一致
        # 如果模型特征列表可用，按该顺序构建 DataFrame
        if model_features:
            # 创建 DataFrame，确保所有特征都存在
            feature_dict = {}
            for feat in model_features:
                feature_dict[feat] = features.get(feat, 0.0)
            
            feature_df = pd.DataFrame([feature_dict])
        else:
            # 使用所有生成的特征
            feature_df = pd.DataFrame([features])
        
        # 填充缺失值
        feature_df = feature_df.fillna(0)
        
        logger.debug(f"为基金 {fund_code} 准备特征: {feature_df.shape}")
        return feature_df
    
    def predict_for_fund(self, fund_code: str, predict_date: str = None) -> Optional[Dict]:
        """
        为单只基金生成预测
        Args:
            fund_code: 基金代码
            predict_date: 预测日期（默认明天）
        Returns:
            预测结果字典，或 None
        """
        if predict_date is None:
            predict_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        
        # 加载模型
        model_data = self.load_latest_model(fund_code)
        if not model_data:
            logger.warning(f"无法为基金 {fund_code} 加载模型")
            return None
        
        model = model_data['model']
        metadata = model_data['metadata']
        
        # 准备特征
        model_features = metadata.get('features', [])
        features_df = self.prepare_prediction_features(fund_code, model_features)
        
        if features_df is None:
            logger.warning(f"无法为基金 {fund_code} 准备特征数据")
            return None
        
        try:
            # 进行预测
            if hasattr(model, 'predict_proba'):
                # 分类模型
                y_pred = model.predict(features_df)[0]
                y_prob = model.predict_proba(features_df)[0]
                
                # 获取预测类别的概率
                confidence = float(max(y_prob))
                
                # 解释预测结果
                # 假设二分类：1=上涨，0=下跌
                if y_pred == 1:
                    direction = "上涨"
                    direction_code = 1
                else:
                    direction = "下跌"
                    direction_code = 0
                
                # 预测变化率（简单估计）
                # 这里可以根据历史变化率的统计信息来估计
                nav_history = self.db.get_nav_history(fund_code, limit=20)
                if nav_history:
                    recent_changes = []
                    for i in range(1, len(nav_history)):
                        if nav_history[i-1]['nav_value'] > 0:
                            change = (nav_history[i]['nav_value'] - nav_history[i-1]['nav_value']) / nav_history[i-1]['nav_value']
                            recent_changes.append(change)
                    
                    if recent_changes:
                        avg_change = np.mean(recent_changes)
                        std_change = np.std(recent_changes)
                        
                        # 根据方向调整预测变化率
                        if direction_code == 1:
                            predicted_change = avg_change + 0.5 * std_change
                        else:
                            predicted_change = avg_change - 0.5 * std_change
                    else:
                        predicted_change = 0.001 if direction_code == 1 else -0.001
                else:
                    predicted_change = 0.001 if direction_code == 1 else -0.001
                
            else:
                # 回归模型
                y_pred = model.predict(features_df)[0]
                confidence = 0.5  # 回归模型置信度需要其他方式计算
                
                # 假设 y_pred 是变化率
                predicted_change = float(y_pred)
                direction_code = 1 if predicted_change > 0 else 0
                direction = "上涨" if direction_code == 1 else "下跌"
            
            # 获取最新净值，计算预测净值
            latest_nav = self.db.get_latest_nav(fund_code)
            if latest_nav:
                latest_nav_value = latest_nav['nav_value']
                predicted_nav = latest_nav_value * (1 + predicted_change)
            else:
                latest_nav_value = None
                predicted_nav = None
            
            # 构建预测结果
            prediction = {
                'fund_code': fund_code,
                'predict_date': predict_date,
                'predicted_direction': direction,
                'direction_code': direction_code,
                'predicted_change': float(predicted_change),
                'predicted_change_pct': float(predicted_change * 100),
                'predicted_nav': float(predicted_nav) if predicted_nav else None,
                'latest_nav': float(latest_nav_value) if latest_nav_value else None,
                'latest_nav_date': latest_nav['nav_date'] if latest_nav else None,
                'confidence': float(confidence),
                'model_version': metadata.get('model_version', 'unknown'),
                'algorithm': metadata.get('algorithm', 'unknown'),
                'predicted_at': datetime.now().isoformat()
            }
            
            # 保存到数据库
            self.db.add_prediction(
                fund_code=fund_code,
                predict_date=predict_date,
                predict_nav=prediction['predicted_nav'] if prediction['predicted_nav'] else 0,
                predict_change=prediction['predicted_change'],
                confidence=prediction['confidence'],
                model_version=prediction['model_version']
            )
            
            logger.info(f"基金 {fund_code} 预测完成: {direction} ({prediction['predicted_change_pct']:.2f}%), "
                       f"置信度 {confidence:.2%}")
            
            return prediction
            
        except Exception as e:
            logger.error(f"预测基金 {fund_code} 失败: {e}")
            return None
    
    def predict_all_funds(self, predict_date: str = None) -> Dict:
        """
        预测所有持仓基金
        Args:
            predict_date: 预测日期
        Returns:
            包含所有预测结果的字典
        """
        if predict_date is None:
            predict_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        
        funds = self.db.get_funds()
        if not funds:
            logger.warning("数据库中没有基金信息，无法预测")
            return {'success': False, 'error': '无基金数据'}
        
        logger.info(f"开始为 {len(funds)} 只基金生成预测，预测日期: {predict_date}")
        
        predictions = []
        successful = 0
        
        for fund in funds:
            fund_code = fund['fund_code']
            logger.info(f"预测基金 {fund_code}...")
            
            prediction = self.predict_for_fund(fund_code, predict_date)
            if prediction:
                predictions.append(prediction)
                successful += 1
            else:
                logger.warning(f"基金 {fund_code} 预测失败")
        
        # 生成总体报告
        if predictions:
            # 统计涨跌分布
            up_count = sum(1 for p in predictions if p['direction_code'] == 1)
            down_count = successful - up_count
            
            avg_confidence = np.mean([p['confidence'] for p in predictions])
            avg_change = np.mean([p['predicted_change'] for p in predictions])
            
            summary = {
                'prediction_date': predict_date,
                'total_funds': len(funds),
                'successful_predictions': successful,
                'failed_predictions': len(funds) - successful,
                'up_funds': up_count,
                'down_funds': down_count,
                'up_ratio': up_count / successful if successful > 0 else 0,
                'avg_confidence': float(avg_confidence),
                'avg_predicted_change': float(avg_change),
                'predictions_generated_at': datetime.now().isoformat()
            }
            
            # 保存预测结果到文件
            output_data = {
                'summary': summary,
                'predictions': predictions
            }
            
            output_path = os.path.join(
                self.predictions_dir, 
                f"predictions_{predict_date}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            )
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"预测完成: 成功 {successful}/{len(funds)}, "
                       f"看涨 {up_count}, 看跌 {down_count}, "
                       f"平均置信度 {avg_confidence:.2%}")
            logger.info(f"预测结果已保存: {output_path}")
            
            return {
                'success': True,
                'summary': summary,
                'predictions': predictions,
                'output_path': output_path
            }
        else:
            logger.error("所有基金预测失败")
            return {'success': False, 'error': '所有预测失败'}

def main():
    """测试函数"""
    import sys
    logging.basicConfig(level=logging.INFO)
    
    predictor = FundPredictor()
    
    if len(sys.argv) > 1:
        # 预测指定基金
        fund_code = sys.argv[1]
        predict_date = sys.argv[2] if len(sys.argv) > 2 else None
        
        result = predictor.predict_for_fund(fund_code, predict_date)
        
        if result:
            print("=" * 80)
            print(f"基金预测结果 - {result['fund_code']}")
            print("=" * 80)
            print(f"预测日期: {result['predict_date']}")
            print(f"最新净值: {result['latest_nav']} ({result['latest_nav_date']})")
            print(f"预测方向: {result['predicted_direction']}")
            print(f"预测涨跌: {result['predicted_change_pct']:.2f}%")
            print(f"预测净值: {result['predicted_nav']:.4f}")
            print(f"置信度: {result['confidence']:.2%}")
            print(f"模型版本: {result['model_version']}")
            print(f"算法: {result['algorithm']}")
            print(f"预测时间: {result['predicted_at']}")
        else:
            print(f"基金 {fund_code} 预测失败")
    else:
        # 预测所有基金
        result = predictor.predict_all_funds()
        
        if result['success']:
            summary = result['summary']
            print("=" * 80)
            print(f"基金组合预测报告 - {summary['prediction_date']}")
            print("=" * 80)
            print(f"基金总数: {summary['total_funds']}")
            print(f"成功预测: {summary['successful_predictions']}")
            print(f"看涨基金: {summary['up_funds']} ({summary['up_ratio']:.1%})")
            print(f"看跌基金: {summary['down_funds']}")
            print(f"平均置信度: {summary['avg_confidence']:.2%}")
            print(f"平均预测涨跌: {summary['avg_predicted_change']:.2%}")
            print(f"\n详细预测结果: {result['output_path']}")
            
            # 显示前5只基金
            print("\n前5只基金预测:")
            for i, pred in enumerate(result['predictions'][:5]):
                print(f"{i+1}. {pred['fund_code']}: {pred['predicted_direction']} "
                      f"({pred['predicted_change_pct']:.2f}%), "
                      f"置信度 {pred['confidence']:.2%}")
        else:
            print(f"预测失败: {result.get('error', '未知错误')}")

if __name__ == "__main__":
    main()