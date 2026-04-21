"""
预测模型训练模块
使用历史净值等特征训练基金涨跌预测模型
"""
import logging
import yaml
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pickle
import os
from typing import List, Dict, Optional, Tuple, Any
from database import get_db

logger = logging.getLogger(__name__)

class FundPredictorTrainer:
    def __init__(self, config_path: str = "config.yaml"):
        """初始化训练器"""
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        model_config = self.config['model']
        self.lookback_days = model_config['lookback_days']
        self.features = model_config['features']
        self.algorithms = model_config['algorithms']
        
        self.db = get_db()
        self.models_dir = "models"
        os.makedirs(self.models_dir, exist_ok=True)
        
        logger.info("基金预测训练器初始化完成")
    
    def prepare_training_data(self, fund_code: str, lookback_days: int = None) -> Optional[Tuple[pd.DataFrame, pd.Series]]:
        """
        准备训练数据
        Args:
            fund_code: 基金代码
            lookback_days: 回溯天数
        Returns:
            (特征 DataFrame, 目标 Series) 或 None
        """
        if lookback_days is None:
            lookback_days = self.lookback_days
        
        # 获取历史净值数据
        nav_records = self.db.get_nav_history(fund_code, limit=lookback_days * 2)  # 多取一些
        
        if len(nav_records) < 30:  # 至少需要30个样本
            logger.warning(f"基金 {fund_code} 历史数据不足 ({len(nav_records)} 条)")
            return None
        
        # 转换为 DataFrame
        df = pd.DataFrame(nav_records)
        df['nav_date'] = pd.to_datetime(df['nav_date'])
        df = df.sort_values('nav_date')
        
        # 计算目标变量：下一天的涨跌（分类）或变化率（回归）
        df['next_nav'] = df['nav_value'].shift(-1)
        df['next_change'] = df['next_nav'] / df['nav_value'] - 1
        
        # 删除最后一行（没有下一天）
        df = df.iloc[:-1]
        
        # 特征工程
        features_df = pd.DataFrame(index=df.index)
        
        # 1. 历史净值特征
        if 'historical_nav' in self.features:
            # 净值序列（归一化）
            nav_series = df['nav_value'].values
            # 滞后特征
            for lag in [1, 2, 3, 5, 10]:
                if lag < len(nav_series):
                    features_df[f'nav_lag_{lag}'] = pd.Series(nav_series).shift(lag).values
            
            # 移动平均
            for window in [5, 10, 20]:
                if window < len(nav_series):
                    features_df[f'nav_ma_{window}'] = pd.Series(nav_series).rolling(window).mean().values
            
            # 技术指标（简化版）
            if len(nav_series) >= 20:
                # 相对变化
                features_df['nav_change_1'] = df['nav_value'].pct_change(1)
                features_df['nav_change_5'] = df['nav_value'].pct_change(5)
                features_df['nav_change_10'] = df['nav_value'].pct_change(10)
        
        # 2. 大盘指数特征（这里需要实际数据，暂时用模拟）
        if 'market_index' in self.features:
            # TODO: 集成真实的大盘指数数据
            # 暂时用随机数据占位
            features_df['market_change'] = np.random.normal(0, 0.01, len(df))
        
        # 3. 宏观指标特征（同样需要实际数据）
        if 'macro_indicator' in self.features:
            # TODO: 集成宏观数据
            pass
        
        # 目标变量：涨跌方向（分类）或变化率（回归）
        # 这里使用分类：1=上涨，0=下跌
        df['target_direction'] = (df['next_change'] > 0).astype(int)
        target = df['target_direction']
        
        # 删除含有 NaN 的行
        features_df = features_df.fillna(0)
        valid_idx = ~features_df.isnull().any(axis=1) & ~target.isnull()
        
        if valid_idx.sum() < 20:
            logger.warning(f"有效训练数据不足: {valid_idx.sum()} 个样本")
            return None
        
        X = features_df[valid_idx]
        y = target[valid_idx]
        
        logger.info(f"训练数据准备完成: {fund_code}, 样本数={len(X)}, 特征数={X.shape[1]}")
        return X, y
    
    def train_model(self, X: pd.DataFrame, y: pd.Series, algorithm: str = 'linear_regression') -> Any:
        """
        训练指定算法模型
        Args:
            X: 特征
            y: 目标
            algorithm: 算法名称
        Returns:
            训练好的模型对象
        """
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import accuracy_score, mean_absolute_error
        
        # 划分训练集和验证集
        X_train, X_val, y_train, y_val = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        logger.info(f"开始训练 {algorithm} 模型, 训练集={len(X_train)}, 验证集={len(X_val)}")
        
        model = None
        
        try:
            if algorithm == 'linear_regression':
                from sklearn.linear_model import LogisticRegression
                model = LogisticRegression(random_state=42, max_iter=1000)
                model.fit(X_train, y_train)
                
            elif algorithm == 'random_forest':
                from sklearn.ensemble import RandomForestClassifier
                model = RandomForestClassifier(
                    n_estimators=100, 
                    random_state=42,
                    max_depth=10
                )
                model.fit(X_train, y_train)
                
            elif algorithm == 'xgboost':
                try:
                    import xgboost as xgb
                    model = xgb.XGBClassifier(
                        n_estimators=100,
                        max_depth=6,
                        learning_rate=0.1,
                        random_state=42,
                        use_label_encoder=False,
                        eval_metric='logloss'
                    )
                    model.fit(X_train, y_train)
                except ImportError:
                    logger.error("XGBoost 未安装，跳过")
                    return None
                
            elif algorithm == 'lstm':
                # LSTM 需要序列数据，这里简化处理
                logger.warning("LSTM 实现较复杂，暂时使用随机森林替代")
                from sklearn.ensemble import RandomForestClassifier
                model = RandomForestClassifier(n_estimators=100, random_state=42)
                model.fit(X_train, y_train)
            
            else:
                logger.error(f"未知算法: {algorithm}")
                return None
            
            # 评估模型
            y_pred = model.predict(X_val)
            accuracy = accuracy_score(y_val, y_pred)
            
            # 如果是回归问题，计算 MAE
            if hasattr(model, 'predict_proba'):
                y_prob = model.predict_proba(X_val)[:, 1]
                # 可以计算 AUC 等
            else:
                y_prob = None
            
            logger.info(f"{algorithm} 模型训练完成, 准确率={accuracy:.4f}")
            
            # 存储评估结果
            return {
                'model': model,
                'algorithm': algorithm,
                'accuracy': accuracy,
                'features': X.columns.tolist(),
                'train_size': len(X_train),
                'val_size': len(X_val)
            }
            
        except Exception as e:
            logger.error(f"训练 {algorithm} 模型失败: {e}")
            return None
    
    def train_for_fund(self, fund_code: str) -> Dict:
        """
        为单个基金训练模型，尝试所有算法
        Args:
            fund_code: 基金代码
        Returns:
            训练结果摘要
        """
        # 准备数据
        data = self.prepare_training_data(fund_code)
        if data is None:
            return {'success': False, 'error': '数据不足'}
        
        X, y = data
        
        results = []
        best_model = None
        best_accuracy = 0
        
        # 尝试所有算法
        for algo in self.algorithms:
            result = self.train_model(X, y, algo)
            if result:
                results.append(result)
                if result['accuracy'] > best_accuracy:
                    best_accuracy = result['accuracy']
                    best_model = result
        
        if not results:
            return {'success': False, 'error': '所有算法训练失败'}
        
        # 保存最佳模型
        if best_model:
            model_version = f"{fund_code}_{best_model['algorithm']}_{datetime.now().strftime('%Y%m%d')}"
            model_path = os.path.join(self.models_dir, f"{model_version}.pkl")
            
            with open(model_path, 'wb') as f:
                pickle.dump(best_model['model'], f)
            
            # 保存模型元数据
            metadata = {
                'fund_code': fund_code,
                'algorithm': best_model['algorithm'],
                'accuracy': best_model['accuracy'],
                'features': best_model['features'],
                'trained_at': datetime.now().isoformat(),
                'model_path': model_path,
                'model_version': model_version
            }
            
            metadata_path = os.path.join(self.models_dir, f"{model_version}_metadata.json")
            import json
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            logger.info(f"模型已保存: {model_path}")
            
            # 记录模型表现到数据库
            self.db.update_model_performance(
                model_version=model_version,
                eval_date=datetime.now().strftime('%Y-%m-%d'),
                accuracy=best_model['accuracy'],
                mean_error=1 - best_model['accuracy'],  # 简化
                total_predictions=len(X),
                correct_predictions=int(best_model['accuracy'] * len(X))
            )
        
        # 返回汇总结果
        summary = {
            'success': True,
            'fund_code': fund_code,
            'sample_count': len(X),
            'feature_count': X.shape[1],
            'algorithms_tried': len(results),
            'best_algorithm': best_model['algorithm'] if best_model else None,
            'best_accuracy': best_accuracy,
            'all_results': [
                {
                    'algorithm': r['algorithm'],
                    'accuracy': r['accuracy']
                } for r in results
            ],
            'model_version': model_version if best_model else None
        }
        
        return summary
    
    def train_all_funds(self):
        """训练所有持仓基金模型"""
        funds = self.db.get_funds()
        if not funds:
            logger.warning("数据库中没有基金信息，请先运行持仓导入和净值抓取")
            return
        
        logger.info(f"开始训练 {len(funds)} 只基金的预测模型")
        
        results = []
        for fund in funds:
            fund_code = fund['fund_code']
            logger.info(f"训练基金 {fund_code}...")
            
            result = self.train_for_fund(fund_code)
            results.append({
                'fund_code': fund_code,
                'success': result.get('success', False),
                'best_accuracy': result.get('best_accuracy', 0),
                'best_algorithm': result.get('best_algorithm', None)
            })
        
        # 生成训练报告
        successful = sum(1 for r in results if r['success'])
        avg_accuracy = np.mean([r['best_accuracy'] for r in results if r['success']])
        
        report = {
            'training_date': datetime.now().strftime('%Y-%m-%d'),
            'total_funds': len(funds),
            'successful_funds': successful,
            'failed_funds': len(funds) - successful,
            'average_accuracy': round(avg_accuracy, 4),
            'fund_results': results
        }
        
        # 保存报告
        report_dir = self.config['output']['prediction_dir']
        os.makedirs(report_dir, exist_ok=True)
        
        report_path = os.path.join(report_dir, f"training_report_{datetime.now().strftime('%Y%m%d')}.json")
        import json
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logger.info(f"模型训练完成，成功 {successful}/{len(funds)}，平均准确率 {avg_accuracy:.2%}")
        logger.info(f"训练报告: {report_path}")

def main():
    """测试函数"""
    import sys
    logging.basicConfig(level=logging.INFO)
    
    trainer = FundPredictorTrainer()
    
    if len(sys.argv) > 1:
        # 训练指定基金
        fund_code = sys.argv[1]
        result = trainer.train_for_fund(fund_code)
        
        if result['success']:
            print(f"基金 {fund_code} 训练结果:")
            print(f"- 样本数: {result['sample_count']}")
            print(f"- 特征数: {result['feature_count']}")
            print(f"- 最佳算法: {result['best_algorithm']}")
            print(f"- 最佳准确率: {result['best_accuracy']:.2%}")
            print(f"- 模型版本: {result['model_version']}")
            
            print("\n所有算法表现:")
            for algo_result in result['all_results']:
                print(f"  {algo_result['algorithm']}: {algo_result['accuracy']:.2%}")
        else:
            print(f"训练失败: {result.get('error', '未知错误')}")
    else:
        # 训练所有基金
        trainer.train_all_funds()

if __name__ == "__main__":
    main()