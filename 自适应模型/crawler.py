#!/usr/bin/env python3
"""
基金净值数据获取模块（重构版 - 支持多数据源）
架构：抽象接口 + 具体实现 + 智能降级
"""
import logging
import time
import random
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Any
import pandas as pd

from config_manager import get_config
from database import FundDatabase
from utils import format_fund_code, parse_date, format_date

logger = logging.getLogger(__name__)


# ==================== 抽象数据源接口 ====================
class FundDataSource(ABC):
    """基金数据源抽象基类"""
    
    @abstractmethod
    def get_fund_info(self, fund_code: str) -> Optional[Dict]:
        """获取基金基本信息"""
        pass
    
    @abstractmethod
    def get_latest_nav(self, fund_code: str) -> Optional[Dict]:
        """获取最新净值"""
        pass
    
    @abstractmethod
    def get_nav_history(self, fund_code: str, start_date: str = None, 
                       end_date: str = None) -> Optional[pd.DataFrame]:
        """获取历史净值数据"""
        pass
    
    @abstractmethod
    def get_source_name(self) -> str:
        """返回数据源名称"""
        pass
    
    def test_connection(self) -> bool:
        """测试数据源连接是否正常"""
        try:
            # 使用一个常见基金代码测试
            test_result = self.get_latest_nav("000001")
            return test_result is not None
        except Exception as e:
            logger.debug(f"数据源 {self.get_source_name()} 连接测试失败: {e}")
            return False


# ==================== akshare 数据源实现 ====================
class AkshareDataSource(FundDataSource):
    """akshare 数据源实现"""
    
    def __init__(self):
        self.source_name = "akshare"
        self._check_dependency()
    
    def _check_dependency(self):
        """检查 akshare 依赖"""
        try:
            import akshare as ak
            self.ak = ak
            logger.debug("akshare 依赖检查通过")
        except ImportError:
            logger.warning("未安装 akshare，请运行: pip install akshare --upgrade")
            self.ak = None
    
    def get_source_name(self) -> str:
        return self.source_name
    
    def get_fund_info(self, fund_code: str) -> Optional[Dict]:
        """获取基金基本信息"""
        if self.ak is None:
            return None
        
        formatted_code = format_fund_code(fund_code)
        try:
            # 使用 akshare 获取基金基本信息
            fund_info = self.ak.fund_open_fund_info_em(
                symbol=formatted_code,
                indicator="基金概况"
            )
            if fund_info is not None and not fund_info.empty:
                return {
                    'fund_code': formatted_code,
                    'fund_name': fund_info.get('基金简称', [''])[0],
                    'fund_type': fund_info.get('基金类型', [''])[0],
                    'establishment_date': fund_info.get('成立日期', [''])[0],
                    'fund_scale': fund_info.get('基金规模', [''])[0],
                    'source': self.source_name
                }
        except Exception as e:
            logger.error(f"akshare 获取基金信息失败 {formatted_code}: {e}")
        return None
    
    def get_latest_nav(self, fund_code: str) -> Optional[Dict]:
        """获取最新净值"""
        if self.ak is None:
            return None
        
        formatted_code = format_fund_code(fund_code)
        try:
            # 获取最近10天的数据，取最新一条
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
            
            df = self.get_nav_history(formatted_code, start_date, end_date)
            if df is not None and not df.empty:
                latest = df.iloc[-1]
                return {
                    'fund_code': formatted_code,
                    'date': latest['nav_date'],
                    'nav': float(latest['nav_value']),
                    'change_rate': float(latest.get('change_rate', 0)),
                    'source': self.source_name
                }
        except Exception as e:
            logger.error(f"akshare 获取最新净值失败 {formatted_code}: {e}")
        return None
    
    def get_nav_history(self, fund_code: str, start_date: str = None, 
                       end_date: str = None) -> Optional[pd.DataFrame]:
        """获取历史净值数据"""
        if self.ak is None:
            return None
        
        formatted_code = format_fund_code(fund_code)
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        try:
            # 调用 akshare 接口
            df = self.ak.fund_open_fund_info_em(
                symbol=formatted_code,
                start_date=start_date.replace("-", ""),
                end_date=end_date.replace("-", ""),
                indicator="单位净值走势"
            )
            
            if df is not None and not df.empty:
                # 标准化数据格式
                column_mapping = {
                    '净值日期': 'nav_date',
                    '单位净值': 'nav_value',
                    '日增长率': 'change_rate',
                    '累计净值': 'accumulated_nav'
                }
                
                # 重命名列
                for old_col, new_col in column_mapping.items():
                    if old_col in df.columns:
                        df.rename(columns={old_col: new_col}, inplace=True)
                
                # 确保必需列存在
                if 'nav_date' in df.columns and 'nav_value' in df.columns:
                    # 转换日期格式
                    df['nav_date'] = pd.to_datetime(df['nav_date']).dt.strftime('%Y-%m-%d')
                    df = df.sort_values('nav_date')
                    
                    # 转换数据类型
                    df['nav_value'] = pd.to_numeric(df['nav_value'], errors='coerce')
                    
                    if 'change_rate' in df.columns:
                        # 处理百分比字符串如 '0.12%'
                        df['change_rate'] = (
                            df['change_rate']
                            .astype(str)
                            .str.replace('%', '')
                            .astype(float) / 100.0
                        )
                    
                    # 清理数据
                    df = df.dropna(subset=['nav_date', 'nav_value'])
                    df = df.reset_index(drop=True)
                    
                    logger.debug(f"akshare 获取 {len(df)} 条历史净值: {formatted_code}")
                    return df
        except Exception as e:
            logger.error(f"akshare 获取历史净值失败 {formatted_code}: {e}")
        
        return None


# ==================== xalpha 数据源实现 ====================
class XalphaDataSource(FundDataSource):
    """xalpha 数据源实现"""
    
    def __init__(self):
        self.source_name = "xalpha"
        self._check_dependency()
    
    def _check_dependency(self):
        """检查 xalpha 依赖"""
        try:
            import xalpha as xa
            self.xa = xa
            logger.debug("xalpha 依赖检查通过")
        except ImportError:
            logger.warning("未安装 xalpha，请运行: pip install xalpha")
            self.xa = None
    
    def get_source_name(self) -> str:
        return self.source_name
    
    def get_fund_info(self, fund_code: str) -> Optional[Dict]:
        """获取基金基本信息"""
        if self.xa is None:
            return None
        
        formatted_code = format_fund_code(fund_code)
        try:
            # 使用 xalpha 获取基金信息
            fund = self.xa.fundinfo(formatted_code)
            if fund:
                return {
                    'fund_code': formatted_code,
                    'fund_name': getattr(fund, 'name', ''),
                    'source': self.source_name
                }
        except Exception as e:
            logger.error(f"xalpha 获取基金信息失败 {formatted_code}: {e}")
        return None
    
    def get_latest_nav(self, fund_code: str) -> Optional[Dict]:
        """获取最新净值"""
        if self.xa is None:
            return None
        
        formatted_code = format_fund_code(fund_code)
        try:
            # 方法1: 使用 fundinfo 获取最新净值
            fund = self.xa.fundinfo(formatted_code)
            if fund and hasattr(fund, 'price') and not fund.price.empty:
                latest = fund.price.iloc[-1]
                return {
                    'fund_code': formatted_code,
                    'date': latest.name.strftime('%Y-%m-%d') if hasattr(latest.name, 'strftime') else str(latest.name),
                    'nav': float(latest.get('netvalue', 0)),
                    'source': self.source_name
                }
            
            # 方法2: 使用 get_daily 获取
            daily_data = self.xa.get_daily(formatted_code)
            if daily_data is not None and not daily_data.empty:
                latest = daily_data.iloc[-1]
                return {
                    'fund_code': formatted_code,
                    'date': latest.name.strftime('%Y-%m-%d') if hasattr(latest.name, 'strftime') else str(latest.name),
                    'nav': float(latest.get('close', 0)),
                    'source': self.source_name
                }
        except Exception as e:
            logger.error(f"xalpha 获取最新净值失败 {formatted_code}: {e}")
        return None
    
    def get_nav_history(self, fund_code: str, start_date: str = None, 
                       end_date: str = None) -> Optional[pd.DataFrame]:
        """获取历史净值数据"""
        if self.xa is None:
            return None
        
        formatted_code = format_fund_code(fund_code)
        try:
            # 使用 xalpha 获取历史数据
            fund = self.xa.fundinfo(formatted_code)
            if fund and hasattr(fund, 'price') and not fund.price.empty:
                df = fund.price.copy()
                
                # 标准化列名
                if 'netvalue' in df.columns:
                    df.rename(columns={'netvalue': 'nav_value'}, inplace=True)
                elif 'close' in df.columns:
                    df.rename(columns={'close': 'nav_value'}, inplace=True)
                
                # 确保有日期列
                if df.index.name is None or df.index.name == '':
                    df.reset_index(inplace=True)
                    if 'date' in df.columns:
                        df.rename(columns={'date': 'nav_date'}, inplace=True)
                else:
                    df['nav_date'] = df.index
                    df.reset_index(drop=True, inplace=True)
                
                # 过滤日期范围
                if start_date:
                    df = df[df['nav_date'] >= start_date]
                if end_date:
                    df = df[df['nav_date'] <= end_date]
                
                # 确保日期格式
                df['nav_date'] = pd.to_datetime(df['nav_date']).dt.strftime('%Y-%m-%d')
                df = df.sort_values('nav_date')
                
                # 转换数据类型
                df['nav_value'] = pd.to_numeric(df['nav_value'], errors='coerce')
                df = df.dropna(subset=['nav_date', 'nav_value'])
                
                logger.debug(f"xalpha 获取 {len(df)} 条历史净值: {formatted_code}")
                return df[['nav_date', 'nav_value']]
        except Exception as e:
            logger.error(f"xalpha 获取历史净值失败 {formatted_code}: {e}")
        
        return None


# ==================== 直接API数据源（备用） ====================
class DirectAPIDataSource(FundDataSource):
    """直接API数据源（天天基金/蛋卷网）"""
    
    def __init__(self, api_type='eastmoney'):
        self.api_type = api_type  # 'eastmoney' 或 'danjuan'
        self.source_name = f"direct_api_{api_type}"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://fundf10.eastmoney.com/'
        }
    
    def get_source_name(self) -> str:
        return self.source_name
    
    def get_fund_info(self, fund_code: str) -> Optional[Dict]:
        # 简化实现，可根据需要完善
        return None
    
    def get_latest_nav(self, fund_code: str) -> Optional[Dict]:
        """获取最新净值（直接调用API）"""
        import requests
        import json
        import re
        
        formatted_code = format_fund_code(fund_code)
        
        try:
            if self.api_type == 'eastmoney':
                # 天天基金API
                url = f"http://fundgz.1234567.com.cn/js/{formatted_code}.js"
                resp = requests.get(url, headers=self.headers, timeout=10)
                if resp.status_code == 200:
                    match = re.search(r'jsonpgz\((.*)\)', resp.text)
                    if match:
                        data = json.loads(match.group(1))
                        return {
                            'fund_code': formatted_code,
                            'date': data.get('jzrq'),
                            'nav': float(data.get('dwjz', 0)),
                            'change_rate': float(data.get('gszzl', 0)) / 100.0,
                            'source': self.source_name
                        }
            
            elif self.api_type == 'danjuan' and hasattr(self, 'danjuan_api_base'):
                # 蛋卷网API（示例）
                url = f"{self.danjuan_api_base}{formatted_code}"
                resp = requests.get(url, headers=self.headers, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get('success'):
                        nav_data = data.get('data', {})
                        return {
                            'fund_code': formatted_code,
                            'date': nav_data.get('date'),
                            'nav': float(nav_data.get('nav', 0)),
                            'source': self.source_name
                        }
        
        except Exception as e:
            logger.debug(f"直接API {self.api_type} 请求失败 {formatted_code}: {e}")
        
        return None
    
    def get_nav_history(self, fund_code: str, start_date: str = None, 
                       end_date: str = None) -> Optional[pd.DataFrame]:
        # 历史数据获取较复杂，这里简化处理
        # 实际可参考原 crawler.py 中的分页逻辑
        return None


# ==================== 数据源管理器 ====================
class DataSourceManager:
    """数据源管理器，负责协调多个数据源"""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config = get_config(config_path).config
        self.data_sources = []
        self._init_data_sources()
    
    def _init_data_sources(self):
        """根据配置初始化数据源"""
        data_source_config = self.config.data_source
        
        # 1. 主数据源：akshare（默认启用）
        if getattr(data_source_config, 'use_akshare', True):
            akshare_source = AkshareDataSource()
            if akshare_source.test_connection():
                self.data_sources.append(akshare_source)
                logger.info("akshare 数据源初始化成功")
            else:
                logger.warning("akshare 数据源初始化失败或未安装")
        
        # 2. 备用数据源：xalpha（可选）
        if getattr(data_source_config, 'use_xalpha', False):
            xalpha_source = XalphaDataSource()
            if xalpha_source.test_connection():
                self.data_sources.append(xalpha_source)
                logger.info("xalpha 数据源初始化成功")
            else:
                logger.warning("xalpha 数据源初始化失败或未安装")
        
        # 3. 直接API备用
        if getattr(data_source_config, 'use_direct_api', True):
            # 天天基金API
            eastmoney_source = DirectAPIDataSource('eastmoney')
            self.data_sources.append(eastmoney_source)
            
            # 蛋卷网API（如果配置启用）
            if getattr(data_source_config, 'use_danjuan', False):
                danjuan_source = DirectAPIDataSource('danjuan')
                self.data_sources.append(danjuan_source)
        
        if not self.data_sources:
            logger.error("没有可用的数据源！")
    
    def get_latest_nav(self, fund_code: str) -> Optional[Dict]:
        """从多个数据源获取最新净值（自动降级）"""
        formatted_code = format_fund_code(fund_code)
        
        for i, source in enumerate(self.data_sources):
            try:
                logger.debug(f"尝试从 {source.get_source_name()} 获取最新净值: {formatted_code}")
                result = source.get_latest_nav(formatted_code)
                if result:
                    logger.info(f"成功从 {source.get_source_name()} 获取净值: {formatted_code}")
                    return result
            except Exception as e:
                logger.warning(f"数据源 {source.get_source_name()} 获取失败: {e}")
                continue
        
        logger.error(f"所有数据源均无法获取基金 {formatted_code} 的最新净值")
        return None
    
    def get_nav_history(self, fund_code: str, start_date: str = None, 
                       end_date: str = None) -> Optional[pd.DataFrame]:
        """从多个数据源获取历史净值（优先使用主数据源）"""
        formatted_code = format_fund_code(fund_code)
        
        # 优先使用第一个数据源（通常是akshare）
        if self.data_sources:
            primary_source = self.data_sources[0]
            try:
                logger.debug(f"从主数据源 {primary_source.get_source_name()} 获取历史净值: {formatted_code}")
                result = primary_source.get_nav_history(formatted_code, start_date, end_date)
                if result is not None and not result.empty:
                    return result
            except Exception as e:
                logger.warning(f"主数据源 {primary_source.get_source_name()} 获取历史数据失败: {e}")
        
        # 降级到其他数据源
        for source in self.data_sources[1:]:
            try:
                logger.debug(f"尝试从备用数据源 {source.get_source_name()} 获取历史净值: {formatted_code}")
                result = source.get_nav_history(formatted_code, start_date, end_date)
                if result is not None and not result.empty:
                    return result
            except Exception as e:
                logger.debug(f"备用数据源 {source.get_source_name()} 获取失败: {e}")
                continue
        
        logger.error(f"所有数据源均无法获取基金 {formatted_code} 的历史净值")
        return None


# ==================== 主爬虫类（兼容原有接口） ====================
class FundDataFetcher:
    """
    基金数据获取器（重构版 - 多数据源支持）
    职责：通过数据源管理器获取基金数据，并存入数据库。
    """
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config = get_config(config_path).config
        self.db = FundDatabase(self.config.output.database_path)
        self.data_source_manager = DataSourceManager(config_path)
        logger.info("基金数据获取器初始化完成（多数据源支持）")
    
    def fetch_latest_nav(self, fund_code: str) -> Optional[Dict]:
        """获取基金最新净值（统一入口）"""
        formatted_code = format_fund_code(fund_code)
        logger.info(f"开始获取基金最新净值: {formatted_code}")
        
        result = self.data_source_manager.get_latest_nav(formatted_code)
        
        if result:
            # 保存到数据库
            success = self.db.add_nav_history(
                fund_code=formatted_code,
                nav_date=result['date'],
                nav_value=result['nav'],
                change_rate=result.get('change_rate'),
                data_source=result.get('source', 'unknown')
            )
            if success:
                logger.info(f"净值保存成功: {formatted_code} -> {result['nav']} ({result['date']})")
            else:
                logger.warning(f"净值保存失败: {formatted_code}")
            
            return result
        else:
            logger.error(f"无法获取基金 {formatted_code} 的最新净值")
            return None
    
    def fetch_nav_history(self, fund_code: str, start_date: str = None,
                         end_date: str = None, force_refresh: bool = False) -> List[Dict]:
        """获取基金历史净值并存入数据库"""
        formatted_code = format_fund_code(fund_code)
        final_records = []
        
        # 检查数据库已有数据
        if not force_refresh:
            existing_history = self.db.get_nav_history(
                formatted_code, start_date=start_date, end_date=end_date
            )
            if existing_history:
                logger.info(f"从数据库加载 {len(existing_history)} 条历史净值: {formatted_code}")
                return existing_history
        
        # 从数据源获取
        logger.info(f"从网络获取历史净值数据: {formatted_code}")
        df = self.data_source_manager.get_nav_history(formatted_code, start_date, end_date)
        
        if df is not None and not df.empty:
            # 保存到数据库
            saved_count = 0
            for _, row in df.iterrows():
                success = self.db.add_nav_history(
                    fund_code=formatted_code,
                    nav_date=row['nav_date'],
                    nav_value=row['nav_value'],
                    change_rate=row.get('change_rate'),
                    data_source='multi_source'  # 标记为多数据源获取
                )
                if success:
                    saved_count += 1
                    final_records.append(row.to_dict())
            
            logger.info(f"历史净值获取完成: {formatted_code}, 新增 {saved_count} 条记录")
        else:
            logger.warning(f"未能获取到历史净值数据: {formatted_code}")
        
        return final_records
    
    def batch_update_latest_nav(self, fund_codes: List[str] = None) -> Dict[str, Any]:
        """批量更新多个基金的最新净值"""
        if fund_codes is None:
            funds = self.db.get_funds()
            fund_codes = [f['fund_code'] for f in funds]
        
        if not fund_codes:
            logger.warning("没有需要更新净值的基金")
            return {'total': 0, 'success': 0, 'failed': 0, 'failed_codes': []}
        
        logger.info(f"开始批量更新 {len(fund_codes)} 只基金的最新净值")
        
        results = {'total': len(fund_codes), 'success': 0, 'failed': 0, 'failed_codes': []}
        
        for fund_code in fund_codes:
            nav_data = self.fetch_latest_nav(fund_code)
            if nav_data:
                results['success'] += 1
                logger.debug(f"更新成功: {fund_code} -> {nav_data['nav']}")
            else:
                results['failed'] += 1
                results['failed_codes'].append(fund_code)
                logger.warning(f"更新失败: {fund_code}")
            
            # 礼貌延迟
            time.sleep(random.uniform(0.2, 0.5))
        
        logger.info(f"批量更新完成。成功: {results['success']}, 失败: {results['failed']}")
        return results


# ==================== 兼容原有接口 ====================
class EastMoneyCrawler(FundDataFetcher):
    """为兼容原有 scheduler.py 而保留的类名别名"""
    pass


# ==================== 配置更新建议 ====================
"""
在 config.yaml 中添加以下配置：

data_source:
  # 数据源优先级配置
  priority: ['akshare', 'xalpha', 'eastmoney_api', 'danjuan_api']
  
  # akshare 配置（默认启用）
  use_akshare: true
  
  # xalpha 配置（可选启用）
  use_xalpha: false  # 设置为 true 以启用 xalpha
  
  # 直接API配置
  use_direct_api: true
  eastmoney_api:
    base_url: "https://fundf10.eastmoney.com"
  
  # 蛋卷网API（备用）
  use_danjuan: false
  danjuan_api_base: "https://danjuanapp.com/djapi/fund/nav/history/"
  
  # 浏览器抓取（终极备用，不建议启用）
  browser_fallback: false
"""


def main():
    """测试函数"""
    import sys
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    fetcher = FundDataFetcher()
    
    # 测试代码
    test_code = "000051"  # 华夏沪深300ETF连接A
    if len(sys.argv) > 1:
        test_code = sys.argv[1]
    
    print(f"\n=== 测试多数据源获取基金最新净值 (代码: {test_code}) ===")
    latest = fetcher.fetch_latest_nav(test_code)
    if latest:
        print(f"最新净值: {latest}")
        print(f"数据来源: {latest.get('source', 'unknown')}")
    else:
        print("获取失败")
    
    print(f"\n=== 测试获取基金历史净值 (代码: {test_code}) ===")
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    history = fetcher.fetch_nav_history(test_code, start_date=start_date, end_date=end_date)
    print(f"获取到 {len(history)} 条历史记录")


if __name__ == "__main__":
    main()
