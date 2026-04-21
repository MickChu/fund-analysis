#!/usr/bin/env python3
"""
统一配置管理器
负责加载、验证和提供系统配置，替代各模块独立加载config.yaml的方式
"""

import os
import yaml
import logging
from typing import Any, Dict, List, Optional, Union
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)


class ConfigValidationError(Exception):
    """配置验证错误"""
    pass


class ConfigManager:
    """
    配置管理器类
    使用单例模式确保全局配置一致性
    """
    
    _instance = None
    _config = None
    _config_path = None
    _last_modified = None
    
    def __new__(cls, config_path: str = "config.yaml"):
        """单例模式实现"""
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._config_path = config_path
            cls._instance._load_config()
        return cls._instance
    
    def _load_config(self) -> None:
        """加载并验证配置文件"""
        config_path = Path(self._config_path)
        
        # 检查配置文件是否存在
        if not config_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {config_path.absolute()}")
        
        # 记录最后修改时间（用于热重载）
        self._last_modified = config_path.stat().st_mtime
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                raw_config = yaml.safe_load(f)
            
            if raw_config is None:
                raw_config = {}
            
            # 将配置转换为对象属性便于访问
            self._config = self._dict_to_object(raw_config)
            
            # 验证配置
            self._validate_config()
            
            logger.info(f"配置文件加载成功: {config_path}")
            
        except yaml.YAMLError as e:
            raise ConfigValidationError(f"YAML解析错误: {e}")
        except Exception as e:
            raise ConfigValidationError(f"配置文件加载失败: {e}")
    
    def _dict_to_object(self, config_dict: Dict) -> 'ConfigObject':
        """将字典转换为对象，支持嵌套属性访问"""
        if not isinstance(config_dict, dict):
            return config_dict
        
        # 创建ConfigObject实例
        obj = ConfigObject()
        
        for key, value in config_dict.items():
            # 处理键名中的连字符，转换为下划线（YAML中常用连字符）
            attr_name = key.replace('-', '_')
            
            if isinstance(value, dict):
                # 递归处理嵌套字典
                setattr(obj, attr_name, self._dict_to_object(value))
            elif isinstance(value, list):
                # 处理列表，递归处理列表中的字典
                processed_list = []
                for item in value:
                    if isinstance(item, dict):
                        processed_list.append(self._dict_to_object(item))
                    else:
                        processed_list.append(item)
                setattr(obj, attr_name, processed_list)
            else:
                setattr(obj, attr_name, value)
        
        return obj
    
    def _validate_config(self) -> None:
        """验证配置的完整性和正确性"""
        if self._config is None:
            raise ConfigValidationError("配置未加载")
        
        errors = []
        
        # 1. 验证输入配置
        if not hasattr(self._config, 'input'):
            errors.append("缺少 'input' 配置节点")
        else:
            if not hasattr(self._config.input, 'excel_path'):
                errors.append("缺少 'input.excel_path' 配置")
            else:
                excel_path = Path(self._config.input.excel_path)
                if not excel_path.exists():
                    # 警告而非错误，因为可能是首次运行
                    logger.warning(f"输入Excel文件不存在: {excel_path.absolute()}")
            
            if not hasattr(self._config.input, 'columns'):
                errors.append("缺少 'input.columns' 配置")
            else:
                required_columns = ['fund_code', 'shares', 'cost_price']
                for col in required_columns:
                    if not hasattr(self._config.input.columns, col):
                        errors.append(f"缺少 'input.columns.{col}' 配置")
        
        # 2. 验证数据源配置
        if not hasattr(self._config, 'data_source'):
            errors.append("缺少 'data_source' 配置节点")
        else:
            if not hasattr(self._config.data_source, 'eastmoney_api'):
                errors.append("缺少 'data_source.eastmoney_api' 配置")
            else:
                if not hasattr(self._config.data_source.eastmoney_api, 'base_url'):
                    errors.append("缺少 'data_source.eastmoney_api.base_url' 配置")
        
        # 3. 验证输出配置
        if not hasattr(self._config, 'output'):
            errors.append("缺少 'output' 配置节点")
        else:
            required_outputs = ['database_path', 'log_path', 'report_dir']
            for output in required_outputs:
                if not hasattr(self._config.output, output):
                    errors.append(f"缺少 'output.{output}' 配置")
        
        # 4. 验证调度配置（可选，但建议存在）
        if not hasattr(self._config, 'schedule'):
            logger.warning("缺少 'schedule' 配置节点，将使用默认值")
        
        # 5. 验证日志配置
        if not hasattr(self._config, 'logging'):
            logger.warning("缺少 'logging' 配置节点，将使用默认日志级别INFO")
        elif hasattr(self._config.logging, 'level'):
            valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
            if self._config.logging.level.upper() not in valid_levels:
                errors.append(f"无效的日志级别: {self._config.logging.level}")
        
        # 如果有错误，抛出异常
        if errors:
            error_msg = "配置验证失败:\n" + "\n".join(f"  - {error}" for error in errors)
            raise ConfigValidationError(error_msg)
        
        logger.debug("配置验证通过")
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        通过点分隔的路径获取配置值
        
        Args:
            key_path: 配置路径，如 'data_source.eastmoney_api.base_url'
            default: 如果路径不存在时返回的默认值
        
        Returns:
            配置值或默认值
        """
        if self._config is None:
            self._load_config()
        
        try:
            value = self._config
            for key in key_path.split('.'):
                # 处理连字符键名
                key = key.replace('-', '_')
                if hasattr(value, key):
                    value = getattr(value, key)
                else:
                    return default
            return value
        except AttributeError:
            return default
    
    def reload_if_modified(self) -> bool:
        """
        检查配置文件是否被修改，如果是则重新加载
        
        Returns:
            bool: 是否重新加载了配置
        """
        if self._config_path is None:
            return False
        
        config_path = Path(self._config_path)
        if not config_path.exists():
            return False
        
        current_mtime = config_path.stat().st_mtime
        if current_mtime != self._last_modified:
            logger.info("检测到配置文件变更，重新加载...")
            self._load_config()
            return True
        
        return False
    
    @property
    def config(self) -> 'ConfigObject':
        """获取配置对象"""
        if self._config is None:
            self._load_config()
        return self._config
    
    def to_dict(self) -> Dict:
        """将配置转换为字典格式"""
        if self._config is None:
            return {}
        return self._object_to_dict(self._config)
    
    def _object_to_dict(self, obj: Any) -> Any:
        """将ConfigObject转换回字典"""
        if isinstance(obj, ConfigObject):
            result = {}
            for key in dir(obj):
                if not key.startswith('_') and not callable(getattr(obj, key)):
                    value = getattr(obj, key)
                    result[key] = self._object_to_dict(value)
            return result
        elif isinstance(obj, list):
            return [self._object_to_dict(item) for item in obj]
        else:
            return obj
    
    def __getattr__(self, name: str) -> Any:
        """支持直接通过属性访问配置"""
        if self._config is None:
            self._load_config()
        
        if hasattr(self._config, name):
            return getattr(self._config, name)
        raise AttributeError(f"配置中没有 '{name}' 属性")


class ConfigObject:
    """配置对象，支持点操作符访问嵌套属性"""
    
    def __repr__(self) -> str:
        attrs = []
        for key in dir(self):
            if not key.startswith('_') and not callable(getattr(self, key)):
                value = getattr(self, key)
                if isinstance(value, ConfigObject):
                    attrs.append(f"{key}=ConfigObject(...)")
                elif isinstance(value, list) and value and isinstance(value[0], ConfigObject):
                    attrs.append(f"{key}=[ConfigObject(...) x {len(value)}]")
                else:
                    attrs.append(f"{key}={repr(value)}")
        return f"ConfigObject({', '.join(attrs)})"
    
    def get(self, key: str, default: Any = None) -> Any:
        """安全获取属性"""
        return getattr(self, key, default)


# 全局配置实例
_global_config: Optional[ConfigManager] = None


def get_config(config_path: str = "config.yaml") -> ConfigManager:
    """
    获取全局配置管理器实例
    
    Args:
        config_path: 配置文件路径，默认为 "config.yaml"
    
    Returns:
        ConfigManager 实例
    """
    global _global_config
    if _global_config is None:
        try:
            _global_config = ConfigManager(config_path)
        except Exception as e:
            logger.error(f"初始化配置管理器失败: {e}")
            # 创建默认配置，避免程序完全崩溃
            _global_config = ConfigManager()
            # 尝试从环境变量获取配置路径
            env_config_path = os.environ.get('FUND_TRACKER_CONFIG')
            if env_config_path and Path(env_config_path).exists():
                try:
                    _global_config = ConfigManager(env_config_path)
                except Exception:
                    pass
    return _global_config


def init_config(config_path: str = "config.yaml") -> ConfigManager:
    """
    显式初始化配置管理器（适用于需要提前初始化的场景）
    
    Args:
        config_path: 配置文件路径
    
    Returns:
        初始化后的ConfigManager实例
    """
    global _global_config
    _global_config = ConfigManager(config_path)
    return _global_config


def test_config() -> None:
    """测试配置管理器功能"""
    import tempfile
    import json
    
    # 创建测试配置
    test_config = {
        'input': {
            'excel_path': 'data/持仓管理表格_V1.xlsx',
            'sheet_name': '持仓',
            'columns': {
                'fund_code': '基金代码',
                'fund_name': '基金名称',
                'shares': '持仓份额',
                'cost_price': '持仓成本'
            }
        },
        'data_source': {
            'eastmoney_api': {
                'base_url': 'https://fundf10.eastmoney.com',
                'history_endpoint': '/F10DataApi.aspx'
            }
        },
        'output': {
            'database_path': 'data/fund.db',
            'log_path': 'logs/fund_tracker.log',
            'report_dir': 'reports'
        }
    }
    
    # 写入临时文件
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(test_config, f)
        temp_path = f.name
    
    try:
        # 测试配置加载
        config = get_config(temp_path)
        
        # 测试属性访问
        print("测试属性访问:")
        print(f"  Excel路径: {config.input.excel_path}")
        print(f"  基金代码列: {config.input.columns.fund_code}")
        print(f"  API基础URL: {config.data_source.eastmoney_api.base_url}")
        
        # 测试get方法
        print("\n测试get方法:")
        print(f"  数据库路径: {config.get('output.database_path')}")
        print(f"  不存在的路径: {config.get('nonexistent.path', '默认值')}")
        
        # 测试转换为字典
        print("\n测试转换为字典:")
        config_dict = config.to_dict()
        print(f"  配置字典类型: {type(config_dict)}")
        print(f"  包含input节点: {'input' in config_dict}")
        
        print("\n✅ 配置管理器测试通过")
        
    finally:
        # 清理临时文件
        os.unlink(temp_path)


if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 运行测试
    test_config()
