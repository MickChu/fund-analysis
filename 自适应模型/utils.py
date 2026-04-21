"""
基金追踪系统 - 公共工具模块
集中管理系统中各模块共用的工具函数，消除代码重复，提高可维护性
"""

import logging
import re
import time
import os
from datetime import datetime, timedelta, date
from typing import Optional, Union, List, Dict, Any, Callable
import functools
from pathlib import Path

logger = logging.getLogger(__name__)


# ============================================================================
# 基金代码处理工具
# ============================================================================

def format_fund_code(fund_code: Union[str, int, float]) -> str:
    """
    统一基金代码格式为6位字符串
    
    规则：
    1. 如果长度大于6，取后6位（如 "OF000051" → "000051"）
    2. 如果长度小于6，左侧补0到6位（如 "51" → "000051"）
    3. 长度等于6，保持不变
    
    Args:
        fund_code: 原始基金代码，可以是字符串或数字
        
    Returns:
        格式化后的6位基金代码字符串
        
    Raises:
        ValueError: 如果输入无法转换为有效的基金代码
        
    Examples:
        >>> format_fund_code("OF000051")
        '000051'
        >>> format_fund_code("51")
        '000051'
        >>> format_fund_code(51)
        '000051'
        >>> format_fund_code("000051")
        '000051'
    """
    if fund_code is None:
        raise ValueError("基金代码不能为None")
    
    # 转换为字符串并清理
    code_str = str(fund_code).strip()
    
    # 移除可能的前缀如"SH", "SZ", "OF"等（但保留数字部分）
    # 这些前缀通常以字母开头，后接数字
    match = re.search(r'(\d{6,})', code_str)
    if match:
        code_str = match.group(1)
    else:
        # 如果没有找到6位以上数字，使用原始字符串
        pass
    
    # 应用格式化规则
    if len(code_str) > 6:
        formatted_code = code_str[-6:]  # 取后6位
        logger.debug(f"基金代码截断: {code_str} -> {formatted_code}")
    elif len(code_str) < 6:
        formatted_code = code_str.zfill(6)  # 左侧补0
        logger.debug(f"基金代码补零: {code_str} -> {formatted_code}")
    else:
        formatted_code = code_str
    
    # 验证结果是否为纯数字（国内基金代码通常为6位数字）
    if not formatted_code.isdigit():
        logger.warning(f"基金代码包含非数字字符: {formatted_code}")
    
    return formatted_code


def validate_fund_code(fund_code: str) -> bool:
    """
    验证基金代码格式是否有效
    
    Args:
        fund_code: 基金代码字符串
        
    Returns:
        如果符合基本格式要求返回True，否则返回False
    """
    if not fund_code or not isinstance(fund_code, str):
        return False
    
    # 移除空格
    code = fund_code.strip()
    
    # 空字符串无效
    if not code:
        return False
    
    # 检查长度（国内基金代码通常为6位）
    if len(code) < 3 or len(code) > 10:
        logger.warning(f"基金代码长度异常: {code} (长度: {len(code)})")
        # 不直接返回False，有些特殊代码可能长度不同
    
    # 检查是否包含有效数字
    if not any(char.isdigit() for char in code):
        logger.warning(f"基金代码不包含数字: {code}")
        return False
    
    return True


def parse_fund_codes(input_str: str) -> List[str]:
    """
    从字符串中解析出多个基金代码
    
    支持格式：
    - 逗号分隔: "000001,000002,000003"
    - 空格分隔: "000001 000002 000003"
    - 混合: "000001, 000002, 000003"
    
    Args:
        input_str: 包含基金代码的字符串
        
    Returns:
        格式化后的基金代码列表
    """
    if not input_str:
        return []
    
    # 使用正则表达式提取所有连续的数字序列（长度3-10位）
    codes = re.findall(r'\d{3,10}', input_str)
    
    # 格式化每个代码
    formatted_codes = []
    for code in codes:
        try:
            formatted = format_fund_code(code)
            formatted_codes.append(formatted)
        except ValueError as e:
            logger.warning(f"解析基金代码失败 '{code}': {e}")
    
    # 去重并保持原始顺序
    seen = set()
    unique_codes = []
    for code in formatted_codes:
        if code not in seen:
            seen.add(code)
            unique_codes.append(code)
    
    logger.debug(f"从字符串解析出 {len(unique_codes)} 个基金代码: {unique_codes}")
    return unique_codes


# ============================================================================
# 日期处理工具
# ============================================================================

def parse_date(date_input: Union[str, datetime, date]) -> datetime:
    """
    将各种日期输入转换为datetime对象
    
    Args:
        date_input: 日期输入，可以是字符串或日期对象
        
    Returns:
        datetime对象
        
    Raises:
        ValueError: 如果日期格式无法解析
    """
    if isinstance(date_input, datetime):
        return date_input
    elif isinstance(date_input, date):
        return datetime.combine(date_input, datetime.min.time())
    elif isinstance(date_input, str):
        # 尝试多种日期格式
        date_formats = [
            '%Y-%m-%d',      # 2023-12-31
            '%Y/%m/%d',      # 2023/12/31
            '%Y%m%d',        # 20231231
            '%Y-%m-%d %H:%M:%S',  # 2023-12-31 23:59:59
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_input, fmt)
            except ValueError:
                continue
        
        raise ValueError(f"无法解析日期字符串: {date_input}")
    else:
        raise TypeError(f"不支持的日期类型: {type(date_input)}")


def format_date(date_obj: Union[datetime, date], fmt: str = '%Y-%m-%d') -> str:
    """
    将日期对象格式化为字符串
    
    Args:
        date_obj: datetime或date对象
        fmt: 输出格式，默认为'%Y-%m-%d'
        
    Returns:
        格式化后的日期字符串
    """
    if isinstance(date_obj, datetime):
        return date_obj.strftime(fmt)
    elif isinstance(date_obj, date):
        return date_obj.strftime(fmt)
    else:
        raise TypeError(f"不支持的日期类型: {type(date_obj)}")


def get_previous_trading_day(target_date: Union[str, datetime, date] = None, 
                            holiday_list: List[str] = None) -> str:
    """
    获取前一个交易日（排除周末和节假日）
    
    注意：这是一个简化版本，实际使用时需要集成真实的交易日历数据
    目前仅排除周末，节假日需要外部提供
    
    Args:
        target_date: 目标日期，默认为今天
        holiday_list: 节假日列表，格式为['2023-01-01', '2023-01-02', ...]
        
    Returns:
        前一个交易日的日期字符串（格式：YYYY-MM-DD）
    """
    if target_date is None:
        target_date = datetime.now()
    
    dt = parse_date(target_date)
    
    # 如果没有提供节假日列表，使用空列表
    if holiday_list is None:
        holiday_list = []
    
    # 向前查找交易日
    days_back = 0
    while True:
        days_back += 1
        previous_day = dt - timedelta(days=days_back)
        previous_day_str = format_date(previous_day)
        
        # 检查是否为周末（周一=0，周日=6）
        if previous_day.weekday() >= 5:  # 5=周六，6=周日
            continue
        
        # 检查是否为节假日
        if previous_day_str in holiday_list:
            continue
        
        # 找到交易日
        logger.debug(f"找到前一个交易日: {target_date} -> {previous_day_str} (跳过 {days_back-1} 天)")
        return previous_day_str


def is_trading_day(check_date: Union[str, datetime, date] = None,
                  holiday_list: List[str] = None) -> bool:
    """
    检查指定日期是否为交易日
    
    Args:
        check_date: 要检查的日期
        holiday_list: 节假日列表
        
    Returns:
        如果是交易日返回True，否则返回False
    """
    if check_date is None:
        check_date = datetime.now()
    
    dt = parse_date(check_date)
    date_str = format_date(dt)
    
    # 检查周末
    if dt.weekday() >= 5:  # 5=周六，6=周日
        return False
    
    # 检查节假日
    if holiday_list and date_str in holiday_list:
        return False
    
    return True


def find_nearest_trading_day(target_date: Union[str, datetime, date] = None,
                            holiday_list: List[str] = None) -> str:
    """
    查找最近的交易日（如果当天不是交易日，则向前查找）
    
    Args:
        target_date: 目标日期
        holiday_list: 节假日列表
        
    Returns:
        最近的交易日日期字符串
    """
    if target_date is None:
        target_date = datetime.now()
    
    dt = parse_date(target_date)
    date_str = format_date(dt)
    
    # 如果当天是交易日，直接返回
    if is_trading_day(dt, holiday_list):
        return date_str
    
    # 否则向前查找
    return get_previous_trading_day(dt, holiday_list)


# ============================================================================
# 净值日期匹配逻辑（解决calculator.py中的日期匹配模糊问题）
# ============================================================================

def find_nav_for_date(fund_code: str, target_date: str, 
                     db_connection = None, max_lookback_days: int = 5) -> Optional[Dict[str, Any]]:
    """
    为指定日期查找可用的净值数据
    
    业务规则：
    1. 首先尝试查找target_date当天的净值
    2. 如果当天没有，向前查找最近的交易日净值（最多向前查找max_lookback_days天）
    3. 如果向前查找也找不到，返回None
    
    Args:
        fund_code: 基金代码
        target_date: 目标日期（格式：YYYY-MM-DD）
        db_connection: 数据库连接或数据库对象，需有get_nav_on_date和get_nav_history方法
        max_lookback_days: 最大向前查找天数
        
    Returns:
        净值记录字典，包含nav_date, nav_value, change_rate等字段，或None
        
    Note:
        此函数需要数据库模块支持，如果未提供db_connection，则仅返回查找逻辑说明
    """
    logger.info(f"为基金 {fund_code} 查找 {target_date} 的净值数据")
    
    if db_connection is None:
        logger.warning("未提供数据库连接，仅返回查找逻辑说明")
        return {
            'fund_code': fund_code,
            'target_date': target_date,
            'logic': f'将查找{target_date}及前{max_lookback_days}个交易日的净值数据',
            'actual_nav_date': None,
            'note': '需要提供数据库连接以执行实际查询'
        }
    
    try:
        # 1. 尝试查找目标日期的净值
        nav_record = db_connection.get_nav_on_date(fund_code, target_date)
        if nav_record:
            logger.info(f"找到目标日期净值: {target_date}")
            return nav_record
        
        # 2. 向前查找最近的净值
        logger.info(f"目标日期 {target_date} 无净值数据，向前查找...")
        
        # 获取最近一段时间的净值记录
        # 计算开始日期（向前推max_lookback_days+10天以确保有足够数据）
        target_dt = parse_date(target_date)
        start_date = format_date(target_dt - timedelta(days=max_lookback_days + 10))
        
        history = db_connection.get_nav_history(
            fund_code, 
            start_date=start_date,
            end_date=target_date
        )
        
        if not history:
            logger.warning(f"基金 {fund_code} 在 {target_date} 附近无任何净值数据")
            return None
        
        # 查找target_date之前最近的净值记录
        for record in history:
            record_date = parse_date(record['nav_date'])
            if record_date < target_dt:
                logger.info(f"使用最近可用净值: {record['nav_date']} (目标日期: {target_date})")
                return record
        
        logger.warning(f"未找到 {target_date} 之前的净值数据")
        return None
        
    except Exception as e:
        logger.error(f"查找净值数据失败: {e}")
        return None


# ============================================================================
# 文件与路径工具
# ============================================================================

def ensure_directory(dir_path: Union[str, Path]) -> str:
    """
    确保目录存在，如果不存在则创建
    
    Args:
        dir_path: 目录路径
        
    Returns:
        标准化后的目录路径字符串
    """
    path = Path(dir_path)
    path.mkdir(parents=True, exist_ok=True)
    return str(path.absolute())


def get_absolute_path(relative_path: str, base_dir: str = None) -> str:
    """
    获取绝对路径，支持相对路径解析
    
    Args:
        relative_path: 相对路径
        base_dir: 基础目录，默认为当前工作目录
        
    Returns:
        绝对路径字符串
    """
    if base_dir is None:
        base_dir = os.getcwd()
    
    # 如果已经是绝对路径，直接返回
    if os.path.isabs(relative_path):
        return relative_path
    
    # 否则相对于base_dir解析
    absolute_path = os.path.join(base_dir, relative_path)
    return os.path.normpath(absolute_path)


def safe_delete_file(file_path: str, max_retries: int = 3) -> bool:
    """
    安全删除文件，支持重试
    
    Args:
        file_path: 文件路径
        max_retries: 最大重试次数
        
    Returns:
        删除成功返回True，否则返回False
    """
    if not os.path.exists(file_path):
        return True
    
    for attempt in range(max_retries):
        try:
            os.remove(file_path)
            logger.debug(f"文件删除成功: {file_path}")
            return True
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = 0.5 * (attempt + 1)  # 递增等待
                logger.warning(f"删除文件失败 (尝试 {attempt+1}/{max_retries}), {e}，等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            else:
                logger.error(f"删除文件失败: {file_path}, 错误: {e}")
    
    return False


# ============================================================================
# HTTP请求与重试工具
# ============================================================================

def retry_on_failure(max_retries: int = 3, delay: float = 1.0, 
                    backoff: float = 2.0, exceptions: tuple = (Exception,)):
    """
    重试装饰器，用于网络请求等可能失败的操作
    
    Args:
        max_retries: 最大重试次数
        delay: 初始延迟时间（秒）
        backoff: 延迟倍数（每次重试延迟时间乘以这个系数）
        exceptions: 需要重试的异常类型
        
    Returns:
        装饰器函数
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        logger.warning(f"操作失败 (尝试 {attempt+1}/{max_retries}): {e}，"
                                      f"{current_delay}秒后重试...")
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(f"操作失败，已达到最大重试次数 {max_retries}")
            
            # 所有重试都失败，抛出最后一次异常
            raise last_exception
        return wrapper
    return decorator


# ============================================================================
# 数据验证与清洗工具
# ============================================================================

def safe_float_conversion(value: Any, default: float = 0.0) -> float:
    """
    安全地将值转换为浮点数
    
    Args:
        value: 要转换的值
        default: 转换失败时的默认值
        
    Returns:
        浮点数值
    """
    if value is None:
        return default
    
    try:
        # 如果是字符串，移除可能的千分位逗号和货币符号
        if isinstance(value, str):
            value = value.replace(',', '').replace('¥', '').replace('$', '').strip()
        
        return float(value)
    except (ValueError, TypeError):
        logger.warning(f"无法将值转换为浮点数: {value}，使用默认值 {default}")
        return default


def safe_int_conversion(value: Any, default: int = 0) -> int:
    """
    安全地将值转换为整数
    
    Args:
        value: 要转换的值
        default: 转换失败时的默认值
        
    Returns:
        整数值
    """
    if value is None:
        return default
    
    try:
        # 先尝试转换为浮点数，再取整（处理"123.45"这种情况）
        float_value = safe_float_conversion(value, float(default))
        return int(float_value)
    except (ValueError, TypeError):
        logger.warning(f"无法将值转换为整数: {value}，使用默认值 {default}")
        return default


def normalize_column_name(col_name: str) -> str:
    """
    规范化列名，移除多余空格和特殊字符
    
    Args:
        col_name: 原始列名
        
    Returns:
        规范化后的列名
    """
    if not col_name or not isinstance(col_name, str):
        return ""
    
    # 移除首尾空格
    normalized = col_name.strip()
    
    # 移除不可见字符
    normalized = ''.join(char for char in normalized if char.isprintable())
    
    # 将多个空格合并为一个
    normalized = ' '.join(normalized.split())
    
    return normalized


# ============================================================================
# 日志与调试工具
# ============================================================================

def setup_logging(level: str = 'INFO', log_file: str = None) -> logging.Logger:
    """
    配置日志系统
    
    Args:
        level: 日志级别（DEBUG, INFO, WARNING, ERROR, CRITICAL）
        log_file: 日志文件路径，如果为None则只输出到控制台
        
    Returns:
        配置好的logger实例
    """
    # 创建logger
    logger = logging.getLogger('fund_tracker')
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    
    # 清除已有的handler，避免重复
    if logger.handlers:
        logger.handlers.clear()
    
    # 创建formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 控制台handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 文件handler（如果指定了日志文件）
    if log_file:
        # 确保日志目录存在
        log_dir = os.path.dirname(log_file)
        if log_dir:
            ensure_directory(log_dir)
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def measure_execution_time(func: Callable) -> Callable:
    """
    测量函数执行时间的装饰器
    
    Args:
        func: 要测量的函数
        
    Returns:
        装饰后的函数
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        
        execution_time = end_time - start_time
        logger.debug(f"函数 {func.__name__} 执行时间: {execution_time:.3f} 秒")
        
        return result
    return wrapper


# ============================================================================
# 主函数：模块测试
# ============================================================================

def test_utils_module():
    """测试utils模块的所有功能"""
    print("=" * 60)
    print("测试 utils.py 模块功能")
    print("=" * 60)
    
    # 测试基金代码格式化
    print("\n1. 测试基金代码格式化:")
    test_cases = ["OF000051", "51", 51, "000051", "SH510300", "  00123  "]
    for code in test_cases:
        try:
            result = format_fund_code(code)
            print(f"  format_fund_code({repr(code)}) = {result}")
        except Exception as e:
            print(f"  format_fund_code({repr(code)}) 错误: {e}")
    
    # 测试日期处理
    print("\n2. 测试日期处理:")
    today = datetime.now()
    print(f"  当前日期: {format_date(today)}")
    print(f"  是否为交易日（仅检查周末）: {is_trading_day(today)}")
    
    # 测试前一个交易日查找
    test_date = "2023-10-01"  # 国庆节，非交易日
    prev_trading = get_previous_trading_day(test_date, ["2023-10-01", "2023-10-02", "2023-10-03"])
    print(f"  {test_date} 的前一个交易日: {prev_trading}")
    
    # 测试路径工具
    print("\n3. 测试路径工具:")
    test_dir = "./test_output"
    abs_path = ensure_directory(test_dir)
    print(f"  确保目录存在: {test_dir} -> {abs_path}")
    
    # 测试数据转换
    print("\n4. 测试数据转换:")
    test_values = ["123.45", "1,234.56", "¥789.01", "invalid", None]
    for val in test_values:
        result = safe_float_conversion(val, 0.0)
        print(f"  safe_float_conversion({repr(val)}) = {result}")
    
    # 清理测试目录
    import shutil
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
        print(f"\n清理测试目录: {test_dir}")
    
    print("\n" + "=" * 60)
    print("utils.py 模块测试完成")
    print("=" * 60)


if __name__ == "__main__":
    # 配置日志以便测试
    logging.basicConfig(level=logging.INFO)
    
    # 运行测试
    test_utils_module()
