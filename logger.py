# -*- coding: utf-8 -*-
"""
日志工具 - 统一日志记录功能
"""
import os
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path
from logging.handlers import RotatingFileHandler

# 日志目录
LOG_DIR = Path(__file__).parent.parent / "日志"


def setup_logger(name: str, log_file: str = None, level=logging.INFO) -> logging.Logger:
    """设置日志记录器"""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # 避免重复添加handler
    if logger.handlers:
        return logger
    
    # 确保日志目录存在
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    # 日志文件名（默认使用脚本名.log）
    if log_file is None:
        log_file = f"{name}.log"
    
    log_path = LOG_DIR / log_file
    
    # 文件handler - 每个日志文件最大5MB，保留3个备份
    file_handler = RotatingFileHandler(
        log_path, 
        maxBytes=5*1024*1024, 
        backupCount=3,
        encoding='utf-8'
    )
    file_handler.setLevel(level)
    
    # 格式
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    
    # 控制台handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


def cleanup_old_logs(days: int = 30, patterns: list = None):
    """清理过期日志
    
    Args:
        days: 保留天数（默认30天）
        patterns: 要删除的日志名模式，默认 ['*.log', '*.log.*']
    """
    if not LOG_DIR.exists():
        return
    
    if patterns is None:
        patterns = ['*.log', '*.log.*']
    
    cutoff = datetime.now() - timedelta(days=days)
    deleted = []
    
    for pattern in patterns:
        for log_file in LOG_DIR.glob(pattern):
            # 检查修改时间
            mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
            if mtime < cutoff:
                log_file.unlink()
                deleted.append(log_file.name)
    
    return deleted


# 便捷函数
def get_logger(name: str = None):
    """获取日志记录器的快捷方式"""
    if name is None:
        # 获取调用者模块名
        import inspect
        frame = inspect.currentframe().f_back
        name = frame.f_globals.get('__name__', 'main')
        name = name.split('.')[-1] if '.' in name else name
    
    return setup_logger(name)


if __name__ == '__main__':
    # 测试
    log = get_logger('test')
    log.info('测试日志')
    log.warning('测试警告')
    log.error('测试错误')
    
    # 清理测试
    deleted = cleanup_old_logs(0)  # 删除0天前 = 删除所有
    print(f'已清理: {deleted}')