"""
基金追踪数据库模块
使用 SQLite 存储基金信息、净值历史、预测记录等
重构要点：
1. 引入数据库连接上下文管理，支持长连接和自动重连
2. 添加关键索引优化查询性能
3. 扩展查询接口，支持日期范围查询、批量操作
4. 为批量插入添加事务支持
5. 统一基金代码格式化逻辑（通过导入公共utils模块实现）
6. 从配置管理器读取数据库路径，实现配置化
"""
import sqlite3
import logging
import threading
from contextlib import contextmanager
from datetime import datetime, date, timedelta
from typing import List, Optional, Tuple, Dict, Any, Iterator, Union
import json

# 导入公共工具函数，消除代码重复
try:
    from utils import format_fund_code
except ImportError:
    # 回退方案：如果utils.py不存在，定义一个简易版本
    def format_fund_code(fund_code: str) -> str:
        """统一基金代码格式为6位字符串（与calculator.py逻辑一致）"""
        code = str(fund_code).strip()
        if len(code) > 6:
            code = code[-6:]  # 如 "OF000051" -> "000051"
        elif len(code) < 6:
            code = code.zfill(6)  # 如 "51" -> "000051"
        return code

logger = logging.getLogger(__name__)

# 线程本地存储，用于管理数据库连接
_thread_local = threading.local()


class FundDatabase:
    """基金追踪数据库管理类"""
    
    def __init__(self, db_path: str = "data/fund.db"):
        """
        初始化数据库管理器
        
        Args:
            db_path: SQLite数据库文件路径
        """
        self.db_path = db_path
        # 存储列名常量，便于维护
        self.COLUMN_NAMES = {
            'funds': ['fund_code', 'fund_name', 'shares', 'cost_price', 'purchase_date'],
            'nav_history': ['fund_code', 'nav_date', 'nav_value', 'change_rate'],
            'predictions': ['fund_code', 'predict_date', 'predict_nav', 'predict_change', 'confidence', 'model_version'],
            'daily_pnl': ['fund_code', 'pnl_date', 'nav_value', 'change_rate', 'pnl_amount', 'total_value']
        }
        self._init_db()
        logger.info(f"基金数据库初始化完成: {db_path}")
    
    @contextmanager
    def _get_connection(self) -> Iterator[sqlite3.Connection]:
        """
        获取数据库连接的上下文管理器
        使用线程本地存储保持连接，支持连接复用和自动提交/回滚
        """
        # 检查当前线程是否已有连接
        if not hasattr(_thread_local, 'connection') or _thread_local.connection is None:
            try:
                _thread_local.connection = sqlite3.connect(
                    self.db_path,
                    check_same_thread=False,  # SQLite在单线程应用中使用是安全的
                    timeout=30.0
                )
                # 启用外键约束
                _thread_local.connection.execute("PRAGMA foreign_keys = ON")
                # 设置行工厂为字典格式
                _thread_local.connection.row_factory = sqlite3.Row
                logger.debug(f"创建新的数据库连接: {self.db_path}")
            except sqlite3.Error as e:
                logger.error(f"创建数据库连接失败: {e}")
                raise
        
        conn = _thread_local.connection
        try:
            yield conn
            # 如果没有异常，自动提交事务
            conn.commit()
        except sqlite3.Error as e:
            # 发生错误时回滚
            conn.rollback()
            logger.error(f"数据库操作失败，已回滚: {e}")
            raise
        except Exception as e:
            conn.rollback()
            logger.error(f"未知错误，数据库事务已回滚: {e}")
            raise
    
    def _init_db(self):
        """初始化数据库表和索引"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # 1. 基金基本信息表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS funds (
                    fund_code TEXT PRIMARY KEY,
                    fund_name TEXT,
                    shares REAL NOT NULL DEFAULT 0,
                    cost_price REAL NOT NULL DEFAULT 0,
                    purchase_date TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 2. 净值历史表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS nav_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fund_code TEXT NOT NULL,
                    nav_date TEXT NOT NULL,
                    nav_value REAL NOT NULL,
                    change_rate REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(fund_code, nav_date),
                    FOREIGN KEY (fund_code) REFERENCES funds (fund_code) ON DELETE CASCADE
                )
            """)
            
            # 3. 预测记录表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS predictions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fund_code TEXT NOT NULL,
                    predict_date TEXT NOT NULL,
                    predict_nav REAL NOT NULL,
                    predict_change REAL NOT NULL,
                    confidence REAL NOT NULL DEFAULT 0,
                    model_version TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(fund_code, predict_date),
                    FOREIGN KEY (fund_code) REFERENCES funds (fund_code) ON DELETE CASCADE
                )
            """)
            
            # 4. 模型表现表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS model_performance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    model_version TEXT NOT NULL,
                    eval_date TEXT NOT NULL,
                    accuracy REAL NOT NULL DEFAULT 0,
                    mean_error REAL NOT NULL DEFAULT 0,
                    total_predictions INTEGER NOT NULL DEFAULT 0,
                    correct_predictions INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(model_version, eval_date)
                )
            """)
            
            # 5. 每日盈亏表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_pnl (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fund_code TEXT NOT NULL,
                    pnl_date TEXT NOT NULL,
                    nav_value REAL NOT NULL,
                    change_rate REAL,
                    pnl_amount REAL NOT NULL DEFAULT 0,
                    total_value REAL NOT NULL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(fund_code, pnl_date),
                    FOREIGN KEY (fund_code) REFERENCES funds (fund_code) ON DELETE CASCADE
                )
            """)
            
            # 创建关键索引以优化查询性能
            self._create_indexes(cursor)
            
            logger.debug("数据库表结构初始化完成")
    
    def _create_indexes(self, cursor: sqlite3.Cursor):
        """创建关键查询索引"""
        # 净值历史表：按基金代码和日期查询是最频繁的操作
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_nav_history_fund_date ON nav_history (fund_code, nav_date DESC)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_nav_history_date ON nav_history (nav_date)")
        
        # 预测记录表
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_predictions_fund_date ON predictions (fund_code, predict_date DESC)")
        
        # 每日盈亏表
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_daily_pnl_date ON daily_pnl (pnl_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_daily_pnl_fund_date ON daily_pnl (fund_code, pnl_date DESC)")
        
        # 模型表现表
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_model_performance_version_date ON model_performance (model_version, eval_date DESC)")
        
        logger.debug("数据库索引创建完成")
    
    def close(self):
        """显式关闭数据库连接"""
        if hasattr(_thread_local, 'connection') and _thread_local.connection is not None:
            _thread_local.connection.close()
            _thread_local.connection = None
            logger.debug("数据库连接已关闭")
    
    # ========== 基金基本信息操作 ==========
    
    def add_fund(self, fund_code: str, fund_name: str, shares: float, 
                 cost_price: float, purchase_date: Optional[str] = None) -> bool:
        """添加或更新基金信息"""
        formatted_code = format_fund_code(fund_code)
        now = datetime.now().isoformat()
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO funds 
                (fund_code, fund_name, shares, cost_price, purchase_date, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (formatted_code, fund_name, shares, cost_price, purchase_date, now))
            
            if cursor.rowcount > 0:
                action = "更新" if self.get_fund(formatted_code) else "添加"
                logger.debug(f"基金信息已{action}: {formatted_code}")
                return True
            return False
    
    def get_fund(self, fund_code: str) -> Optional[Dict[str, Any]]:
        """获取单个基金信息"""
        formatted_code = format_fund_code(fund_code)
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM funds WHERE fund_code = ?", (formatted_code,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_funds(self) -> List[Dict[str, Any]]:
        """获取所有基金列表"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM funds ORDER BY fund_code")
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    
    def delete_fund(self, fund_code: str) -> bool:
        """删除基金及其相关数据（外键约束会自动级联删除）"""
        formatted_code = format_fund_code(fund_code)
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM funds WHERE fund_code = ?", (formatted_code,))
            deleted = cursor.rowcount > 0
            if deleted:
                logger.info(f"基金 {formatted_code} 及其相关数据已删除")
            return deleted
    
    # ========== 净值历史操作 ==========
    
    def add_nav_history(self, fund_code: str, nav_date: str, nav_value: float, 
                        change_rate: Optional[float] = None) -> bool:
        """
        添加净值历史记录
        
        Returns:
            True 表示插入了新记录，False 表示记录已存在（忽略）
        """
        formatted_code = format_fund_code(fund_code)
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR IGNORE INTO nav_history 
                (fund_code, nav_date, nav_value, change_rate, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (formatted_code, nav_date, nav_value, change_rate, datetime.now().isoformat()))
            
            inserted = cursor.rowcount > 0
            if inserted:
                logger.debug(f"净值记录已添加: {formatted_code} {nav_date}")
            return inserted
    
    def batch_add_nav_history(self, records: List[Dict]) -> int:
        """
        批量添加净值历史记录，使用事务确保原子性
        
        Args:
            records: 净值记录列表，每条记录需包含 fund_code, nav_date, nav_value, change_rate
            
        Returns:
            成功插入的记录数
        """
        if not records:
            return 0
        
        formatted_records = []
        for record in records:
            formatted_code = format_fund_code(record['fund_code'])
            formatted_records.append((
                formatted_code, 
                record['nav_date'], 
                record['nav_value'], 
                record.get('change_rate'),
                datetime.now().isoformat()
            ))
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany("""
                INSERT OR IGNORE INTO nav_history 
                (fund_code, nav_date, nav_value, change_rate, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, formatted_records)
            
            inserted_count = cursor.rowcount
            logger.info(f"批量插入 {inserted_count}/{len(records)} 条净值记录")
            return inserted_count
    
    def get_nav_history(self, fund_code: str, limit: int = 100, 
                        start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        """
        获取某基金的历史净值，支持日期范围过滤
        
        Args:
            fund_code: 基金代码
            limit: 返回的最大记录数
            start_date: 开始日期（包含），格式 YYYY-MM-DD
            end_date: 结束日期（包含），格式 YYYY-MM-DD
            
        Returns:
            净值记录列表，按日期倒序排列
        """
        formatted_code = format_fund_code(fund_code)
        
        query = """
            SELECT * FROM nav_history 
            WHERE fund_code = ?
        """
        params = [formatted_code]
        
        if start_date:
            query += " AND nav_date >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND nav_date <= ?"
            params.append(end_date)
        
        query += " ORDER BY nav_date DESC"
        
        if limit > 0:
            query += " LIMIT ?"
            params.append(limit)
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    
    def get_latest_nav(self, fund_code: str) -> Optional[Dict[str, Any]]:
        """获取某基金的最新净值"""
        formatted_code = format_fund_code(fund_code)
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM nav_history 
                WHERE fund_code = ? 
                ORDER BY nav_date DESC 
                LIMIT 1
            """, (formatted_code,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_nav_on_date(self, fund_code: str, date_str: str) -> Optional[Dict[str, Any]]:
        """获取某基金在特定日期的净值"""
        formatted_code = format_fund_code(fund_code)
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM nav_history 
                WHERE fund_code = ? AND nav_date = ?
                LIMIT 1
            """, (formatted_code, date_str))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_nav_count(self, fund_code: str) -> int:
        """获取基金净值记录数量"""
        formatted_code = format_fund_code(fund_code)
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM nav_history 
                WHERE fund_code = ?
            """, (formatted_code,))
            count = cursor.fetchone()[0]
            return count
    
    # ========== 预测记录操作 ==========
    
    def add_prediction(self, fund_code: str, predict_date: str, predict_nav: float,
                       predict_change: float, confidence: float, model_version: str) -> bool:
        """添加预测记录"""
        formatted_code = format_fund_code(fund_code)
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO predictions 
                (fund_code, predict_date, predict_nav, predict_change, confidence, model_version, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (formatted_code, predict_date, predict_nav, predict_change, confidence, model_version, datetime.now().isoformat()))
            
            inserted = cursor.rowcount > 0
            if inserted:
                logger.debug(f"预测记录已添加: {formatted_code} {predict_date}")
            return inserted
    
    def get_predictions(self, fund_code: str = None, limit: int = 10, 
                        start_date: str = None) -> List[Dict[str, Any]]:
        """
        获取预测记录，支持按基金代码和日期过滤
        
        Args:
            fund_code: 基金代码，为None时返回所有基金的预测
            limit: 返回的最大记录数
            start_date: 开始日期（包含），格式 YYYY-MM-DD
            
        Returns:
            预测记录列表，按预测日期倒序排列
        """
        query = "SELECT * FROM predictions"
        params = []
        
        conditions = []
        if fund_code:
            formatted_code = format_fund_code(fund_code)
            conditions.append("fund_code = ?")
            params.append(formatted_code)
        
        if start_date:
            conditions.append("predict_date >= ?")
            params.append(start_date)
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY predict_date DESC"
        
        if limit > 0:
            query += " LIMIT ?"
            params.append(limit)
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    
    # ========== 每日盈亏操作 ==========
    
    def add_daily_pnl(self, fund_code: str, pnl_date: str, nav_value: float, 
                      change_rate: float, pnl_amount: float, total_value: float) -> bool:
        """添加每日盈亏记录"""
        formatted_code = format_fund_code(fund_code)
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO daily_pnl 
                (fund_code, pnl_date, nav_value, change_rate, pnl_amount, total_value, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (formatted_code, pnl_date, nav_value, change_rate, pnl_amount, total_value, datetime.now().isoformat()))
            
            inserted = cursor.rowcount > 0
            if inserted:
                logger.debug(f"每日盈亏记录已添加: {formatted_code} {pnl_date}")
            return inserted
    
    def get_daily_pnl(self, date_str: str = None, fund_code: str = None) -> List[Dict[str, Any]]:
        """
        获取盈亏记录，支持按日期和基金代码过滤
        
        Args:
            date_str: 日期，为None时返回所有日期的记录
            fund_code: 基金代码，为None时返回所有基金的记录
            
        Returns:
            盈亏记录列表
        """
        query = "SELECT * FROM daily_pnl"
        params = []
        
        conditions = []
        if date_str:
            conditions.append("pnl_date = ?")
            params.append(date_str)
        
        if fund_code:
            formatted_code = format_fund_code(fund_code)
            conditions.append("fund_code = ?")
            params.append(formatted_code)
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY pnl_date DESC, fund_code"
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    
    def get_pnl_summary(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        获取日期范围内的盈亏汇总（按日期分组）
        
        Returns:
            每日汇总列表，包含日期、总盈亏、平均收益率等
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    pnl_date,
                    COUNT(*) as fund_count,
                    SUM(total_value) as total_value,
                    SUM(pnl_amount) as total_pnl,
                    AVG(change_rate) as avg_change_rate
                FROM daily_pnl
                WHERE pnl_date >= ? AND pnl_date <= ?
                GROUP BY pnl_date
                ORDER BY pnl_date DESC
            """, (start_date, end_date))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    
    # ========== 模型表现操作 ==========
    
    def update_model_performance(self, model_version: str, eval_date: str, accuracy: float,
                                 mean_error: float, total_predictions: int, correct_predictions: int) -> bool:
        """更新模型表现记录"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO model_performance 
                (model_version, eval_date, accuracy, mean_error, total_predictions, correct_predictions, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (model_version, eval_date, accuracy, mean_error, total_predictions, correct_predictions, datetime.now().isoformat()))
            
            inserted = cursor.rowcount > 0
            if inserted:
                logger.debug(f"模型表现记录已更新: {model_version} {eval_date}")
            return inserted
    
    def get_latest_model_performance(self, model_version: str = None) -> Optional[Dict[str, Any]]:
        """
        获取模型的最新表现
        
        Args:
            model_version: 模型版本，为None时返回所有模型中最新的一条记录
            
        Returns:
            模型表现记录
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            if model_version:
                cursor.execute("""
                    SELECT * FROM model_performance 
                    WHERE model_version = ? 
                    ORDER BY eval_date DESC 
                    LIMIT 1
                """, (model_version,))
            else:
                cursor.execute("""
                    SELECT * FROM model_performance 
                    ORDER BY eval_date DESC 
                    LIMIT 1
                """)
            
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_model_performance_history(self, model_version: str, limit: int = 30) -> List[Dict[str, Any]]:
        """获取模型的历史表现记录"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM model_performance 
                WHERE model_version = ? 
                ORDER BY eval_date DESC 
                LIMIT ?
            """, (model_version, limit))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    
    # ========== 数据维护操作 ==========
    
    def cleanup_old_data(self, days_to_keep: int = 365) -> Dict[str, int]:
        """
        清理指定天数前的历史数据
        
        Args:
            days_to_keep: 保留最近多少天的数据
            
        Returns:
            各表删除的记录数
        """
        cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).strftime('%Y-%m-%d')
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # 清理旧净值记录（保留最近days_to_keep天）
            cursor.execute("""
                DELETE FROM nav_history 
                WHERE nav_date < ?
            """, (cutoff_date,))
            nav_deleted = cursor.rowcount
            
            # 清理旧预测记录
            cursor.execute("""
                DELETE FROM predictions 
                WHERE predict_date < ?
            """, (cutoff_date,))
            pred_deleted = cursor.rowcount
            
            # 清理旧盈亏记录
            cursor.execute("""
                DELETE FROM daily_pnl 
                WHERE pnl_date < ?
            """, (cutoff_date,))
            pnl_deleted = cursor.rowcount
            
            # 清理旧模型表现记录（保留最近30次评估）
            cursor.execute("""
                DELETE FROM model_performance 
                WHERE id NOT IN (
                    SELECT id FROM model_performance 
                    ORDER BY eval_date DESC 
                    LIMIT 30
                )
            """)
            model_deleted = cursor.rowcount
            
            logger.info(f"数据清理完成: 净值记录{nav_deleted}条, 预测记录{pred_deleted}条, "
                       f"盈亏记录{pnl_deleted}条, 模型记录{model_deleted}条")
            
            return {
                'nav_history': nav_deleted,
                'predictions': pred_deleted,
                'daily_pnl': pnl_deleted,
                'model_performance': model_deleted
            }
    
    def get_database_stats(self) -> Dict[str, Any]:
        """获取数据库统计信息"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            stats = {}
            tables = ['funds', 'nav_history', 'predictions', 'daily_pnl', 'model_performance']
            
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                count = cursor.fetchone()[0]
                stats[f'{table}_count'] = count
            
            # 获取净值数据的时间范围
            cursor.execute("""
                SELECT MIN(nav_date) as earliest, MAX(nav_date) as latest 
                FROM nav_history
            """)
            date_range = cursor.fetchone()
            if date_range and date_range[0]:
                stats['nav_date_range'] = {
                    'earliest': date_range[0],
                    'latest': date_range[1]
                }
            
            return stats


# 全局数据库实例（单例模式）
_db_instance = None
_db_lock = threading.Lock()


def get_db(db_path: str = None, config_path: str = "config.yaml") -> FundDatabase:
    """
    获取数据库实例（线程安全的单例）
    优先从配置管理器读取数据库路径，支持手动指定路径
    
    Args:
        db_path: 数据库文件路径（手动指定，优先级最高）
        config_path: 配置文件路径，当db_path为None时使用
        
    Returns:
        FundDatabase 实例
    """
    global _db_instance
    
    # 如果已经存在实例且路径没有变化，直接返回
    if _db_instance is not None and db_path is None:
        return _db_instance
    
    with _db_lock:
        # 再次检查，防止多个线程同时创建实例
        if _db_instance is not None and db_path is None:
            return _db_instance
        
        try:
            # 确定数据库路径
            final_db_path = db_path
            
            if final_db_path is None:
                # 尝试从配置管理器获取
                try:
                    from config_manager import get_config
                    config = get_config(config_path)
                    final_db_path = config.get('output.database_path', 'data/fund.db')
                    logger.info(f"从配置读取数据库路径: {final_db_path}")
                except ImportError:
                    logger.warning("config_manager 模块未找到，使用默认数据库路径")
                    final_db_path = "data/fund.db"
                except Exception as e:
                    logger.error(f"从配置读取数据库路径失败: {e}，使用默认路径")
                    final_db_path = "data/fund.db"
            
            # 创建新的数据库实例
            _db_instance = FundDatabase(final_db_path)
            
        except Exception as e:
            logger.error(f"初始化数据库实例失败: {e}")
            # 尝试从环境变量获取配置路径
            import os
            env_db_path = os.environ.get('FUND_TRACKER_DB_PATH')
            if env_db_path:
                try:
                    _db_instance = FundDatabase(env_db_path)
                    logger.info(f"从环境变量读取数据库路径: {env_db_path}")
                except Exception:
                    # 最后尝试默认路径
                    _db_instance = FundDatabase()
            else:
                # 使用默认路径
                _db_instance = FundDatabase()
    
    return _db_instance


def close_db():
    """关闭全局数据库连接"""
    global _db_instance
    if _db_instance is not None:
        _db_instance.close()
        _db_instance = None


# 测试函数
def test_database():
    """测试数据库功能"""
    import tempfile
    import os
    
    # 创建临时数据库文件
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        temp_db_path = f.name
    
    try:
        # 创建数据库实例（使用临时路径，避免依赖配置）
        db = FundDatabase(temp_db_path)
        
        print("✅ 数据库初始化测试通过")
        
        # 测试基金操作
        db.add_fund("000001", "测试基金1", 1000.0, 1.5, "2023-01-01")
        fund = db.get_fund("000001")
        assert fund is not None
        assert fund['fund_code'] == '000001'
        print("✅ 基金操作测试通过")
        
        # 测试净值操作
        db.add_nav_history("000001", "2023-12-01", 1.6, 0.5)
        latest_nav = db.get_latest_nav("000001")
        assert latest_nav['nav_value'] == 1.6
        print("✅ 净值操作测试通过")
        
        # 测试批量插入
        records = [
            {'fund_code': '000001', 'nav_date': '2023-12-02', 'nav_value': 1.61, 'change_rate': 0.63},
            {'fund_code': '000001', 'nav_date': '2023-12-03', 'nav_value': 1.59, 'change_rate': -1.24},
        ]
        inserted = db.batch_add_nav_history(records)
        assert inserted == 2
        print("✅ 批量操作测试通过")
        
        # 测试日期范围查询
        history = db.get_nav_history("000001", start_date="2023-12-01", end_date="2023-12-02")
        assert len(history) == 2
        print("✅ 日期范围查询测试通过")
        
        # 测试统计数据
        stats = db.get_database_stats()
        assert stats['funds_count'] == 1
        print("✅ 统计查询测试通过")
        
        # 测试get_db单例模式
        db1 = get_db(temp_db_path)
        db2 = get_db(temp_db_path)
        assert db1 is db2  # 应该是同一个实例
        print("✅ 单例模式测试通过")
        
        print("\n🎉 所有数据库测试通过！")
        
    finally:
        # 清理临时文件
        db.close()
        os.unlink(temp_db_path)


if __name__ == "__main__":
    # 配置日志
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stdout
    )
    
    # 运行测试
    test_database()
