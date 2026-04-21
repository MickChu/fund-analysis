"""
持仓盈亏计算模块
读取持仓表格，结合净值数据计算每日盈亏
"""
import logging
import pandas as pd
from datetime import datetime, timedelta
import yaml
import os
from typing import List, Dict, Optional, Tuple
from database import get_db

logger = logging.getLogger(__name__)

class PnLCalculator:
    def __init__(self, config_path: str = "config.yaml"):
        """初始化计算器，加载配置"""
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        input_config = self.config['input']
        self.excel_path = input_config['excel_path']
        self.sheet_name = input_config['sheet_name']
        self.columns = input_config['columns']
        
        self.db = get_db()
        logger.info("盈亏计算器初始化完成")
    
    def load_holdings(self) -> List[Dict]:
        """
        加载持仓表格，返回基金列表
        Returns:
            List[Dict]: 每个基金的信息
        """
        try:
            # 读取 Excel 文件
            df = pd.read_excel(self.excel_path, sheet_name=self.sheet_name)
            logger.info(f"成功读取持仓表格: {self.excel_path}")
            
            # 重命名列（使用配置中的映射）
            column_mapping = {
                'fund_code': self.columns.get('fund_code', '基金代码'),
                'fund_name': self.columns.get('fund_name', '基金名称'),
                'shares': self.columns.get('shares', '持仓份额'),
                'cost_price': self.columns.get('cost_price', '成本价'),
                'purchase_date': self.columns.get('purchase_date', '买入日期')
            }
            
            # 检查必需的列
            required_cols = ['fund_code', 'shares', 'cost_price']
            for col in required_cols:
                if column_mapping[col] not in df.columns:
                    logger.error(f"持仓表格缺少必需列: {column_mapping[col]}")
                    return []
            
            funds = []
            for _, row in df.iterrows():
                try:
                    fund_code = str(row[column_mapping['fund_code']]).strip()
                    # 统一基金代码格式为6位
                    # 1. 如果长度大于6，取后6位（如 "OF000051" → "000051"）
                    if len(fund_code) > 6:
                        fund_code = fund_code[-6:]
                    # 2. 如果长度小于6，左侧补0到6位（如 "51" → "000051"）
                    elif len(fund_code) < 6:
                        fund_code = fund_code.zfill(6)
                    # 3. 长度等于6，保持不变
                    
                    fund = {
                        'fund_code': fund_code,
                        'fund_name': row[column_mapping['fund_name']] if column_mapping['fund_name'] in df.columns else '',
                        'shares': float(row[column_mapping['shares']]),
                        'cost_price': float(row[column_mapping['cost_price']]),
                        'purchase_date': row[column_mapping['purchase_date']] if column_mapping['purchase_date'] in df.columns else None
                    }
                    funds.append(fund)
                except (ValueError, KeyError) as e:
                    logger.warning(f"解析行数据失败: {row}, 错误: {e}")
                    continue
            
            logger.info(f"成功加载 {len(funds)} 只基金持仓")
            return funds
            
        except FileNotFoundError:
            logger.error(f"持仓表格文件不存在: {self.excel_path}")
            return []
        except Exception as e:
            logger.error(f"读取持仓表格失败: {e}")
            return []
    
    def calculate_daily_pnl(self, date_str: str = None) -> Dict:
        """
        计算指定日期的盈亏
        Args:
            date_str: 日期（YYYY-MM-DD），默认为最新净值日期
        Returns:
            Dict: 包含总体统计和每只基金明细
        """
        if date_str is None:
            date_str = datetime.now().strftime('%Y-%m-%d')
        
        funds = self.load_holdings()
        if not funds:
            logger.error("无持仓数据，无法计算盈亏")
            return {}
        
        # 更新基金信息到数据库
        for fund in funds:
            self.db.add_fund(
                fund_code=fund['fund_code'],
                fund_name=fund['fund_name'],
                shares=fund['shares'],
                cost_price=fund['cost_price'],
                purchase_date=fund['purchase_date']
            )
        
        total_cost = 0.0
        total_value = 0.0
        total_pnl = 0.0
        fund_details = []
        
        for fund in funds:
            fund_code = fund['fund_code']
            shares = fund['shares']
            cost_price = fund['cost_price']
            
            # 获取最新净值（尝试指定日期，如果没有则用最新）
            nav_record = self.db.get_latest_nav(fund_code)
            
            if not nav_record:
                logger.warning(f"基金 {fund_code} 无净值数据，跳过")
                continue
            
            # 检查净值日期是否匹配（允许最近交易日）
            nav_date = nav_record['nav_date']
            nav_value = nav_record['nav_value']
            change_rate = nav_record.get('change_rate', 0)
            
            # 如果指定了日期但净值日期不匹配，尝试找该日期的净值
            if date_str != nav_date:
                # 这里可以添加查询特定日期净值的逻辑
                # 暂时使用最新净值
                logger.info(f"基金 {fund_code} 指定日期 {date_str} 无净值，使用最新 {nav_date}")
            
            # 计算
            cost_value = shares * cost_price
            current_value = shares * nav_value
            pnl_amount = current_value - cost_value
            pnl_rate = (nav_value - cost_price) / cost_price * 100 if cost_price > 0 else 0
            
            total_cost += cost_value
            total_value += current_value
            total_pnl += pnl_amount
            
            fund_detail = {
                'fund_code': fund_code,
                'fund_name': fund['fund_name'],
                'shares': shares,
                'cost_price': cost_price,
                'nav_date': nav_date,
                'nav_value': nav_value,
                'change_rate': change_rate,
                'cost_value': round(cost_value, 2),
                'current_value': round(current_value, 2),
                'pnl_amount': round(pnl_amount, 2),
                'pnl_rate': round(pnl_rate, 2)
            }
            fund_details.append(fund_detail)
            
            # 保存每日盈亏到数据库
            self.db.add_daily_pnl(
                fund_code=fund_code,
                pnl_date=date_str,
                nav_value=nav_value,
                change_rate=change_rate,
                pnl_amount=pnl_amount,
                total_value=current_value
            )
        
        # 总体统计
        overall_pnl_rate = (total_pnl / total_cost * 100) if total_cost > 0 else 0
        
        result = {
            'date': date_str,
            'total_funds': len(funds),
            'total_cost': round(total_cost, 2),
            'total_value': round(total_value, 2),
            'total_pnl': round(total_pnl, 2),
            'overall_pnl_rate': round(overall_pnl_rate, 2),
            'fund_details': fund_details,
            'calculated_at': datetime.now().isoformat()
        }
        
        logger.info(f"盈亏计算完成: 日期={date_str}, 总成本={result['total_cost']}, "
                   f"总市值={result['total_value']}, 总盈亏={result['total_pnl']}, "
                   f"收益率={result['overall_pnl_rate']}%")
        
        return result
    
    def generate_report(self, pnl_result: Dict, output_format: str = 'excel') -> str:
        """
        生成盈亏报告
        Args:
            pnl_result: calculate_daily_pnl 返回的结果
            output_format: 'excel' 或 'json'
        Returns:
            输出文件路径
        """
        if not pnl_result:
            logger.error("无盈亏数据，无法生成报告")
            return ""
        
        output_dir = self.config['output']['report_dir']
        os.makedirs(output_dir, exist_ok=True)
        
        date_str = pnl_result['date']
        
        if output_format == 'excel':
            # 生成 Excel 报告
            output_path = os.path.join(output_dir, f"daily_pnl_{date_str}.xlsx")
            
            # 创建两个 sheet：汇总和明细
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # 汇总 sheet
                summary_data = {
                    '指标': ['日期', '基金数量', '总成本', '总市值', '总盈亏', '总收益率'],
                    '数值': [
                        pnl_result['date'],
                        pnl_result['total_funds'],
                        pnl_result['total_cost'],
                        pnl_result['total_value'],
                        pnl_result['total_pnl'],
                        f"{pnl_result['overall_pnl_rate']}%"
                    ]
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='汇总', index=False)
                
                # 明细 sheet
                details_df = pd.DataFrame(pnl_result['fund_details'])
                details_df.to_excel(writer, sheet_name='明细', index=False)
                
                # 添加计算说明 sheet
                notes_df = pd.DataFrame({
                    '说明': [
                        '1. 数据来源：持仓表格 + 天天基金网净值',
                        '2. 成本价和持仓份额来自持仓表格',
                        '3. 净值使用最新可用数据',
                        '4. 盈亏 = (最新净值 - 成本价) × 持仓份额',
                        '5. 收益率 = 盈亏 / 成本 × 100%',
                        f'6. 报告生成时间：{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
                    ]
                })
                notes_df.to_excel(writer, sheet_name='说明', index=False)
            
            logger.info(f"Excel 报告已生成: {output_path}")
            return output_path
            
        elif output_format == 'json':
            # 生成 JSON 报告
            output_path = os.path.join(output_dir, f"daily_pnl_{date_str}.json")
            
            import json
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(pnl_result, f, ensure_ascii=False, indent=2)
            
            logger.info(f"JSON 报告已生成: {output_path}")
            return output_path
        
        else:
            logger.error(f"不支持的输出格式: {output_format}")
            return ""
    
    def generate_excel_update_copy(self, pnl_result: Dict, output_suffix: str = "更新") -> str:
        """
        生成Excel更新副本，保留原始列结构，用系统计算的数据更新净值相关列
        Args:
            pnl_result: calculate_daily_pnl 返回的结果
            output_suffix: 输出文件后缀（默认为"更新"）
        Returns:
            输出文件路径
        """
        if not pnl_result:
            logger.error("无盈亏数据，无法生成更新副本")
            return ""
        
        try:
            # 读取原始Excel文件
            df = pd.read_excel(self.excel_path, sheet_name=self.sheet_name)
            original_columns = df.columns.tolist()
            logger.info(f"读取原始Excel文件，共 {len(df)} 行，{len(original_columns)} 列")
            
            # 获取列名映射
            column_mapping = self.columns
            
            # 创建一个结果字典，便于按基金代码查找
            fund_details_dict = {}
            for fund in pnl_result['fund_details']:
                fund_details_dict[fund['fund_code']] = fund
            
            # 获取原始Excel中基金代码列名
            fund_code_col = column_mapping.get('fund_code', '基金代码')
            if fund_code_col not in df.columns:
                logger.error(f"原始Excel中找不到基金代码列: {fund_code_col}")
                return ""
            
            # 准备更新的列
            update_columns = {
                'latest_nav': '最新净值',
                'current_value': '当前市值',
                'cumulative_pnl': '累计盈亏',
                'yield_rate': '收益率',
                'position_percentage': '持仓占比'
            }
            
            # 确保这些列存在（如果不存在则创建）
            for col_name in update_columns.values():
                if col_name not in df.columns:
                    df[col_name] = 0.0
                    logger.info(f"创建缺失列: {col_name}")
            
            # 更新总投入成本列（如果存在）
            total_investment_col = '总投入成本'
            if total_investment_col not in df.columns:
                df[total_investment_col] = 0.0
            
            # 计算总市值（用于持仓占比）
            total_market_value = 0.0
            
            # 遍历原始数据行，更新计算列
            updated_count = 0
            for idx, row in df.iterrows():
                fund_code = str(row[fund_code_col]).strip()
                # 统一基金代码格式（取后6位）
                if len(fund_code) > 6:
                    fund_code = fund_code[-6:]
                
                if fund_code in fund_details_dict:
                    fund_detail = fund_details_dict[fund_code]
                    
                    # 更新净值相关列
                    df.at[idx, '最新净值'] = fund_detail['nav_value']
                    df.at[idx, '当前市值'] = fund_detail['current_value']
                    df.at[idx, '累计盈亏'] = fund_detail['pnl_amount']
                    df.at[idx, '收益率'] = fund_detail['pnl_rate'] / 100  # 转换为小数
                    
                    # 更新总投入成本 = 持仓份额 × 持仓成本
                    shares_col = column_mapping.get('shares', '持仓份额')
                    cost_price_col = column_mapping.get('cost_price', '持仓成本')
                    if shares_col in df.columns and cost_price_col in df.columns:
                        try:
                            shares = float(row[shares_col]) if not pd.isna(row[shares_col]) else 0
                            cost_price = float(row[cost_price_col]) if not pd.isna(row[cost_price_col]) else 0
                            df.at[idx, total_investment_col] = shares * cost_price
                        except (ValueError, TypeError):
                            df.at[idx, total_investment_col] = 0
                    
                    total_market_value += fund_detail['current_value']
                    updated_count += 1
            
            # 计算持仓占比
            if total_market_value > 0:
                for idx, row in df.iterrows():
                    fund_code = str(row[fund_code_col]).strip()
                    if len(fund_code) > 6:
                        fund_code = fund_code[-6:]
                    
                    if fund_code in fund_details_dict:
                        fund_detail = fund_details_dict[fund_code]
                        df.at[idx, '持仓占比'] = fund_detail['current_value'] / total_market_value
                    else:
                        df.at[idx, '持仓占比'] = 0
            else:
                df['持仓占比'] = 0
            
            # 生成输出文件名
            base_name = os.path.splitext(os.path.basename(self.excel_path))[0]
            date_str = pnl_result['date']
            output_filename = f"{base_name}_{output_suffix}_{date_str}.xlsx"
            output_dir = self.config['output']['report_dir']
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, output_filename)
            
            # 保存更新后的Excel
            df.to_excel(output_path, index=False)
            
            logger.info(f"Excel更新副本已生成: {output_path}")
            logger.info(f"成功更新 {updated_count}/{len(pnl_result['fund_details'])} 只基金数据")
            logger.info(f"总市值: {total_market_value:,.2f}")
            
            return output_path
            
        except Exception as e:
            logger.error(f"生成Excel更新副本失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return ""

def main():
    """测试函数"""
    import sys
    logging.basicConfig(level=logging.INFO)
    
    date_str = sys.argv[1] if len(sys.argv) > 1 else None
    
    calculator = PnLCalculator()
    result = calculator.calculate_daily_pnl(date_str)
    
    if result:
        print("=" * 80)
        print(f"持仓盈亏报告 - {result['date']}")
        print("=" * 80)
        print(f"基金数量: {result['total_funds']}")
        print(f"总成本: ¥{result['total_cost']:,.2f}")
        print(f"总市值: ¥{result['total_value']:,.2f}")
        print(f"总盈亏: ¥{result['total_pnl']:,.2f}")
        print(f"总收益率: {result['overall_pnl_rate']:.2f}%")
        print("\n基金明细:")
        print("-" * 80)
        
        for fund in result['fund_details']:
            print(f"{fund['fund_code']} {fund['fund_name']}: "
                  f"份额={fund['shares']:.2f}, "
                  f"成本价={fund['cost_price']:.4f}, "
                  f"净值={fund['nav_value']:.4f} ({fund['change_rate']}%), "
                  f"盈亏=¥{fund['pnl_amount']:,.2f} ({fund['pnl_rate']:.2f}%)")
        
        # 生成报告
        report_path = calculator.generate_report(result, 'excel')
        if report_path:
            print(f"\n报告已保存: {report_path}")

if __name__ == "__main__":
    main()