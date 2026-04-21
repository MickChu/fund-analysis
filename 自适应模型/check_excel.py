#!/usr/bin/env python3
"""
检查持仓Excel表格结构
"""

import pandas as pd
import sys

def main():
    excel_path = r"C:\Users\xbt\.openclaw\workspace\持仓管理表格_V1.xlsx"
    
    try:
        # 读取Excel文件
        df = pd.read_excel(excel_path)
        
        print("成功读取Excel文件！")
        print(f"文件路径: {excel_path}")
        print(f"工作表数量: {len(pd.ExcelFile(excel_path).sheet_names)}")
        print(f"数据形状: {df.shape} (行数 x 列数)")
        print(f"列名列表:")
        for i, col in enumerate(df.columns, 1):
            print(f"  {i:2d}. {col}")
        
        print(f"\n前5行数据预览:")
        print(df.head())
        
        print(f"\n数据类型:")
        print(df.dtypes)
        
        print(f"\n基本信息:")
        print(f"总行数: {len(df)}")
        print(f"总列数: {len(df.columns)}")
        
        # 检查是否有典型基金相关列
        fund_related_keywords = ['基金', '代码', '名称', '份额', '成本', '买入', '市值']
        found_columns = []
        for col in df.columns:
            for keyword in fund_related_keywords:
                if keyword in str(col):
                    found_columns.append(col)
                    break
        
        if found_columns:
            print(f"\n发现可能的基金相关列: {found_columns}")
        
        # 检查是否有数值型列
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        if numeric_cols:
            print(f"\n数值型列: {numeric_cols}")
        
        return 0
        
    except FileNotFoundError:
        print(f"错误: 文件不存在 - {excel_path}")
        return 1
    except Exception as e:
        print(f"错误: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())