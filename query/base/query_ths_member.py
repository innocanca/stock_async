#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
同花顺概念指数成分股数据查询脚本

功能：
1. 查询指定概念指数的成分股
2. 按股票代码/名称搜索所属概念指数
3. 显示成分股统计信息
4. 查询指定股票所属的所有概念指数

使用方法：
python3 query_ths_member.py [选项]

示例：
python3 query_ths_member.py --index 885556.TI    # 查询5G概念的成分股
python3 query_ths_member.py --stock 000063.SZ   # 查询中兴通讯所属的概念指数
python3 query_ths_member.py --stock-name 腾讯    # 搜索包含"腾讯"的股票
python3 query_ths_member.py --stats              # 显示统计信息
"""

import argparse
import logging
import sys
import os

# 添加项目根目录到 Python 路径
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from database import StockDatabase

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def query_by_index(db: StockDatabase, ts_code: str, limit: int = None):
    """按指数代码查询成分股"""
    print(f"\n🔍 查询指数 {ts_code} 的成分股:")
    print("-" * 70)
    
    df = db.query_ths_member(ts_code=ts_code, limit=limit)
    
    if df is None or df.empty:
        print(f"❌ 没有找到指数 {ts_code} 的成分股数据")
        return
    
    # 获取指数信息
    index_name = df.iloc[0]['index_name'] if 'index_name' in df.columns and not df.empty else '未知'
    index_type = df.iloc[0]['index_type'] if 'index_type' in df.columns and not df.empty else '未知'
    
    print(f"📊 指数名称: {index_name}")
    print(f"📊 指数类型: {index_type}")
    print(f"📊 成分股数量: {len(df)} 只")
    print("📋 成分股列表：")
    
    for i, (_, row) in enumerate(df.iterrows(), 1):
        con_name = row.get('con_name', 'N/A')
        con_code = row.get('con_code', 'N/A')
        print(f"  {i:3d}. {con_name:<20} ({con_code})")


def query_by_stock_code(db: StockDatabase, con_code: str, limit: int = None):
    """按股票代码查询所属概念指数"""
    print(f"\n🔍 查询股票 {con_code} 所属的概念指数:")
    print("-" * 70)
    
    df = db.query_ths_member(con_code=con_code, limit=limit)
    
    if df is None or df.empty:
        print(f"❌ 没有找到股票 {con_code} 的概念指数数据")
        return
    
    # 获取股票信息
    stock_name = df.iloc[0]['con_name'] if 'con_name' in df.columns and not df.empty else '未知'
    
    print(f"📊 股票名称: {stock_name}")
    print(f"📊 所属概念指数数量: {len(df)} 个")
    print("📋 概念指数列表：")
    
    for i, (_, row) in enumerate(df.iterrows(), 1):
        index_name = row.get('index_name', 'N/A')
        ts_code = row.get('ts_code', 'N/A')
        index_type = row.get('index_type', 'N/A')
        print(f"  {i:3d}. {index_name:<30} ({ts_code}) - {index_type}")


def query_by_stock_name(db: StockDatabase, con_name: str, limit: int = None):
    """按股票名称关键字搜索"""
    print(f"\n🔍 搜索包含'{con_name}'的股票及其概念指数:")
    print("-" * 70)
    
    df = db.query_ths_member(con_name=con_name, limit=limit)
    
    if df is None or df.empty:
        print(f"❌ 没有找到包含'{con_name}'的股票")
        return
    
    # 按股票分组显示
    grouped = df.groupby(['con_code', 'con_name'])
    
    print(f"📊 找到 {len(grouped)} 只相关股票")
    
    for i, ((code, name), group) in enumerate(grouped, 1):
        print(f"\n{i:3d}. {name} ({code}) - 所属 {len(group)} 个概念指数:")
        
        for j, (_, row) in enumerate(group.head(10).iterrows(), 1):  # 最多显示10个概念
            index_name = row.get('index_name', 'N/A')
            ts_code = row.get('ts_code', 'N/A')
            print(f"     {j:2d}. {index_name} ({ts_code})")
        
        if len(group) > 10:
            print(f"     ... 还有 {len(group) - 10} 个概念指数")


def show_statistics(db: StockDatabase):
    """显示统计信息"""
    print("\n📊 同花顺概念指数成分股数据统计:")
    print("=" * 70)
    
    df = db.query_ths_member()
    
    if df is None or df.empty:
        print("❌ 数据库中没有成分股数据")
        return
    
    print(f"📈 总成分股记录: {len(df):,} 条")
    print(f"📈 涉及概念指数: {df['ts_code'].nunique()} 个")
    print(f"📈 不重复股票数: {df['con_code'].nunique()} 只")
    
    # 成分股数量最多的指数TOP10
    print("\n🏆 成分股数量最多的概念指数TOP10:")
    top_indexes = df.groupby(['ts_code', 'index_name']).size().reset_index(name='member_count')
    top_indexes = top_indexes.sort_values('member_count', ascending=False).head(10)
    
    for i, (_, row) in enumerate(top_indexes.iterrows(), 1):
        index_name = row.get('index_name', 'N/A')
        ts_code = row.get('ts_code', 'N/A')
        count = row.get('member_count', 0)
        print(f"   {i:2d}. {index_name:<30} ({ts_code}): {count} 只")
    
    # 被纳入概念指数最多的股票TOP10
    print("\n🏆 被纳入概念指数最多的股票TOP10:")
    top_stocks = df.groupby(['con_code', 'con_name']).size().reset_index(name='index_count')
    top_stocks = top_stocks.sort_values('index_count', ascending=False).head(10)
    
    for i, (_, row) in enumerate(top_stocks.iterrows(), 1):
        con_name = row.get('con_name', 'N/A')
        con_code = row.get('con_code', 'N/A')
        count = row.get('index_count', 0)
        print(f"   {i:2d}. {con_name:<20} ({con_code}): {count} 个概念")
    
    # 按概念指数类型统计
    if 'index_type' in df.columns:
        print("\n📈 按指数类型分布:")
        type_counts = df['index_type'].value_counts()
        for idx_type, count in type_counts.items():
            percentage = count / len(df) * 100
            print(f"   {idx_type:<15}: {count:4d} 条记录 ({percentage:5.1f}%)")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='查询同花顺概念指数成分股数据')
    parser.add_argument('--index', '-i', help='查询指定概念指数的成分股（如：885556.TI）')
    parser.add_argument('--stock', '-s', help='查询指定股票所属的概念指数（如：000063.SZ）')
    parser.add_argument('--stock-name', '-n', help='按股票名称关键字搜索（如：腾讯）')
    parser.add_argument('--limit', '-l', type=int, help='限制返回数量')
    parser.add_argument('--stats', action='store_true', help='显示统计信息')
    
    args = parser.parse_args()
    
    # 如果没有任何参数，显示帮助信息
    if not any([args.index, args.stock, args.stock_name, args.stats]):
        parser.print_help()
        print("\n💡 使用示例:")
        print("   python3 query_ths_member.py --index 885556.TI      # 查询5G概念成分股")
        print("   python3 query_ths_member.py --stock 000063.SZ     # 查询中兴通讯所属概念")
        print("   python3 query_ths_member.py --stock-name 腾讯      # 搜索腾讯相关股票")
        print("   python3 query_ths_member.py --stats               # 显示统计信息")
        return
    
    try:
        print("🚀 同花顺概念指数成分股查询工具")
        print("=" * 70)
        
        with StockDatabase() as db:
            if args.stats:
                show_statistics(db)
            
            if args.index:
                query_by_index(db, args.index.upper(), args.limit)
            
            if args.stock:
                query_by_stock_code(db, args.stock.upper(), args.limit)
            
            if args.stock_name:
                query_by_stock_name(db, args.stock_name, args.limit)
                
    except Exception as e:
        logger.error(f"查询过程中发生错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
