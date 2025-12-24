#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
同花顺概念和行业指数数据查询脚本

功能：
1. 查询概念指数数据
2. 按类型筛选指数
3. 搜索指定名称的指数
4. 显示指数统计信息

使用方法：
python3 query_ths_index.py [选项]

示例：
python3 query_ths_index.py --type N --limit 20  # 查询前20个概念指数
python3 query_ths_index.py --name 人工智能      # 搜索包含"人工智能"的指数
python3 query_ths_index.py --stats             # 显示统计信息
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


def get_type_name(index_type: str) -> str:
    """获取指数类型中文名称"""
    type_mapping = {
        'N': '概念指数',
        'I': '行业指数', 
        'R': '地域指数',
        'S': '同花顺特色指数',
        'ST': '同花顺风格指数',
        'TH': '同花顺主题指数',
        'BB': '同花顺宽基指数'
    }
    return type_mapping.get(index_type, '未知类型')


def query_by_type(db: StockDatabase, index_type: str, limit: int = None):
    """按类型查询指数"""
    type_name = get_type_name(index_type)
    print(f"\n🔍 查询{type_name}({index_type}):")
    print("-" * 60)
    
    df = db.query_ths_index(index_type=index_type, limit=limit)
    
    if df is None or df.empty:
        print(f"❌ 没有找到{type_name}数据")
        return
    
    print(f"📊 找到 {len(df)} 个{type_name}")
    print("📋 指数列表：")
    
    for i, (_, row) in enumerate(df.iterrows(), 1):
        count = row['count'] if str(row['count']) != 'nan' else '未知'
        print(f"  {i:3d}. {row['name']:<30} ({row['ts_code']:<12}) - 成分股:{count}个")


def query_by_name(db: StockDatabase, name_keyword: str, limit: int = None):
    """按名称关键字搜索指数"""
    print(f"\n🔍 搜索包含'{name_keyword}'的指数:")
    print("-" * 60)
    
    # 获取所有数据然后筛选
    df = db.query_ths_index()
    
    if df is None or df.empty:
        print("❌ 数据库中没有数据")
        return
    
    # 按名称筛选
    filtered_df = df[df['name'].str.contains(name_keyword, case=False, na=False)]
    
    if limit:
        filtered_df = filtered_df.head(limit)
    
    if filtered_df.empty:
        print(f"❌ 没有找到包含'{name_keyword}'的指数")
        return
    
    print(f"📊 找到 {len(filtered_df)} 个相关指数")
    print("📋 指数列表：")
    
    for i, (_, row) in enumerate(filtered_df.iterrows(), 1):
        count = row['count'] if str(row['count']) != 'nan' else '未知'
        type_name = get_type_name(row['type'])
        print(f"  {i:3d}. {row['name']:<30} ({row['ts_code']:<12}) - {type_name} - 成分股:{count}个")


def show_statistics(db: StockDatabase):
    """显示统计信息"""
    print("\n📊 同花顺指数数据统计:")
    print("=" * 60)
    
    df = db.query_ths_index()
    
    if df is None or df.empty:
        print("❌ 数据库中没有数据")
        return
    
    print(f"📈 总指数数量: {len(df)} 个")
    print("\n📋 按类型分布:")
    
    type_counts = df['type'].value_counts()
    for idx_type, count in type_counts.items():
        type_name = get_type_name(idx_type)
        percentage = count / len(df) * 100
        print(f"   {type_name:<15} ({idx_type:<2}): {count:4d} 个 ({percentage:5.1f}%)")
    
    # 显示成分股数量统计
    print("\n📊 成分股数量分布:")
    valid_counts = df[df['count'].notna()]['count']
    if len(valid_counts) > 0:
        print(f"   平均成分股数量: {valid_counts.mean():.1f} 个")
        print(f"   最大成分股数量: {valid_counts.max():.0f} 个")
        print(f"   最小成分股数量: {valid_counts.min():.0f} 个")
        
        # 显示成分股数量最多的指数
        max_count_idx = df.loc[df['count'].idxmax()]
        print(f"   成分股最多指数: {max_count_idx['name']} ({max_count_idx['count']:.0f}个)")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='查询同花顺概念和行业指数数据')
    parser.add_argument('--type', '-t', help='指数类型 (N=概念指数, I=行业指数, R=地域指数, S=特色指数, ST=风格指数, TH=主题指数, BB=宽基指数)')
    parser.add_argument('--name', '-n', help='按指数名称关键字搜索')
    parser.add_argument('--limit', '-l', type=int, help='限制返回数量')
    parser.add_argument('--stats', '-s', action='store_true', help='显示统计信息')
    
    args = parser.parse_args()
    
    # 如果没有任何参数，显示帮助信息
    if not any([args.type, args.name, args.stats]):
        parser.print_help()
        print("\n💡 使用示例:")
        print("   python3 query_ths_index.py --type N --limit 20    # 查询前20个概念指数")
        print("   python3 query_ths_index.py --name 人工智能         # 搜索AI相关指数")
        print("   python3 query_ths_index.py --stats                # 显示统计信息")
        return
    
    try:
        print("🚀 同花顺概念和行业指数查询工具")
        print("=" * 60)
        
        with StockDatabase() as db:
            if args.stats:
                show_statistics(db)
            
            if args.type:
                query_by_type(db, args.type.upper(), args.limit)
            
            if args.name:
                query_by_name(db, args.name, args.limit)
                
    except Exception as e:
        logger.error(f"查询过程中发生错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
